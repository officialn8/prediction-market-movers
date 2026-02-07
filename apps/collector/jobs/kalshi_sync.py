"""
Kalshi sync job - fetches markets and prices, stores in database.

Adapted for the actual PMM schema (markets, market_tokens, snapshots).
"""

from __future__ import annotations

import logging
import time
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional
from uuid import UUID, uuid4

from apps.collector.adapters.kalshi import KalshiAdapter, KalshiMarket
from packages.core.storage.db import get_db_pool

logger = logging.getLogger(__name__)

# Configuration
SOURCE_NAME = "kalshi"
MAX_MARKETS = 2000
MARKET_SYNC_INTERVAL = 15 * 60  # 15 minutes


@dataclass
class KalshiSyncState:
    """Tracks state between sync cycles."""
    ticker_to_market_id: Dict[str, UUID] = field(default_factory=dict)
    ticker_to_token_id: Dict[str, UUID] = field(default_factory=dict)
    last_market_sync: Optional[float] = None
    markets_count: int = 0


_sync_state: Optional[KalshiSyncState] = None


def get_sync_state() -> KalshiSyncState:
    global _sync_state
    if _sync_state is None:
        _sync_state = KalshiSyncState()
    return _sync_state


def _upsert_kalshi_status(status_data: dict) -> None:
    """Best-effort status write for Kalshi REST polling path."""
    try:
        db = get_db_pool()
        db.execute(
            """
            INSERT INTO system_status (key, value, updated_at)
            VALUES ('kalshi_wss', %s, NOW())
            ON CONFLICT (key) DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = NOW()
            """,
            (json.dumps(status_data),),
        )
    except Exception as e:
        logger.debug("Failed to upsert kalshi status: %s", e)


def _normalize_resolved_outcome(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    if text in {"YES", "Y", "TRUE", "1"}:
        return "YES"
    if text in {"NO", "N", "FALSE", "0"}:
        return "NO"
    return None


def _map_market_status(status: Optional[str]) -> str:
    text = str(status or "").strip().lower()
    if text in {"settled", "resolved"}:
        return "resolved"
    # Kalshi API uses "open" for tradable markets.
    if text in {"active", "open", "trading"}:
        return "active"
    if text in {"closed", "inactive", "expired"}:
        return "closed"
    return "closed"


def _build_ticker_maps() -> None:
    """Build maps from Kalshi tickers to our DB IDs."""
    state = get_sync_state()
    db = get_db_pool()
    
    # Using actual schema: markets.source_id and market_tokens
    rows = db.execute("""
        SELECT m.market_id, m.source_id, mt.token_id, mt.symbol
        FROM markets m
        JOIN market_tokens mt ON mt.market_id = m.market_id
        WHERE m.source = 'kalshi'
    """, fetch=True)
    
    state.ticker_to_market_id = {}
    state.ticker_to_token_id = {}
    
    for row in (rows or []):
        ticker = row.get("source_id", "")
        if ticker:
            state.ticker_to_market_id[ticker] = row["market_id"]
            state.ticker_to_token_id[ticker] = row["token_id"]
    
    logger.debug(f"Built Kalshi maps: {len(state.ticker_to_market_id)} markets")


def sync_markets(adapter: KalshiAdapter, max_markets: int = MAX_MARKETS) -> int:
    """Sync market metadata from Kalshi."""
    state = get_sync_state()
    db = get_db_pool()
    
    logger.info("Starting Kalshi market sync...")
    start_time = time.time()
    
    # Use event-based fetching to get real single-outcome markets (not parlays)
    markets = adapter.get_all_events_with_markets(status="open", max_events=500)
    
    if not markets:
        logger.warning("No markets fetched from Kalshi")
        return 0
    
    synced_count = 0
    
    for market in markets:
        try:
            # Skip markets with no price data
            if market.yes_bid == 0 and market.yes_ask == 0 and market.last_price == 0:
                continue
            
            # Check if market exists (using source_id, not source_market_id)
            existing = db.execute(
                "SELECT market_id FROM markets WHERE source = 'kalshi' AND source_id = %s",
                (market.ticker,),
                fetch=True
            )
            
            if existing:
                market_id = existing[0]["market_id"]
                market_status = _map_market_status(market.status)
                resolved_outcome = (
                    _normalize_resolved_outcome(market.result)
                    if market_status == "resolved"
                    else None
                )
                db.execute("""
                    UPDATE markets SET
                        title = %s,
                        status = %s,
                        resolved_outcome = CASE
                            WHEN %s = 'resolved' THEN COALESCE(%s, resolved_outcome)
                            ELSE NULL
                        END,
                        resolved_at = CASE
                            WHEN %s = 'resolved' THEN COALESCE(resolved_at, NOW())
                            ELSE NULL
                        END,
                        updated_at = NOW()
                    WHERE market_id = %s
                """, (
                    market.title[:500] if market.title else "Unknown",
                    market_status,
                    market_status,
                    resolved_outcome,
                    market_status,
                    market_id,
                ))
            else:
                # Insert new market
                market_id = uuid4()
                # Use category from market (from event), fallback to Politics
                category = market.category if hasattr(market, 'category') and market.category else "Politics"
                market_status = _map_market_status(market.status)
                resolved_outcome = (
                    _normalize_resolved_outcome(market.result)
                    if market_status == "resolved"
                    else None
                )
                db.execute("""
                    INSERT INTO markets (
                        market_id, source, source_id, title, category, status, url,
                        resolved_outcome, resolved_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                        CASE WHEN %s = 'resolved' THEN NOW() ELSE NULL END)
                """, (
                    market_id,
                    SOURCE_NAME,
                    market.ticker,
                    market.title[:500] if market.title else "Unknown",
                    category,
                    market_status,
                    market.url,
                    resolved_outcome,
                    market_status,
                ))
                
                # Insert YES token (using market_tokens table)
                token_id = uuid4()
                db.execute("""
                    INSERT INTO market_tokens (
                        token_id, market_id, outcome, symbol, source_token_id
                    ) VALUES (%s, %s, %s, %s, %s)
                """, (
                    token_id,
                    market_id,
                    "YES",
                    market.ticker,
                    market.ticker,
                ))
                
                state.ticker_to_market_id[market.ticker] = market_id
                state.ticker_to_token_id[market.ticker] = token_id
            
            synced_count += 1
            
        except Exception as e:
            logger.warning(f"Failed to sync Kalshi market {market.ticker}: {e}")
            continue
    
    state.last_market_sync = time.time()
    state.markets_count = synced_count
    
    elapsed = time.time() - start_time
    logger.info(f"Kalshi market sync complete: {synced_count} markets in {elapsed:.2f}s")
    
    return synced_count


def sync_prices(adapter: KalshiAdapter, max_markets: int = MAX_MARKETS) -> int:
    """Sync price snapshots from Kalshi."""
    state = get_sync_state()
    db = get_db_pool()
    
    if not state.ticker_to_token_id:
        _build_ticker_maps()
    
    # Use event-based fetching to get real single-outcome markets (not parlays)
    markets = adapter.get_all_events_with_markets(status="open", max_events=500)
    
    if not markets:
        return 0
    
    snapshots_inserted = 0
    now = datetime.now(timezone.utc)
    
    for market in markets:
        try:
            token_id = state.ticker_to_token_id.get(market.ticker)
            
            if not token_id:
                continue
            
            price = market.mid_price
            if price <= 0 or price >= 1:
                continue
            
            # Using actual schema: snapshots table with ts column
            db.execute("""
                INSERT INTO snapshots (ts, token_id, price, volume_24h, spread)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                now,
                token_id,
                price,
                float(market.volume_24h) if market.volume_24h else None,
                float(market.spread) if market.spread else None,
            ))
            
            snapshots_inserted += 1
            
        except Exception as e:
            logger.debug(f"Failed to insert Kalshi snapshot for {market.ticker}: {e}")
            continue
    
    logger.debug(f"Kalshi price sync: {snapshots_inserted} snapshots inserted")
    return snapshots_inserted


async def sync_once() -> None:
    """Run one Kalshi sync cycle."""
    state = get_sync_state()
    adapter = KalshiAdapter()
    
    try:
        needs_market_sync = (
            state.last_market_sync is None or
            time.time() - state.last_market_sync > MARKET_SYNC_INTERVAL
        )
        
        if needs_market_sync:
            sync_markets(adapter)
        
        snapshots_inserted = sync_prices(adapter)
        _upsert_kalshi_status(
            {
                "connected": False,
                "state": "polling_sync",
                "messages_received": 0,
                "trades_received": 0,
                "subscription_count": len(state.ticker_to_token_id),
                "snapshot_inserted_window": snapshots_inserted,
                "snapshot_skipped_window": 0,
                "snapshot_inserted_per_min": 0.0,
                "snapshot_skipped_per_min": 0.0,
                "latency_ms": 0.0,
                "last_updated": time.time(),
            }
        )
        
    except Exception as e:
        logger.error(f"Kalshi sync error: {e}")
    finally:
        adapter.close()


async def sync_kalshi() -> int:
    """Full Kalshi sync - markets and prices."""
    adapter = KalshiAdapter()
    try:
        count = sync_markets(adapter)
        sync_prices(adapter)
        return count
    finally:
        adapter.close()


if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)
    asyncio.run(sync_once())
