"""
Kalshi sync job - fetches markets and prices, stores in database.

Similar to polymarket_sync but adapted for Kalshi's API structure.

Two main operations:
1. sync_markets() - Fetch and upsert market metadata
2. sync_prices() - Fetch and insert price snapshots
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional
from uuid import UUID, uuid4

from apps.collector.adapters.kalshi import KalshiAdapter, KalshiMarket
from packages.core.storage.db import get_db_pool

logger = logging.getLogger(__name__)

# Configuration
SOURCE_NAME = "kalshi"
MAX_MARKETS = 2000  # Kalshi has more markets
MARKET_SYNC_INTERVAL = 15 * 60  # 15 minutes


@dataclass
class KalshiSyncState:
    """Tracks state between sync cycles."""
    # ticker -> our market_id (UUID)
    ticker_to_market_id: Dict[str, UUID] = field(default_factory=dict)
    # ticker -> our token_id (UUID) for YES token
    ticker_to_token_id: Dict[str, UUID] = field(default_factory=dict)
    last_market_sync: Optional[float] = None
    markets_count: int = 0


# Module-level state
_sync_state: Optional[KalshiSyncState] = None


def get_sync_state() -> KalshiSyncState:
    """Get or create sync state singleton."""
    global _sync_state
    if _sync_state is None:
        _sync_state = KalshiSyncState()
    return _sync_state


def _build_ticker_maps() -> None:
    """Build maps from Kalshi tickers to our DB IDs."""
    state = get_sync_state()
    db = get_db_pool()
    
    # Get all Kalshi markets from DB
    rows = db.execute("""
        SELECT m.market_id, m.source_market_id, t.token_id, t.source_token_id
        FROM markets m
        JOIN tokens t ON t.market_id = m.market_id
        WHERE m.source = 'kalshi'
    """, fetch=True)
    
    state.ticker_to_market_id = {}
    state.ticker_to_token_id = {}
    
    for row in (rows or []):
        ticker = row.get("source_market_id", "")
        if ticker:
            state.ticker_to_market_id[ticker] = row["market_id"]
            state.ticker_to_token_id[ticker] = row["token_id"]
    
    logger.debug(f"Built Kalshi maps: {len(state.ticker_to_market_id)} markets")


def sync_markets(adapter: KalshiAdapter, max_markets: int = MAX_MARKETS) -> int:
    """
    Sync market metadata from Kalshi.
    
    Returns number of markets synced.
    """
    state = get_sync_state()
    db = get_db_pool()
    
    logger.info("Starting Kalshi market sync...")
    start_time = time.time()
    
    # Fetch all open markets
    markets = adapter.get_all_markets(status="open", max_markets=max_markets)
    
    if not markets:
        logger.warning("No markets fetched from Kalshi")
        return 0
    
    synced_count = 0
    
    for market in markets:
        try:
            # Skip markets with no price data
            if market.yes_bid == 0 and market.yes_ask == 0 and market.last_price == 0:
                continue
            
            # Check if market exists
            existing = db.execute(
                "SELECT market_id FROM markets WHERE source = 'kalshi' AND source_market_id = %s",
                (market.ticker,),
                fetch=True
            )
            
            if existing:
                # Update existing market
                market_id = existing[0]["market_id"]
                db.execute("""
                    UPDATE markets SET
                        title = %s,
                        close_time = %s,
                        active = %s,
                        updated_at = NOW()
                    WHERE market_id = %s
                """, (
                    market.title[:500] if market.title else "Unknown",
                    market.close_time,
                    market.status == "active",
                    market_id,
                ))
            else:
                # Insert new market
                market_id = uuid4()
                db.execute("""
                    INSERT INTO markets (
                        market_id, source, source_market_id, title,
                        slug, category, close_time, active
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    market_id,
                    SOURCE_NAME,
                    market.ticker,
                    market.title[:500] if market.title else "Unknown",
                    market.ticker.lower(),
                    "kalshi",  # TODO: Extract category from event
                    market.close_time,
                    market.status == "active",
                ))
                
                # Insert YES token
                token_id = uuid4()
                db.execute("""
                    INSERT INTO tokens (
                        token_id, market_id, source_token_id, outcome
                    ) VALUES (%s, %s, %s, %s)
                """, (
                    token_id,
                    market_id,
                    market.ticker,  # Use ticker as token ID
                    "YES",
                ))
                
                # Update state maps
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
    """
    Sync price snapshots from Kalshi.
    
    Returns number of snapshots inserted.
    """
    state = get_sync_state()
    db = get_db_pool()
    
    # Rebuild maps if needed
    if not state.ticker_to_token_id:
        _build_ticker_maps()
    
    # Fetch markets (includes price data)
    markets = adapter.get_all_markets(status="open", max_markets=max_markets)
    
    if not markets:
        return 0
    
    snapshots_inserted = 0
    now = datetime.now(timezone.utc)
    
    for market in markets:
        try:
            # Get our token_id for this market
            token_id = state.ticker_to_token_id.get(market.ticker)
            
            if not token_id:
                # Market not in DB yet, skip
                continue
            
            # Calculate price (mid or last)
            price = market.mid_price
            if price <= 0 or price >= 1:
                continue  # Invalid price
            
            # Insert snapshot
            db.execute("""
                INSERT INTO price_snapshots (token_id, price, volume_24h, recorded_at)
                VALUES (%s, %s, %s, %s)
            """, (
                token_id,
                price,
                float(market.volume_24h) if market.volume_24h else None,
                now,
            ))
            
            snapshots_inserted += 1
            
        except Exception as e:
            logger.debug(f"Failed to insert Kalshi snapshot for {market.ticker}: {e}")
            continue
    
    logger.debug(f"Kalshi price sync: {snapshots_inserted} snapshots inserted")
    return snapshots_inserted


async def sync_once() -> None:
    """
    Run one Kalshi sync cycle.
    
    - Syncs markets if interval has passed
    - Always syncs prices
    """
    state = get_sync_state()
    adapter = KalshiAdapter()
    
    try:
        # Check if we need to sync markets
        needs_market_sync = (
            state.last_market_sync is None or
            time.time() - state.last_market_sync > MARKET_SYNC_INTERVAL
        )
        
        if needs_market_sync:
            sync_markets(adapter)
        
        # Always sync prices
        sync_prices(adapter)
        
    except Exception as e:
        logger.error(f"Kalshi sync error: {e}")
    finally:
        adapter.close()


async def sync_kalshi() -> int:
    """
    Full Kalshi sync - markets and prices.
    
    Returns number of markets synced.
    """
    adapter = KalshiAdapter()
    try:
        count = sync_markets(adapter)
        sync_prices(adapter)
        return count
    finally:
        adapter.close()


# Quick test
if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)
    asyncio.run(sync_once())
