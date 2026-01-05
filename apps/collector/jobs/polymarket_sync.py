"""
Polymarket sync job - fetches markets and prices, stores in database.

Two main operations:
1. sync_markets() - Fetch and upsert market metadata (run every ~15 min)
2. sync_prices() - Fetch and insert price snapshots (run every ~15-60 sec)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Dict, Optional
from uuid import UUID

from apps.collector.adapters.polymarket import (
    PolymarketAdapter,
    PolymarketMarket,
    get_polymarket_adapter,
)
from packages.core.storage.db import get_db_pool
from packages.core.storage.queries import MarketQueries

logger = logging.getLogger(__name__)

# Configuration
SOURCE_NAME = "polymarket"
MAX_MARKETS = 500  # Max markets to track
MARKET_SYNC_INTERVAL = 15 * 60  # 15 minutes in seconds


@dataclass
class SyncState:
    """Tracks state between sync cycles."""
    # market_id -> {token_id -> source_token_id}
    token_map: Dict[str, Dict[str, str]] = field(default_factory=dict)
    # source_token_id -> our token_id (UUID)
    source_to_db_token: Dict[str, UUID] = field(default_factory=dict)
    last_market_sync: Optional[float] = None
    last_gamma_markets: Optional[list] = None  # Cache of recent markets
    markets_count: int = 0
    tokens_count: int = 0
    # token_id (UUID) -> last known volume_24h (float)
    volume_cache: Dict[str, float] = field(default_factory=dict)
    # Token map cache: timestamp of last rebuild and validity duration
    last_token_map_rebuild: Optional[float] = None
    token_map_ttl_seconds: int = 300  # Rebuild token map every 5 minutes max
    # Volume cache: timestamp of last rebuild from DB
    last_volume_cache_rebuild: Optional[float] = None
    volume_cache_ttl_seconds: int = 900  # Rebuild volume cache every 15 minutes max


# Module-level state
_sync_state: Optional[SyncState] = None


def get_sync_state() -> SyncState:
    """Get or create sync state singleton."""
    global _sync_state
    if _sync_state is None:
        _sync_state = SyncState()
    return _sync_state


def sync_markets_and_prices(adapter: PolymarketAdapter, max_markets: int = MAX_MARKETS) -> tuple[int, int]:
    """
    Sync market metadata AND prices from Polymarket Gamma API.
    
    The Gamma API returns prices in the market data, so we store snapshots
    at the same time as syncing market metadata.
    
    Returns:
        Tuple of (markets_synced, snapshots_inserted)
    """
    state = get_sync_state()
    logger.info("Starting Polymarket market+price sync...")
    
    start_time = time.time()
    
    # Fetch markets from API (includes prices!)
    markets = adapter.fetch_all_markets(max_markets=max_markets, active=True)
    
    # Update cache
    state.last_gamma_markets = markets
    state.last_market_sync = time.time()
    
    if not markets:
        logger.warning("No markets fetched from Polymarket")
        return 0, 0
    
    synced_count = 0
    tokens_count = 0
    snapshots = []  # Collect all snapshots for batch insert
    
    for pm_market in markets:
        try:
            # Skip non-binary markets for now (simpler)
            if not pm_market.is_binary:
                continue
            
            # Upsert market
            db_market = MarketQueries.upsert_market(
                source=SOURCE_NAME,
                source_id=pm_market.condition_id,
                title=pm_market.title,
                category=pm_market.category,
                end_date=pm_market.end_date,
                status="active" if pm_market.active and not pm_market.closed else "closed",
                url=pm_market.url,
            )
            
            if not db_market:
                continue
            
            market_id = UUID(str(db_market["market_id"]))
            
            # Upsert tokens AND collect price snapshots
            token_map = {}
            for token_data in pm_market.tokens:
                source_token_id = token_data["token_id"]
                outcome = token_data["outcome"]
                price = token_data.get("price", 0.5)  # Price from Gamma API!
                
                db_token = MarketQueries.upsert_token(
                    market_id=market_id,
                    outcome=outcome,
                    symbol=outcome,
                    source_token_id=source_token_id,
                )
                
                if db_token:
                    token_id = UUID(str(db_token["token_id"]))
                    token_map[outcome] = source_token_id
                    state.source_to_db_token[source_token_id] = token_id
                    tokens_count += 1
                    
                    # Collect snapshot with price from Gamma API
                    snapshots.append({
                        "token_id": token_id,
                        "price": price,
                        "volume_24h": pm_market.volume_24h,
                        "spread": None,
                    })

                    # Update volume cache with normalized key (always str UUID)
                    if pm_market.volume_24h is not None:
                        # Normalize: ensure token_id is string for consistent lookup
                        cache_key = str(token_id) if not isinstance(token_id, str) else token_id
                        state.volume_cache[cache_key] = float(pm_market.volume_24h)

            
            state.token_map[str(market_id)] = token_map
            synced_count += 1
            
        except Exception as e:
            logger.warning(f"Failed to sync market {pm_market.condition_id}: {e}")
            continue
    
    # Batch insert all snapshots
    # Note: We keep only the last snapshot per token_id in a single sync cycle
    # since they'd have the same timestamp anyway. DB has ON CONFLICT DO NOTHING
    # for (token_id, ts) uniqueness, so multiple updates in same second are safe.
    snapshots_inserted = 0
    if snapshots:
        # Keep last snapshot per token to avoid duplicate key errors in same batch
        # but log if we're losing data (indicates multiple tokens with same ID)
        seen_tokens = {}
        for s in snapshots:
            token_key = str(s["token_id"])
            if token_key in seen_tokens:
                logger.debug(
                    f"Duplicate token_id in batch: {token_key}, "
                    f"price {seen_tokens[token_key]['price']} -> {s['price']}"
                )
            seen_tokens[token_key] = s
        
        if len(seen_tokens) < len(snapshots):
            logger.debug(
                f"Deduplicated {len(snapshots)} snapshots to {len(seen_tokens)} unique tokens"
            )
        
        snapshots_inserted = MarketQueries.insert_snapshots_batch(list(seen_tokens.values()))
    
    elapsed = time.time() - start_time
    state.last_market_sync = time.time()
    state.markets_count = synced_count
    state.tokens_count = tokens_count
    
    logger.info(
        f"Polymarket sync complete: "
        f"{synced_count} markets, {tokens_count} tokens, {snapshots_inserted} snapshots in {elapsed:.2f}s"
    )
    
    return synced_count, snapshots_inserted


def sync_markets(adapter: PolymarketAdapter, max_markets: int = MAX_MARKETS) -> int:
    """Legacy wrapper - now syncs markets AND prices together."""
    markets, _ = sync_markets_and_prices(adapter, max_markets)
    return markets


def sync_prices(adapter: PolymarketAdapter, use_clob: bool = True) -> int:
    """
    Sync current prices using CLOB API (real-time) with Gamma fallback for missing tokens.
    
    Args:
        use_clob: If True, try CLOB API first for real-time prices
        
    Returns:
        Number of snapshots inserted
    """
    state = get_sync_state()
    
    # Check if token map needs rebuilding (empty or stale)
    token_map_needs_rebuild = (
        not state.source_to_db_token
        or state.last_token_map_rebuild is None
        or (time.time() - state.last_token_map_rebuild) > state.token_map_ttl_seconds
    )
    
    if token_map_needs_rebuild:
        _rebuild_token_map()
    
    if not state.source_to_db_token:
        logger.warning("No tokens to sync prices for")
        return 0
    
    # Check if volume cache needs rebuilding from DB
    # This ensures CLOB-only syncs have volume context for scoring
    volume_cache_needs_rebuild = (
        not state.volume_cache
        or state.last_volume_cache_rebuild is None
        or (time.time() - state.last_volume_cache_rebuild) > state.volume_cache_ttl_seconds
    )
    
    if volume_cache_needs_rebuild:
        _rebuild_volume_cache()
    
    logger.debug(f"Syncing prices for {len(state.source_to_db_token)} tokens")
    
    start_time = time.time()
    source_token_ids = list(state.source_to_db_token.keys())
    total_tokens = len(source_token_ids)
    
    snapshots = []
    missing_token_ids: list[str] = []
    
    if use_clob:
        # Try CLOB API for real-time prices, track missing tokens
        sample_ids = source_token_ids[:3] if source_token_ids else []
        logger.debug(f"Requesting CLOB prices for {total_tokens} tokens. Sample: {sample_ids}")
        
        prices, missing_token_ids = adapter.fetch_prices_batch(
            source_token_ids, return_missing=True
        )
        
        if prices:
            for source_token_id, token_price in prices.items():
                db_token_id = state.source_to_db_token.get(source_token_id)
                if db_token_id:
                    # Normalize key for volume cache lookup (consistent with store)
                    cache_key = str(db_token_id) if not isinstance(db_token_id, str) else db_token_id
                    snapshots.append({
                        "token_id": db_token_id,
                        "price": token_price.price,
                        "volume_24h": state.volume_cache.get(cache_key),
                        "spread": token_price.spread,
                    })
        
        # Fallback to Gamma API for missing tokens (if > 10% missing)
        if missing_token_ids and len(missing_token_ids) > total_tokens * 0.1:
            logger.info(
                f"CLOB missing {len(missing_token_ids)}/{total_tokens} tokens, "
                f"fetching missing from Gamma API"
            )
            gamma_snapshots = _fetch_missing_from_gamma(adapter, missing_token_ids, state)
            snapshots.extend(gamma_snapshots)
            logger.info(f"Gamma fallback recovered {len(gamma_snapshots)} prices")
    
    # Full fallback to Gamma API if CLOB returned nothing
    if not snapshots:
        logger.info("Using Gamma API for all prices (full fallback)")
        snapshots = _fetch_all_from_gamma(adapter, state)
    
    # Batch insert
    if snapshots:
        inserted = MarketQueries.insert_snapshots_batch(snapshots)
        elapsed = time.time() - start_time
        logger.info(f"Inserted {inserted} price snapshots in {elapsed:.2f}s")
        return inserted
    
    return 0


def _fetch_missing_from_gamma(
    adapter: PolymarketAdapter,
    missing_token_ids: list[str],
    state: SyncState,
) -> list[dict]:
    """
    Fetch prices for specific missing tokens from Gamma API.
    
    Uses cached markets if available and recent, otherwise fetches fresh.
    """
    # Build set of missing token IDs for quick lookup
    missing_set = set(missing_token_ids)
    
    # Use cached markets if available and recent (10 mins)
    markets = state.last_gamma_markets
    is_stale = False
    if state.last_market_sync:
        is_stale = (time.time() - state.last_market_sync) > 600  # 10 mins
        
    if not markets or is_stale:
        logger.debug("Gamma cache empty or stale, fetching fresh markets for fallback")
        markets = adapter.fetch_all_markets(max_markets=MAX_MARKETS, active=True)
        state.last_gamma_markets = markets
        state.last_market_sync = time.time()
    
    snapshots = []
    if markets:
        for pm_market in markets:
            if not pm_market.is_binary:
                continue
            for token_data in pm_market.tokens:
                source_token_id = token_data["token_id"]
                # Only fetch prices for missing tokens
                if source_token_id not in missing_set:
                    continue
                db_token_id = state.source_to_db_token.get(source_token_id)
                if db_token_id:
                    snapshots.append({
                        "token_id": db_token_id,
                        "price": token_data.get("price", 0.5),
                        "volume_24h": pm_market.volume_24h,
                        "spread": None,
                    })
                    # Update volume cache since Gamma provides volume
                    if pm_market.volume_24h is not None:
                        cache_key = str(db_token_id)
                        state.volume_cache[cache_key] = float(pm_market.volume_24h)
    
    return snapshots


def _fetch_all_from_gamma(adapter: PolymarketAdapter, state: SyncState) -> list[dict]:
    """
    Fetch all prices from Gamma API (full fallback).
    """
    # Use cached markets if available and recent (10 mins)
    markets = state.last_gamma_markets
    is_stale = False
    if state.last_market_sync:
        is_stale = (time.time() - state.last_market_sync) > 600  # 10 mins
        
    if not markets or is_stale:
        logger.info("Gamma cache empty or stale, fetching fresh markets")
        markets = adapter.fetch_all_markets(max_markets=MAX_MARKETS, active=True)
        state.last_gamma_markets = markets
        state.last_market_sync = time.time()
    else:
        logger.info(f"Using cached Gamma markets ({len(markets)} markets)")
    
    snapshots = []
    if markets:
        for pm_market in markets:
            if not pm_market.is_binary:
                continue
            for token_data in pm_market.tokens:
                source_token_id = token_data["token_id"]
                db_token_id = state.source_to_db_token.get(source_token_id)
                if db_token_id:
                    snapshots.append({
                        "token_id": db_token_id,
                        "price": token_data.get("price", 0.5),
                        "volume_24h": pm_market.volume_24h,
                        "spread": None,
                    })
                    # Update volume cache since Gamma provides volume
                    if pm_market.volume_24h is not None:
                        cache_key = str(db_token_id)
                        state.volume_cache[cache_key] = float(pm_market.volume_24h)
    
    return snapshots


def _rebuild_token_map() -> None:
    """
    Rebuild the source_token_id -> db_token_id map from database.
    
    Only includes tokens from active markets that haven't ended yet.
    This ensures we don't waste resources syncing prices for closed markets.
    
    Uses caching to avoid full DB scans on every price sync cycle.
    """
    state = get_sync_state()
    state.source_to_db_token.clear()
    
    db = get_db_pool()
    
    # Get polymarket tokens only from active, non-expired markets
    # This aligns token status with market status
    query = """
        SELECT mt.token_id, mt.source_token_id
        FROM market_tokens mt
        JOIN markets m ON mt.market_id = m.market_id
        WHERE m.source = %s 
          AND m.status = 'active'
          AND mt.source_token_id IS NOT NULL
          -- Exclude markets that have ended (no point syncing prices)
          AND (m.end_date IS NULL OR m.end_date > NOW())
    """
    
    result = db.execute(query, (SOURCE_NAME,), fetch=True)
    
    if result:
        for row in result:
            source_token_id = row["source_token_id"]
            db_token_id = UUID(str(row["token_id"]))
            state.source_to_db_token[source_token_id] = db_token_id
    
    # Record rebuild timestamp for cache TTL
    state.last_token_map_rebuild = time.time()
    
    logger.info(f"Rebuilt token map with {len(state.source_to_db_token)} active tokens")


def _rebuild_volume_cache() -> None:
    """
    Rebuild the volume cache from database.
    
    Queries the latest non-null volume_24h for each active token.
    This ensures CLOB-only price syncs have volume context for scoring
    even if no recent Gamma sync has occurred.
    """
    state = get_sync_state()
    
    db = get_db_pool()
    
    # Get latest non-null volume for all active tokens
    query = """
        SELECT DISTINCT ON (mt.token_id)
            mt.token_id,
            s.volume_24h
        FROM market_tokens mt
        JOIN markets m ON mt.market_id = m.market_id
        JOIN snapshots s ON mt.token_id = s.token_id
        WHERE m.source = %s
          AND m.status = 'active'
          AND s.volume_24h IS NOT NULL
          AND (m.end_date IS NULL OR m.end_date > NOW())
        ORDER BY mt.token_id, s.ts DESC
    """
    
    result = db.execute(query, (SOURCE_NAME,), fetch=True)
    
    count = 0
    if result:
        for row in result:
            token_id = str(row["token_id"])
            volume = row["volume_24h"]
            if volume is not None:
                state.volume_cache[token_id] = float(volume)
                count += 1
    
    # Record rebuild timestamp for cache TTL
    state.last_volume_cache_rebuild = time.time()
    
    logger.info(f"Rebuilt volume cache with {count} tokens from DB")


def should_sync_markets() -> bool:
    """Check if it's time to sync market metadata."""
    state = get_sync_state()
    
    if state.last_market_sync is None:
        return True
    
    elapsed = time.time() - state.last_market_sync
    return elapsed >= MARKET_SYNC_INTERVAL


async def sync_once() -> None:
    """
    Run one sync cycle (called by collector main loop).
    
    - Syncs market metadata periodically (every 15 min)
    - Syncs prices on every cycle using CLOB API (real-time)
    """
    adapter = get_polymarket_adapter()
    
    try:
        # Sync market metadata periodically
        if should_sync_markets():
            sync_markets_and_prices(adapter, max_markets=MAX_MARKETS)
        else:
            # Just sync prices using CLOB API for real-time updates
            sync_prices(adapter, use_clob=True)
        
    except Exception as e:
        logger.exception(f"Polymarket sync error: {e}")


def run_sync_loop(
    market_interval_sec: int = 900,  # 15 min
    price_interval_sec: int = 30,    # 30 sec
) -> None:
    """
    Run continuous sync loop (for standalone execution).
    
    Args:
        market_interval_sec: How often to sync market metadata
        price_interval_sec: How often to sync prices
    """
    adapter = get_polymarket_adapter()
    last_market_sync = 0.0
    
    logger.info(
        f"Starting Polymarket sync loop "
        f"(markets every {market_interval_sec}s, prices every {price_interval_sec}s)"
    )
    
    try:
        while True:
            now = time.time()
            
            # Sync markets if interval elapsed
            if now - last_market_sync >= market_interval_sec:
                try:
                    sync_markets(adapter)
                    last_market_sync = now
                except Exception as e:
                    logger.exception(f"Market sync failed: {e}")
            
            # Sync prices
            try:
                sync_prices(adapter)
            except Exception as e:
                logger.exception(f"Price sync failed: {e}")
            
            time.sleep(price_interval_sec)
            
    except KeyboardInterrupt:
        logger.info("Sync loop interrupted")
    finally:
        adapter.close()


# For backwards compatibility with existing code
async def sync_polymarket() -> int:
    """Legacy entry point."""
    await sync_once()
    state = get_sync_state()
    return state.markets_count


if __name__ == "__main__":
    # Allow running standalone for testing
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )
    
    # Initialize DB
    from packages.core.storage import get_db_pool
    db = get_db_pool()
    
    if not db.health_check():
        logger.error("Database not available")
        sys.exit(1)
    
    run_sync_loop()
