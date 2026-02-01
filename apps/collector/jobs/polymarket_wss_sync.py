import asyncio
import logging
import time
import json
from collections import defaultdict
from datetime import datetime
from uuid import UUID

from packages.core.settings import settings
from packages.core.storage.db import get_db_pool
from apps.collector.jobs.polymarket_sync import sync_markets
from apps.collector.adapters.polymarket_wss import PolymarketWebSocket
from apps.collector.adapters.wss_messages import (
    PriceUpdate, 
    TradeEvent, 
    SpreadUpdate,
    BookUpdate,
    MarketResolved,
    NewMarket,
)
from apps.collector.jobs.movers_cache import check_instant_mover, broadcast_mover_alert

logger = logging.getLogger(__name__)

# Health logging interval
HEALTH_LOG_INTERVAL = 60  # seconds

class Shutdown:
    """Simple shutdown signal carrier"""
    def __init__(self):
        self.is_set = False

async def run_wss_loop(shutdown: Shutdown) -> None:
    """
    Main WSS loop with:
    - Initial REST sync to get token list
    - WSS connection and subscription
    - Message handling with batched DB writes
    - Automatic reconnection
    - Fallback to polling on prolonged disconnect
    """
    
    # 1. Initial Sync to discover all assets
    logger.info("Performing initial REST sync before WSS...")
    
    # Use the adapter to sync
    from apps.collector.adapters.polymarket import get_polymarket_adapter
    adapter = get_polymarket_adapter() 
    # sync_markets is synchronous in logic but might call API async? 
    # Actually looking at polymarket_sync.py, sync_markets is sync (def sync_markets).
    # Running sync function in async loop blocks it.
    # It's fine for initial setup.
    sync_markets(adapter)
    adapter.close()
    
    # Get all asset IDs to subscribe to
    db_pool = get_db_pool()

    # Query for Polymarket source_token_ids (what WSS expects) AND map to DB token_ids
    # The WSS uses Polymarket's token IDs, not our internal UUIDs
    query = """
        SELECT mt.token_id as db_token_id,
               mt.source_token_id,
               (SELECT price FROM snapshots s WHERE s.token_id = mt.token_id ORDER BY ts DESC LIMIT 1) as price
        FROM markets m
        JOIN market_tokens mt ON m.market_id = mt.market_id
        WHERE m.status = 'active'
          AND mt.source_token_id IS NOT NULL
          AND m.source = 'polymarket'
    """
    rows = db_pool.execute(query, fetch=True) or []

    # Map: source_token_id (Polymarket) -> db_token_id (UUID) for DB writes
    source_to_db_token = {}
    # Map: source_token_id -> last known price (for mover detection)
    price_map = {}

    for r in rows:
        source_id = r["source_token_id"]
        db_id = str(r["db_token_id"])
        source_to_db_token[source_id] = db_id
        price_map[source_id] = float(r["price"] or 0)

    # Subscribe using Polymarket source_token_ids (NOT UUIDs!)
    asset_ids = list(source_to_db_token.keys())
    logger.info(f"Loaded {len(asset_ids)} assets for WSS subscription")

    # Enable custom features for spread, new markets, and resolution events!
    client = PolymarketWebSocket(enable_custom_features=True)
    pending_updates: list[PriceUpdate] = []
    pending_trades: list[TradeEvent] = []  # NEW: Track trades for volume
    pending_spreads: list[SpreadUpdate] = []  # NEW: Track spreads
    last_batch_flush = time.time()
    last_status_flush = time.time()
    latest_latency_ms = 0.0

    consecutive_failures = 0
    
    # Health logging state
    last_health_log = time.time()
    messages_since_last_health = 0
    
    # Volume tracking: token_id -> accumulated volume this batch
    volume_accumulator: dict[str, float] = defaultdict(float)

    while not shutdown.is_set:
        try:
            await client.connect(asset_ids)
            consecutive_failures = 0
            
            # Reset health counters on new connection
            last_health_log = time.time()
            messages_since_last_health = 0
            logger.info(f"WSS connected, starting message loop (watchdog={settings.wss_watchdog_timeout}s)")

            # Use iterator with timeout for watchdog
            listen_iter = client.listen().__aiter__()
            
            while not shutdown.is_set:
                try:
                    # Wait for next message with watchdog timeout
                    update = await asyncio.wait_for(
                        listen_iter.__anext__(),
                        timeout=settings.wss_watchdog_timeout
                    )
                except StopAsyncIteration:
                    # Iterator exhausted (connection closed cleanly)
                    logger.warning("WSS listen() iterator exhausted")
                    break
                except asyncio.TimeoutError:
                    # Watchdog timeout - no messages for too long
                    logger.error(
                        f"WSS watchdog timeout: no messages received for {settings.wss_watchdog_timeout}s, forcing reconnect"
                    )
                    raise ConnectionError("Watchdog timeout - no messages received")

                # Track message for health stats
                messages_since_last_health += 1
                
                # Handle different event types from WSS
                # The 'update' variable is now a union of all event types
                event = update  # Rename for clarity
                
                # ================================================================
                # PRICE_UPDATE - Best bid/ask price changes
                # ================================================================
                if isinstance(event, PriceUpdate):
                    source_token_id = event.token_id

                    # 1. Update local cache for mover detection
                    if source_token_id in price_map:
                        old_price = price_map[source_token_id]
                        # Check for instant mover (use db_token_id for DB operations)
                        db_token_id = source_to_db_token.get(source_token_id)
                        if db_token_id:
                            mover = await check_instant_mover(db_token_id, old_price, event.price)
                            if mover:
                                logger.info(f"Instant Mover Detected: {source_token_id} {old_price:.4f} -> {event.price:.4f}")
                                asyncio.create_task(broadcast_mover_alert(mover))

                    # Update local price map
                    price_map[source_token_id] = event.price

                    # 2. Add to batch (include db_token_id for flush)
                    pending_updates.append(event)
                
                # ================================================================
                # TRADE_EVENT - Individual trades WITH SIZE (for volume!)
                # This is key for accurate volume calculation!
                # ================================================================
                elif isinstance(event, TradeEvent):
                    source_token_id = event.token_id
                    
                    # Accumulate volume from trades
                    # Volume = size * price (notional value)
                    trade_volume = event.size * event.price
                    volume_accumulator[source_token_id] += trade_volume
                    
                    # Also update price from trade (more recent than price_change)
                    if source_token_id in price_map:
                        price_map[source_token_id] = event.price
                    
                    pending_trades.append(event)
                    logger.debug(f"Trade: {source_token_id} @ {event.price:.4f} x {event.size:.2f}")
                    
                    # Accumulate trade volume in database using stored function
                    # This provides real-time volume data for the dashboard
                    db_token_id = source_to_db_token.get(source_token_id)
                    if db_token_id and trade_volume > 0:
                        try:
                            db_pool.execute(
                                "SELECT accumulate_trade_volume(%s, %s, %s)",
                                (db_token_id, trade_volume, event.timestamp)
                            )
                        except Exception as e:
                            logger.warning(f"Failed to accumulate trade volume: {e}")
                
                # ================================================================
                # SPREAD_UPDATE - Bid/ask spreads (requires custom_feature_enabled)
                # ================================================================
                elif isinstance(event, SpreadUpdate):
                    pending_spreads.append(event)
                    logger.debug(f"Spread: {event.token_id} bid={event.best_bid:.4f} ask={event.best_ask:.4f}")
                
                # ================================================================
                # MARKET_RESOLVED - Resolution events
                # ================================================================
                elif isinstance(event, MarketResolved):
                    logger.info(f"Market Resolved: {event.market_id} -> {event.outcome}")
                    # TODO: Mark market as resolved in DB
                
                # ================================================================
                # NEW_MARKET - New market creation
                # ================================================================
                elif isinstance(event, NewMarket):
                    logger.info(f"New Market: {event.market_id} with {len(event.tokens)} tokens")
                    # TODO: Trigger sync to fetch new market details

                # --- NEW: Latency Tracking ---
                if hasattr(event, "timestamp") and event.timestamp:
                    try:
                        # event.timestamp is a naive datetime from wss_messages
                        # Convert to timestamp to compare with time.time()
                        msg_ts = event.timestamp.timestamp()
                        now_ts = time.time()
                        # Calculate latency in ms
                        latency = (now_ts - msg_ts) * 1000
                        # Simple moving average or just latest? Latest is fine for "current status"
                        # Use a small smoothing to avoid jitter
                        # But for "Real-time" display, latest is often what people want to see
                        latest_latency_ms = max(0.0, latency)
                    except Exception:
                        latest_latency_ms = 0.0
                else:
                    latest_latency_ms = 0.0

                # --- NEW: System Status Update ---
                # Update DB every 5 seconds to avoid spamming
                if time.time() - last_status_flush > 5.0:
                    try:
                        status_data = {
                            "connected": True,
                            "latency_ms": round(latest_latency_ms, 2),
                            "messages_received": messages_since_last_health, # Since last log, decent proxy
                            "last_updated": time.time()
                        }
                        db_pool.execute("""
                            INSERT INTO system_status (key, value, updated_at)
                            VALUES ('polymarket_wss', %s, NOW())
                            ON CONFLICT (key) DO UPDATE SET
                                value = EXCLUDED.value,
                                updated_at = NOW()
                        """, (json.dumps(status_data),))
                        last_status_flush = time.time()
                    except Exception as e:
                        logger.warning(f"Failed to update system status: {e}")

                # 3. Check flush conditions
                now = time.time()
                
                total_pending = len(pending_updates) + len(pending_trades) + len(pending_spreads)

                if (total_pending >= settings.wss_batch_size or
                    (now - last_batch_flush) >= settings.wss_batch_interval):

                    # Flush price updates with accumulated volume data
                    await flush_price_batch(
                        pending_updates, 
                        source_to_db_token,
                        volume_accumulator,
                        pending_spreads,
                    )
                    pending_updates.clear()
                    pending_trades.clear()
                    pending_spreads.clear()
                    volume_accumulator.clear()
                    last_batch_flush = now

                    # Update metrics
                    client._metrics.save()
                
                # 4. Periodic health logging
                if now - last_health_log >= HEALTH_LOG_INTERVAL:
                    elapsed = now - last_health_log
                    msgs_per_min = (messages_since_last_health / elapsed) * 60 if elapsed > 0 else 0
                    logger.info(
                        f"WSS Health: {messages_since_last_health} msgs in {elapsed:.0f}s "
                        f"({msgs_per_min:.1f}/min), subscriptions={len(asset_ids)}"
                    )
                    last_health_log = now
                    messages_since_last_health = 0
                    
        except Exception as e:
            logger.error(f"WSS Loop Error: {e}")
            consecutive_failures += 1
            
            if consecutive_failures >= settings.wss_max_reconnect_attempts:
                if settings.wss_fallback_to_polling:
                    logger.critical("Max WSS reconnects reached. Falling back to POLLING mode.")
                    await client.close()
                    # Fallback: run polling loop using sync_once
                    from apps.collector.jobs.polymarket_sync import sync_once
                    logger.info(f"Starting fallback polling (interval={settings.sync_interval_seconds}s)")
                    while not shutdown.is_set:
                        try:
                            await sync_once()
                        except Exception as poll_err:
                            logger.exception(f"Fallback polling error: {poll_err}")
                        await asyncio.sleep(settings.sync_interval_seconds)
                    return
                else:
                    logger.critical("Max WSS reconnects reached and fallback disabled. Exiting.")
                    break
            
            await asyncio.sleep(settings.wss_reconnect_delay)
            
        finally:
             await client.close()

async def flush_price_batch(
    updates: list[PriceUpdate], 
    source_to_db_token: dict[str, str],
    volume_accumulator: dict[str, float] = None,
    spread_updates: list[SpreadUpdate] = None,
) -> None:
    """
    Batch insert pending updates to database with volume and spread data.

    Args:
        updates: List of PriceUpdate objects (token_id is Polymarket source_token_id)
        source_to_db_token: Mapping of source_token_id -> db_token_id (UUID string)
        volume_accumulator: Accumulated trade volume by source_token_id
        spread_updates: Spread updates from best_bid_ask messages
    """
    if not updates:
        return
    
    volume_accumulator = volume_accumulator or {}
    spread_updates = spread_updates or []

    # Build spread lookup: source_token_id -> latest spread
    spread_map = {}
    for s in spread_updates:
        spread_map[s.token_id] = s.spread

    # Deduplicate: keep only the LATEST price for each token in this batch
    latest_map = {}
    for u in updates:
        latest_map[u.token_id] = u

    unique_updates = list(latest_map.values())

    if not unique_updates:
        return

    db_pool = get_db_pool()

    # Convert source_token_ids to db_token_ids for DB operations
    db_token_ids = []
    for u in unique_updates:
        db_id = source_to_db_token.get(u.token_id)
        if db_id:
            db_token_ids.append(db_id)

    if not db_token_ids:
        return

    # Get token_id -> market_id mapping from database
    q = """
        SELECT mt.market_id as id, mt.token_id
        FROM market_tokens mt
        WHERE mt.token_id = ANY(%s::uuid[])
    """
    rows = db_pool.execute(q, (db_token_ids,), fetch=True) or []
    db_token_to_market_id = {str(r["token_id"]): r["id"] for r in rows}

    values = []
    volume_count = 0
    spread_count = 0
    
    for u in unique_updates:
        source_token_id = u.token_id
        db_token_id = source_to_db_token.get(source_token_id)
        if db_token_id:
            market_id = db_token_to_market_id.get(db_token_id)
            if market_id:
                # Get volume from trade accumulator (real-time from trade stream!)
                volume = volume_accumulator.get(source_token_id)
                if volume and volume > 0:
                    volume_count += 1
                
                # Get spread from spread updates
                spread = spread_map.get(source_token_id)
                if spread is not None:
                    spread_count += 1
                
                values.append((
                    db_token_id,  # token_id (UUID)
                    u.price,
                    volume,  # volume from trade stream!
                    spread,  # spread from best_bid_ask
                ))

    if values:
        # Use existing MarketQueries for consistent snapshot insertion
        from packages.core.storage.queries import MarketQueries
        snapshots = [
            {"token_id": v[0], "price": v[1], "volume_24h": v[2], "spread": v[3]}
            for v in values
        ]
        inserted = MarketQueries.insert_snapshots_batch(snapshots)
        
        # Log volume/spread stats periodically
        if volume_count > 0 or spread_count > 0:
            logger.debug(f"Flushed {inserted} updates (volume: {volume_count}, spreads: {spread_count})")
