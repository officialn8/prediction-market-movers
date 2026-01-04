import asyncio
import logging
import time
from datetime import datetime
from uuid import UUID

from packages.core.settings import settings
from packages.core.storage.db import get_db_pool
from apps.collector.jobs.polymarket_sync import sync_markets
from apps.collector.adapters.polymarket_wss import PolymarketWebSocket
from apps.collector.adapters.wss_messages import PriceUpdate
from apps.collector.jobs.movers_cache import check_instant_mover, broadcast_mover_alert

logger = logging.getLogger(__name__)

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
    
    # Simple query to get all token_ids from markets
    # We join with market_tokens because token_id is 1:many with markets
    # We also need the latest price for the initial map
    query = """
        SELECT mt.token_id, 
               (SELECT price FROM snapshots s WHERE s.token_id = mt.token_id ORDER BY ts DESC LIMIT 1) as price
        FROM markets m
        JOIN market_tokens mt ON m.market_id = mt.market_id
        WHERE m.status = 'active'
    """
    rows = db_pool.execute(query, fetch=True) or []
    
    token_map = {str(r["token_id"]): float(r["price"] or 0) for r in rows}

    asset_ids = list(token_map.keys())
    logger.info(f"Loaded {len(asset_ids)} assets for WSS subscription")

    client = PolymarketWebSocket()
    pending_updates: list[PriceUpdate] = []
    last_batch_flush = time.time()
    
    consecutive_failures = 0
    
    while not shutdown.is_set:
        try:
            await client.connect(asset_ids)
            consecutive_failures = 0
            
            async for update in client.listen():
                if shutdown.is_set:
                    break
                    
                # 1. Update local cache for mover detection
                if update.token_id in token_map:
                    old_price = token_map[update.token_id]
                    # Check for instant mover
                    mover = await check_instant_mover(update.token_id, old_price, update.price)
                    if mover:
                        logger.info(f"Instant Mover Detected: {update.token_id} {old_price} -> {update.price}")
                        # Fire and forget alert
                        asyncio.create_task(broadcast_mover_alert(mover))
                
                # Update local map
                token_map[update.token_id] = update.price
                
                # 2. Add to batch
                pending_updates.append(update)
                
                # 3. Check flush conditions
                now = time.time()
                
                # Update metrics file every second approx (on message) or just periodically
                # Doing it on every message is too much I/O.
                # Let's do it on batch flush or roughly every second.
                
                if (len(pending_updates) >= settings.wss_batch_size or 
                    (now - last_batch_flush) >= settings.wss_batch_interval):
                    
                    await flush_price_batch(pending_updates)
                    pending_updates.clear()
                    last_batch_flush = now
                    
                    # Update metrics
                    client._metrics.save()
                    
        except Exception as e:
            logger.error(f"WSS Loop Error: {e}")
            consecutive_failures += 1
            
            if consecutive_failures >= settings.wss_max_reconnect_attempts:
                if settings.wss_fallback_to_polling:
                    logger.critical("Max WSS reconnects reached. Falling back to POLLING mode.")
                    await client.close()
                    # Fallback loop - basically run the standard polling logic
                    # We can't easily return to main.py to switch modes without restarting.
                    # So we run the polling logic here indefinitely or until restart.
                    from apps.collector.jobs.polymarket_sync import run_polymarket
                    await run_polymarket(shutdown, every_seconds=settings.sync_interval_seconds)
                    return
                else:
                    logger.critical("Max WSS reconnects reached and fallback disabled. Exiting.")
                    break
            
            await asyncio.sleep(settings.wss_reconnect_delay)
            
        finally:
             await client.close()

async def flush_price_batch(updates: list[PriceUpdate]) -> None:
    """Batch insert pending updates to database."""
    if not updates:
        return
        
    # We only want to snapshot the LATEST price for each token in this batch
    # to avoid writing intermediate ticks to DB.
    latest_map = {}
    for u in updates:
        latest_map[u.token_id] = u
        
    unique_updates = list(latest_map.values())
    
    unique_updates = list(latest_map.values())
    
    db_pool = get_db_pool()
    
    # Bulk insert into snapshots
    # We need market_id for the snapshot.
    token_ids = [u.token_id for u in unique_updates]
    # SQL IN clause
    
    if not token_ids:
        return

    # Updated query to use market_tokens table for mapping
    q = """
        SELECT mt.market_id as id, mt.token_id 
        FROM market_tokens mt
        WHERE mt.token_id = ANY(%s::uuid[])
    """
    rows = db_pool.execute(q, (token_ids,), fetch=True) or []
    token_to_market_id = {str(r["token_id"]): r["id"] for r in rows}
    
    values = []
    for u in unique_updates:
        mid = token_to_market_id.get(u.token_id)
        if mid:
            values.append((
                mid,
                u.price,
                0, # volume
                u.timestamp
            ))
    
    if values:
        # logic from existing sync: insert into snapshots
        # "INSERT INTO snapshots (market_id, price, volume, ts) VALUES (:market_id, :price, :volume, :ts)"
        db_pool.execute_many(
            "INSERT INTO snapshots (market_id, price, volume, ts) VALUES (%s, %s, %s, %s)",
            values
        )
        # Update markets updated_at timestamp
        # We can do this in one go or separate.
        # "UPDATE markets SET updated_at = :ts WHERE market_id = :market_id"
        update_values = [
            (v[3], v[0]) # ts, market_id
            for v in values
        ]
        db_pool.execute_many(
            "UPDATE markets SET updated_at = %s WHERE market_id = %s",
            update_values
        )
        
        logger.debug(f"Flushed {len(values)} price updates to DB")
