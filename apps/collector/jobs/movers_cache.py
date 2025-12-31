import asyncio
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from packages.core.storage.queries import MarketQueries, AnalyticsQueries
from packages.core.storage.db import get_db_pool
from packages.core.analytics import metrics

logger = logging.getLogger(__name__)

# Windows to precompute: 1h, 24h
WINDOWS = [3600, 86400]

async def update_movers_cache() -> None:
    """
    Calculate top movers and update the cache table.
    Uses Python-level logic for scoring to allow for complex metrics.
    """
    logger.info("Running movers cache update...")
    
    db = get_db_pool()
    now = datetime.now(timezone.utc)
    
    for window in WINDOWS:
        try:
            # 1. Fetch latest prices
            # We get all active tokens with their latest price
            latest_query = """
                SELECT DISTINCT ON (s.token_id)
                    s.token_id, s.price, s.volume_24h, s.ts
                FROM snapshots s
                JOIN market_tokens mt ON s.token_id = mt.token_id
                JOIN markets m ON mt.market_id = m.market_id
                WHERE m.status = 'active'
                ORDER BY s.token_id, s.ts DESC
            """
            latest_rows = db.execute(latest_query, fetch=True) or []
            
            # 2. Fetch historical prices (window ago)
            # This is a bit expensive, in prod we might optimize or using window functions in SQL
            # For Python-side logic, we'll just fetch what we need.
            # Using a simplified approach: fetch snapshots closest to (now - window)
            
            time_threshold = now - timedelta(seconds=window)
            
            # We need a way to batch this efficiently. 
            # For now, we'll trust the SQL based 'old_price' logic from the original query 
            # but implement the SCORING in Python.
            
            # Hybrid approach: Get the raw price changes from SQL, then rank in Python
            
            raw_movers = MarketQueries.get_top_movers(
                hours=window // 3600, 
                limit=1000, # Fetch broad set
                direction="both"
            )
            
            cache_buffer = []
            
            for rank, row in enumerate(raw_movers, 1):
                token_id = row['token_id']
                price_now = row['latest_price']
                price_then = row['old_price']
                # Calculate quality score using our new module
                # We now fetch latest_volume from the query
                volume = row.get('latest_volume') or Decimal(0)
                
                # Re-calculate cleanly
                move_pp = metrics.calculate_move_pp(price_now, price_then)
                abs_move_pp = abs(move_pp)
                
                quality_score = metrics.calculate_quality_score(abs_move_pp, volume)
                
                # Filter noise: Skip if quality score is too low (e.g. < 1.0)
                # This prevents low volume garbage from cluttering the top list
                if quality_score < 1.0:
                    continue
                
                cache_buffer.append({
                    "as_of_ts": now,
                    "window_seconds": window,
                    "token_id": token_id,
                    "price_now": price_now,
                    "price_then": price_then,
                    "move_pp": move_pp,
                    "abs_move_pp": abs_move_pp,
                    "rank": rank, # Preliminary rank
                    "quality_score": quality_score
                })
            
            # Optional: Re-sort buffer by quality_score if desired?
            # cache_buffer.sort(key=lambda x: x['quality_score'], reverse=True)
            # For now, we respect the pct_change sort from SQL but store the score
            
            if cache_buffer:
                AnalyticsQueries.insert_movers_batch(cache_buffer)
                logger.info(f"Updated cache for window {window}s: {len(cache_buffer)} records")
                
        except Exception as e:
            logger.exception(f"Failed to update movers cache for window {window}s")

