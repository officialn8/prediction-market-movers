
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from packages.core.storage.db import get_db_pool

logger = logging.getLogger(__name__)

async def run_ohlc_rollups() -> None:
    """
    Aggregates raw snapshots into OHLC candles and runs retention policy.
    """
    logger.info("Running OHLC rollups and retention...")
    db = get_db_pool()
    
    try:
        # 1. Generate 1m candles
        # We aggregate snapshots that happened in the last interval that haven't been rolled up?
        # A simple approach for MVP is to run this every minute and aggregate the "last minute"
        # or use an idempotent query that inserts missing candles from snapshots.
        
        # This query aggregates raw snapshots into 1m buckets for any data newer than 2 hours 
        # (to catch up) that doesn't exist in ohlc_1m yet.
        
        query_1m = """
            INSERT INTO ohlc_1m (token_id, bucket_ts, open, high, low, close, volume)
            SELECT
                token_id,
                DATE_TRUNC('minute', ts) as bucket,
                (ARRAY_AGG(price ORDER BY ts ASC))[1] as open,
                MAX(price) as high,
                MIN(price) as low,
                (ARRAY_AGG(price ORDER BY ts DESC))[1] as close,
                MAX(volume_24h) as volume -- Approximate volume taking max since it is cumulative usually? 
                                          -- Actually volume_24h is a rolling 24h vol. Creating "candle volume" from it is tricky.
                                          -- For now, let's just store the max volume_24h seen in that bucket as a proxy, 
                                          -- or 0 if we assume it's delta.
                                          -- Context: Polymarket API gives 24h volume.
            FROM snapshots
            WHERE ts >= NOW() - INTERVAL '2 hours'
            GROUP BY token_id, 2
            ON CONFLICT (token_id, bucket_ts) DO UPDATE SET
                high = GREATEST(ohlc_1m.high, EXCLUDED.high),
                low = LEAST(ohlc_1m.low, EXCLUDED.low),
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
        """
        db.execute(query_1m)
        
        # 2. Generate 1h candles from 1m candles (hierarchical rollup)
        # Much more efficient than going back to snapshots
        query_1h = """
            INSERT INTO ohlc_1h (token_id, bucket_ts, open, high, low, close, volume)
            SELECT 
                token_id,
                DATE_TRUNC('hour', bucket_ts) as bucket,
                (ARRAY_AGG(open ORDER BY bucket_ts ASC))[1] as open,
                MAX(high) as high,
                MIN(low) as low,
                (ARRAY_AGG(close ORDER BY bucket_ts DESC))[1] as close,
                MAX(volume) as volume
            FROM ohlc_1m
            WHERE bucket_ts >= NOW() - INTERVAL '4 hours'
            GROUP BY token_id, 2
            ON CONFLICT (token_id, bucket_ts) DO UPDATE SET
                high = GREATEST(ohlc_1h.high, EXCLUDED.high),
                low = LEAST(ohlc_1h.low, EXCLUDED.low),
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
        """
        db.execute(query_1h)
        
        # 3. Retention Policy
        # Call the procedure we defined in migration 003
        db.execute("CALL clean_old_snapshots()")
        
        logger.info("OHLC rollups and retention complete.")
        
    except Exception as e:
        logger.exception("Failed to run OHLC rollups")

