
import asyncio
import logging

from packages.core.storage.db import get_db_pool

logger = logging.getLogger(__name__)

async def run_ohlc_rollups() -> None:
    """
    Aggregates raw snapshots into OHLC candles.
    """
    logger.info("Running OHLC rollups...")
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
                MAX(volume_24h) as volume -- Max 24h volume seen in bucket
            FROM snapshots
            WHERE ts >= NOW() - INTERVAL '2 hours'
            GROUP BY token_id, 2
            ON CONFLICT (token_id, bucket_ts) DO UPDATE SET
                high = GREATEST(ohlc_1m.high, EXCLUDED.high),
                low = LEAST(ohlc_1m.low, EXCLUDED.low),
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
        """
        await asyncio.to_thread(db.execute, query_1m)
        
        # 2. Generate 1h candles from 1m candles.
        
        query_1h = """
            INSERT INTO ohlc_1h (token_id, bucket_ts, open, high, low, close, volume)
            SELECT 
                token_id,
                DATE_TRUNC('hour', bucket_ts) as bucket,
                (ARRAY_AGG(open ORDER BY bucket_ts ASC))[1] as open,
                MAX(high) as high,
                MIN(low) as low,
                (ARRAY_AGG(close ORDER BY bucket_ts DESC))[1] as close,
                MAX(volume) as volume -- Max 24h vol in this 1h bucket
            FROM ohlc_1m
            WHERE bucket_ts >= NOW() - INTERVAL '4 hours'
            GROUP BY token_id, 2
            ON CONFLICT (token_id, bucket_ts) DO UPDATE SET
                high = GREATEST(ohlc_1h.high, EXCLUDED.high),
                low = LEAST(ohlc_1h.low, EXCLUDED.low),
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
        """
        await asyncio.to_thread(db.execute, query_1h)
        
        logger.info("OHLC rollups complete.")
        
    except Exception as e:
        logger.exception("Failed to run OHLC rollups")
