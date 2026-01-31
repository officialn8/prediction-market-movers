"""
Data retention job - prunes old snapshots to prevent disk exhaustion.

Runs daily, removes snapshots older than configured retention period.
"""

import asyncio
import logging
import os
from datetime import datetime

from packages.core.storage.db import get_db_pool

logger = logging.getLogger(__name__)

# Default: keep 7 days of data
RETENTION_DAYS = int(os.getenv("SNAPSHOT_RETENTION_DAYS", "7"))


async def run_retention_cleanup() -> dict:
    """
    Delete snapshots older than RETENTION_DAYS.
    
    Returns:
        Dict with cleanup stats
    """
    db = get_db_pool()
    
    logger.info(f"Starting retention cleanup (keeping {RETENTION_DAYS} days)")
    
    try:
        # Count before delete (for logging)
        count_result = await asyncio.to_thread(
            db.execute,
            f"SELECT COUNT(*) as cnt FROM snapshots WHERE ts < NOW() - INTERVAL '{RETENTION_DAYS} days'",
            fetch=True
        )
        to_delete = count_result[0]["cnt"] if count_result else 0
        
        if to_delete == 0:
            logger.info("No old snapshots to clean up")
            return {"deleted": 0, "retention_days": RETENTION_DAYS}
        
        # Delete old snapshots in batches to avoid long locks
        total_deleted = 0
        batch_size = 10000
        
        while True:
            result = await asyncio.to_thread(
                db.execute,
                f"""
                DELETE FROM snapshots 
                WHERE ts IN (
                    SELECT ts FROM snapshots 
                    WHERE ts < NOW() - INTERVAL '{RETENTION_DAYS} days'
                    LIMIT {batch_size}
                )
                """,
                fetch=False
            )
            
            # psycopg3 returns rowcount via cursor, but our wrapper might not expose it
            # We'll just loop until no more old records
            check = await asyncio.to_thread(
                db.execute,
                f"SELECT COUNT(*) as cnt FROM snapshots WHERE ts < NOW() - INTERVAL '{RETENTION_DAYS} days'",
                fetch=True
            )
            remaining = check[0]["cnt"] if check else 0
            
            deleted_this_batch = to_delete - remaining - total_deleted
            total_deleted = to_delete - remaining
            
            logger.info(f"Deleted batch: {deleted_this_batch}, remaining: {remaining}")
            
            if remaining == 0:
                break
            
            # Small delay between batches to reduce DB pressure
            await asyncio.sleep(0.5)
        
        logger.info(f"Retention cleanup complete: deleted {total_deleted} snapshots")
        
        # Get current DB size for monitoring
        size_result = await asyncio.to_thread(
            db.execute,
            "SELECT pg_size_pretty(pg_database_size(current_database())) as db_size",
            fetch=True
        )
        db_size = size_result[0]["db_size"] if size_result else "unknown"
        logger.info(f"Current database size: {db_size}")
        
        return {
            "deleted": total_deleted,
            "retention_days": RETENTION_DAYS,
            "db_size": db_size,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.exception(f"Retention cleanup failed: {e}")
        raise
