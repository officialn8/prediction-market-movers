"""
Market Statistics Job

Calculates per-market volatility statistics for Z-score normalization.
Run periodically (e.g., daily) to keep stats current.

These stats enable the ZScoreMoverScorer to:
1. Normalize moves by market-specific volatility
2. Rank a 5pp move in a stable market higher than 10pp in a volatile one
3. Properly weight volume relative to market norms
"""

import asyncio
import logging
import math
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional

from packages.core.storage.db import get_db_pool

logger = logging.getLogger(__name__)

# Minimum samples required for reliable stats
MIN_SAMPLES = 10
LOOKBACK_DAYS = 14  # Use 2 weeks of history


async def calculate_market_stats() -> int:
    """
    Calculate volatility statistics for all active tokens.
    
    Returns number of tokens updated.
    """
    logger.info("Starting market stats calculation...")
    db = get_db_pool()
    
    # Get all active tokens with sufficient history
    tokens = db.execute("""
        SELECT DISTINCT mt.token_id
        FROM market_tokens mt
        JOIN markets m ON mt.market_id = m.market_id
        WHERE m.status = 'active'
          AND EXISTS (
              SELECT 1 FROM snapshots s 
              WHERE s.token_id = mt.token_id 
              AND s.ts > NOW() - INTERVAL '%s days'
          )
    """, (LOOKBACK_DAYS,), fetch=True) or []
    
    logger.info(f"Processing {len(tokens)} tokens...")
    updated_count = 0
    
    for token in tokens:
        token_id = str(token["token_id"])
        
        try:
            stats = await asyncio.to_thread(
                _calculate_token_stats, token_id, LOOKBACK_DAYS
            )
            
            if stats and stats.get("sample_count", 0) >= MIN_SAMPLES:
                await asyncio.to_thread(_upsert_stats, token_id, stats)
                updated_count += 1
                
        except Exception as e:
            logger.debug(f"Failed to calc stats for {token_id}: {e}")
            continue
    
    logger.info(f"Market stats updated: {updated_count} tokens")
    return updated_count


def _calculate_token_stats(token_id: str, lookback_days: int) -> Optional[Dict]:
    """Calculate volatility stats for a single token."""
    db = get_db_pool()
    
    # Get hourly price samples (using OHLC or snapshots)
    samples = db.execute("""
        WITH hourly_prices AS (
            SELECT 
                date_trunc('hour', ts) as hour_ts,
                FIRST_VALUE(price) OVER (
                    PARTITION BY date_trunc('hour', ts) 
                    ORDER BY ts ASC
                ) as open_price,
                LAST_VALUE(price) OVER (
                    PARTITION BY date_trunc('hour', ts) 
                    ORDER BY ts ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) as close_price,
                volume_24h
            FROM snapshots
            WHERE token_id = %s
              AND ts > NOW() - INTERVAL '%s days'
        ),
        distinct_hours AS (
            SELECT DISTINCT ON (hour_ts)
                hour_ts,
                open_price,
                close_price,
                volume_24h
            FROM hourly_prices
            ORDER BY hour_ts, open_price
        )
        SELECT 
            open_price,
            close_price,
            volume_24h,
            LAG(close_price) OVER (ORDER BY hour_ts) as prev_close
        FROM distinct_hours
        ORDER BY hour_ts
    """, (token_id, lookback_days), fetch=True) or []
    
    if len(samples) < MIN_SAMPLES:
        return None
    
    # Calculate move statistics
    moves_pp = []
    log_odds_changes = []
    volumes = []
    
    for s in samples:
        prev_close = s.get("prev_close")
        close = s.get("close_price")
        vol = s.get("volume_24h")
        
        if prev_close and close and float(prev_close) > 0:
            # Percentage point move
            move = abs(float(close) - float(prev_close)) * 100
            moves_pp.append(move)
            
            # Log-odds change
            try:
                eps = 0.001
                p1 = max(eps, min(1 - eps, float(prev_close)))
                p2 = max(eps, min(1 - eps, float(close)))
                lo1 = math.log(p1 / (1 - p1))
                lo2 = math.log(p2 / (1 - p2))
                log_odds_changes.append(abs(lo2 - lo1))
            except (ValueError, ZeroDivisionError):
                pass
        
        if vol and float(vol) > 0:
            volumes.append(float(vol))
    
    if len(moves_pp) < MIN_SAMPLES:
        return None
    
    # Calculate statistics
    def calc_stats(values: List[float]) -> tuple:
        if not values:
            return 0.0, 0.0, 0.0
        n = len(values)
        mean = sum(values) / n
        variance = sum((x - mean) ** 2 for x in values) / n if n > 1 else 0
        stddev = math.sqrt(variance)
        max_val = max(values)
        return mean, stddev, max_val
    
    avg_move, stddev_move, max_move = calc_stats(moves_pp)
    avg_lo, stddev_lo, _ = calc_stats(log_odds_changes) if log_odds_changes else (0.2, 0.5, 0)
    avg_vol, stddev_vol, _ = calc_stats(volumes) if volumes else (10000, 20000, 0)
    
    return {
        "avg_move_pp": avg_move,
        "stddev_move_pp": stddev_move,
        "max_move_pp": max_move,
        "avg_log_odds": avg_lo,
        "stddev_log_odds": stddev_lo,
        "avg_volume": avg_vol,
        "stddev_volume": stddev_vol,
        "sample_count": len(moves_pp),
        "has_sufficient_data": len(moves_pp) >= MIN_SAMPLES,
    }


def _upsert_stats(token_id: str, stats: Dict) -> None:
    """Upsert stats into market_stats table."""
    db = get_db_pool()
    
    db.execute("""
        INSERT INTO market_stats (
            token_id, avg_move_pp, stddev_move_pp, max_move_pp,
            avg_log_odds, stddev_log_odds, avg_volume, stddev_volume,
            sample_count, has_sufficient_data, last_updated
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
        )
        ON CONFLICT (token_id) DO UPDATE SET
            avg_move_pp = EXCLUDED.avg_move_pp,
            stddev_move_pp = EXCLUDED.stddev_move_pp,
            max_move_pp = EXCLUDED.max_move_pp,
            avg_log_odds = EXCLUDED.avg_log_odds,
            stddev_log_odds = EXCLUDED.stddev_log_odds,
            avg_volume = EXCLUDED.avg_volume,
            stddev_volume = EXCLUDED.stddev_volume,
            sample_count = EXCLUDED.sample_count,
            has_sufficient_data = EXCLUDED.has_sufficient_data,
            last_updated = NOW()
    """, (
        token_id,
        stats["avg_move_pp"],
        stats["stddev_move_pp"],
        stats["max_move_pp"],
        stats["avg_log_odds"],
        stats["stddev_log_odds"],
        stats["avg_volume"],
        stats["stddev_volume"],
        stats["sample_count"],
        stats["has_sufficient_data"],
    ))


def get_market_stats_map() -> Dict[str, Dict]:
    """
    Get all market stats as a map for scoring.
    
    Returns:
        Dict mapping token_id -> stats dict
    """
    db = get_db_pool()
    
    rows = db.execute("""
        SELECT 
            token_id::text,
            avg_move_pp,
            stddev_move_pp,
            avg_log_odds,
            stddev_log_odds,
            avg_volume,
            stddev_volume,
            has_sufficient_data
        FROM market_stats
        WHERE has_sufficient_data = true
    """, fetch=True) or []
    
    return {
        row["token_id"]: {
            "avg_move_pp": float(row["avg_move_pp"]) if row["avg_move_pp"] else 2.0,
            "stddev_move_pp": float(row["stddev_move_pp"]) if row["stddev_move_pp"] else 3.0,
            "avg_log_odds": float(row["avg_log_odds"]) if row["avg_log_odds"] else 0.2,
            "stddev_log_odds": float(row["stddev_log_odds"]) if row["stddev_log_odds"] else 0.5,
            "avg_volume": float(row["avg_volume"]) if row["avg_volume"] else 10000,
            "stddev_volume": float(row["stddev_volume"]) if row["stddev_volume"] else 20000,
        }
        for row in rows
    }


# Entry point for collector main loop
async def update_market_stats() -> int:
    """Update market stats (called from collector main)."""
    return await calculate_market_stats()


if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)
    asyncio.run(calculate_market_stats())
