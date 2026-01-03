"""
Movers Cache Update Job

Precomputes top movers for fast dashboard loading.
Now uses composite scoring that factors in:
- Price movement (percentage points)
- Volume (liquidity/legitimacy)
- Volume spikes (unusual activity indicator)
"""

import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal

from packages.core.storage.queries import MarketQueries, AnalyticsQueries, VolumeQueries
from packages.core.storage.db import get_db_pool
from packages.core.analytics import metrics

logger = logging.getLogger(__name__)

# Windows to precompute: 5m, 15m, 1h, 24h
WINDOWS = [300, 900, 3600, 86400]

# Minimum quality score to be included (filters noise)
MIN_QUALITY_SCORE = Decimal("1.0")


async def update_movers_cache() -> None:
    """
    Calculate top movers and update the cache table.

    Uses composite scoring that combines:
    1. Price movement magnitude
    2. Volume (log-scaled for diminishing returns)
    3. Volume spike bonus (if current volume >> historical avg)

    This ensures high-volume, actively-traded markets rank higher
    than low-volume micro-markets with wild swings.
    """
    logger.info("Running movers cache update...")

    db = get_db_pool()
    now = datetime.now(timezone.utc)

    # Pre-fetch volume averages for spike detection
    volume_avg_map = {}
    try:
        volume_avgs = VolumeQueries.get_volume_averages()
        for va in volume_avgs:
            token_id = str(va.get("token_id"))
            avg_vol = va.get("avg_volume_7d")
            if token_id and avg_vol:
                volume_avg_map[token_id] = Decimal(str(avg_vol))
    except Exception as e:
        logger.warning(f"Could not fetch volume averages (table may not exist yet): {e}")

    for window in WINDOWS:
        try:
            # Fetch raw movers from the centralized query
            raw_movers = MarketQueries.get_movers_window(
                window_seconds=window,
                limit=1000,
                direction="both"
            )

            cache_buffer = []

            for row in raw_movers:
                token_id = row['token_id']
                token_id_str = str(token_id)
                price_now = Decimal(str(row['latest_price']))
                price_then = Decimal(str(row['old_price']))
                volume = Decimal(str(row.get('latest_volume') or 0))

                # Calculate base metrics
                move_pp = metrics.calculate_move_pp(price_now, price_then)
                abs_move_pp = abs(move_pp)

                # Calculate volume spike ratio if we have historical data
                spike_ratio = None
                if token_id_str in volume_avg_map:
                    avg_volume = volume_avg_map[token_id_str]
                    spike_ratio = metrics.calculate_volume_spike_ratio(volume, avg_volume)

                # Calculate composite score (the new ranking metric)
                composite_score = metrics.calculate_composite_score(
                    abs_move_pp=abs_move_pp,
                    volume=volume,
                    spike_ratio=spike_ratio,
                    weight_move=1.0,
                    weight_volume=1.0,
                    weight_spike=0.5,  # 50% bonus per spike multiple
                )

                # Filter noise: Skip if composite score is too low
                if composite_score < MIN_QUALITY_SCORE:
                    continue

                cache_buffer.append({
                    "as_of_ts": now,
                    "window_seconds": window,
                    "token_id": token_id,
                    "price_now": price_now,
                    "price_then": price_then,
                    "move_pp": move_pp,
                    "abs_move_pp": abs_move_pp,
                    "rank": 0,  # Will be set after sorting
                    "quality_score": composite_score,  # Now stores composite score
                    # Store spike ratio for dashboard display (via extra query if needed)
                    "_spike_ratio": spike_ratio,
                    "_volume": volume,
                })

            # CRITICAL: Sort by composite score (quality_score) NOT by pct_change
            # This is the key change that makes volume matter in rankings
            cache_buffer.sort(key=lambda x: x['quality_score'], reverse=True)

            # Assign ranks after sorting
            for rank, item in enumerate(cache_buffer, 1):
                item['rank'] = rank
                # Remove internal fields not stored in DB
                item.pop('_spike_ratio', None)
                item.pop('_volume', None)

            # Keep top 100 per window
            cache_buffer = cache_buffer[:100]

            if cache_buffer:
                AnalyticsQueries.insert_movers_batch(cache_buffer)
                logger.info(
                    f"Updated cache for {window}s window: {len(cache_buffer)} records "
                    f"(top score: {cache_buffer[0]['quality_score']:.2f})"
                )

        except Exception as e:
            logger.exception(f"Failed to update movers cache for window {window}s")


async def get_enhanced_movers(
    window_seconds: int = 3600,
    limit: int = 20,
    source: str | None = None,
) -> list[dict]:
    """
    Get movers with enhanced volume context for dashboard display.

    This is an alternative to the cached movers that includes
    real-time volume spike information.
    """
    try:
        return VolumeQueries.get_movers_with_volume_context(
            window_seconds=window_seconds,
            limit=limit,
            source=source,
        )
    except Exception as e:
        logger.warning(f"Enhanced movers query failed, falling back to cached: {e}")
        return AnalyticsQueries.get_cached_movers(
            window_seconds=window_seconds,
            limit=limit,
            source=source,
        )
