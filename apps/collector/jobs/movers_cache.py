"""
Movers Cache Update Job

Precomputes top movers for fast dashboard loading.
Uses centralized MoverScorer for composite scoring that factors in:
- Price movement (percentage points)
- Volume (liquidity/legitimacy)
- Volume spikes (unusual activity indicator)

All scoring is done via MoverScorer to ensure consistency between
cache updates, real-time alerts, and dashboard queries.
"""

import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional
from dataclasses import dataclass

from packages.core.storage.queries import MarketQueries, AnalyticsQueries, VolumeQueries
from packages.core.storage.db import get_db_pool
from packages.core.analytics import metrics
from packages.core.analytics.metrics import MoverScorer

logger = logging.getLogger(__name__)

# Windows to precompute: 5m, 15m, 1h, 24h
WINDOWS = [300, 900, 3600, 86400]

# Minimum quality score to be included (filters noise)
MIN_QUALITY_SCORE = Decimal("1.0")

# Canonical scorer for cache updates - same weights as real-time detection
_cache_scorer = MoverScorer(
    weight_move=1.0,
    weight_volume=1.0,
    weight_spike=0.5,
    min_quality_score=MIN_QUALITY_SCORE,
)


async def update_movers_cache() -> None:
    """
    Calculate top movers and update the cache table.

    Uses centralized MoverScorer for composite scoring that combines:
    1. Price movement magnitude
    2. Volume (log-scaled for diminishing returns)
    3. Volume spike bonus (if current volume >> historical avg)

    This ensures high-volume, actively-traded markets rank higher
    than low-volume micro-markets with wild swings.
    """
    logger.info("Running movers cache update...")

    now = datetime.now(timezone.utc)

    # Pre-fetch volume averages for spike detection
    volume_avg_map: dict[str, Decimal] = {}
    try:
        volume_avgs = await asyncio.to_thread(VolumeQueries.get_volume_averages)
        for va in volume_avgs:
            token_id = str(va.get("token_id"))
            avg_vol = va.get("avg_volume_7d")
            if token_id and avg_vol:
                volume_avg_map[token_id] = Decimal(str(avg_vol))
    except Exception as e:
        logger.warning(f"Could not fetch volume averages (table may not exist yet): {e}")

    for window in WINDOWS:
        try:
            # Fetch raw movers from query - returns raw metrics only
            # SQL orders by abs(pct_change) for initial filtering
            raw_movers = await asyncio.to_thread(
                MarketQueries.get_movers_window,
                window_seconds=window,
                limit=500,  # Fetch more than needed, scorer will filter
                direction="both",
            )

            # Use centralized MoverScorer for consistent scoring
            # This ensures cache scoring matches real-time alert scoring
            scored_movers = _cache_scorer.rank_movers(
                movers=raw_movers,
                price_now_key="latest_price",
                price_then_key="old_price",
                volume_key="latest_volume",
                avg_volume_map=volume_avg_map,
            )

            # Build cache records from scored movers
            cache_buffer = []
            for mover in scored_movers[:100]:  # Keep top 100 per window
                cache_buffer.append({
                    "as_of_ts": now,
                    "window_seconds": window,
                    "token_id": mover["token_id"],
                    "price_now": Decimal(str(mover["latest_price"])),
                    "price_then": Decimal(str(mover["old_price"])),
                    "move_pp": mover["move_pp"],
                    "abs_move_pp": mover["abs_move_pp"],
                    "rank": mover["rank"],
                    "quality_score": mover["quality_score"],
                    # Persist volume context for dashboard display consistency
                    "volume_24h": Decimal(str(mover.get("latest_volume") or 0)),
                    "spike_ratio": mover.get("spike_ratio"),
                })

            if cache_buffer:
                await asyncio.to_thread(AnalyticsQueries.insert_movers_batch, cache_buffer)
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
        return await asyncio.to_thread(
            VolumeQueries.get_movers_with_volume_context,
            window_seconds=window_seconds,
            limit=limit,
            source=source,
        )
    except Exception as e:
        logger.warning(f"Enhanced movers query failed, falling back to cached: {e}")
        return await asyncio.to_thread(
            AnalyticsQueries.get_cached_movers,
            window_seconds=window_seconds,
            limit=limit,
            source=source,
        )


@dataclass
class MoverAlert:
    token_id: str
    old_price: float
    new_price: float
    change_pct: float
    move_pp: float
    detected_at: datetime
    quality_score: Optional[float] = None

# Shared scorer instance for real-time mover detection
# Uses same weights as cache job for consistency
_instant_scorer = MoverScorer(
    weight_move=1.0,
    weight_volume=1.0,
    weight_spike=0.5,
    min_quality_score=Decimal("0.5"),  # Lower threshold for instant detection
)

async def check_instant_mover(
    token_id: str,
    old_price: float,
    new_price: float,
    threshold_pp: float = 5.0,  # 5 percentage points
    volume: Optional[float] = None,
) -> Optional[MoverAlert]:
    """
    Check if price change qualifies as instant mover.
    Called directly from WSS handler for sub-second detection.
    
    Uses the same scoring logic as the cache job for consistency.
    
    Args:
        token_id: Token identifier
        old_price: Previous price (0-1 range)
        new_price: Current price (0-1 range)
        threshold_pp: Minimum move in percentage points (default 5pp)
        volume: Optional volume for quality scoring
    """
    if old_price <= 0:
        return None
    
    # Calculate move in percentage points (consistent with cache)
    move_pp = (new_price - old_price) * 100
    abs_move_pp = abs(move_pp)
    
    # Quick threshold check
    if abs_move_pp < threshold_pp:
        return None
    
    # Calculate quality score if volume available
    quality_score = None
    if volume is not None and volume > 0:
        score, _, _ = _instant_scorer.score(
            price_now=Decimal(str(new_price)),
            price_then=Decimal(str(old_price)),
            volume=Decimal(str(volume)),
        )
        quality_score = float(score)
    
    # Also calculate percentage change for backwards compatibility
    change_pct = (new_price - old_price) / old_price
    
    return MoverAlert(
        token_id=token_id,
        old_price=old_price,
        new_price=new_price,
        change_pct=change_pct,
        move_pp=move_pp,
        detected_at=datetime.utcnow(),
        quality_score=quality_score,
    )

async def broadcast_mover_alert(alert: MoverAlert) -> None:
    """
    Broadcast instant mover alert.
    For now just log, but could push to frontend via websocket or db alert table.
    """
    score_str = f" (score={alert.quality_score:.2f})" if alert.quality_score else ""
    logger.info(
        f"INSTANT MOVER: {alert.token_id} moved {alert.move_pp:+.2f}pp "
        f"({alert.old_price:.4f} -> {alert.new_price:.4f}){score_str}"
    )
    # TODO: Implement real alerting logic (e.g. insert into alerts table)

