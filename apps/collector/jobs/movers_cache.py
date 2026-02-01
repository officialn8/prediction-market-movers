"""
Movers Cache Update Job

Precomputes top movers for fast dashboard loading.

Supports two scoring modes:
1. Legacy: MoverScorer (volume-weighted pp moves)
2. Z-Score: ZScoreMoverScorer (statistically normalized by market volatility)

Z-score mode is preferred when market_stats are available, as it:
- Normalizes for each market's typical volatility
- Ranks stable-market moves higher than volatile-market moves
- Uses log-odds change for proper probability-space scoring
"""

import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Dict
from dataclasses import dataclass

from packages.core.storage.queries import MarketQueries, AnalyticsQueries, VolumeQueries
from packages.core.storage.db import get_db_pool
from packages.core.analytics import metrics
from packages.core.analytics.metrics import MoverScorer, ZScoreMoverScorer
from packages.core.settings import settings

logger = logging.getLogger(__name__)

# Windows to precompute: 5m, 15m, 1h, 24h
WINDOWS = [300, 900, 3600, 86400]
WINDOW_TO_MINUTES = {300: 5, 900: 15, 3600: 60, 86400: 1440}

# Minimum quality score to be included (filters noise)
MIN_QUALITY_SCORE = Decimal("1.0")
MIN_Z_SCORE = 1.5  # ~top 7% of statistical outliers

# Enable Z-score mode when stats are available
USE_ZSCORE_SCORING = True

# Canonical scorers
_legacy_scorer = MoverScorer(
    weight_move=1.0,
    weight_volume=1.0,
    weight_spike=0.5,
    min_quality_score=MIN_QUALITY_SCORE,
)

_zscore_scorer = ZScoreMoverScorer(
    weight_price_z=1.0,
    weight_volume_z=0.5,
    weight_velocity=0.3,
    min_z_score=MIN_Z_SCORE,
    use_log_odds=True,
)


def _get_market_stats_map() -> Dict[str, Dict]:
    """Try to load market stats for Z-score scoring."""
    try:
        from apps.collector.jobs.market_stats import get_market_stats_map
        return get_market_stats_map()
    except Exception as e:
        logger.debug(f"Could not load market stats: {e}")
        return {}


async def update_movers_cache() -> None:
    """
    Calculate top movers and update the cache table.

    Scoring modes:
    1. Z-Score (preferred): Normalizes by market-specific volatility
       - A 5pp move in a stable market ranks higher than 10pp in volatile one
       - Uses log-odds for proper probability-space scoring
    2. Legacy fallback: Volume-weighted pp moves when stats unavailable

    This ensures high-quality, statistically significant moves rank highest.
    """
    logger.info("Running movers cache update...")

    now = datetime.now(timezone.utc)

    # Pre-fetch volume averages for spike detection (legacy scorer)
    volume_avg_map: dict[str, Decimal] = {}
    try:
        volume_avgs = await asyncio.to_thread(VolumeQueries.get_volume_averages)
        for va in volume_avgs:
            token_id = str(va.get("token_id"))
            avg_vol = va.get("avg_volume_7d")
            if token_id and avg_vol:
                volume_avg_map[token_id] = Decimal(str(avg_vol))
    except Exception as e:
        logger.warning(f"Could not fetch volume averages: {e}")

    # Try to load market stats for Z-score mode
    market_stats_map: Dict[str, Dict] = {}
    use_zscore = USE_ZSCORE_SCORING
    if use_zscore:
        market_stats_map = await asyncio.to_thread(_get_market_stats_map)
        if not market_stats_map:
            logger.info("No market stats available, falling back to legacy scoring")
            use_zscore = False
        else:
            logger.info(f"Z-score mode: {len(market_stats_map)} markets with stats")

    for window in WINDOWS:
        try:
            # Fetch raw movers from query
            raw_movers = await asyncio.to_thread(
                MarketQueries.get_movers_window,
                window_seconds=window,
                limit=500,
                direction="both",
            )

            # Score using appropriate method
            if use_zscore:
                scored_movers = _zscore_scorer.rank_movers(
                    movers=raw_movers,
                    market_stats_map=market_stats_map,
                    price_now_key="latest_price",
                    price_then_key="old_price",
                    volume_key="latest_volume",
                    window_minutes=WINDOW_TO_MINUTES.get(window),
                )
            else:
                scored_movers = _legacy_scorer.rank_movers(
                    movers=raw_movers,
                    price_now_key="latest_price",
                    price_then_key="old_price",
                    volume_key="latest_volume",
                    avg_volume_map=volume_avg_map,
                )

            # Build cache records
            cache_buffer = []
            for mover in scored_movers[:100]:
                cache_buffer.append({
                    "as_of_ts": now,
                    "window_seconds": window,
                    "token_id": mover["token_id"],
                    "price_now": Decimal(str(mover["latest_price"])),
                    "price_then": Decimal(str(mover["old_price"])),
                    "move_pp": mover.get("move_pp", mover.get("abs_move_pp", Decimal("0"))),
                    "abs_move_pp": mover.get("abs_move_pp", abs(mover.get("move_pp", Decimal("0")))),
                    "rank": mover["rank"],
                    "quality_score": mover["quality_score"],
                    "volume_24h": Decimal(str(mover.get("latest_volume") or 0)),
                    "spike_ratio": mover.get("spike_ratio"),
                })

            if cache_buffer:
                await asyncio.to_thread(AnalyticsQueries.insert_movers_batch, cache_buffer)
                score_type = "Z" if use_zscore else "Q"
                logger.info(
                    f"Updated cache for {window}s window: {len(cache_buffer)} records "
                    f"(top {score_type}-score: {cache_buffer[0]['quality_score']:.2f})"
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
    min_quality_score=Decimal(str(settings.instant_mover_min_quality_score)),
)

async def check_instant_mover(
    token_id: str,
    old_price: float,
    new_price: float,
    threshold_pp: float | None = None,
    volume: Optional[float] = None,
    min_quality_score: float | None = None,
    min_volume: float | None = None,
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

    threshold_pp = threshold_pp if threshold_pp is not None else settings.instant_mover_threshold_pp
    min_quality_score = (
        min_quality_score if min_quality_score is not None else settings.instant_mover_min_quality_score
    )
    min_volume = min_volume if min_volume is not None else settings.instant_mover_min_volume
    
    # Calculate move in percentage points (consistent with cache)
    move_pp = (new_price - old_price) * 100
    abs_move_pp = abs(move_pp)
    
    # Quick threshold check
    if abs_move_pp < threshold_pp:
        return None
    
    # Calculate quality score if volume available
    quality_score = None
    if volume is not None and volume > 0:
        if min_volume and volume < min_volume:
            return None
        score, _, _ = _instant_scorer.score(
            price_now=Decimal(str(new_price)),
            price_then=Decimal(str(old_price)),
            volume=Decimal(str(volume)),
        )
        quality_score = float(score)
        if quality_score < float(min_quality_score):
            return None
    
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

