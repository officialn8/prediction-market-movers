"""
Alerts Job - Price Movement and Volume Spike Detection

Monitors markets for significant events:
1. Large price movements (>10% in 1 hour)
2. Volume spikes (>3x normal volume)
3. Combined signals (moderate move + moderate spike)

Generates alerts for significant market activity.
"""

import asyncio
import logging
from decimal import Decimal
from uuid import UUID

from packages.core.storage.queries import AnalyticsQueries, MarketQueries, VolumeQueries
from packages.core.analytics import metrics
from packages.core.settings import settings

logger = logging.getLogger(__name__)

# Configuration
ALERT_THRESHOLD_PP = Decimal("10.0")       # 10% price move threshold
ALERT_WINDOW_HOURS = 1                      # Look at 1-hour window
MIN_VOLUME_FOR_ALERT = Decimal("1000")      # $1,000 minimum volume
VOLUME_SPIKE_THRESHOLD = Decimal("3.0")     # 3x normal volume for alert
COMBINED_MOVE_THRESHOLD = Decimal("5.0")    # 5% move when combined with spike
COMBINED_SPIKE_THRESHOLD = Decimal("2.0")   # 2x spike when combined with move


async def run_alerts_check() -> None:
    """
    Check for significant price movements and volume spikes.

    Now uses the unified is_significant_event() function to determine
    if an alert should be generated based on multiple criteria.
    """
    logger.info("Running alerts check...")

    try:
        # Get top movers for analysis
        movers = await asyncio.to_thread(
            MarketQueries.get_top_movers,
            hours=ALERT_WINDOW_HOURS,
            limit=100,
            direction="both"
        )

        # Get volume spike data for enrichment
        volume_spike_map = {}
        try:
            spike_candidates = await asyncio.to_thread(
                VolumeQueries.get_volume_spike_candidates,
                min_spike_ratio=float(COMBINED_SPIKE_THRESHOLD),
                min_volume=float(MIN_VOLUME_FOR_ALERT),
                limit=200,
            )
            for sc in spike_candidates:
                token_id = str(sc.get("token_id"))
                volume_spike_map[token_id] = Decimal(str(sc.get("spike_ratio", 0)))
        except Exception as e:
            logger.debug(f"Could not fetch volume spikes (table may not exist): {e}")

        alerts_generated = 0

        for mover in movers:
            try:
                token_id = UUID(str(mover["token_id"]))
                token_id_str = str(token_id)
                move_pp = Decimal(str(mover["pct_change"]))
                abs_move = abs(move_pp)
                title = mover["title"]
                outcome = mover["outcome"]

                # Get volume
                vol = mover.get("latest_volume") or mover.get("volume_24h")
                volume = Decimal(str(vol)) if vol else Decimal("0")

                # Skip low volume
                if volume < MIN_VOLUME_FOR_ALERT:
                    continue

                # Check spread quality gate
                spread = mover.get("spread")
                if spread is not None:
                    try:
                        if float(spread) > 0.05:
                            continue
                    except:
                        pass

                # Get volume spike ratio if available
                spike_ratio = volume_spike_map.get(token_id_str)

                # Use unified significance check
                is_significant, reason = metrics.is_significant_event(
                    abs_move_pp=abs_move,
                    volume=volume,
                    spike_ratio=spike_ratio,
                    min_move_pp=ALERT_THRESHOLD_PP,
                    min_volume=MIN_VOLUME_FOR_ALERT,
                    min_spike_ratio=VOLUME_SPIKE_THRESHOLD,
                )

                if not is_significant:
                    continue

                # Deduplication: Check recent alerts
                window_seconds = ALERT_WINDOW_HOURS * 3600
                existing = await asyncio.to_thread(
                    AnalyticsQueries.get_recent_alert_for_token,
                    token_id=token_id,
                    window_seconds=window_seconds,
                    lookback_minutes=30
                )

                if existing:
                    # High-watermark: Only alert if significantly larger
                    try:
                        last_move_pp = Decimal(str(existing["move_pp"]))
                        last_spike = Decimal(str(existing.get("volume_spike_ratio") or 0))

                        # Skip if neither move nor spike is significantly larger
                        move_increase = abs_move > abs(last_move_pp) * Decimal("1.2")
                        spike_increase = (
                            spike_ratio is not None
                            and last_spike > 0
                            and spike_ratio > last_spike * Decimal("1.2")
                        )

                        if not move_increase and not spike_increase:
                            continue
                    except Exception:
                        continue

                # Build alert message
                sign = "+" if move_pp > 0 else ""
                alert_parts = [f"{title} ({outcome}): {sign}{move_pp:.2f}pp"]

                if spike_ratio and spike_ratio >= COMBINED_SPIKE_THRESHOLD:
                    alert_parts.append(f"📊 {spike_ratio:.1f}x volume")

                alert_parts.append(f"${volume:,.0f} vol")
                alert_parts.append(f"[{reason}]")

                reason_text = " | ".join(alert_parts)

                # Insert alert
                await asyncio.to_thread(
                    AnalyticsQueries.insert_alert,
                    token_id=token_id,
                    window_seconds=window_seconds,
                    move_pp=move_pp,
                    threshold_pp=ALERT_THRESHOLD_PP,
                    reason=reason_text,
                )

                alerts_generated += 1
                logger.info(f"Generated alert: {reason_text}")

            except Exception as e:
                logger.error(f"Error processing mover {mover.get('token_id')}: {e}")
                continue

        if alerts_generated > 0:
            logger.info(f"Generated {alerts_generated} new alerts")

    except Exception as e:
        logger.exception("Failed to run alerts check")


async def check_volume_only_alerts() -> None:
    """
    Separate check for pure volume spike alerts.

    These are markets with abnormal volume but may not have
    moved in price yet - early warning signals.
    """
    logger.info("Checking volume-only alerts...")

    try:
        # Get volume spikes that might not have price movement yet
        spikes = await asyncio.to_thread(
            VolumeQueries.get_volume_spike_candidates,
            min_spike_ratio=float(VOLUME_SPIKE_THRESHOLD),
            min_volume=float(MIN_VOLUME_FOR_ALERT),
            limit=50,
        )

        alerts_generated = 0

        for spike in spikes:
            try:
                token_id = UUID(str(spike["token_id"]))
                spike_ratio = Decimal(str(spike["spike_ratio"]))
                current_volume = Decimal(str(spike["current_volume"]))
                avg_volume = Decimal(str(spike["avg_volume"]))
                title = spike.get("title", "Unknown")
                outcome = spike.get("outcome", "")

                # Only alert on high+ severity spikes for volume-only
                severity = metrics.classify_volume_spike(spike_ratio)
                if severity not in ("high", "extreme"):
                    continue

                # Check for existing recent alert
                existing = await asyncio.to_thread(
                    VolumeQueries.get_recent_spike_for_token,
                    token_id=token_id,
                    lookback_minutes=60,
                )

                if existing:
                    last_ratio = Decimal(str(existing.get("spike_ratio", 0)))
                    if spike_ratio <= last_ratio * Decimal("1.2"):
                        continue

                # Record the spike
                await asyncio.to_thread(
                    VolumeQueries.insert_volume_spike,
                    token_id=token_id,
                    current_volume=current_volume,
                    avg_volume=avg_volume,
                    spike_ratio=spike_ratio,
                    current_price=Decimal(str(spike.get("current_price", 0))),
                    severity=severity,
                )

                # Generate alert
                reason = (
                    f"🔥 VOLUME ALERT: {title} ({outcome}) | "
                    f"{spike_ratio:.1f}x normal volume | "
                    f"${current_volume:,.0f} (avg ${avg_volume:,.0f}) | "
                    f"[{severity.upper()}]"
                )

                await asyncio.to_thread(
                    AnalyticsQueries.insert_alert,
                    token_id=token_id,
                    window_seconds=3600,
                    move_pp=Decimal("0"),
                    threshold_pp=Decimal("0"),
                    reason=reason,
                )

                alerts_generated += 1
                logger.info(f"Volume-only alert: {title} - {spike_ratio:.1f}x")

            except Exception as e:
                logger.error(f"Error processing volume spike {spike.get('token_id')}: {e}")
                continue

        if alerts_generated > 0:
            logger.info(f"Generated {alerts_generated} volume-only alerts")

    except Exception as e:
        logger.warning(f"Volume-only alerts check failed (table may not exist): {e}")
