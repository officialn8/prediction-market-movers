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
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional
from uuid import UUID

from packages.core.storage.queries import AnalyticsQueries, MarketQueries, VolumeQueries
from packages.core.analytics import metrics
from packages.core.settings import settings

logger = logging.getLogger(__name__)

# Configuration - Time-to-expiry aware thresholds
ALERT_THRESHOLD_PP = Decimal("10.0")       # Default for distant markets (>48h)
CLOSING_THRESHOLD_PP = Decimal("25.0")     # Higher for markets closing within 48h
IMMINENT_THRESHOLD_PP = Decimal("50.0")    # Even higher for markets closing within 6h
ALERT_WINDOW_HOURS = 1                      # Look at 1-hour window
MIN_VOLUME_FOR_ALERT = Decimal("1000")      # $1,000 minimum volume
VOLUME_SPIKE_THRESHOLD = Decimal("3.0")     # 3x normal volume for alert
COMBINED_MOVE_THRESHOLD = Decimal("5.0")    # 5% move when combined with spike
COMBINED_SPIKE_THRESHOLD = Decimal("2.0")   # 2x spike when combined with move


def _passes_hold_zone(
    move_edge_pp: Decimal,
    spike_edge_ratio: Optional[Decimal] = None,
) -> bool:
    """Suppress borderline triggers while preserving ranking behavior elsewhere."""
    if not settings.signal_hold_zone_enabled:
        return True

    move_gate = move_edge_pp >= Decimal(str(settings.signal_hold_zone_move_pp))
    spike_gate = False
    if spike_edge_ratio is not None:
        spike_gate = spike_edge_ratio >= Decimal(str(settings.signal_hold_zone_spike_ratio))

    return move_gate or spike_gate


def get_dynamic_threshold(end_date: Optional[datetime]) -> Decimal:
    """
    Calculate alert threshold based on time remaining until market closes.
    
    Markets closing soon naturally have larger price swings as they approach
    settlement. We require bigger moves to trigger alerts for these markets
    to filter out settlement mechanics noise.
    
    Returns:
        Decimal threshold in percentage points (pp)
    """
    if end_date is None:
        return ALERT_THRESHOLD_PP
    
    now = datetime.now(timezone.utc)
    # Handle naive datetime by assuming UTC
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)
    
    hours_remaining = (end_date - now).total_seconds() / 3600
    
    if hours_remaining <= 6:
        return IMMINENT_THRESHOLD_PP   # 50pp - only extreme moves matter
    if hours_remaining <= 48:
        return CLOSING_THRESHOLD_PP    # 25pp - higher bar for closing markets
    return ALERT_THRESHOLD_PP          # 10pp - normal sensitivity


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
            # Log at warning level so schema issues don't silently disable alerts
            logger.warning(
                f"Could not fetch volume spikes (may indicate schema issue or missing table): {e}"
            )

        alerts_generated = 0

        for mover in movers:
            try:
                token_id = UUID(str(mover["token_id"]))
                token_id_str = str(token_id)
                move_pp = Decimal(str(mover["pct_change"]))
                abs_move = abs(move_pp)
                title = mover["title"]
                outcome = mover["outcome"]
                end_date = mover.get("end_date")

                # Get dynamic threshold based on time-to-expiry
                threshold = get_dynamic_threshold(end_date)

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

                # Use unified significance check with dynamic threshold
                is_significant, reason = metrics.is_significant_event(
                    abs_move_pp=abs_move,
                    volume=volume,
                    spike_ratio=spike_ratio,
                    min_move_pp=threshold,
                    min_volume=MIN_VOLUME_FOR_ALERT,
                    min_spike_ratio=VOLUME_SPIKE_THRESHOLD,
                )

                if not is_significant:
                    continue

                move_edge = abs_move - threshold
                spike_edge = None
                if spike_ratio is not None:
                    spike_edge = spike_ratio - COMBINED_SPIKE_THRESHOLD
                if not _passes_hold_zone(move_edge_pp=move_edge, spike_edge_ratio=spike_edge):
                    logger.debug(
                        "Suppressed borderline alert via hold-zone "
                        f"(token={token_id_str}, move_edge={move_edge:.3f}, spike_edge={spike_edge})"
                    )
                    continue

                # Deduplication: Check recent alerts
                window_seconds = ALERT_WINDOW_HOURS * 3600
                alert_type = (
                    "combined"
                    if spike_ratio is not None and spike_ratio >= COMBINED_SPIKE_THRESHOLD
                    else "price_move"
                )
                existing = await asyncio.to_thread(
                    AnalyticsQueries.get_recent_alert_for_token,
                    token_id=token_id,
                    window_seconds=window_seconds,
                    lookback_minutes=30,
                    alert_type=alert_type,
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
                
                # Add time-to-close context for closing markets
                if end_date:
                    now = datetime.now(timezone.utc)
                    if end_date.tzinfo is None:
                        end_date = end_date.replace(tzinfo=timezone.utc)
                    hours_left = (end_date - now).total_seconds() / 3600
                    if hours_left <= 48:
                        alert_parts.append(f"closes in {hours_left:.0f}h")
                
                alert_parts.append(f"[{reason}]")

                reason_text = " | ".join(alert_parts)

                # Insert alert
                await asyncio.to_thread(
                    AnalyticsQueries.insert_alert,
                    token_id=token_id,
                    window_seconds=window_seconds,
                    move_pp=move_pp,
                    threshold_pp=threshold,
                    reason=reason_text,
                    alert_type=alert_type,
                    volume_spike_ratio=spike_ratio,
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

                spike_edge = spike_ratio - ALERT_SPIKE_RATIO
                if not _passes_hold_zone(move_edge_pp=Decimal("0"), spike_edge_ratio=spike_edge):
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
                    alert_type="volume_spike",
                    volume_spike_ratio=spike_ratio,
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
