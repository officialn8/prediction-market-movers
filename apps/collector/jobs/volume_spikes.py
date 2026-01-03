"""
Volume Spike Detection Job

Monitors market volumes for unusual activity that may indicate
breaking news or significant market events.

Runs periodically to:
1. Compare current volume against 7-day historical average
2. Detect and record significant volume spikes
3. Generate alerts for extreme volume anomalies
"""

import asyncio
import logging
from decimal import Decimal
from uuid import UUID

from packages.core.storage.queries import VolumeQueries, AnalyticsQueries, MarketQueries
from packages.core.analytics import metrics

logger = logging.getLogger(__name__)

# Configuration
MIN_SPIKE_RATIO = Decimal("2.0")      # 2x normal volume to consider
MIN_VOLUME = Decimal("1000")          # $1k minimum to avoid noise
ALERT_SPIKE_RATIO = Decimal("3.0")    # 3x to generate alert
LOOKBACK_MINUTES = 60                 # Dedup window for same token


async def check_volume_spikes() -> None:
    """
    Main volume spike detection job.

    Finds tokens with abnormally high volume compared to their
    7-day historical average and records/alerts accordingly.
    """
    logger.info("Running volume spike detection...")

    try:
        # Get all tokens with potential volume spikes
        candidates = VolumeQueries.get_volume_spike_candidates(
            min_spike_ratio=float(MIN_SPIKE_RATIO),
            min_volume=float(MIN_VOLUME),
            limit=200,
        )

        if not candidates:
            logger.debug("No volume spike candidates found")
            return

        logger.info(f"Found {len(candidates)} potential volume spikes")

        spikes_recorded = 0
        alerts_generated = 0

        for candidate in candidates:
            try:
                token_id = UUID(str(candidate["token_id"]))
                current_volume = Decimal(str(candidate["current_volume"]))
                avg_volume = Decimal(str(candidate["avg_volume"]))
                spike_ratio = Decimal(str(candidate["spike_ratio"]))
                current_price = Decimal(str(candidate["current_price"])) if candidate.get("current_price") else None
                title = candidate.get("title", "Unknown Market")
                outcome = candidate.get("outcome", "")

                # Classify severity
                severity = metrics.classify_volume_spike(spike_ratio)
                if severity == "none":
                    continue

                # Deduplication: Check if we recently recorded a spike for this token
                existing = VolumeQueries.get_recent_spike_for_token(
                    token_id=token_id,
                    lookback_minutes=LOOKBACK_MINUTES,
                )

                if existing:
                    # High-watermark: Only record if new spike is significantly larger
                    try:
                        last_ratio = Decimal(str(existing["spike_ratio"]))
                        if spike_ratio <= last_ratio * Decimal("1.2"):  # 20% larger to update
                            continue
                    except Exception:
                        continue

                # Get 1h price change for context
                price_change_1h = None
                try:
                    movers = MarketQueries.get_movers_window(
                        window_seconds=3600,
                        limit=1000,
                        direction="both",
                    )
                    for m in movers:
                        if str(m.get("token_id")) == str(token_id):
                            price_change_1h = Decimal(str(m.get("pct_change", 0)))
                            break
                except Exception:
                    pass

                # Record the volume spike
                VolumeQueries.insert_volume_spike(
                    token_id=token_id,
                    current_volume=current_volume,
                    avg_volume=avg_volume,
                    spike_ratio=spike_ratio,
                    current_price=current_price,
                    price_change_1h=price_change_1h,
                    severity=severity,
                )
                spikes_recorded += 1
                logger.info(
                    f"Volume spike: {title} ({outcome}) - "
                    f"{spike_ratio:.1f}x normal (${current_volume:,.0f} vs avg ${avg_volume:,.0f}) "
                    f"[{severity}]"
                )

                # Generate alert for significant spikes
                if spike_ratio >= ALERT_SPIKE_RATIO:
                    await _generate_volume_alert(
                        token_id=token_id,
                        title=title,
                        outcome=outcome,
                        spike_ratio=spike_ratio,
                        current_volume=current_volume,
                        avg_volume=avg_volume,
                        price_change_1h=price_change_1h,
                        severity=severity,
                    )
                    alerts_generated += 1

            except Exception as e:
                logger.error(f"Error processing candidate {candidate.get('token_id')}: {e}")
                continue

        if spikes_recorded > 0:
            logger.info(f"Recorded {spikes_recorded} volume spikes, generated {alerts_generated} alerts")

    except Exception as e:
        logger.exception("Failed to run volume spike detection")


async def _generate_volume_alert(
    token_id: UUID,
    title: str,
    outcome: str,
    spike_ratio: Decimal,
    current_volume: Decimal,
    avg_volume: Decimal,
    price_change_1h: Decimal | None,
    severity: str,
) -> None:
    """
    Generate an alert for a significant volume spike.

    Uses the existing alerts table with alert_type='volume_spike'.
    """
    try:
        # Check for recent volume alert on this token (separate from price alerts)
        existing = AnalyticsQueries.get_recent_alert_for_token(
            token_id=token_id,
            window_seconds=3600,  # Use 1h window for volume alerts
            lookback_minutes=30,
        )

        # If we have a recent alert, check if it was a volume alert
        # Only skip if it was already a volume alert
        if existing and existing.get("alert_type") == "volume_spike":
            try:
                last_ratio = Decimal(str(existing.get("volume_spike_ratio", 0)))
                if spike_ratio <= last_ratio * Decimal("1.2"):
                    return
            except Exception:
                pass

        # Build reason message
        price_context = ""
        if price_change_1h is not None and abs(price_change_1h) >= 1:
            sign = "+" if price_change_1h > 0 else ""
            price_context = f", price {sign}{price_change_1h:.1f}% 1h"

        reason = (
            f"🔥 VOLUME SPIKE: {title} ({outcome}) - "
            f"{spike_ratio:.1f}x normal volume "
            f"(${current_volume:,.0f} vs avg ${avg_volume:,.0f}){price_context} "
            f"[{severity.upper()}]"
        )

        # Insert alert with volume context
        AnalyticsQueries.insert_alert(
            token_id=token_id,
            window_seconds=3600,
            move_pp=price_change_1h or Decimal("0"),
            threshold_pp=Decimal("0"),  # No price threshold for volume alerts
            reason=reason,
        )

        logger.info(f"Generated volume alert: {title} ({outcome}) - {spike_ratio:.1f}x")

    except Exception as e:
        logger.error(f"Failed to generate volume alert for {token_id}: {e}")


async def get_volume_spike_summary() -> dict:
    """
    Get a summary of current volume spike status.

    Useful for dashboard display or health checks.
    """
    try:
        # Get recent spikes by severity
        spikes = VolumeQueries.get_recent_volume_spikes(
            limit=100,
            min_severity="low",
            unacknowledged_only=False,
        )

        summary = {
            "total_spikes_24h": 0,
            "by_severity": {"low": 0, "medium": 0, "high": 0, "extreme": 0},
            "top_spikes": [],
        }

        for spike in spikes:
            severity = spike.get("severity", "low")
            summary["by_severity"][severity] = summary["by_severity"].get(severity, 0) + 1
            summary["total_spikes_24h"] += 1

            if len(summary["top_spikes"]) < 5:
                summary["top_spikes"].append({
                    "title": spike.get("title"),
                    "outcome": spike.get("outcome"),
                    "spike_ratio": float(spike.get("spike_ratio", 0)),
                    "severity": severity,
                    "current_volume": float(spike.get("current_volume", 0)),
                })

        return summary

    except Exception as e:
        logger.error(f"Failed to get volume spike summary: {e}")
        return {"error": str(e)}
