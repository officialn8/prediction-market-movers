"""
User Alerts Checker - Evaluates custom user-defined alerts.
"""

import asyncio
import logging
from decimal import Decimal
from datetime import datetime, timezone, timedelta

from packages.core.storage.queries import UserAlertsQueries
from packages.core.storage.db import get_db_pool

logger = logging.getLogger(__name__)

# Minimum time between triggers for the same alert (prevents spam)
MIN_RETRIGGER_MINUTES = 5


async def check_user_alerts() -> None:
    """
    Check all active user alerts and trigger notifications where conditions are met.
    """
    logger.info("Checking user alerts...")

    try:
        alerts = UserAlertsQueries.get_active_user_alerts()

        if not alerts:
            logger.debug("No active user alerts to check.")
            return

        triggered_count = 0

        for alert in alerts:
            try:
                if should_trigger(alert):
                    trigger_alert(alert)
                    triggered_count += 1
            except Exception as e:
                logger.error(f"Error processing user alert {alert.get('alert_id')}: {e}")

        if triggered_count > 0:
            logger.info(f"Triggered {triggered_count} user alert(s).")

    except Exception as e:
        logger.exception("Failed to check user alerts")


def should_trigger(alert: dict) -> bool:
    """
    Determine if an alert condition is met.
    """
    current_price = alert.get('current_price')
    if current_price is None:
        return False

    current_price = float(current_price)
    threshold = float(alert.get('threshold', 0))
    condition_type = alert.get('condition_type')

    # Check retrigger cooldown
    last_triggered = alert.get('last_triggered')
    if last_triggered:
        cooldown_end = last_triggered + timedelta(minutes=MIN_RETRIGGER_MINUTES)
        if datetime.now(timezone.utc) < cooldown_end.replace(tzinfo=timezone.utc):
            return False

    if condition_type == 'above':
        return current_price >= threshold

    elif condition_type == 'below':
        return current_price <= threshold

    elif condition_type == 'change_pct':
        # For change_pct, we need to fetch historical price
        window_seconds = alert.get('window_seconds', 3600)
        old_price = get_historical_price(
            token_id=alert['token_id'],
            seconds_ago=window_seconds
        )
        if old_price is None or old_price == 0:
            return False

        pct_change = abs((current_price - old_price) / old_price * 100)
        return pct_change >= threshold

    return False


def get_historical_price(token_id: str, seconds_ago: int) -> float | None:
    """
    Get the price of a token from N seconds ago.
    """
    db = get_db_pool()
    query = """
        SELECT price FROM snapshots
        WHERE token_id = %s
          AND ts <= NOW() - (%s * INTERVAL '1 second')
        ORDER BY ts DESC
        LIMIT 1
    """
    result = db.execute(query, (str(token_id), seconds_ago), fetch=True)
    if result:
        return float(result[0]['price'])
    return None


def trigger_alert(alert: dict) -> None:
    """
    Trigger an alert: create notification and handle notify_once.
    """
    alert_id = alert['alert_id']
    current_price = float(alert.get('current_price', 0))
    threshold = float(alert.get('threshold', 0))
    condition_type = alert.get('condition_type')
    market_title = alert.get('market_title', 'Unknown')
    outcome = alert.get('outcome', 'YES')

    # Build message
    if condition_type == 'above':
        message = f"{outcome} price ${current_price:.2f} exceeded threshold ${threshold:.2f}"
    elif condition_type == 'below':
        message = f"{outcome} price ${current_price:.2f} dropped below threshold ${threshold:.2f}"
    else:
        window_seconds = alert.get('window_seconds', 3600)
        old_price = get_historical_price(alert['token_id'], window_seconds)
        if old_price:
            # Use percentage points (pp) for prediction markets
            change_pp = (current_price - old_price) * 100
            message = f"{outcome} price changed {change_pp:+.1f}pp (${old_price:.2f} -> ${current_price:.2f})"
        else:
            message = f"{outcome} price changed significantly"

    logger.info(f"Triggering user alert: {market_title} - {message}")

    # Record the trigger
    UserAlertsQueries.record_alert_trigger(
        alert_id=alert_id,
        current_price=current_price,
        threshold=threshold,
        message=message
    )

    # Handle notify_once
    if alert.get('notify_once'):
        UserAlertsQueries.deactivate_user_alert(alert_id)
        logger.info(f"Deactivated notify_once alert {alert_id}")
