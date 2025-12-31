import asyncio
import logging
from decimal import Decimal

from packages.core.storage.queries import AnalyticsQueries, MarketQueries
from packages.core.settings import settings

logger = logging.getLogger(__name__)

# Config - could move to settings
ALERT_THRESHOLD_PP = Decimal("10.0")  # 10% move
ALERT_WINDOW_HOURS = 1
MIN_VOLUME_FOR_ALERT = Decimal("1000") # $1000 volume

async def run_alerts_check() -> None:
    """
    Check for significant price movements and generate alerts.
    """
    logger.info("Running alerts check...")
    
    try:
        # Re-use the existing top movers query to find candidates
        # We look at the 1 hour window
        movers = MarketQueries.get_top_movers(
            hours=ALERT_WINDOW_HOURS,
            limit=50, # Check top 50 movers
            direction="both"
        )
        
        alerts_generated = 0
        
        for mover in movers:
            try:
                move_pp = mover["pct_change"]
                abs_move = abs(move_pp)
                
                # Check thresholds
                if abs_move < ALERT_THRESHOLD_PP:
                    continue
                    
                # TODO: Check volume if available in mover dict (it's not currently joined in get_top_movers explicitly as volume_24h, 
                # but we can fetch it or trust the mover quality)
                # For now, just alert on price.
                
                token_id = mover["token_id"]
                market_title = mover["title"]
                outcome = mover["outcome"]
                
                # Deduplication: Check if we alerted on this token recently (e.g. last 15 mins)
                # For MVP Phase 2, we skip complex dedupe and just insert. 
                # Ideally we'd query: SELECT 1 FROM alerts WHERE token_id=%s AND created_at > NOW() - INTERVAL '15 minutes'
                
                sign = "+" if move_pp > 0 else ""
                reason = f"{market_title} ({outcome}): {sign}{move_pp:.2f}% in last hour"
                
                AnalyticsQueries.insert_alert(
                    token_id=token_id,
                    window_seconds=ALERT_WINDOW_HOURS * 3600,
                    move_pp=move_pp,
                    threshold_pp=ALERT_THRESHOLD_PP,
                    reason=reason
                )
                alerts_generated += 1
                logger.info(f"Generated alert: {reason}")
                
            except Exception as e:
                logger.error(f"Error processing mover {mover.get('token_id')}: {e}")
                continue
                
        if alerts_generated > 0:
            logger.info(f"Generated {alerts_generated} new alerts.")
            
    except Exception as e:
        logger.exception("Failed to run alerts check")
