import asyncio
import logging
from decimal import Decimal
from uuid import UUID

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
                # Type safety
                token_id = UUID(str(mover["token_id"]))
                move_pp = Decimal(str(mover["pct_change"]))
                abs_move = abs(move_pp)
                
                # Check thresholds
                if abs_move < ALERT_THRESHOLD_PP:
                    continue
                
                # Quality Gates
                # 1. Volume
                vol = mover.get("latest_volume") or mover.get("volume_24h")
                if vol is not None:
                    try:
                        if Decimal(str(vol)) < MIN_VOLUME_FOR_ALERT:
                            # logger.debug(f"Skipping {mover['title']}: low volume {vol}")
                            continue
                    except:
                        pass
                
                # 2. Spread (if available)
                spread = mover.get("spread")
                if spread is not None:
                     try:
                        if float(spread) > 0.05:
                            continue
                     except:
                        pass

                market_title = mover["title"]
                outcome = mover["outcome"]
                
                # Deduplication & High-Watermark
                # Check if we alerted on this token regarding this window recently (last 30 mins)
                window_seconds_val = ALERT_WINDOW_HOURS * 3600
                existing = AnalyticsQueries.get_recent_alert_for_token(
                    token_id=token_id, 
                    window_seconds=window_seconds_val,
                    lookback_minutes=30
                )
                
                if existing:
                    # High-watermark check
                    # Only alert again if the new move is significantly larger (>20% larger than previous alert)
                    try:
                        last_move_pp = Decimal(str(existing["move_pp"]))
                        # If current move is not at least 20% larger than the last reported move, skip
                        # e.g. last was 10%, current is 11% -> skip. current is 13% -> alert.
                        if abs_move <= abs(last_move_pp) * Decimal("1.2"):
                            continue
                    except Exception:
                        # If can't parse last move, safer to skip or alert? 
                        # Let's skip to avoid spam if DB is messy
                        continue
                
                sign = "+" if move_pp > 0 else ""
                reason = f"{market_title} ({outcome}): {sign}{move_pp:.2f}% in last {ALERT_WINDOW_HOURS}h"
                
                AnalyticsQueries.insert_alert(
                    token_id=token_id,
                    window_seconds=window_seconds_val,
                    move_pp=move_pp,
                    threshold_pp=ALERT_THRESHOLD_PP,
                    reason=reason
                )
                alerts_generated += 1
                logger.info(f"Generated alert: {reason} (Vol: {vol}, Move: {move_pp}%)")
                
            except Exception as e:
                logger.error(f"Error processing mover {mover.get('token_id')}: {e}")
                continue
                
        if alerts_generated > 0:
            logger.info(f"Generated {alerts_generated} new alerts.")
            
    except Exception as e:
        logger.exception("Failed to run alerts check")
