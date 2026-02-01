"""
Cross-platform arbitrage detection job.

Detects arbitrage opportunities between Polymarket and Kalshi by:
1. Loading active market pairs
2. Fetching latest prices from both platforms
3. Calculating combined cost for YES/NO strategies
4. Recording opportunities where combined cost < $1

Arbitrage exists when:
- Buy YES on Platform A + Buy NO on Platform B < $1
- Or: Buy NO on Platform A + Buy YES on Platform B < $1

Either way, you guarantee a $1 payout for less than $1 spent.
"""

import asyncio
import logging
from decimal import Decimal
from typing import Optional

from packages.core.storage.queries import ArbitrageQueries

logger = logging.getLogger(__name__)

# Configuration
MIN_PROFIT_MARGIN = Decimal("0.002")  # 0.2% minimum profit to record
MIN_VOLUME_24H = Decimal("100")  # $100 minimum volume for actionable opportunities
CHECK_INTERVAL_SECONDS = 30  # How often to check for opportunities
OPPORTUNITY_EXPIRY_MINUTES = 5  # How long an opportunity is considered valid


def calculate_arbitrage(
    poly_yes: Decimal,
    poly_no: Decimal,
    kalshi_yes: Decimal,
    kalshi_no: Decimal,
) -> Optional[dict]:
    """
    Calculate if an arbitrage opportunity exists.
    
    Returns:
        Dict with arbitrage details if opportunity exists, None otherwise
    """
    # Strategy 1: Buy YES on Polymarket + Buy NO on Kalshi
    cost_yes_no = poly_yes + kalshi_no
    
    # Strategy 2: Buy NO on Polymarket + Buy YES on Kalshi
    cost_no_yes = poly_no + kalshi_yes
    
    # Find the better strategy (lower cost = higher profit)
    if cost_yes_no < Decimal("1") and cost_yes_no < cost_no_yes:
        profit_margin = Decimal("1") - cost_yes_no
        if profit_margin >= MIN_PROFIT_MARGIN:
            profit_pct = (profit_margin / cost_yes_no) * 100
            return {
                "arbitrage_type": "YES_NO",
                "total_cost": cost_yes_no,
                "profit_margin": profit_margin,
                "profit_percentage": round(profit_pct, 4),
            }
    
    if cost_no_yes < Decimal("1"):
        profit_margin = Decimal("1") - cost_no_yes
        if profit_margin >= MIN_PROFIT_MARGIN:
            profit_pct = (profit_margin / cost_no_yes) * 100
            return {
                "arbitrage_type": "NO_YES",
                "total_cost": cost_no_yes,
                "profit_margin": profit_margin,
                "profit_percentage": round(profit_pct, 4),
            }
    
    return None


def detect_opportunities() -> list[dict]:
    """
    Check all active market pairs for arbitrage opportunities.
    
    Returns:
        List of detected opportunities
    """
    opportunities = []
    
    # Get all active pairs with latest prices
    pairs = ArbitrageQueries.get_active_pairs()
    
    if not pairs:
        logger.debug("No active market pairs configured")
        return opportunities
    
    logger.info(f"Checking {len(pairs)} market pairs for arbitrage...")
    
    for pair in pairs:
        pair_id = pair["pair_id"]
        
        # Get prices (default to 0.5 if missing)
        poly_yes = Decimal(str(pair.get("polymarket_yes_price") or "0.5"))
        poly_no = Decimal("1") - poly_yes  # Binary market: NO = 1 - YES
        
        kalshi_yes = Decimal(str(pair.get("kalshi_yes_price") or "0.5"))
        kalshi_no = Decimal("1") - kalshi_yes
        
        # Calculate arbitrage
        arb = calculate_arbitrage(poly_yes, poly_no, kalshi_yes, kalshi_no)
        
        if arb:
            # Get volumes
            poly_vol = pair.get("polymarket_volume_24h")
            kalshi_vol = pair.get("kalshi_volume_24h")
            
            # Record the opportunity
            opportunity = ArbitrageQueries.record_opportunity(
                pair_id=pair_id,
                arbitrage_type=arb["arbitrage_type"],
                polymarket_yes_price=poly_yes,
                polymarket_no_price=poly_no,
                kalshi_yes_price=kalshi_yes,
                kalshi_no_price=kalshi_no,
                total_cost=arb["total_cost"],
                profit_margin=arb["profit_margin"],
                profit_percentage=arb["profit_percentage"],
                polymarket_volume_24h=Decimal(str(poly_vol)) if poly_vol else None,
                kalshi_volume_24h=Decimal(str(kalshi_vol)) if kalshi_vol else None,
                expires_minutes=OPPORTUNITY_EXPIRY_MINUTES,
            )
            
            opportunities.append(opportunity)
            
            logger.info(
                f"🎯 Arbitrage detected: {pair.get('polymarket_title', 'Unknown')}"
                f" | Type: {arb['arbitrage_type']}"
                f" | Profit: {arb['profit_percentage']:.2f}%"
                f" | Cost: ${arb['total_cost']:.4f}"
            )
    
    return opportunities


def run_arbitrage_check() -> None:
    """
    Single synchronous run of arbitrage detection.
    Can be called from scheduler or manually.
    """
    logger.info("Running arbitrage detection check...")
    
    # Expire old opportunities first
    expired_count = ArbitrageQueries.expire_old_opportunities()
    if expired_count > 0:
        logger.info(f"Expired {expired_count} old opportunities")
    
    # Detect new opportunities
    opportunities = detect_opportunities()
    
    if opportunities:
        logger.info(f"Detected {len(opportunities)} new arbitrage opportunities")
    else:
        logger.debug("No arbitrage opportunities found")


async def run_arbitrage_loop() -> None:
    """
    Continuous async loop for arbitrage detection.
    Runs every CHECK_INTERVAL_SECONDS.
    """
    logger.info(
        f"Starting arbitrage detection loop "
        f"(interval: {CHECK_INTERVAL_SECONDS}s, min_profit: {MIN_PROFIT_MARGIN*100}%)"
    )
    
    while True:
        try:
            run_arbitrage_check()
        except Exception as e:
            logger.error(f"Error in arbitrage detection: {e}", exc_info=True)
        
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)


# Entry point for standalone running
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    run_arbitrage_check()
