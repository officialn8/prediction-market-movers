import math
from decimal import Decimal

def calculate_move_pp(price_now: Decimal, price_then: Decimal) -> Decimal:
    """
    Calculate movement in percentage points.
    
    Formula: (price_now - price_then) * 100
    Example: 0.60 - 0.50 = 0.10 -> 10.0 pp
    """
    return (price_now - price_then) * 100

def calculate_quality_score(abs_move_pp: Decimal, volume: Decimal) -> Decimal:
    """
    Calculate quality score to filter noise.
    
    Formula: abs_move_pp * log1p(volume)
    This prevents illiquid micro-markets from dominating.
    """
    if volume <= 0:
        return Decimal("0")
    
    # log1p is ln(1+x), base e.
    # Convert Decimal to float for math operations, then back if needed.
    vol_log = Decimal(math.log1p(float(volume)))
    return abs_move_pp * vol_log
