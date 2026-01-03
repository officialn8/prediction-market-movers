"""
Analytics metrics for prediction market analysis.

Provides calculations for:
- Price movement (percentage points)
- Quality scoring (volume-weighted moves)
- Volume spike detection
- Composite scoring for ranking
"""

import math
from decimal import Decimal
from typing import Optional, Tuple


def calculate_move_pp(price_now: Decimal, price_then: Decimal) -> Decimal:
    """
    Calculate movement in percentage points.

    Formula: (price_now - price_then) * 100
    Example: 0.60 - 0.50 = 0.10 -> 10.0 pp
    """
    return (price_now - price_then) * 100


def calculate_pct_change(price_now: Decimal, price_then: Decimal) -> Optional[Decimal]:
    """
    Calculate percentage change relative to old price.

    Formula: ((price_now - price_then) / price_then) * 100
    Example: 0.60 from 0.50 = 20% increase
    """
    if price_then <= 0:
        return None
    return ((price_now - price_then) / price_then) * 100


def calculate_quality_score(abs_move_pp: Decimal, volume: Decimal) -> Decimal:
    """
    Calculate quality score to filter noise.

    Formula: abs_move_pp * log1p(volume)
    This prevents illiquid micro-markets from dominating.

    A 10% move with $10k volume scores higher than 50% move with $100 volume.
    """
    if volume <= 0:
        return Decimal("0")

    # log1p is ln(1+x), base e.
    vol_log = Decimal(str(math.log1p(float(volume))))
    return abs_move_pp * vol_log


def calculate_volume_spike_ratio(
    current_volume: Decimal,
    avg_volume: Decimal,
) -> Optional[Decimal]:
    """
    Calculate volume spike ratio (current vs historical average).

    Args:
        current_volume: Current 24h volume
        avg_volume: Historical average volume (e.g., 7-day avg)

    Returns:
        Ratio of current to average (e.g., 3.0 means 3x normal volume)
        Returns None if average is zero/invalid

    Example:
        current=50000, avg=10000 -> ratio=5.0 (5x normal activity)
    """
    if avg_volume is None or avg_volume <= 0:
        return None
    if current_volume is None or current_volume < 0:
        return None

    return Decimal(str(float(current_volume) / float(avg_volume)))


def classify_volume_spike(spike_ratio: Optional[Decimal]) -> str:
    """
    Classify volume spike severity.

    Args:
        spike_ratio: Ratio of current to average volume

    Returns:
        Severity level: 'none', 'low', 'medium', 'high', 'extreme'
    """
    if spike_ratio is None or spike_ratio < Decimal("1.5"):
        return "none"
    elif spike_ratio < Decimal("3.0"):
        return "low"      # 1.5x - 3x normal
    elif spike_ratio < Decimal("5.0"):
        return "medium"   # 3x - 5x normal
    elif spike_ratio < Decimal("10.0"):
        return "high"     # 5x - 10x normal
    else:
        return "extreme"  # 10x+ normal


def calculate_composite_score(
    abs_move_pp: Decimal,
    volume: Decimal,
    spike_ratio: Optional[Decimal] = None,
    weight_move: float = 1.0,
    weight_volume: float = 1.0,
    weight_spike: float = 0.5,
) -> Decimal:
    """
    Calculate composite score combining price move, volume, and spike detection.

    This creates a unified ranking that prioritizes:
    1. Large price movements
    2. High volume (liquidity/legitimacy)
    3. Unusual volume activity (something is happening)

    Formula:
        score = (abs_move * weight_move) * log1p(volume) * weight_volume * spike_bonus

    Where spike_bonus = 1 + (spike_ratio - 1) * weight_spike if spike detected

    Args:
        abs_move_pp: Absolute price movement in percentage points
        volume: Current 24h volume
        spike_ratio: Volume spike ratio (current/avg), None if not calculated
        weight_move: Weight for price movement component
        weight_volume: Weight for volume component
        weight_spike: Weight for spike bonus (0.5 = 50% boost per spike multiple)

    Returns:
        Composite score (higher = more significant)
    """
    if volume <= 0:
        return Decimal("0")

    # Base quality score
    base_score = float(abs_move_pp) * weight_move * math.log1p(float(volume)) * weight_volume

    # Apply spike bonus if detected
    if spike_ratio is not None and spike_ratio > Decimal("1.5"):
        # Bonus scales with spike ratio
        # e.g., 3x volume -> 1 + (3-1)*0.5 = 2.0x bonus
        spike_bonus = 1.0 + (float(spike_ratio) - 1.0) * weight_spike
        # Cap the bonus to prevent extreme spikes from dominating
        spike_bonus = min(spike_bonus, 5.0)
        base_score *= spike_bonus

    return Decimal(str(base_score))


def is_significant_event(
    abs_move_pp: Decimal,
    volume: Decimal,
    spike_ratio: Optional[Decimal] = None,
    min_move_pp: Decimal = Decimal("5.0"),
    min_volume: Decimal = Decimal("1000"),
    min_spike_ratio: Decimal = Decimal("3.0"),
) -> Tuple[bool, str]:
    """
    Determine if a market event is significant enough to alert on.

    An event is significant if it meets ANY of these criteria:
    1. Large price move (>= min_move_pp) with decent volume (>= min_volume)
    2. Volume spike (>= min_spike_ratio) regardless of price move
    3. Combination of moderate move + moderate spike

    Args:
        abs_move_pp: Absolute price movement in percentage points
        volume: Current 24h volume
        spike_ratio: Volume spike ratio
        min_move_pp: Minimum price move threshold
        min_volume: Minimum volume threshold
        min_spike_ratio: Minimum spike ratio threshold

    Returns:
        Tuple of (is_significant: bool, reason: str)
    """
    reasons = []

    # Check price movement criterion
    price_significant = abs_move_pp >= min_move_pp and volume >= min_volume
    if price_significant:
        reasons.append(f"price_move_{abs_move_pp:.1f}pp")

    # Check volume spike criterion
    spike_significant = (
        spike_ratio is not None
        and spike_ratio >= min_spike_ratio
        and volume >= min_volume
    )
    if spike_significant:
        reasons.append(f"volume_spike_{spike_ratio:.1f}x")

    # Check combination criterion (lower thresholds when both present)
    combo_significant = (
        abs_move_pp >= min_move_pp * Decimal("0.5")  # 50% of normal threshold
        and spike_ratio is not None
        and spike_ratio >= min_spike_ratio * Decimal("0.5")  # 50% of normal threshold
        and volume >= min_volume
    )
    if combo_significant and not (price_significant or spike_significant):
        reasons.append("combo_move_and_spike")

    is_significant = bool(reasons)
    reason = "+".join(reasons) if reasons else "none"

    return is_significant, reason


def calculate_price_velocity(
    prices: list[Tuple[Decimal, float]],  # List of (price, timestamp_seconds)
) -> Optional[Decimal]:
    """
    Calculate price velocity (rate of change over time).

    Useful for detecting rapid movements vs slow drifts.

    Args:
        prices: List of (price, unix_timestamp) tuples, oldest first

    Returns:
        Price change per minute (pp/min), or None if insufficient data
    """
    if len(prices) < 2:
        return None

    first_price, first_ts = prices[0]
    last_price, last_ts = prices[-1]

    time_diff_minutes = (last_ts - first_ts) / 60.0
    if time_diff_minutes <= 0:
        return None

    price_diff = float(last_price - first_price) * 100  # Convert to pp
    velocity = price_diff / time_diff_minutes

    return Decimal(str(velocity))
