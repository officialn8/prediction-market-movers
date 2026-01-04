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


# =============================================================================
# MOVERS SCORING - Unified scoring for ranking top movers
# =============================================================================

class MoverScorer:
    """
    Unified scorer for ranking market movers.
    
    Encapsulates the scoring logic used in both:
    - Background cache job (movers_cache.py)
    - Real-time WSS handler (check_instant_mover)
    
    This ensures consistent ranking across all code paths.
    """
    
    def __init__(
        self,
        weight_move: float = 1.0,
        weight_volume: float = 1.0,
        weight_spike: float = 0.5,
        min_quality_score: Decimal = Decimal("1.0"),
    ):
        """
        Initialize scorer with configurable weights.
        
        Args:
            weight_move: Weight for price movement component
            weight_volume: Weight for volume component
            weight_spike: Weight for spike bonus
            min_quality_score: Minimum score to be considered significant
        """
        self.weight_move = weight_move
        self.weight_volume = weight_volume
        self.weight_spike = weight_spike
        self.min_quality_score = min_quality_score
    
    def score(
        self,
        price_now: Decimal,
        price_then: Decimal,
        volume: Decimal,
        avg_volume: Optional[Decimal] = None,
    ) -> Tuple[Decimal, Optional[Decimal], Decimal]:
        """
        Calculate composite score for a mover.
        
        Args:
            price_now: Current price
            price_then: Historical price
            volume: Current 24h volume
            avg_volume: Historical average volume (for spike detection)
            
        Returns:
            Tuple of (composite_score, spike_ratio, move_pp)
        """
        # Calculate base metrics
        move_pp = calculate_move_pp(price_now, price_then)
        abs_move_pp = abs(move_pp)
        
        # Calculate spike ratio if we have historical volume
        spike_ratio = None
        if avg_volume is not None and avg_volume > 0:
            spike_ratio = calculate_volume_spike_ratio(volume, avg_volume)
        
        # Calculate composite score
        composite_score = calculate_composite_score(
            abs_move_pp=abs_move_pp,
            volume=volume,
            spike_ratio=spike_ratio,
            weight_move=self.weight_move,
            weight_volume=self.weight_volume,
            weight_spike=self.weight_spike,
        )
        
        return composite_score, spike_ratio, move_pp
    
    def is_significant(self, score: Decimal) -> bool:
        """Check if a score meets the minimum threshold."""
        return score >= self.min_quality_score
    
    def rank_movers(
        self,
        movers: list[dict],
        price_now_key: str = "latest_price",
        price_then_key: str = "old_price",
        volume_key: str = "latest_volume",
        avg_volume_map: Optional[dict] = None,
    ) -> list[dict]:
        """
        Score and rank a list of mover candidates.
        
        Args:
            movers: List of mover dicts from query
            price_now_key: Dict key for current price
            price_then_key: Dict key for historical price
            volume_key: Dict key for volume
            avg_volume_map: Optional map of token_id -> avg_volume for spike detection
            
        Returns:
            Sorted list with added score, spike_ratio, move_pp, and rank fields
        """
        scored = []
        
        for mover in movers:
            token_id = str(mover.get("token_id", ""))
            
            try:
                price_now = Decimal(str(mover.get(price_now_key, 0)))
                price_then = Decimal(str(mover.get(price_then_key, 0)))
                volume = Decimal(str(mover.get(volume_key, 0) or 0))
                
                # Get average volume if available
                avg_volume = None
                if avg_volume_map and token_id in avg_volume_map:
                    avg_volume = avg_volume_map[token_id]
                
                score, spike_ratio, move_pp = self.score(
                    price_now, price_then, volume, avg_volume
                )
                
                if not self.is_significant(score):
                    continue
                
                mover["quality_score"] = score
                mover["spike_ratio"] = spike_ratio
                mover["move_pp"] = move_pp
                mover["abs_move_pp"] = abs(move_pp)
                scored.append(mover)
                
            except (ValueError, TypeError) as e:
                # Skip invalid data
                continue
        
        # Sort by score descending
        scored.sort(key=lambda x: x["quality_score"], reverse=True)
        
        # Assign ranks
        for rank, mover in enumerate(scored, 1):
            mover["rank"] = rank
        
        return scored


# Default scorer instance for common use
default_mover_scorer = MoverScorer()
