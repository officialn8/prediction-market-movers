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

from packages.core.analytics.feature_manifest import validate_live_feature_rows
from packages.core.settings import settings


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
    current_price: Optional[Decimal] = None,
) -> Decimal:
    """
    Calculate composite score combining price move, volume, and spike detection.

    This creates a unified ranking that prioritizes:
    1. Large price movements
    2. High volume (liquidity/legitimacy)
    3. Unusual volume activity (something is happening)
    4. Mid-range prices (moves near 0 or 1 are often noise)

    Formula:
        score = (abs_move * weight_move) * log1p(volume) * weight_volume * spike_bonus * price_factor

    Where:
        - spike_bonus = 1 + (spike_ratio - 1) * weight_spike if spike detected (capped at 10x)
        - price_factor = penalty for extreme prices (near 0 or 1)

    Args:
        abs_move_pp: Absolute price movement in percentage points
        volume: Current 24h volume
        spike_ratio: Volume spike ratio (current/avg), None if not calculated
        weight_move: Weight for price movement component
        weight_volume: Weight for volume component
        weight_spike: Weight for spike bonus (0.5 = 50% boost per spike multiple)
        current_price: Current price (0-1) for boundary penalty

    Returns:
        Composite score (higher = more significant)
    """
    # Minimum volume threshold - filter out illiquid noise
    MIN_VOLUME = Decimal("100")
    if volume < MIN_VOLUME:
        return Decimal("0")

    # Minimum move threshold - filter out bid/ask bounce
    MIN_MOVE_PP = Decimal("0.5")
    if abs_move_pp < MIN_MOVE_PP:
        return Decimal("0")

    # Base quality score
    base_score = float(abs_move_pp) * weight_move * math.log1p(float(volume)) * weight_volume

    # Apply spike bonus if detected
    if spike_ratio is not None and spike_ratio > Decimal("1.5"):
        # Bonus scales with spike ratio
        # e.g., 3x volume -> 1 + (3-1)*0.5 = 2.0x bonus
        spike_bonus = 1.0 + (float(spike_ratio) - 1.0) * weight_spike
        # Increased cap to 10x for truly extreme spikes (something big is happening)
        spike_bonus = min(spike_bonus, 10.0)
        base_score *= spike_bonus

    # Apply price boundary penalty
    # Prices near 0 or 1 are more susceptible to noise (small $ moves = big pp moves)
    # Full credit for prices in [0.10, 0.90], linear penalty outside
    if current_price is not None:
        price_f = float(current_price)
        if price_f < 0.05 or price_f > 0.95:
            # Extreme prices: 50% penalty
            base_score *= 0.5
        elif price_f < 0.10 or price_f > 0.90:
            # Near-extreme: 25% penalty
            base_score *= 0.75

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


def alert_volume_threshold_multiplier(volume: Optional[Decimal | float]) -> float:
    """
    Adjust alert severity thresholds by liquidity.

    Low-volume markets require larger moves to earn the same severity.
    High-volume markets can earn severity at slightly smaller moves.
    """
    if volume is None:
        return 1.0

    volume_f = float(volume)
    if volume_f < 5_000:
        return 1.2
    if volume_f > 25_000:
        return 0.85
    return 1.0


def get_alert_severity_thresholds(
    hours_to_expiry: Optional[float],
    volume: Optional[Decimal | float] = None,
) -> tuple[float, float, float]:
    """
    Compute (notable, significant, extreme) thresholds in pp.

    Base thresholds tighten as expiry approaches to reduce resolution noise.
    Volume then modulates those thresholds.
    """
    if hours_to_expiry is None or hours_to_expiry >= 240:
        base = (10.0, 20.0, 40.0)
    elif hours_to_expiry >= 48:
        base = (15.0, 30.0, 50.0)
    elif hours_to_expiry >= 24:
        base = (20.0, 40.0, 60.0)
    else:
        base = (30.0, 55.0, 70.0)

    mult = alert_volume_threshold_multiplier(volume)
    return (
        round(base[0] * mult, 2),
        round(base[1] * mult, 2),
        round(base[2] * mult, 2),
    )


def classify_alert_severity(
    move_pp: Decimal | float,
    hours_to_expiry: Optional[float],
    volume: Optional[Decimal | float] = None,
) -> str:
    """
    Classify alert severity using time-to-expiry and volume-aware thresholds.
    """
    abs_move = abs(float(move_pp))
    notable, significant, extreme = get_alert_severity_thresholds(
        hours_to_expiry=hours_to_expiry,
        volume=volume,
    )
    if abs_move >= extreme:
        return "extreme"
    if abs_move >= significant:
        return "significant"
    if abs_move >= notable:
        return "notable"
    return "none"


def should_suppress_settlement_snap(
    move_pp: Decimal | float,
    hours_to_expiry: Optional[float],
    suppress_hours: float = 48.0,
    suppress_move_pp: float = 80.0,
) -> bool:
    """
    Suppress late-life settlement snaps that are usually non-actionable.
    """
    if hours_to_expiry is None:
        return False
    return hours_to_expiry < suppress_hours and abs(float(move_pp)) >= suppress_move_pp


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
        
        # Calculate composite score with price boundary awareness
        composite_score = calculate_composite_score(
            abs_move_pp=abs_move_pp,
            volume=volume,
            spike_ratio=spike_ratio,
            weight_move=self.weight_move,
            weight_volume=self.weight_volume,
            weight_spike=self.weight_spike,
            current_price=price_now,  # Pass for boundary penalty
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


# =============================================================================
# Z-SCORE BASED SCORING - Statistically normalized ranking
# =============================================================================

def calculate_z_score(value: float, mean: float, stddev: float) -> float:
    """
    Calculate Z-score (standard score).
    
    Z = (x - μ) / σ
    
    Measures how many standard deviations a value is from the mean.
    """
    if stddev <= 0:
        return 0.0
    return (value - mean) / stddev


def calculate_log_odds_change(price_before: float, price_after: float) -> float:
    """
    Calculate log-odds change (information-theoretic measure).
    
    Log-odds captures the "surprise" of a price move in probability space.
    A move from 90%→95% is more surprising than 50%→55%.
    
    Formula: |ln(p_after/(1-p_after)) - ln(p_before/(1-p_before))|
    """
    # Clamp prices to avoid log(0) or division by zero
    eps = 0.001
    p1 = max(eps, min(1 - eps, price_before))
    p2 = max(eps, min(1 - eps, price_after))
    
    log_odds_before = math.log(p1 / (1 - p1))
    log_odds_after = math.log(p2 / (1 - p2))
    
    return abs(log_odds_after - log_odds_before)


class ZScoreMoverScorer:
    """
    Z-Score based scorer for ranking market movers.
    
    Advantages over raw percentage point scoring:
    1. Normalizes for market-specific volatility
    2. A 5pp move in a stable market ranks higher than 10pp in a volatile one
    3. Combines price and volume Z-scores for robust anomaly detection
    4. Optional velocity weighting for time-sensitive detection
    
    Based on research from:
    - Abnormal returns analysis (CAPM)
    - Statistical anomaly detection
    - Factor investing methodologies
    """
    
    def __init__(
        self,
        weight_price_z: float = 1.0,
        weight_volume_z: float = 0.5,
        weight_velocity: float = 0.3,
        min_z_score: float = 1.5,  # 1.5 std devs = top ~7% of moves
        use_log_odds: bool = True,  # Use log-odds for price moves
    ):
        """
        Initialize Z-score based scorer.
        
        Args:
            weight_price_z: Weight for price Z-score component
            weight_volume_z: Weight for volume Z-score component
            weight_velocity: Weight for velocity bonus (0 to disable)
            min_z_score: Minimum combined Z-score to be significant
            use_log_odds: Use log-odds change instead of raw pp
        """
        self.weight_price_z = weight_price_z
        self.weight_volume_z = weight_volume_z
        self.weight_velocity = weight_velocity
        self.min_z_score = min_z_score
        self.use_log_odds = use_log_odds
    
    def score(
        self,
        price_now: Decimal,
        price_then: Decimal,
        volume: Decimal,
        market_stats: Optional[dict] = None,
        time_elapsed_minutes: Optional[float] = None,
    ) -> Tuple[Decimal, dict]:
        """
        Calculate Z-score based composite score.
        
        Args:
            price_now: Current price (0-1)
            price_then: Historical price (0-1)
            volume: Current 24h volume
            market_stats: Dict with market-specific stats:
                - avg_move_pp: Mean absolute move for this market
                - stddev_move_pp: Std dev of moves
                - avg_volume: Mean volume
                - stddev_volume: Std dev of volume
            time_elapsed_minutes: Time window for velocity calc
            
        Returns:
            Tuple of (composite_z_score, metrics_dict)
        """
        price_f = float(price_now)
        price_old_f = float(price_then)
        volume_f = float(volume)
        
        # Calculate raw move
        move_pp = (price_f - price_old_f) * 100
        abs_move_pp = abs(move_pp)
        
        # Calculate log-odds change if enabled
        log_odds_change = 0.0
        if self.use_log_odds and price_old_f > 0:
            log_odds_change = calculate_log_odds_change(price_old_f, price_f)
        
        # Default stats if not provided (fallback to simple scoring)
        if market_stats is None:
            market_stats = {
                "avg_move_pp": 2.0,  # Assume ~2pp avg move
                "stddev_move_pp": 3.0,  # ~3pp std dev
                "avg_volume": 10000.0,
                "stddev_volume": 20000.0,
            }
        
        # Calculate price Z-score
        # Use log-odds change if enabled (better for prediction markets)
        if self.use_log_odds and log_odds_change > 0:
            # For log-odds, use empirical mean ~0.2, stddev ~0.5
            price_z = calculate_z_score(
                log_odds_change,
                market_stats.get("avg_log_odds", 0.2),
                market_stats.get("stddev_log_odds", 0.5),
            )
        else:
            price_z = calculate_z_score(
                abs_move_pp,
                market_stats.get("avg_move_pp", 2.0),
                market_stats.get("stddev_move_pp", 3.0),
            )
        
        # Calculate volume Z-score
        volume_z = 0.0
        if volume_f > 0:
            volume_z = calculate_z_score(
                volume_f,
                market_stats.get("avg_volume", 10000.0),
                market_stats.get("stddev_volume", 20000.0),
            )
        
        # Calculate velocity bonus
        velocity_bonus = 0.0
        if time_elapsed_minutes and time_elapsed_minutes > 0 and self.weight_velocity > 0:
            # Velocity = pp/minute, normalized by sqrt(time) to not over-penalize longer windows
            velocity = abs_move_pp / math.sqrt(time_elapsed_minutes)
            # Typical velocity might be ~0.5 pp/sqrt(min), 2.0 is fast
            velocity_z = calculate_z_score(velocity, 0.5, 1.0)
            velocity_bonus = max(0, velocity_z) * self.weight_velocity
        
        # Combine Z-scores
        # Only positive Z-scores contribute (we want outliers, not underperformers)
        composite_z = (
            max(0, price_z) * self.weight_price_z +
            max(0, volume_z) * self.weight_volume_z +
            velocity_bonus
        )
        
        # Build metrics dict for transparency
        metrics = {
            "move_pp": Decimal(str(move_pp)),
            "abs_move_pp": Decimal(str(abs_move_pp)),
            "log_odds_change": log_odds_change,
            "price_z": price_z,
            "volume_z": volume_z,
            "velocity_bonus": velocity_bonus,
            "composite_z": composite_z,
        }
        
        return Decimal(str(composite_z)), metrics
    
    def is_significant(self, z_score: Decimal) -> bool:
        """Check if Z-score meets minimum threshold."""
        return float(z_score) >= self.min_z_score
    
    def rank_movers(
        self,
        movers: list[dict],
        market_stats_map: Optional[dict] = None,
        price_now_key: str = "latest_price",
        price_then_key: str = "old_price",
        volume_key: str = "latest_volume",
        window_minutes: Optional[float] = None,
    ) -> list[dict]:
        """
        Score and rank movers using Z-score methodology.
        
        Args:
            movers: List of mover dicts
            market_stats_map: Map of token_id -> market stats dict
            price_now_key: Key for current price
            price_then_key: Key for historical price
            volume_key: Key for volume
            window_minutes: Time window for velocity calc
            
        Returns:
            Sorted list with Z-score metrics
        """
        prepared_rows: list[dict] = []
        feature_rows: list[dict] = []

        for mover in movers:
            token_id = str(mover.get("token_id", ""))

            try:
                price_now = Decimal(str(mover.get(price_now_key, 0)))
                price_then = Decimal(str(mover.get(price_then_key, 0)))
                volume = Decimal(str(mover.get(volume_key, 0) or 0))

                market_stats = None
                if market_stats_map and token_id in market_stats_map:
                    market_stats = market_stats_map[token_id]

                stats_for_features = market_stats or {
                    "avg_move_pp": 2.0,
                    "stddev_move_pp": 3.0,
                    "avg_log_odds": 0.2,
                    "stddev_log_odds": 0.5,
                    "avg_volume": 10000.0,
                    "stddev_volume": 20000.0,
                }
                feature_rows.append(
                    {
                        "price_now": float(price_now),
                        "price_then": float(price_then),
                        "volume_24h": float(volume),
                        "avg_move_pp": float(stats_for_features.get("avg_move_pp", 2.0)),
                        "stddev_move_pp": float(stats_for_features.get("stddev_move_pp", 3.0)),
                        "avg_log_odds": float(stats_for_features.get("avg_log_odds", 0.2)),
                        "stddev_log_odds": float(stats_for_features.get("stddev_log_odds", 0.5)),
                        "avg_volume": float(stats_for_features.get("avg_volume", 10000.0)),
                        "stddev_volume": float(stats_for_features.get("stddev_volume", 20000.0)),
                        "window_minutes": float(window_minutes or 0.0),
                    }
                )

                prepared_rows.append(
                    {
                        "mover": mover,
                        "price_now": price_now,
                        "price_then": price_then,
                        "volume": volume,
                        "market_stats": market_stats,
                    }
                )
            except (ValueError, TypeError):
                continue

        if settings.model_feature_manifest_strict:
            validate_live_feature_rows(
                feature_rows,
                manifest_path=settings.model_feature_manifest_path,
            )

        scored = []

        for prepared in prepared_rows:
            mover = prepared["mover"]
            z_score, metrics = self.score(
                prepared["price_now"],
                prepared["price_then"],
                prepared["volume"],
                market_stats=prepared["market_stats"],
                time_elapsed_minutes=window_minutes,
            )

            if not self.is_significant(z_score):
                continue

            # Add all metrics to mover
            mover["z_score"] = z_score
            mover["quality_score"] = z_score  # Alias for compatibility
            mover.update(metrics)
            scored.append(mover)
        
        # Sort by Z-score descending
        scored.sort(key=lambda x: float(x["z_score"]), reverse=True)
        
        # Assign ranks
        for rank, mover in enumerate(scored, 1):
            mover["rank"] = rank
        
        return scored


# Z-score scorer instance (recommended for production)
zscore_mover_scorer = ZScoreMoverScorer()
