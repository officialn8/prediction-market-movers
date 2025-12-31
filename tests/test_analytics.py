from decimal import Decimal
from packages.core.analytics import metrics

def test_calculate_move_pp():
    """Test percentage point calculation."""
    # 0.60 - 0.50 = 0.10 -> 10.0 pp
    assert metrics.calculate_move_pp(Decimal("0.60"), Decimal("0.50")) == Decimal("10.00")
    
    # 0.40 - 0.50 = -0.10 -> -10.0 pp
    assert metrics.calculate_move_pp(Decimal("0.40"), Decimal("0.50")) == Decimal("-10.00")


def test_calculate_quality_score():
    """Test quality score calculation."""
    # Score = abs_move * ln(1 + volume)
    
    # Zero volume -> Zero score
    assert metrics.calculate_quality_score(Decimal("10.0"), Decimal("0")) == Decimal("0")
    
    # Positive volume
    # ln(1 + 100) approx 4.615
    # score approx 10 * 4.615 = 46.15
    score = metrics.calculate_quality_score(Decimal("10.0"), Decimal("100"))
    assert score > Decimal("46.0")
    assert score < Decimal("46.2")
