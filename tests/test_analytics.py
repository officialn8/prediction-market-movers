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


def test_should_suppress_settlement_snap():
    assert metrics.should_suppress_settlement_snap(move_pp=80, hours_to_expiry=47.9)
    assert metrics.should_suppress_settlement_snap(move_pp=-95, hours_to_expiry=1.0)
    assert not metrics.should_suppress_settlement_snap(move_pp=79.9, hours_to_expiry=47.9)
    assert not metrics.should_suppress_settlement_snap(move_pp=90, hours_to_expiry=48.0)
    assert not metrics.should_suppress_settlement_snap(move_pp=90, hours_to_expiry=None)


def test_alert_severity_thresholds_are_time_and_volume_weighted():
    # Mid-life, high volume: lower thresholds.
    notable, significant, extreme = metrics.get_alert_severity_thresholds(
        hours_to_expiry=400,
        volume=60_000,
    )
    assert (notable, significant, extreme) == (8.5, 17.0, 34.0)

    # Near expiry, low volume: stricter thresholds.
    notable, significant, extreme = metrics.get_alert_severity_thresholds(
        hours_to_expiry=26,
        volume=3_000,
    )
    assert (notable, significant, extreme) == (24.0, 48.0, 72.0)


def test_alert_severity_classification_changes_with_context():
    # Same move can rank differently depending on time-to-expiry.
    assert (
        metrics.classify_alert_severity(
            move_pp=25,
            hours_to_expiry=400,
            volume=60_000,
        )
        == "significant"
    )
    assert (
        metrics.classify_alert_severity(
            move_pp=25,
            hours_to_expiry=26,
            volume=60_000,
        )
        == "notable"
    )
