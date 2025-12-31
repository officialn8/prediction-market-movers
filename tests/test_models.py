from datetime import datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from packages.core.models import (
    Market,
    MarketSource,
    MarketStatus,
    Snapshot,
    TokenOutcome,
    Alert,
)



def test_market_model_creation():
    """Test creating a valid Market model."""
    market = Market(
        market_id=uuid4(),
        source=MarketSource.POLYMARKET,
        source_id="0x123",
        title="Test Market",
        category="Tech",
        status=MarketStatus.ACTIVE,
        url="https://polymarket.com/market/123",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    assert market.title == "Test Market"
    assert market.source == "polymarket"
    assert market.status == "active"


def test_market_validation_missing_fields():
    """Test market validation fails with missing fields."""
    with pytest.raises(ValidationError):
        Market(
            market_id=uuid4(),
            # source missing
            source_id="0x123",
            title="Test Market",
            status=MarketStatus.ACTIVE,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )


def test_snapshot_price_validation():
    """Test that snapshot price must be between 0 and 1."""
    token_id = uuid4()
    
    # Valid price
    snap = Snapshot(
        token_id=token_id,
        price=Decimal("0.55"),
        ts=datetime.utcnow(),
    )
    assert snap.price == Decimal("0.55")

    # Invalid price > 1
    with pytest.raises(ValidationError):
        Snapshot(
            token_id=token_id,
            price=Decimal("1.1"),
            ts=datetime.utcnow(),
        )

    # Invalid price < 0
    with pytest.raises(ValidationError):
        Snapshot(
            token_id=token_id,
            price=Decimal("-0.1"),
            ts=datetime.utcnow(),
        )


def test_alert_model():
    """Test creating an Alert model."""
    alert = Alert(
        alert_id=uuid4(),
        created_at=datetime.utcnow(),
        token_id=uuid4(),
        window_seconds=3600,
        move_pp=Decimal("15.5"),
        threshold_pp=Decimal("10.0"),
        reason="Big move",
    )
    assert alert.move_pp == Decimal("15.5")
    assert alert.reason == "Big move"
