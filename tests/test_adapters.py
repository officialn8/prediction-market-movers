from unittest.mock import MagicMock, patch

import pytest

from apps.collector.adapters.polymarket import PolymarketAdapter, PolymarketMarket


@pytest.fixture
def adapter():
    return PolymarketAdapter()


def test_parse_market_valid(adapter):
    """Test parsing a valid market response from Gamma API."""
    raw_data = {
        "condition_id": "0x123",
        "question": "Will AI take over?",
        "slug": "ai-takeover",
        "active": True,
        "closed": False,
        "end_date_iso": "2024-12-31T00:00:00Z",
        "tokens": [
            {"token_id": "T1", "outcome": "Yes", "price": 0.6},
            {"token_id": "T2", "outcome": "No", "price": 0.4},
        ],
        "volume24hr": 1000.50,
        "liquidity": 500.0,
        "clobTokenIds": ["T1", "T2"],
        "outcomes": ["Yes", "No"],
        "outcomePrices": ["0.6", "0.4"]
    }

    market = adapter._parse_market(raw_data)
    
    assert isinstance(market, PolymarketMarket)
    assert market.condition_id == "0x123"
    assert market.title == "Will AI take over?"
    assert market.active is True
    assert len(market.tokens) == 2
    assert market.tokens[0]["outcome"] == "YES"
    assert market.tokens[0]["price"] == 0.6
    assert market.volume_24h == 1000.50


def test_parse_market_invalid_missing_id(adapter):
    """Test parsing fails gracefully when condition_id is missing."""
    raw_data = {
        "question": "Invalid Market",
    }
    market = adapter._parse_market(raw_data)
    assert market is None


@patch("apps.collector.adapters.polymarket.requests.Session")
def test_fetch_markets_success(mock_session_cls, adapter):
    """Test fetching markets calls the correct endpoint."""
    mock_session = mock_session_cls.return_value
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {
            "condition_id": "0x123",
            "question": "Test Market",
            "clobTokenIds": ["T1", "T2"],
            "outcomes": ["Yes", "No"],
            "outcomePrices": ["0.5", "0.5"]
        }
    ]
    adapter.session = mock_session
    mock_session.get.return_value = mock_response

    markets = adapter.fetch_markets(limit=10)
    
    assert len(markets) == 1
    assert markets[0].condition_id == "0x123"
    mock_session.get.assert_called_with(
        "https://gamma-api.polymarket.com/markets",
        params={"limit": 10, "offset": 0, "active": "true", "closed": "false"},
        timeout=30
    )


@patch("apps.collector.adapters.polymarket.requests.Session")
def test_fetch_prices_batch(mock_session_cls, adapter):
    """Test batch price fetching."""
    mock_session = mock_session_cls.return_value
    mock_response = MagicMock()
    # Mock response from CLOB /prices endpoint
    mock_response.json.return_value = [
        {"token_id": "T1", "price": "0.75"},
        {"token_id": "T2", "price": "0.25"}
    ]
    adapter.session = mock_session
    mock_session.post.return_value = mock_response

    prices = adapter.fetch_prices_batch(["T1", "T2"])
    
    assert len(prices) == 2
    assert prices["T1"].price == 0.75
    assert prices["T2"].price == 0.25
    mock_session.post.assert_called()
