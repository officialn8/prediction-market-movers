import importlib
import uuid

from apps.dashboard.components import normalize_market_url


def test_normalize_market_url_polymarket_two_segment():
    normalized = normalize_market_url(
        "https://polymarket.com/event/nba-mvp-694/will-lebron-james-win-the-20252026-nba-mvp?ref=abc",
        source="polymarket",
    )
    assert (
        normalized
        == "https://polymarket.com/event/nba-mvp-694/will-lebron-james-win-the-20252026-nba-mvp"
    )


def test_normalize_market_url_polymarket_one_segment():
    normalized = normalize_market_url(
        "https://polymarket.com/event/negative-gdp-growth-in-2025",
        source="polymarket",
    )
    assert normalized == "https://polymarket.com/event/negative-gdp-growth-in-2025"


def test_normalize_market_url_rejects_invalid_host():
    assert normalize_market_url("https://example.com/event/foo", source="polymarket") is None


def test_normalize_market_url_kalshi():
    normalized = normalize_market_url(
        "http://www.kalshi.com/markets/kxbtc?foo=bar",
        source="kalshi",
    )
    assert normalized == "https://kalshi.com/markets/kxbtc"


def test_build_market_header_html_is_dedented_and_escaped():
    detail_page = importlib.import_module("apps.dashboard.pages.2_Market_Detail")
    html = detail_page.build_market_header_html(
        source_badge='<span class="source-badge source-polymarket">POLYMARKET</span>',
        category='Sports<script>alert(1)</script>',
        title='Will Team A win?</h2><script>alert(1)</script>',
        status="active",
        external_link_html='<a href="https://polymarket.com/event/foo">View</a>',
    )

    assert html.startswith('<div class="market-header">')
    assert "<script>" not in html
    assert "</script>" not in html
    assert "\n    <div class=\"market-header\">" not in html
    assert '<a href="https://polymarket.com/event/foo">View</a>' in html


def test_top_movers_hydration_overwrites_stale_url(monkeypatch):
    movers_page = importlib.import_module("apps.dashboard.pages.1_Top_Movers")
    market_id = str(uuid.uuid4())

    class FakeDB:
        def execute(self, _query, _params=None, fetch=False):
            assert fetch is True
            return [
                {
                    "market_id": market_id,
                    "title": "Example Market",
                    "source": "polymarket",
                    "source_id": "0xabc",
                    "category": "Sports",
                    "url": "https://polymarket.com/event/canonical-link",
                }
            ]

    monkeypatch.setattr(movers_page, "get_db_pool", lambda: FakeDB())

    hydrated = movers_page._hydrate_market_context(
        [
            {
                "market_id": market_id,
                "title": "Example Market",
                "source": "polymarket",
                "category": "Sports",
                "url": "https://polymarket.com/event/stale-link",
            }
        ]
    )

    assert hydrated[0]["url"] == "https://polymarket.com/event/canonical-link"


def test_top_movers_stale_volume_fallback(monkeypatch):
    movers_page = importlib.import_module("apps.dashboard.pages.1_Top_Movers")
    token_id = str(uuid.uuid4())

    class FakeDB:
        def execute(self, _query, _params=None, fetch=False):
            assert fetch is True
            return [
                {
                    "token_id": token_id,
                    "volume_24h": 12345.0,
                    "volume_source": "gamma",
                    "volume_age_seconds": 7800,
                    "is_volume_fresh": False,
                }
            ]

    monkeypatch.setattr(movers_page, "get_db_pool", lambda: FakeDB())

    movers = movers_page._apply_stale_volume_fallback(
        [
            {
                "token_id": token_id,
                "latest_volume": 0,
            }
        ],
        enabled=True,
    )

    assert movers[0]["display_volume"] == 12345.0
    assert movers[0]["display_volume_source"] == "gamma"
    assert movers[0]["display_volume_is_stale"] is True
