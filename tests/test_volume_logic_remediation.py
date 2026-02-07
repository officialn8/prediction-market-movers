from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from apps.collector.adapters.kalshi_wss import KalshiTrade
from apps.collector.adapters.wss_messages import NewMarket, PriceUpdate, TradeEvent
from apps.collector.jobs import kalshi_wss_sync, polymarket_wss_sync
from packages.core.settings import settings
from packages.core.storage import queries
from packages.core.storage.queries import AnalyticsQueries, MarketQueries, VolumeQueries


class QueryCaptureDB:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.calls = []

    def execute(self, query, params=None, fetch=False):
        self.calls.append((query, params, fetch))
        if fetch:
            return self.rows
        return None


@pytest.mark.asyncio
async def test_polymarket_flush_writes_null_snapshot_volume(monkeypatch):
    captured = {}

    def _capture_insert(snapshots):
        captured["snapshots"] = snapshots
        return len(snapshots)

    monkeypatch.setattr(
        polymarket_wss_sync.MarketQueries,
        "insert_snapshots_batch",
        staticmethod(_capture_insert),
    )

    inserted, skipped = await polymarket_wss_sync.flush_price_batch(
        updates=[
            PriceUpdate(
                token_id="pm-token",
                price=0.55,
                timestamp=datetime.now(timezone.utc),
            )
        ],
        source_to_db_token={"pm-token": "00000000-0000-0000-0000-000000000111"},
        volume_accumulator={"pm-token": 125.0},
        spread_updates=[],
        last_written_price={},
        last_written_ts={},
        last_written_spread={},
    )

    assert inserted == 1
    assert skipped == 0
    assert captured["snapshots"][0]["volume_24h"] is None


@pytest.mark.asyncio
async def test_kalshi_trade_accumulates_and_flushes_null_snapshot_volume(monkeypatch):
    fake_db = QueryCaptureDB()
    monkeypatch.setattr(kalshi_wss_sync, "get_db_pool", lambda: fake_db)

    captured = {}

    def _capture_insert(snapshots):
        captured["snapshots"] = snapshots
        return len(snapshots)

    monkeypatch.setattr(
        kalshi_wss_sync.MarketQueries,
        "insert_snapshots_batch",
        staticmethod(_capture_insert),
    )

    handler = kalshi_wss_sync.KalshiWSSSync()
    handler.ticker_to_token_id["KX-TEST"] = "00000000-0000-0000-0000-000000000222"

    trade = KalshiTrade(
        ticker="KX-TEST",
        trade_id="trade-1",
        price=60,
        count=10,
        taker_side="yes",
        timestamp=datetime.now(timezone.utc),
    )
    await handler._handle_trade(trade)

    assert any("accumulate_trade_volume" in q for q, _, _ in fake_db.calls)

    inserted = await handler.flush_snapshots()
    assert inserted == 1
    assert captured["snapshots"][0]["volume_24h"] is None


@pytest.mark.asyncio
async def test_kalshi_trade_passes_volume_to_instant_mover(monkeypatch):
    fake_db = QueryCaptureDB()
    monkeypatch.setattr(kalshi_wss_sync, "get_db_pool", lambda: fake_db)

    seen: dict[str, float] = {}

    async def _fake_check(token_id, old_price, new_price, volume=None, **_kwargs):
        seen["token_id"] = token_id
        seen["volume"] = volume
        return None

    monkeypatch.setattr(kalshi_wss_sync, "check_instant_mover", _fake_check)

    handler = kalshi_wss_sync.KalshiWSSSync()
    handler.ticker_to_token_id["KX-TEST"] = "00000000-0000-0000-0000-000000000222"
    handler.price_cache["KX-TEST"] = 0.50

    trade = KalshiTrade(
        ticker="KX-TEST",
        trade_id="trade-2",
        price=60,
        count=10,
        taker_side="yes",
        timestamp=datetime.now(timezone.utc),
    )
    await handler._handle_trade(trade)

    assert seen["token_id"] == "00000000-0000-0000-0000-000000000222"
    assert seen["volume"] == trade.notional_value


@pytest.mark.asyncio
async def test_polymarket_trade_passes_volume_to_instant_mover(monkeypatch):
    fake_db = QueryCaptureDB()
    monkeypatch.setattr(polymarket_wss_sync, "get_db_pool", lambda: fake_db)
    monkeypatch.setattr(polymarket_wss_sync, "_sync_polymarket_markets_once", lambda: None)

    shutdown = polymarket_wss_sync.Shutdown()
    seen: dict[str, float] = {}

    async def _fake_check(token_id, old_price, new_price, volume=None, **_kwargs):
        seen["token_id"] = token_id
        seen["volume"] = volume
        shutdown.is_set = True
        return None

    monkeypatch.setattr(polymarket_wss_sync, "check_instant_mover", _fake_check)

    class _Metrics:
        def __init__(self):
            self.current_subscriptions = 1
            self.last_message_time = 0.0

        def save(self):
            return None

    class _FakeClient:
        def __init__(self, enable_custom_features=True):
            self._metrics = _Metrics()
            self.subscription_target = 1

        @property
        def is_subscription_in_progress(self):
            return False

        def pop_subscription_error(self):
            return None

        async def connect(self, _asset_ids):
            return None

        async def close(self):
            return None

        def listen(self):
            async def _gen():
                yield TradeEvent(
                    token_id="pm-token",
                    price=0.55,
                    size=100.0,
                    side="BUY",
                    fee_rate_bps=None,
                    timestamp=datetime.now(timezone.utc),
                )

            return _gen()

    monkeypatch.setattr(polymarket_wss_sync, "PolymarketWebSocket", _FakeClient)
    monkeypatch.setattr(
        polymarket_wss_sync,
        "_load_active_asset_state",
        lambda _db: (
            {"pm-token": "00000000-0000-0000-0000-000000000111"},
            {"pm-token": 0.50},
            {},
            {},
            {},
        ),
    )

    await polymarket_wss_sync.run_wss_loop(shutdown)

    assert seen["token_id"] == "00000000-0000-0000-0000-000000000111"
    assert seen["volume"] == pytest.approx(55.0)


def test_alert_insert_persists_alert_type_and_spike_ratio(monkeypatch):
    fake_db = QueryCaptureDB(
        rows=[
            {
                "alert_type": "volume_spike",
                "volume_spike_ratio": Decimal("3.2000"),
            }
        ]
    )
    monkeypatch.setattr(queries, "get_db_pool", lambda: fake_db)

    result = AnalyticsQueries.insert_alert(
        token_id="00000000-0000-0000-0000-000000000333",
        window_seconds=3600,
        move_pp=Decimal("0"),
        threshold_pp=Decimal("0"),
        reason="volume",
        alert_type="volume_spike",
        volume_spike_ratio=Decimal("3.2"),
    )

    query, params, _ = fake_db.calls[-1]
    assert "alert_type" in query
    assert "volume_spike_ratio" in query
    assert params[-2] == "volume_spike"
    assert params[-1] == Decimal("3.2")
    assert result["alert_type"] == "volume_spike"


def test_get_recent_alert_filters_by_alert_type(monkeypatch):
    fake_db = QueryCaptureDB(rows=[])
    monkeypatch.setattr(queries, "get_db_pool", lambda: fake_db)

    AnalyticsQueries.get_recent_alert_for_token(
        token_id="00000000-0000-0000-0000-000000000444",
        window_seconds=3600,
        lookback_minutes=30,
        alert_type="volume_spike",
    )

    query, params, _ = fake_db.calls[-1]
    assert "AND alert_type = %s" in query
    assert params[-1] == "volume_spike"


def test_volume_queries_apply_freshness_thresholds(monkeypatch):
    fake_db = QueryCaptureDB(rows=[])
    monkeypatch.setattr(queries, "get_db_pool", lambda: fake_db)

    VolumeQueries.get_top_volumes(limit=5)
    _, params_top, _ = fake_db.calls[-1]
    assert params_top[0] == settings.volume_wss_stale_after_seconds
    assert params_top[1] == settings.volume_provider_stale_after_seconds

    VolumeQueries.get_volume_spike_candidates(limit=5)
    _, params_spike, _ = fake_db.calls[-1]
    assert params_spike[0] == settings.volume_wss_stale_after_seconds
    assert params_spike[1] == settings.volume_provider_stale_after_seconds

    MarketQueries.get_movers_window(window_seconds=3600, limit=5)
    _, params_movers, _ = fake_db.calls[-1]
    assert params_movers[0] == settings.volume_wss_stale_after_seconds
    assert params_movers[1] == settings.volume_provider_stale_after_seconds


def test_polymarket_wss_sync_runs_full_metadata_refresh_when_due(monkeypatch):
    calls = {"full": 0, "light": 0}
    state = SimpleNamespace(last_full_metadata_sync=None)

    class DummyAdapter:
        def close(self):
            return None

    def _full_sync(_adapter, max_markets):
        calls["full"] += 1
        assert max_markets == settings.polymarket_full_metadata_max_markets
        return (0, 0)

    monkeypatch.setattr(
        "apps.collector.adapters.polymarket.get_polymarket_adapter",
        lambda: DummyAdapter(),
    )
    monkeypatch.setattr(polymarket_wss_sync, "sync_markets_and_prices", _full_sync)
    monkeypatch.setattr(
        polymarket_wss_sync,
        "sync_markets",
        lambda _adapter: calls.__setitem__("light", calls["light"] + 1),
    )
    monkeypatch.setattr(polymarket_wss_sync, "should_sync_full_metadata", lambda: True)
    monkeypatch.setattr(polymarket_wss_sync, "get_sync_state", lambda: state)
    monkeypatch.setattr(
        polymarket_wss_sync.settings,
        "polymarket_full_metadata_sync_enabled",
        True,
    )
    monkeypatch.setattr(polymarket_wss_sync.time, "time", lambda: 1234.0)

    polymarket_wss_sync._sync_polymarket_markets_once()

    assert calls["full"] == 1
    assert calls["light"] == 0
    assert state.last_full_metadata_sync == 1234.0


def test_polymarket_wss_sync_uses_light_sync_when_full_not_due(monkeypatch):
    calls = {"full": 0, "light": 0}

    class DummyAdapter:
        def close(self):
            return None

    monkeypatch.setattr(
        "apps.collector.adapters.polymarket.get_polymarket_adapter",
        lambda: DummyAdapter(),
    )
    monkeypatch.setattr(
        polymarket_wss_sync,
        "sync_markets_and_prices",
        lambda _adapter, max_markets: calls.__setitem__("full", calls["full"] + 1),
    )
    monkeypatch.setattr(
        polymarket_wss_sync,
        "sync_markets",
        lambda _adapter: calls.__setitem__("light", calls["light"] + 1),
    )
    monkeypatch.setattr(polymarket_wss_sync, "should_sync_full_metadata", lambda: False)

    polymarket_wss_sync._sync_polymarket_markets_once()

    assert calls["full"] == 0
    assert calls["light"] == 1


def test_polymarket_refresh_interval_scales_for_large_subscriptions(monkeypatch):
    monkeypatch.setattr(
        polymarket_wss_sync.settings,
        "polymarket_subscription_refresh_seconds",
        300,
    )

    # 32,050 assets -> 1,603 chunks -> ~320.6s subscription + 120s buffer.
    effective = polymarket_wss_sync._effective_subscription_refresh_seconds(32050)
    assert effective >= 440
    assert effective > 300


@pytest.mark.asyncio
async def test_polymarket_new_market_event_does_not_force_immediate_reconnect(monkeypatch):
    fake_db = QueryCaptureDB()
    monkeypatch.setattr(polymarket_wss_sync, "get_db_pool", lambda: fake_db)

    sync_calls = {"count": 0}

    def _fake_sync():
        sync_calls["count"] += 1

    monkeypatch.setattr(polymarket_wss_sync, "_sync_polymarket_markets_once", _fake_sync)
    monkeypatch.setattr(
        polymarket_wss_sync,
        "_load_active_asset_state",
        lambda _db: (
            {"pm-token": "00000000-0000-0000-0000-000000000111"},
            {"pm-token": 0.50},
            {},
            {},
            {},
        ),
    )

    shutdown = polymarket_wss_sync.Shutdown()

    async def _fake_check(_token_id, _old_price, _new_price, volume=None, **_kwargs):
        shutdown.is_set = True
        return None

    monkeypatch.setattr(polymarket_wss_sync, "check_instant_mover", _fake_check)

    connect_calls = {"count": 0}

    class _Metrics:
        def __init__(self):
            self.current_subscriptions = 1
            self.last_message_time = 0.0

        def save(self):
            return None

    class _FakeClient:
        def __init__(self, enable_custom_features=True):
            self._metrics = _Metrics()
            self.subscription_target = 1

        @property
        def is_subscription_in_progress(self):
            return False

        def pop_subscription_error(self):
            return None

        async def connect(self, _asset_ids):
            connect_calls["count"] += 1

        async def close(self):
            return None

        def listen(self):
            async def _gen():
                yield NewMarket(
                    market_id="pm-market-1",
                    condition_id="pm-cond-1",
                    tokens=[{"token_id": "pm-token", "outcome": "YES"}],
                    timestamp=datetime.now(timezone.utc),
                )
                yield TradeEvent(
                    token_id="pm-token",
                    price=0.55,
                    size=100.0,
                    side="BUY",
                    fee_rate_bps=None,
                    timestamp=datetime.now(timezone.utc),
                )

            return _gen()

    monkeypatch.setattr(polymarket_wss_sync, "PolymarketWebSocket", _FakeClient)
    monkeypatch.setattr(
        polymarket_wss_sync.settings,
        "polymarket_subscription_refresh_seconds",
        600,
    )

    await polymarket_wss_sync.run_wss_loop(shutdown)

    # 1 initial sync + 1 sync triggered by NewMarket event.
    assert sync_calls["count"] == 2
    # No immediate reconnect loop: only one connect call occurred.
    assert connect_calls["count"] == 1
