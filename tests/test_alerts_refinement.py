from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from apps.collector.jobs import alerts as alerts_job
from packages.core.storage import queries
from packages.core.storage.queries import AnalyticsQueries


class QueryCaptureDB:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.calls = []

    def execute(self, query, params=None, fetch=False, **kwargs):
        self.calls.append((query, params, fetch, kwargs))
        if fetch:
            return self.rows
        return None


def test_get_cached_movers_filters_inactive_and_expired(monkeypatch):
    fake_db = QueryCaptureDB(rows=[])
    monkeypatch.setattr(queries, "get_db_pool", lambda: fake_db)

    AnalyticsQueries.get_cached_movers(window_seconds=3600, limit=5)

    query, _, _, _ = fake_db.calls[-1]
    assert "m.status = 'active'" in query
    assert "(m.end_date IS NULL OR m.end_date > NOW())" in query


def test_get_recent_alerts_dedupes_market_events_and_excludes_expired(monkeypatch):
    fake_db = QueryCaptureDB(rows=[])
    monkeypatch.setattr(queries, "get_db_pool", lambda: fake_db)

    AnalyticsQueries.get_recent_alerts(limit=10, dedupe_market_events=True, exclude_expired=True)

    query, _, _, _ = fake_db.calls[-1]
    assert "market_event_rank" in query
    assert "m.end_date IS NULL OR m.end_date > a.created_at" in query


def test_get_recent_alert_for_market_supports_alert_type_filter(monkeypatch):
    fake_db = QueryCaptureDB(rows=[])
    monkeypatch.setattr(queries, "get_db_pool", lambda: fake_db)

    AnalyticsQueries.get_recent_alert_for_market(
        market_id="00000000-0000-0000-0000-000000000100",
        window_seconds=3600,
        lookback_minutes=30,
        alert_type="price_move",
    )

    query, params, _, _ = fake_db.calls[-1]
    assert "JOIN market_tokens mt ON a.token_id = mt.token_id" in query
    assert "mt.market_id = %s" in query
    assert "AND a.alert_type = %s" in query
    assert params[-1] == "price_move"


def test_select_market_level_candidates_prefers_yes_for_binary_ties():
    movers = [
        {"market_id": "m1", "token_id": "t1", "pct_change": "30", "outcome": "No"},
        {"market_id": "m1", "token_id": "t2", "pct_change": "-30", "outcome": "Yes"},
        {"market_id": "m2", "token_id": "t3", "pct_change": "15", "outcome": "Yes"},
    ]

    selected = alerts_job._select_market_level_candidates(movers)
    by_market = {row["market_id"]: row for row in selected}

    assert len(selected) == 2
    assert by_market["m1"]["outcome"] == "Yes"
    assert by_market["m2"]["token_id"] == "t3"


@pytest.mark.asyncio
async def test_run_alerts_check_dedupes_yes_no_and_skips_expired(monkeypatch):
    now = datetime.now(timezone.utc)
    active_end = now + timedelta(days=7)
    expired_end = now - timedelta(minutes=5)

    movers = [
        {
            "token_id": "00000000-0000-0000-0000-000000000201",
            "market_id": "00000000-0000-0000-0000-000000000301",
            "pct_change": "32.0",
            "title": "Binary Market",
            "outcome": "Yes",
            "latest_volume": "4500",
            "end_date": active_end,
            "status": "active",
        },
        {
            "token_id": "00000000-0000-0000-0000-000000000202",
            "market_id": "00000000-0000-0000-0000-000000000301",
            "pct_change": "-32.0",
            "title": "Binary Market",
            "outcome": "No",
            "latest_volume": "4500",
            "end_date": active_end,
            "status": "active",
        },
        {
            "token_id": "00000000-0000-0000-0000-000000000203",
            "market_id": "00000000-0000-0000-0000-000000000302",
            "pct_change": "75.0",
            "title": "Expired Market",
            "outcome": "Yes",
            "latest_volume": "8000",
            "end_date": expired_end,
            "status": "active",
        },
    ]

    inserted = []

    monkeypatch.setattr(
        alerts_job.AnalyticsQueries,
        "get_cached_movers",
        staticmethod(lambda **_kwargs: movers),
    )
    monkeypatch.setattr(
        alerts_job.MarketQueries,
        "get_top_movers",
        staticmethod(lambda **_kwargs: []),
    )
    monkeypatch.setattr(
        alerts_job.VolumeQueries,
        "get_volume_spike_candidates",
        staticmethod(lambda **_kwargs: []),
    )
    monkeypatch.setattr(
        alerts_job.AnalyticsQueries,
        "get_recent_alert_for_market",
        staticmethod(lambda **_kwargs: None),
    )
    monkeypatch.setattr(
        alerts_job.AnalyticsQueries,
        "insert_alert",
        staticmethod(lambda **kwargs: inserted.append(kwargs) or kwargs),
    )
    monkeypatch.setattr(alerts_job.settings, "signal_hold_zone_enabled", False)

    await alerts_job.run_alerts_check()

    assert len(inserted) == 1
    assert inserted[0]["token_id"] == UUID("00000000-0000-0000-0000-000000000201")
    assert inserted[0]["threshold_pp"] == Decimal("10.0")
