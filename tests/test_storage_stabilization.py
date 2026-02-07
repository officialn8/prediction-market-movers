from datetime import datetime, timedelta, timezone

import pytest

from apps.collector.jobs import retention
from apps.collector.jobs.snapshot_gate import (
    should_write_kalshi_snapshot,
    should_write_polymarket_snapshot,
)


class FakeDB:
    def __init__(self):
        self.calls = []

    def execute(self, query, params=None, fetch=False):
        self.calls.append((query, params, fetch))
        q = " ".join(query.split())

        if "SELECT tablename FROM pg_tables" in q:
            return [
                {"tablename": "snapshots"},
                {"tablename": "ohlc_1m"},
                {"tablename": "alerts"},
            ]

        if "WITH deleted AS" in q and "DELETE FROM snapshots" in q:
            # batch loop: two batches then stop
            snapshot_delete_calls = sum(
                1
                for call_query, _, _ in self.calls
                if "WITH deleted AS" in " ".join(call_query.split())
                and "DELETE FROM snapshots" in " ".join(call_query.split())
            )
            if snapshot_delete_calls == 1:
                return [{"cnt": 2}]
            if snapshot_delete_calls == 2:
                return [{"cnt": 1}]
            return [{"cnt": 0}]

        if "WITH deleted AS" in q and "DELETE FROM ohlc_1m" in q:
            return [{"cnt": 3}]

        if "WITH deleted AS" in q and "DELETE FROM alerts" in q:
            return [{"cnt": 0}]

        if "pg_total_relation_size" in q:
            return [
                {"table_name": "snapshots", "bytes": 1000, "size_pretty": "1000 bytes"},
                {"table_name": "ohlc_1m", "bytes": 500, "size_pretty": "500 bytes"},
            ]

        if "pg_database_size(current_database())" in q:
            return [{"db_size_bytes": 5000, "db_size_pretty": "5000 bytes"}]

        if "INSERT INTO system_status" in q:
            return None

        raise AssertionError(f"Unexpected query: {query}")


@pytest.mark.asyncio
async def test_retention_cleanup_applies_table_policies(monkeypatch):
    fake_db = FakeDB()
    monkeypatch.setattr(retention, "get_db_pool", lambda: fake_db)
    monkeypatch.setattr(
        retention,
        "RETENTION_POLICIES",
        {
            "snapshots": ("ts", 3),
            "ohlc_1m": ("bucket_ts", 14),
            "alerts": ("created_at", 30),
        },
    )

    telemetry = await retention.run_retention_cleanup()

    assert telemetry["deleted_by_table"]["snapshots"] == 3
    assert telemetry["deleted_by_table"]["ohlc_1m"] == 3
    assert telemetry["deleted_by_table"]["alerts"] == 0
    assert telemetry["db_size_bytes"] == 5000
    assert "deleted_per_minute" in telemetry


def test_kalshi_snapshot_gate_first_write():
    now_ts = datetime.now(tz=timezone.utc).timestamp()
    assert should_write_kalshi_snapshot(
        last_price=None,
        last_written_ts=None,
        new_price=0.55,
        batch_volume=None,
        now_ts=now_ts,
        min_interval_seconds=5,
        force_delta_pp=0.5,
    )


def test_kalshi_snapshot_gate_skip_unchanged_without_volume():
    now_ts = datetime.now(tz=timezone.utc).timestamp()
    assert not should_write_kalshi_snapshot(
        last_price=0.55,
        last_written_ts=now_ts - 30,
        new_price=0.55,
        batch_volume=0.0,
        now_ts=now_ts,
        min_interval_seconds=5,
        force_delta_pp=0.5,
    )


def test_kalshi_snapshot_gate_force_write_on_move():
    now_ts = datetime.now(tz=timezone.utc).timestamp()
    assert should_write_kalshi_snapshot(
        last_price=0.50,
        last_written_ts=now_ts - 1,
        new_price=0.52,  # 2pp move
        batch_volume=0.0,
        now_ts=now_ts,
        min_interval_seconds=5,
        force_delta_pp=0.5,
    )


def test_polymarket_snapshot_gate_skip_unchanged_without_volume_or_spread_change():
    now_ts = datetime.now(tz=timezone.utc).timestamp()
    assert not should_write_polymarket_snapshot(
        last_price=0.41,
        last_written_ts=now_ts - 120,
        new_price=0.41,
        batch_volume=0.0,
        spread=None,
        last_spread=None,
        now_ts=now_ts,
        min_interval_seconds=5,
        force_delta_pp=0.5,
    )


def test_polymarket_snapshot_gate_write_on_spread_change():
    now_ts = datetime.now(tz=timezone.utc).timestamp()
    assert should_write_polymarket_snapshot(
        last_price=0.41,
        last_written_ts=now_ts - 1,
        new_price=0.41,
        batch_volume=0.0,
        spread=0.02,
        last_spread=0.01,
        now_ts=now_ts,
        min_interval_seconds=5,
        force_delta_pp=0.5,
    )


def test_polymarket_snapshot_gate_write_after_interval():
    now = datetime.now(tz=timezone.utc)
    assert should_write_polymarket_snapshot(
        last_price=0.41,
        last_written_ts=(now - timedelta(seconds=10)).timestamp(),
        new_price=0.4100001,
        batch_volume=0.0,
        spread=None,
        last_spread=None,
        now_ts=now.timestamp(),
        min_interval_seconds=5,
        force_delta_pp=0.5,
    )
