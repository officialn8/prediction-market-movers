"""
Data retention job with per-table policies and storage telemetry.

Runs on a fixed interval and applies retention for raw/derived/event tables.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone

from packages.core.settings import settings
from packages.core.storage.db import get_db_pool

logger = logging.getLogger(__name__)

# table_name -> (timestamp_column, retention_days)
RETENTION_POLICIES: dict[str, tuple[str, int]] = {
    "snapshots": ("ts", settings.snapshot_retention_days),
    "ohlc_1m": ("bucket_ts", settings.ohlc_1m_retention_days),
    "ohlc_1h": ("bucket_ts", settings.ohlc_1h_retention_days),
    "movers_cache": ("as_of_ts", settings.movers_cache_retention_days),
    "alerts": ("created_at", settings.alerts_retention_days),
    "volume_spikes": ("created_at", settings.volume_spikes_retention_days),
    "arbitrage_opportunities": ("detected_at", settings.arbitrage_retention_days),
    "volume_hourly": ("bucket_ts", settings.volume_hourly_retention_days),
}

TABLES_FOR_SIZE_TELEMETRY = [
    "snapshots",
    "ohlc_1m",
    "ohlc_5m",
    "ohlc_1h",
    "movers_cache",
    "alerts",
    "volume_spikes",
    "arbitrage_opportunities",
    "trade_volumes",
    "volume_hourly",
]


def _safe_count(row: dict | None, key: str = "cnt") -> int:
    if not row:
        return 0
    value = row.get(key)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


async def _delete_with_batch(
    db,
    table: str,
    ts_column: str,
    retention_days: int,
    batch_size: int,
) -> int:
    """Delete old rows in batches and return total deleted."""
    total_deleted = 0

    while True:
        rows = await asyncio.to_thread(
            db.execute,
            f"""
            WITH deleted AS (
                DELETE FROM {table}
                WHERE ctid IN (
                    SELECT ctid
                    FROM {table}
                    WHERE {ts_column} < NOW() - (%s * INTERVAL '1 day')
                    LIMIT %s
                )
                RETURNING 1
            )
            SELECT COUNT(*) AS cnt FROM deleted
            """,
            (retention_days, batch_size),
            fetch=True,
        )
        deleted = _safe_count(rows[0] if rows else None)
        total_deleted += deleted
        if deleted == 0:
            break
        await asyncio.sleep(0.05)

    return total_deleted


async def _delete_all(
    db,
    table: str,
    ts_column: str,
    retention_days: int,
) -> int:
    """Delete all old rows in one statement and return count."""
    rows = await asyncio.to_thread(
        db.execute,
        f"""
        WITH deleted AS (
            DELETE FROM {table}
            WHERE {ts_column} < NOW() - (%s * INTERVAL '1 day')
            RETURNING 1
        )
        SELECT COUNT(*) AS cnt FROM deleted
        """,
        (retention_days,),
        fetch=True,
    )
    return _safe_count(rows[0] if rows else None)


async def run_retention_cleanup() -> dict:
    """
    Apply retention policies across high-growth tables and emit telemetry.
    """
    db = get_db_pool()
    started_at = time.time()

    try:
        table_rows = await asyncio.to_thread(
            db.execute,
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
            """,
            fetch=True,
        )
        existing_tables = {r["tablename"] for r in (table_rows or [])}

        deleted_by_table: dict[str, int] = {}
        for table, (ts_column, retention_days) in RETENTION_POLICIES.items():
            if table not in existing_tables:
                continue

            if table == "snapshots":
                deleted = await _delete_with_batch(
                    db=db,
                    table=table,
                    ts_column=ts_column,
                    retention_days=retention_days,
                    batch_size=25_000,
                )
            else:
                deleted = await _delete_all(
                    db=db,
                    table=table,
                    ts_column=ts_column,
                    retention_days=retention_days,
                )

            deleted_by_table[table] = deleted

        size_rows = await asyncio.to_thread(
            db.execute,
            """
            SELECT
                c.relname AS table_name,
                pg_total_relation_size(c.oid) AS bytes,
                pg_size_pretty(pg_total_relation_size(c.oid)) AS size_pretty
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public'
              AND c.relkind = 'r'
              AND c.relname = ANY(%s)
            ORDER BY bytes DESC
            """,
            (TABLES_FOR_SIZE_TELEMETRY,),
            fetch=True,
        )

        db_size_rows = await asyncio.to_thread(
            db.execute,
            """
            SELECT
                pg_database_size(current_database()) AS db_size_bytes,
                pg_size_pretty(pg_database_size(current_database())) AS db_size_pretty
            """,
            fetch=True,
        )

        elapsed_seconds = max(time.time() - started_at, 1e-6)
        elapsed_minutes = max(elapsed_seconds / 60.0, 1.0 / 60.0)
        deleted_per_minute = {
            table: round(deleted / elapsed_minutes, 2)
            for table, deleted in deleted_by_table.items()
        }

        table_sizes = {
            r["table_name"]: {
                "bytes": int(r["bytes"]),
                "pretty": r["size_pretty"],
            }
            for r in (size_rows or [])
        }
        db_size = (db_size_rows or [{}])[0]

        telemetry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "elapsed_seconds": round(elapsed_seconds, 3),
            "retention_days": {t: d for t, (_, d) in RETENTION_POLICIES.items()},
            "deleted_by_table": deleted_by_table,
            "deleted_per_minute": deleted_per_minute,
            "table_sizes": table_sizes,
            "db_size_bytes": int(db_size.get("db_size_bytes") or 0),
            "db_size_pretty": db_size.get("db_size_pretty") or "unknown",
        }

        await asyncio.to_thread(
            db.execute,
            """
            INSERT INTO system_status (key, value, updated_at)
            VALUES ('storage_metrics', %s, NOW())
            ON CONFLICT (key) DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = NOW()
            """,
            (json.dumps(telemetry),),
        )

        logger.info(
            "Retention cleanup complete: "
            f"{deleted_by_table} | db_size={telemetry['db_size_pretty']}"
        )
        return telemetry

    except Exception as e:
        logger.exception(f"Retention cleanup failed: {e}")
        raise
