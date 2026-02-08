#!/usr/bin/env python3
"""
One-time cleanup for historical settlement-noise alerts.

Targets two high-noise patterns:
1. Alerts created at/after market expiry or resolution.
2. Mirror YES/NO duplicate alerts for the same market event.

Optional third bucket:
3. Very large moves right before expiry (disabled by default).

Usage:
  Dry run:
    python scripts/cleanup_settlement_noise_alerts.py

  Purge:
    python scripts/cleanup_settlement_noise_alerts.py --apply

  Purge with near-expiry spike cleanup:
    python scripts/cleanup_settlement_noise_alerts.py \
      --apply \
      --include-near-expiry-spikes \
      --near-expiry-hours 1 \
      --near-expiry-min-move-pp 80
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass

# Add project root to path for direct script execution.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from packages.core.storage import get_db_pool


logger = logging.getLogger("cleanup_settlement_noise_alerts")


@dataclass(frozen=True)
class CleanupStep:
    name: str
    candidate_sql: str
    params: tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="One-time cleanup for historical settlement-noise alerts",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Delete matching rows (default is dry-run report only)",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Restrict cleanup to recent alerts (optional)",
    )
    parser.add_argument(
        "--include-near-expiry-spikes",
        action="store_true",
        help="Also remove very large moves close to expiry (disabled by default)",
    )
    parser.add_argument(
        "--near-expiry-hours",
        type=float,
        default=1.0,
        help="Near-expiry window in hours when near-expiry spike cleanup is enabled",
    )
    parser.add_argument(
        "--near-expiry-min-move-pp",
        type=float,
        default=80.0,
        help="Minimum absolute move_pp for near-expiry spike cleanup",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )
    return parser.parse_args()


def _lookback_clause(alias: str, lookback_days: int | None) -> tuple[str, tuple]:
    if lookback_days is None:
        return "", ()
    return f"AND {alias}.created_at >= NOW() - (%s * INTERVAL '1 day')", (lookback_days,)


def _expired_or_resolved_candidate_sql(lookback_days: int | None) -> tuple[str, tuple]:
    lookback_sql, lookback_params = _lookback_clause("a", lookback_days)
    sql = f"""
        SELECT a.alert_id
        FROM alerts a
        JOIN market_tokens mt ON a.token_id = mt.token_id
        JOIN markets m ON mt.market_id = m.market_id
        WHERE (
            (m.end_date IS NOT NULL AND a.created_at >= m.end_date)
            OR (
                COALESCE(m.status, '') <> 'active'
                AND COALESCE(m.resolved_at, m.end_date) IS NOT NULL
                AND a.created_at >= COALESCE(m.resolved_at, m.end_date)
            )
        )
        {lookback_sql}
    """
    return sql, lookback_params


def _near_expiry_spike_candidate_sql(
    lookback_days: int | None,
    near_expiry_hours: float,
    near_expiry_min_move_pp: float,
) -> tuple[str, tuple]:
    lookback_sql, lookback_params = _lookback_clause("a", lookback_days)
    sql = f"""
        SELECT a.alert_id
        FROM alerts a
        JOIN market_tokens mt ON a.token_id = mt.token_id
        JOIN markets m ON mt.market_id = m.market_id
        WHERE m.end_date IS NOT NULL
          AND a.created_at < m.end_date
          AND m.end_date <= a.created_at + (%s * INTERVAL '1 hour')
          AND ABS(a.move_pp) >= %s
          AND COALESCE(a.alert_type, 'price_move') IN ('price_move', 'combined')
          {lookback_sql}
    """
    params = (near_expiry_hours, near_expiry_min_move_pp, *lookback_params)
    return sql, params


def _mirror_yes_no_duplicate_candidate_sql(lookback_days: int | None) -> tuple[str, tuple]:
    lookback_sql, lookback_params = _lookback_clause("a", lookback_days)
    sql = f"""
        WITH scoped AS (
            SELECT
                a.alert_id,
                a.window_seconds,
                COALESCE(a.alert_type, 'price_move') AS alert_type,
                DATE_TRUNC('minute', a.created_at) AS minute_bucket,
                mt.market_id,
                mt.outcome,
                a.move_pp,
                a.created_at
            FROM alerts a
            JOIN market_tokens mt ON a.token_id = mt.token_id
            WHERE 1 = 1
              {lookback_sql}
        ),
        mirror_groups AS (
            SELECT
                market_id,
                window_seconds,
                alert_type,
                minute_bucket
            FROM scoped
            GROUP BY market_id, window_seconds, alert_type, minute_bucket
            HAVING BOOL_OR(UPPER(COALESCE(outcome, '')) IN ('YES', 'Y'))
               AND BOOL_OR(UPPER(COALESCE(outcome, '')) IN ('NO', 'N'))
        ),
        ranked AS (
            SELECT
                s.alert_id,
                ROW_NUMBER() OVER (
                    PARTITION BY s.market_id, s.window_seconds, s.alert_type, s.minute_bucket
                    ORDER BY
                        ABS(s.move_pp) DESC,
                        CASE
                            WHEN UPPER(COALESCE(s.outcome, '')) IN ('YES', 'Y') THEN 0
                            ELSE 1
                        END,
                        s.created_at DESC
                ) AS rn
            FROM scoped s
            JOIN mirror_groups g
              ON g.market_id = s.market_id
             AND g.window_seconds = s.window_seconds
             AND g.alert_type = s.alert_type
             AND g.minute_bucket = s.minute_bucket
        )
        SELECT alert_id
        FROM ranked
        WHERE rn > 1
    """
    return sql, lookback_params


def _count_candidates(db, candidate_sql: str, params: tuple) -> int:
    rows = db.execute(
        f"""
        WITH candidates AS (
            {candidate_sql}
        )
        SELECT COUNT(*) AS cnt
        FROM candidates
        """,
        params,
        fetch=True,
    ) or []
    return int(rows[0]["cnt"]) if rows else 0


def _delete_candidates(db, candidate_sql: str, params: tuple) -> int:
    rows = db.execute(
        f"""
        WITH to_delete AS (
            {candidate_sql}
        ),
        deleted AS (
            DELETE FROM alerts a
            USING to_delete d
            WHERE a.alert_id = d.alert_id
            RETURNING a.alert_id
        )
        SELECT COUNT(*) AS cnt
        FROM deleted
        """,
        params,
        fetch=True,
    ) or []
    return int(rows[0]["cnt"]) if rows else 0


def _count_total_alerts(db) -> int:
    rows = db.execute("SELECT COUNT(*) AS cnt FROM alerts", fetch=True) or []
    return int(rows[0]["cnt"]) if rows else 0


def _build_steps(args: argparse.Namespace) -> list[CleanupStep]:
    steps: list[CleanupStep] = []

    expired_sql, expired_params = _expired_or_resolved_candidate_sql(args.lookback_days)
    steps.append(
        CleanupStep(
            name="expired_or_resolved_at_alert_time",
            candidate_sql=expired_sql,
            params=expired_params,
        )
    )

    if args.include_near_expiry_spikes:
        near_sql, near_params = _near_expiry_spike_candidate_sql(
            lookback_days=args.lookback_days,
            near_expiry_hours=args.near_expiry_hours,
            near_expiry_min_move_pp=args.near_expiry_min_move_pp,
        )
        steps.append(
            CleanupStep(
                name="near_expiry_extreme_spikes",
                candidate_sql=near_sql,
                params=near_params,
            )
        )

    mirror_sql, mirror_params = _mirror_yes_no_duplicate_candidate_sql(args.lookback_days)
    steps.append(
        CleanupStep(
            name="mirror_yes_no_duplicates",
            candidate_sql=mirror_sql,
            params=mirror_params,
        )
    )

    return steps


def run(args: argparse.Namespace) -> dict[str, int]:
    db = get_db_pool()
    if not db.health_check():
        raise RuntimeError("Database not available")

    before_total = _count_total_alerts(db)
    steps = _build_steps(args)

    counts_by_step: dict[str, int] = {}
    for step in steps:
        counts_by_step[step.name] = _count_candidates(db, step.candidate_sql, step.params)

    deleted_by_step: dict[str, int] = {}
    if args.apply:
        for step in steps:
            deleted = _delete_candidates(db, step.candidate_sql, step.params)
            deleted_by_step[step.name] = deleted
            logger.info("Deleted %s rows for step=%s", deleted, step.name)

    after_total = _count_total_alerts(db)

    stats = {
        "total_before": before_total,
        "total_after": after_total,
        "dry_run_candidate_sum": sum(counts_by_step.values()),
        "deleted_sum": sum(deleted_by_step.values()),
    }

    mode = "APPLY" if args.apply else "DRY-RUN"
    logger.info("%s | alerts_before=%s alerts_after=%s", mode, before_total, after_total)

    for step_name, count in counts_by_step.items():
        logger.info(
            "%s | step=%s candidates=%s",
            mode,
            step_name,
            count,
        )
    if args.apply:
        for step_name, deleted in deleted_by_step.items():
            logger.info(
                "APPLY | step=%s deleted=%s",
                step_name,
                deleted,
            )

    if not args.apply:
        logger.info(
            "DRY-RUN note: candidate counts can overlap across steps; "
            "run with --apply to see exact deleted counts.",
        )

    return stats


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    try:
        run(args)
    except Exception as exc:
        logger.error("Settlement-noise cleanup failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
