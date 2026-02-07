"""Daily resolved-market scoring diagnostics (Brier, log-loss, calibration)."""

from __future__ import annotations

import json
import logging
import math
from datetime import date, datetime, time, timedelta, timezone
from typing import Optional

from packages.core.settings import settings
from packages.core.storage.db import get_db_pool

logger = logging.getLogger(__name__)

_EPS = 1e-6


def _normalize_outcome(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    if text in {"YES", "Y", "TRUE", "1"}:
        return "YES"
    if text in {"NO", "N", "FALSE", "0"}:
        return "NO"
    return None


def _clamp_probability(value: float) -> float:
    return min(max(float(value), _EPS), 1.0 - _EPS)


def _build_calibration_bins(
    samples: list[dict],
    bin_count: int,
) -> tuple[list[dict], float]:
    bins = [
        {
            "bin": idx,
            "lower": idx / bin_count,
            "upper": (idx + 1) / bin_count,
            "count": 0,
            "avg_pred": 0.0,
            "empirical": 0.0,
        }
        for idx in range(bin_count)
    ]

    for sample in samples:
        p = float(sample["pred"])
        y = float(sample["actual"])
        idx = min(int(p * bin_count), bin_count - 1)
        entry = bins[idx]
        entry["count"] += 1
        entry["avg_pred"] += p
        entry["empirical"] += y

    total = max(len(samples), 1)
    ece = 0.0
    for entry in bins:
        count = entry["count"]
        if count <= 0:
            continue
        entry["avg_pred"] /= count
        entry["empirical"] /= count
        ece += abs(entry["avg_pred"] - entry["empirical"]) * (count / total)

    return bins, ece


def _compute_scores(samples: list[dict]) -> Optional[dict]:
    if not samples:
        return None

    n = len(samples)
    brier = sum((float(s["pred"]) - float(s["actual"])) ** 2 for s in samples) / n
    log_loss = -sum(
        (float(s["actual"]) * math.log(float(s["pred"])))
        + ((1.0 - float(s["actual"])) * math.log(1.0 - float(s["pred"])))
        for s in samples
    ) / n

    bins, ece = _build_calibration_bins(samples, settings.model_scoring_calibration_bins)
    return {
        "sample_count": n,
        "brier_score": brier,
        "log_loss": log_loss,
        "ece": ece,
        "calibration_bins": bins,
    }


def _fetch_resolved_forecasts(start_ts: datetime, end_ts: datetime) -> list[dict]:
    db = get_db_pool()
    query = """
        WITH resolved AS (
            SELECT
                m.market_id,
                m.source,
                m.resolved_outcome,
                COALESCE(m.resolved_at, m.updated_at) AS resolved_ts,
                mt_yes.token_id AS yes_token_id
            FROM markets m
            JOIN market_tokens mt_yes
              ON mt_yes.market_id = m.market_id
             AND mt_yes.outcome = 'YES'
            WHERE m.status = 'resolved'
              AND m.resolved_outcome IN ('YES', 'NO')
              AND COALESCE(m.resolved_at, m.updated_at) >= %s
              AND COALESCE(m.resolved_at, m.updated_at) < %s
        )
        SELECT
            r.market_id,
            r.source,
            r.resolved_outcome,
            r.resolved_ts,
            p.price AS yes_prob,
            p.ts AS prob_ts
        FROM resolved r
        LEFT JOIN LATERAL (
            SELECT s.price, s.ts
            FROM snapshots s
            WHERE s.token_id = r.yes_token_id
              AND s.ts <= r.resolved_ts
            ORDER BY s.ts DESC
            LIMIT 1
        ) p ON true
    """
    return db.execute(query, (start_ts, end_ts), fetch=True) or []


def _upsert_daily_score(score_date: date, source: str, metrics: dict) -> None:
    db = get_db_pool()
    db.execute(
        """
        INSERT INTO model_scoring_daily (
            score_date,
            source,
            sample_count,
            brier_score,
            log_loss,
            ece,
            calibration_bins,
            generated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
        ON CONFLICT (score_date, source) DO UPDATE SET
            sample_count = EXCLUDED.sample_count,
            brier_score = EXCLUDED.brier_score,
            log_loss = EXCLUDED.log_loss,
            ece = EXCLUDED.ece,
            calibration_bins = EXCLUDED.calibration_bins,
            generated_at = NOW()
        """,
        (
            score_date,
            source,
            int(metrics["sample_count"]),
            float(metrics["brier_score"]),
            float(metrics["log_loss"]),
            float(metrics["ece"]),
            json.dumps(metrics["calibration_bins"]),
        ),
    )


def _write_system_status(score_date: date, metrics_by_source: dict[str, dict]) -> None:
    payload = {
        "score_date": score_date.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": metrics_by_source,
    }
    db = get_db_pool()
    db.execute(
        """
        INSERT INTO system_status (key, value, updated_at)
        VALUES ('model_scoring', %s, NOW())
        ON CONFLICT (key) DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = NOW()
        """,
        (json.dumps(payload),),
    )


async def update_daily_model_scoring(target_date: Optional[date] = None) -> int:
    """
    Compute and persist daily scoring diagnostics for resolved markets.

    Uses YES-token implied probability immediately before resolution as forecast.
    """
    score_date = target_date or (datetime.now(timezone.utc).date() - timedelta(days=1))
    start_ts = datetime.combine(score_date, time.min, tzinfo=timezone.utc)
    end_ts = start_ts + timedelta(days=1)

    rows = _fetch_resolved_forecasts(start_ts, end_ts)
    if not rows:
        logger.info(f"Model scoring: no resolved markets found for {score_date}")
        _write_system_status(score_date, {})
        return 0

    grouped_samples: dict[str, list[dict]] = {"all": []}

    for row in rows:
        resolved_outcome = _normalize_outcome(row.get("resolved_outcome"))
        yes_prob_raw = row.get("yes_prob")
        source = str(row.get("source") or "unknown")

        if resolved_outcome not in {"YES", "NO"}:
            continue
        if yes_prob_raw is None:
            continue

        sample = {
            "pred": _clamp_probability(float(yes_prob_raw)),
            "actual": 1.0 if resolved_outcome == "YES" else 0.0,
        }

        grouped_samples.setdefault(source, []).append(sample)
        grouped_samples["all"].append(sample)

    metrics_by_source: dict[str, dict] = {}
    total_scored = len(grouped_samples.get("all", []))

    for source, samples in grouped_samples.items():
        metrics = _compute_scores(samples)
        if not metrics:
            continue
        _upsert_daily_score(score_date, source, metrics)
        metrics_by_source[source] = {
            "sample_count": metrics["sample_count"],
            "brier_score": round(float(metrics["brier_score"]), 6),
            "log_loss": round(float(metrics["log_loss"]), 6),
            "ece": round(float(metrics["ece"]), 6),
            "calibration_bins": metrics["calibration_bins"],
        }

    _write_system_status(score_date, metrics_by_source)
    logger.info(
        "Model scoring updated for %s: %s markets",
        score_date,
        total_scored,
    )
    return total_scored
