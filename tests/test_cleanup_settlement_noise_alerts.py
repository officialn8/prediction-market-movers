import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_cleanup_module():
    script_path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "cleanup_settlement_noise_alerts.py"
    )
    spec = importlib.util.spec_from_file_location(
        "cleanup_settlement_noise_alerts",
        script_path,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_expired_candidate_sql_targets_expiry_or_resolution():
    cleanup = _load_cleanup_module()
    sql, params = cleanup._expired_or_resolved_candidate_sql(lookback_days=30)

    assert "a.created_at >= m.end_date" in sql
    assert "a.created_at >= COALESCE(m.resolved_at, m.end_date)" in sql
    assert "a.created_at >= NOW() - (%s * INTERVAL '1 day')" in sql
    assert params == (30,)


def test_mirror_duplicate_sql_targets_yes_no_pairs_only():
    cleanup = _load_cleanup_module()
    sql, params = cleanup._mirror_yes_no_duplicate_candidate_sql(lookback_days=None)

    assert "BOOL_OR(UPPER(COALESCE(outcome, '')) IN ('YES', 'Y'))" in sql
    assert "BOOL_OR(UPPER(COALESCE(outcome, '')) IN ('NO', 'N'))" in sql
    assert "WHERE rn > 1" in sql
    assert params == ()


def test_build_steps_default_and_optional_near_expiry():
    cleanup = _load_cleanup_module()

    default_args = SimpleNamespace(
        lookback_days=None,
        include_near_expiry_spikes=False,
        near_expiry_hours=1.0,
        near_expiry_min_move_pp=80.0,
    )
    default_steps = cleanup._build_steps(default_args)
    assert [step.name for step in default_steps] == [
        "expired_or_resolved_at_alert_time",
        "mirror_yes_no_duplicates",
    ]

    near_args = SimpleNamespace(
        lookback_days=None,
        include_near_expiry_spikes=True,
        near_expiry_hours=2.0,
        near_expiry_min_move_pp=75.0,
    )
    near_steps = cleanup._build_steps(near_args)
    assert [step.name for step in near_steps] == [
        "expired_or_resolved_at_alert_time",
        "near_expiry_extreme_spikes",
        "mirror_yes_no_duplicates",
    ]


def test_validate_identifier_allows_safe_names_only():
    cleanup = _load_cleanup_module()

    assert cleanup._validate_identifier("alerts_suppressed_archive") == "alerts_suppressed_archive"
    with pytest.raises(ValueError):
        cleanup._validate_identifier("alerts-suppressed-archive")


def test_run_rejects_invalid_archive_mode_combo(monkeypatch):
    cleanup = _load_cleanup_module()

    class DummyDB:
        def health_check(self):
            return True

    monkeypatch.setattr(cleanup, "get_db_pool", lambda: DummyDB())

    args = SimpleNamespace(
        apply=False,
        archive_only=True,
        archive_first=True,
    )
    with pytest.raises(ValueError):
        cleanup.run(args)
