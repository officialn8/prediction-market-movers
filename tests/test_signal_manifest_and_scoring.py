from __future__ import annotations

import pytest

from apps.collector.jobs import model_scoring
from apps.collector.jobs.movers_cache import check_instant_mover
from packages.core.analytics.feature_manifest import FeatureManifestMismatchError
from packages.core.analytics.metrics import ZScoreMoverScorer
from packages.core.settings import settings


@pytest.mark.asyncio
async def test_instant_mover_hold_zone_suppresses_borderline(monkeypatch):
    monkeypatch.setattr(settings, "signal_hold_zone_enabled", True, raising=False)
    monkeypatch.setattr(settings, "signal_hold_zone_move_pp", 2.0, raising=False)
    monkeypatch.setattr(settings, "instant_mover_threshold_pp", 5.0, raising=False)

    # 5.2pp move, but only 0.2pp over threshold -> hold-zone suppresses.
    alert = await check_instant_mover(
        token_id="token-1",
        old_price=0.50,
        new_price=0.552,
        volume=None,
    )
    assert alert is None


@pytest.mark.asyncio
async def test_instant_mover_hold_zone_allows_clear_edge(monkeypatch):
    monkeypatch.setattr(settings, "signal_hold_zone_enabled", True, raising=False)
    monkeypatch.setattr(settings, "signal_hold_zone_move_pp", 0.1, raising=False)
    monkeypatch.setattr(settings, "instant_mover_threshold_pp", 5.0, raising=False)

    alert = await check_instant_mover(
        token_id="token-2",
        old_price=0.50,
        new_price=0.56,
        volume=None,
    )
    assert alert is not None
    assert alert.move_pp > 0


def test_zscore_ranker_fails_fast_on_manifest_mismatch(tmp_path, monkeypatch):
    bad_manifest = tmp_path / "bad_manifest.json"
    bad_manifest.write_text(
        '{"model":"zscore","version":1,"features":[{"name":"wrong_col","dtype":"float"}]}',
        encoding="utf-8",
    )

    monkeypatch.setattr(settings, "model_feature_manifest_strict", True, raising=False)
    monkeypatch.setattr(settings, "model_feature_manifest_path", str(bad_manifest), raising=False)

    scorer = ZScoreMoverScorer(min_z_score=0.0)
    movers = [
        {
            "token_id": "token-1",
            "latest_price": 0.62,
            "old_price": 0.50,
            "latest_volume": 20000,
        }
    ]

    with pytest.raises(FeatureManifestMismatchError):
        scorer.rank_movers(
            movers=movers,
            market_stats_map={
                "token-1": {
                    "avg_move_pp": 1.0,
                    "stddev_move_pp": 0.5,
                    "avg_log_odds": 0.1,
                    "stddev_log_odds": 0.1,
                    "avg_volume": 1000.0,
                    "stddev_volume": 500.0,
                }
            },
            price_now_key="latest_price",
            price_then_key="old_price",
            volume_key="latest_volume",
            window_minutes=5,
        )


def test_compute_scores_outputs_expected_metrics(monkeypatch):
    monkeypatch.setattr(settings, "model_scoring_calibration_bins", 5, raising=False)

    samples = [
        {"pred": 0.8, "actual": 1.0},
        {"pred": 0.3, "actual": 0.0},
    ]

    scores = model_scoring._compute_scores(samples)
    assert scores is not None
    assert scores["sample_count"] == 2
    assert scores["brier_score"] == pytest.approx(0.065, rel=1e-4)
    assert scores["log_loss"] == pytest.approx(0.2899, rel=1e-3)
    assert isinstance(scores["calibration_bins"], list)
    assert len(scores["calibration_bins"]) == 5
