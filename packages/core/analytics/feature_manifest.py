"""Strict feature-manifest validation for scorer inference."""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from typing import Any


class FeatureManifestMismatchError(RuntimeError):
    """Raised when live inference features mismatch the training manifest."""


@dataclass(frozen=True)
class ManifestFeature:
    name: str
    dtype: str


@dataclass(frozen=True)
class FeatureManifest:
    model: str
    version: int
    features: tuple[ManifestFeature, ...]

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(feature.name for feature in self.features)


@lru_cache(maxsize=8)
def load_feature_manifest(path: str) -> FeatureManifest:
    with open(path, "r", encoding="utf-8") as fh:
        payload = json.load(fh)

    features = tuple(
        ManifestFeature(name=str(item["name"]), dtype=str(item["dtype"]))
        for item in payload.get("features", [])
    )
    if not features:
        raise FeatureManifestMismatchError(f"Manifest at {path} contains no features")

    return FeatureManifest(
        model=str(payload.get("model") or "unknown"),
        version=int(payload.get("version") or 1),
        features=features,
    )


def _dtype_matches(value: Any, expected_dtype: str) -> bool:
    if expected_dtype == "float":
        return isinstance(value, (float, int)) and not isinstance(value, bool)
    if expected_dtype == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected_dtype == "bool":
        return isinstance(value, bool)
    if expected_dtype == "str":
        return isinstance(value, str)
    return False


def validate_live_feature_rows(rows: list[dict[str, Any]], manifest_path: str) -> None:
    """
    Validate live feature vectors against a training manifest.

    Enforces:
    - exact column set and order
    - strict type compatibility for every value
    """
    if not rows:
        return

    manifest = load_feature_manifest(manifest_path)
    expected_columns = manifest.column_names

    for row_idx, row in enumerate(rows):
        actual_columns = tuple(row.keys())
        if actual_columns != expected_columns:
            raise FeatureManifestMismatchError(
                "Feature column mismatch for model "
                f"{manifest.model} v{manifest.version}: expected {expected_columns}, got {actual_columns}"
            )

        for feature in manifest.features:
            value = row.get(feature.name)
            if value is None or not _dtype_matches(value, feature.dtype):
                raise FeatureManifestMismatchError(
                    "Feature type mismatch for model "
                    f"{manifest.model} v{manifest.version} at row {row_idx}, "
                    f"column {feature.name}: expected {feature.dtype}, got {type(value).__name__}"
                )
