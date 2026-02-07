"""Snapshot write-gating helpers used by WSS ingestion jobs."""

from typing import Optional


def should_write_kalshi_snapshot(
    *,
    last_price: Optional[float],
    last_written_ts: Optional[float],
    new_price: float,
    batch_volume: Optional[float],
    now_ts: float,
    min_interval_seconds: float,
    force_delta_pp: float,
) -> bool:
    """
    Determine whether we should persist a Kalshi snapshot for a ticker.

    Rules:
    - Always write first observation.
    - Skip unchanged price with no new volume (dedupe).
    - Force write when price move >= force_delta_pp.
    - Write when min interval elapsed.
    - Write when there is new volume.
    """
    has_volume = bool(batch_volume and batch_volume > 0)

    if last_price is None or last_written_ts is None:
        return True

    price_unchanged = abs(new_price - last_price) < 1e-9
    if price_unchanged and not has_volume:
        return False

    move_pp = abs(new_price - last_price) * 100.0
    if move_pp >= force_delta_pp:
        return True

    if has_volume:
        return True

    return (now_ts - last_written_ts) >= min_interval_seconds


def should_write_polymarket_snapshot(
    *,
    last_price: Optional[float],
    last_written_ts: Optional[float],
    new_price: float,
    batch_volume: Optional[float],
    spread: Optional[float],
    last_spread: Optional[float],
    now_ts: float,
    min_interval_seconds: float,
    force_delta_pp: float,
) -> bool:
    """
    Decide if we should write a Polymarket snapshot for this token.

    Rules:
    - Always write first observation.
    - Skip unchanged price with no new volume/spread change.
    - Force write on sufficiently large move.
    - Write if new volume/spread data arrives.
    - Write if minimum interval elapsed.
    """
    has_volume = bool(batch_volume and batch_volume > 0)
    spread_changed = False
    if spread is not None:
        spread_changed = last_spread is None or abs(spread - last_spread) >= 1e-9

    if last_price is None or last_written_ts is None:
        return True

    price_unchanged = abs(new_price - last_price) < 1e-9
    if price_unchanged and not has_volume and not spread_changed:
        return False

    move_pp = abs(new_price - last_price) * 100.0
    if move_pp >= force_delta_pp:
        return True

    if has_volume or spread_changed:
        return True

    return (now_ts - last_written_ts) >= min_interval_seconds
