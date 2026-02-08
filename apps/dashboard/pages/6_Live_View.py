"""
Live View - real-time snapshot tape and short-window movers.
"""

from datetime import datetime, timezone

import pandas as pd
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
    AUTOREFRESH_AVAILABLE = True
except ImportError:
    AUTOREFRESH_AVAILABLE = False

    def st_autorefresh(*_args, **_kwargs):
        return None

from apps.dashboard.components import (
    get_watchlist,
    init_watchlist,
    render_mover_card,
    render_volume_spike_alert,
    to_user_tz,
)
from packages.core.storage import get_db_pool
from packages.core.storage.queries import AnalyticsQueries, MarketQueries, VolumeQueries
from packages.core.wss import WSSMetrics


st.set_page_config(
    page_title="Live View | PM Movers",
    page_icon="🟢",
    layout="wide",
)


def get_live_status() -> dict:
    """Summarize live status based on WSS metrics + recent DB activity."""
    metrics = WSSMetrics.load_with_activity_check()
    db = get_db_pool()

    last_snapshot_ts = None
    try:
        result = db.execute(
            "SELECT MAX(ts) as latest FROM snapshots",
            fetch=True,
        )
        if result and result[0]["latest"]:
            last_snapshot_ts = result[0]["latest"]
    except Exception:
        pass

    if metrics.mode == "wss":
        status = "LIVE"
        status_class = "🟢"
    elif metrics.mode == "polling":
        status = "SYNCING"
        status_class = "🟡"
    else:
        status = "OFFLINE"
        status_class = "🔴"

    last_snapshot_age = None
    if last_snapshot_ts:
        if last_snapshot_ts.tzinfo is None:
            last_snapshot_ts = last_snapshot_ts.replace(tzinfo=timezone.utc)
        last_snapshot_age = (datetime.now(timezone.utc) - last_snapshot_ts).total_seconds()

    return {
        "status": status,
        "status_class": status_class,
        "messages_per_second": metrics.messages_per_second,
        "subscriptions": metrics.current_subscriptions,
        "last_message_age": metrics.last_message_age_seconds,
        "last_snapshot_ts": last_snapshot_ts,
        "last_snapshot_age": last_snapshot_age,
    }


def check_database_connection() -> tuple[bool, str]:
    """Check if database is accessible."""
    try:
        db = get_db_pool()
        if db.health_check():
            stats = db.get_pool_stats()
            return True, f"pool {stats.get('size', 0)}/{stats.get('max_size', 0)}"
        return False, "Health check failed"
    except Exception as exc:
        return False, str(exc)


def get_system_status_entries() -> dict:
    """Fetch system_status rows as a keyed dict."""
    db = get_db_pool()
    rows = db.execute("SELECT key, value, updated_at FROM system_status", fetch=True) or []
    entries: dict[str, dict] = {}
    for row in rows:
        key = row.get("key")
        if not key:
            continue
        value = row.get("value") or {}
        updated_at = row.get("updated_at")
        if updated_at:
            value["db_updated_at"] = updated_at
        entries[key] = value
    return entries


def _normalize_source_filter(source: str | None) -> str | None:
    if not source:
        return None
    normalized = source.strip().lower()
    if normalized in {"polymarket", "kalshi"}:
        return normalized
    return None


def _cached_window_for_live_movers(window_seconds: int) -> int:
    """Map requested live window to available movers-cache windows."""
    if window_seconds <= 300:
        return 300
    if window_seconds <= 900:
        return 900
    if window_seconds <= 3600:
        return 3600
    return 86400


def get_live_tape(seconds: int, limit: int, source: str | None = None) -> list[dict]:
    """Fetch latest per-token snapshots within the time window."""
    db = get_db_pool()
    sample_limit = max(limit * 12, 240)
    source_filter = _normalize_source_filter(source)
    source_clause = "AND m.source = %s" if source_filter else ""

    query = f"""
        SELECT
            s.ts,
            s.price,
            s.volume_24h,
            s.spread,
            mt.outcome,
            mt.market_id,
            m.title,
            m.source,
            m.category
        FROM snapshots s
        JOIN market_tokens mt ON mt.token_id = s.token_id
        JOIN markets m ON m.market_id = mt.market_id
        WHERE s.ts > NOW() - (%s * INTERVAL '1 second')
          AND m.status = 'active'
          {source_clause}
        ORDER BY s.ts DESC
        LIMIT %s
    """
    params: list[object] = [seconds]
    if source_filter:
        params.append(source_filter)
    params.append(sample_limit)
    try:
        rows = db.execute(
            query,
            tuple(params),
            fetch=True,
            statement_timeout_ms=3000,
        ) or []
    except Exception:
        return []
    if source_filter:
        return rows[:limit]
    return balance_rows_by_source(rows, limit)


def balance_rows_by_source(rows: list[dict], limit: int) -> list[dict]:
    """Keep Live View readable by preventing one source from crowding out others."""
    if limit <= 0 or not rows:
        return []

    source_order: list[str] = []
    for row in rows:
        source = str(row.get("source") or "unknown").lower()
        if source not in source_order:
            source_order.append(source)

    if not source_order:
        return rows[:limit]

    per_source_cap = max(1, limit // len(source_order))
    counts = {source: 0 for source in source_order}
    selected: list[dict] = []
    selected_indexes: set[int] = set()

    for idx, row in enumerate(rows):
        source = str(row.get("source") or "unknown").lower()
        if counts.get(source, 0) >= per_source_cap:
            continue
        selected.append(row)
        selected_indexes.add(idx)
        counts[source] = counts.get(source, 0) + 1
        if len(selected) >= limit:
            return selected

    if len(selected) < limit:
        for idx, row in enumerate(rows):
            if idx in selected_indexes:
                continue
            selected.append(row)
            if len(selected) >= limit:
                break

    return selected[:limit]


def get_live_movers_balanced(window_seconds: int, limit: int) -> list[dict]:
    """Fetch movers with source balancing so both venues stay visible."""
    if limit <= 0:
        return []

    cached_window = _cached_window_for_live_movers(window_seconds)
    per_source_limit = max(1, limit // 2)
    kalshi = AnalyticsQueries.get_cached_movers(
        window_seconds=cached_window,
        limit=limit,
        direction="both",
        source="kalshi",
    )
    polymarket = AnalyticsQueries.get_cached_movers(
        window_seconds=cached_window,
        limit=limit,
        direction="both",
        source="polymarket",
    )

    def _move_strength(item: dict) -> float:
        try:
            return abs(float(item.get("move_pp") or 0.0))
        except (TypeError, ValueError):
            return 0.0

    selected = list(kalshi[:per_source_limit]) + list(polymarket[:per_source_limit])
    if len(selected) < limit:
        remainder = list(kalshi[per_source_limit:]) + list(polymarket[per_source_limit:])
        remainder.sort(key=_move_strength, reverse=True)
        selected.extend(remainder[: limit - len(selected)])

    selected.sort(key=_move_strength, reverse=True)
    return selected[:limit]


def get_live_movers(window_seconds: int, limit: int, source: str | None = None) -> list[dict]:
    """Fetch movers for a specific venue, or balanced across venues."""
    source_filter = _normalize_source_filter(source)
    cached_window = _cached_window_for_live_movers(window_seconds)
    if source_filter:
        return AnalyticsQueries.get_cached_movers(
            window_seconds=cached_window,
            limit=limit,
            direction="both",
            source=source_filter,
        )
    return get_live_movers_balanced(window_seconds=window_seconds, limit=limit)


def format_age(age_seconds: float | None) -> str:
    if age_seconds is None:
        return "—"
    if age_seconds < 60:
        return f"{age_seconds:.0f}s"
    minutes = age_seconds / 60
    return f"{minutes:.1f}m"


def get_service_badge(service: dict, last_age: float | None) -> tuple[str, str]:
    """Map service status payload into a dashboard badge and state label."""
    state = str(service.get("state") or "").strip().lower()
    connected = bool(service.get("connected"))

    stale_after = 60.0
    refresh_interval = service.get("refresh_interval_seconds")
    if refresh_interval is not None:
        try:
            stale_after = max(stale_after, float(refresh_interval) * 1.5)
        except (TypeError, ValueError):
            pass

    if state in {"subscribing", "refreshing", "connecting"}:
        return "🟡", state
    if last_age is not None and last_age > stale_after:
        return "🟡", "stale"
    if connected:
        return "🟢", state or "connected"
    return "🔴", state or "disconnected"


def render_status_bar():
    status = get_live_status()

    last_ts = status["last_snapshot_ts"]
    last_ts_display = "—"
    if last_ts:
        last_ts_display = to_user_tz(last_ts).strftime("%H:%M:%S")

    cols = st.columns(4)
    cols[0].metric("Collector", f"{status['status_class']} {status['status']}")
    cols[1].metric("Last Snapshot", last_ts_display)
    cols[2].metric("Snapshot Age", format_age(status["last_snapshot_age"]))
    cols[3].metric("WSS Msg/s", f"{status['messages_per_second']:.1f}")


def render_system_health_panel():
    status_entries = get_system_status_entries()
    db_ok, db_info = check_database_connection()
    db_badge = "🟢" if db_ok else "🔴"

    with st.expander("System Health", expanded=False):
        col1, col2, col3 = st.columns(3)
        col1.metric("Database", f"{db_badge} {db_info}")

        wss_metrics = status_entries.get("wss_metrics")
        if wss_metrics:
            col2.metric(
                "WSS Subscriptions",
                str(wss_metrics.get("current_subscriptions", 0)),
            )
            col3.metric(
                "Last WSS Message",
                format_age(wss_metrics.get("last_message_age_seconds")),
            )
        else:
            col2.metric("WSS Subscriptions", "—")
            col3.metric("Last WSS Message", "—")

        service_cols = st.columns(2)
        for idx, key in enumerate(["polymarket_wss", "kalshi_wss"]):
            service = status_entries.get(key)
            with service_cols[idx]:
                if not service:
                    st.caption(f"{key}: no recent status")
                    continue
                latency = float(service.get("latency_ms", 0) or 0)
                last_updated = service.get("db_updated_at")
                last_age = None
                if last_updated:
                    if last_updated.tzinfo is None:
                        last_updated = last_updated.replace(tzinfo=timezone.utc)
                    last_age = (datetime.now(timezone.utc) - last_updated).total_seconds()
                badge, state_label = get_service_badge(service, last_age)
                subs_count = service.get("subscription_count")
                subs_target = service.get("subscription_target")
                subs_text = ""
                try:
                    if subs_target is not None:
                        subs_text = f" | subs {int(float(subs_count or 0))}/{int(float(subs_target))}"
                    elif subs_count is not None:
                        subs_text = f" | subs {int(float(subs_count))}"
                except (TypeError, ValueError):
                    subs_text = ""
                st.caption(
                    f"{key}: {badge} {state_label} | latency {latency:.0f}ms | "
                    f"last {format_age(last_age)}{subs_text}"
                )

        model_scoring = status_entries.get("model_scoring") or {}
        if model_scoring:
            st.markdown("---")
            st.markdown("**Resolved-Market Scoring (Daily)**")
            score_date = model_scoring.get("score_date", "—")
            sources = model_scoring.get("sources", {}) or {}
            overall = sources.get("all")

            if overall:
                cols = st.columns(4)
                cols[0].metric("Score Date", score_date)
                cols[1].metric("Samples", int(overall.get("sample_count", 0)))
                cols[2].metric("Brier", f"{float(overall.get('brier_score', 0.0)):.4f}")
                cols[3].metric("Log Loss", f"{float(overall.get('log_loss', 0.0)):.4f}")
                st.caption(f"Calibration ECE: {float(overall.get('ece', 0.0)):.4f}")
            else:
                st.caption(f"Score date: {score_date} | no resolved samples scored yet")


def render_live_movers(window_seconds: int, limit: int, source: str | None = None):
    movers = get_live_movers(window_seconds=window_seconds, limit=limit, source=source)
    if not movers:
        cached_window = _cached_window_for_live_movers(window_seconds)
        if source:
            st.info(f"No recent {source.title()} cached movers yet (window {cached_window}s).")
        else:
            st.info(f"No cached movers yet (window {cached_window}s).")
        return
    for mover in movers:
        render_mover_card(mover, show_watchlist=False)


def render_live_tape(seconds: int, limit: int, source: str | None = None):
    rows = get_live_tape(seconds, limit, source=source)
    if not rows:
        if source:
            st.info(f"No recent {source.title()} snapshots yet.")
        else:
            st.info("No recent snapshots yet.")
        return

    df = pd.DataFrame(rows)
    df["ts"] = df["ts"].apply(to_user_tz)
    df["price"] = df["price"].astype(float)
    df["volume_24h"] = pd.to_numeric(df["volume_24h"], errors="coerce").fillna(0.0)
    df["source"] = df["source"].str.upper()

    st.dataframe(
        df[["ts", "source", "title", "outcome", "price", "volume_24h", "category"]],
        column_config={
            "ts": st.column_config.DatetimeColumn("Time", format="HH:mm:ss"),
            "volume_24h": st.column_config.NumberColumn("24h Volume", format="$%.0f"),
        },
        width="stretch",
        hide_index=True,
    )


def classify_alert_severity(move_pp: float) -> str:
    abs_move = abs(move_pp)
    if abs_move >= 10:
        return "extreme"
    if abs_move >= 6:
        return "high"
    if abs_move >= 3:
        return "medium"
    if abs_move >= 1:
        return "low"
    return "none"


def render_alerts_stream(min_severity: str, limit: int):
    alerts = AnalyticsQueries.get_recent_alerts(limit=limit)
    if not alerts:
        st.info("No recent alerts.")
        return

    severity_order = {"none": 0, "low": 1, "medium": 2, "high": 3, "extreme": 4}
    min_level = severity_order.get(min_severity, 1)

    rows = []
    for alert in alerts:
        move_pp = float(alert.get("move_pp") or 0)
        severity = classify_alert_severity(move_pp)
        if severity_order.get(severity, 0) < min_level:
            continue
        rows.append(
            {
                "created_at": alert.get("created_at"),
                "market": alert.get("market_title"),
                "outcome": alert.get("outcome"),
                "move_pp": move_pp,
                "severity": severity,
                "source": alert.get("source"),
                "reason": alert.get("reason"),
            }
        )

    if not rows:
        st.info("No alerts matched this severity filter.")
        return

    rows = balance_rows_by_source(rows, limit)
    df = pd.DataFrame(rows)
    st.dataframe(
        df,
        column_config={
            "created_at": st.column_config.DatetimeColumn("Time", format="HH:mm:ss"),
            "move_pp": st.column_config.NumberColumn("Move (pp)", format="%.2f"),
        },
        width="stretch",
        hide_index=True,
    )


def render_volume_spikes(min_severity: str, limit: int, unack_only: bool):
    spikes = VolumeQueries.get_recent_volume_spikes(
        limit=limit,
        min_severity=min_severity,
        unacknowledged_only=unack_only,
    )
    if not spikes:
        st.info("No recent volume spikes.")
        return
    for spike in spikes:
        render_volume_spike_alert(spike)


def render_watchlist_tiles(delta_minutes: int, limit: int):
    init_watchlist()
    watchlist = get_watchlist()
    if not watchlist:
        st.info("Your watchlist is empty.")
        return

    market_ids = list(watchlist.keys())[:limit]
    markets = MarketQueries.get_markets_batch_with_prices(market_ids)
    if not markets:
        st.info("Watchlist markets are not available yet.")
        return

    token_ids = []
    for market in markets:
        for token in market.get("tokens", []) or []:
            if token and token.get("token_id"):
                token_ids.append(token["token_id"])

    delta_rows = MarketQueries.get_token_price_deltas(
        token_ids=token_ids,
        window_minutes=delta_minutes,
    )
    delta_map = {str(row["token_id"]): row for row in delta_rows}

    col_left, col_right = st.columns(2)
    for idx, market in enumerate(markets):
        with col_left if idx % 2 == 0 else col_right:
            title = market.get("title", "Unknown Market")
            source = (market.get("source") or "unknown").upper()
            st.markdown(f"**{title}**")
            st.caption(f"{source}")

            tokens = market.get("tokens", []) or []
            for token in tokens:
                token_id = str(token.get("token_id"))
                outcome = token.get("outcome", "YES")
                latest_price = float(token.get("latest_price") or 0)
                delta = delta_map.get(token_id, {})
                move_pp = delta.get("move_pp")
                delta_label = "—" if move_pp is None else f"{move_pp:+.2f}pp"
                st.metric(
                    f"{outcome}",
                    f"${latest_price:.2f}",
                    delta_label,
                )

            if st.button(
                "View Details",
                key=f"live_watch_{market.get('market_id')}",
                width="stretch",
            ):
                st.switch_page("pages/2_Market_Detail.py")


def main():
    st.title("🟢 Live View")
    st.caption("Real-time movers, alerts, spikes, and watchlist activity.")

    with st.sidebar:
        st.markdown("### Live Settings")
        enable_refresh = st.toggle("Auto refresh", value=True)
        refresh_seconds = st.slider("Refresh interval (seconds)", 2, 30, 5)
        if enable_refresh and not AUTOREFRESH_AVAILABLE:
            st.caption("Auto-refresh dependency missing; using manual refresh only.")
        movers_window = st.select_slider(
            "Movers window",
            options=[30, 60, 120, 300, 600],
            value=120,
            format_func=lambda v: f"{v}s",
        )
        movers_limit = st.slider("Movers per source", 5, 50, 20)
        tape_window = st.select_slider(
            "Tape window",
            options=[60, 120, 300, 600],
            value=300,
            format_func=lambda v: f"{v}s",
        )
        tape_limit = st.slider("Tape rows per source", 10, 200, 60)

        st.markdown("---")
        st.markdown("### Volume Spikes")
        spikes_min_severity = st.selectbox(
            "Min severity",
            options=["low", "medium", "high", "extreme"],
            index=1,
        )
        spikes_limit = st.slider("Spike rows", 5, 100, 20)
        spikes_unack = st.toggle("Unacknowledged only", value=False)

        st.markdown("---")
        st.markdown("### Alerts")
        alerts_min_severity = st.selectbox(
            "Min alert severity",
            options=["low", "medium", "high", "extreme"],
            index=1,
        )
        alerts_limit = st.slider("Alert rows", 10, 200, 50)

        st.markdown("---")
        st.markdown("### Watchlist")
        watchlist_delta_minutes = st.slider("Delta window (minutes)", 1, 60, 5)
        watchlist_limit = st.slider("Watchlist markets", 5, 100, 20)

    if enable_refresh:
        st_autorefresh(interval=refresh_seconds * 1000, key="live_refresh")

    render_status_bar()
    render_system_health_panel()
    st.markdown("---")

    st.subheader("⚡ Live Movers")
    movers_col1, movers_col2 = st.columns(2)
    with movers_col1:
        st.markdown("**Polymarket**")
        render_live_movers(movers_window, movers_limit, source="polymarket")
    with movers_col2:
        st.markdown("**Kalshi**")
        render_live_movers(movers_window, movers_limit, source="kalshi")

    st.markdown("---")
    st.subheader("🧾 Snapshot Tape")
    tape_col1, tape_col2 = st.columns(2)
    with tape_col1:
        st.markdown("**Polymarket**")
        render_live_tape(tape_window, tape_limit, source="polymarket")
    with tape_col2:
        st.markdown("**Kalshi**")
        render_live_tape(tape_window, tape_limit, source="kalshi")

    st.markdown("---")
    col3, col4 = st.columns([2, 3])
    with col3:
        st.subheader("🚨 Live Alerts")
        render_alerts_stream(alerts_min_severity, alerts_limit)
    with col4:
        st.subheader("🔥 Volume Spikes")
        render_volume_spikes(spikes_min_severity, spikes_limit, spikes_unack)

    st.markdown("---")
    st.subheader("★ Watchlist Live Tiles")
    render_watchlist_tiles(watchlist_delta_minutes, watchlist_limit)


if __name__ == "__main__":
    main()
