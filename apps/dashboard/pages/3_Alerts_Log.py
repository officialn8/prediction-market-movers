from datetime import timedelta

import pandas as pd
import streamlit as st

from packages.core.analytics import metrics
from packages.core.storage.queries import AnalyticsQueries

st.set_page_config(page_title="Alerts Log", page_icon="🚨", layout="wide")

st.title("🚨 Alerts Log")
st.markdown("History of significant market movements.")


SEVERITY_ORDER = {"none": 0, "notable": 1, "significant": 2, "extreme": 3}


def compute_hours_to_expiry(alert: dict) -> float | None:
    raw_hours = alert.get("hours_to_expiry")
    if raw_hours is not None:
        try:
            return float(raw_hours)
        except (TypeError, ValueError):
            return None

    created_at = pd.to_datetime(alert.get("created_at"), utc=True, errors="coerce")
    end_date = pd.to_datetime(alert.get("end_date"), utc=True, errors="coerce")
    if pd.isna(created_at) or pd.isna(end_date):
        return None
    return float((end_date - created_at).total_seconds() / 3600.0)


def compute_volume_at_alert(alert: dict) -> float | None:
    raw_volume = alert.get("volume_at_alert")
    if raw_volume is None:
        return None
    try:
        return float(raw_volume)
    except (TypeError, ValueError):
        return None


def render_filters():
    col1, col2, col3, col4 = st.columns([2, 2, 2, 3])
    with col1:
        limit = st.slider("Alerts to show", 10, 200, 50, step=10)
    with col2:
        min_severity = st.selectbox(
            "Min severity",
            options=["notable", "significant", "extreme", "none"],
            index=0,
        )
    with col3:
        unack_only = st.toggle("Unacknowledged only", value=False)
    with col4:
        hours = st.selectbox(
            "Time window",
            options=[1, 3, 6, 12, 24, 72, "All"],
            index=3,
        )
    return {
        "limit": limit,
        "min_severity": min_severity,
        "unack_only": unack_only,
        "hours": hours,
    }


def get_alerts_data(limit: int, unack_only: bool) -> list[dict]:
    return AnalyticsQueries.get_recent_alerts(
        limit=limit,
        unconverged_only=unack_only,
        dedupe_market_events=True,
        exclude_expired=True,
    )


def normalize_alerts(alerts: list[dict]) -> pd.DataFrame:
    rows = []
    for alert in alerts:
        move_pp = float(alert.get("move_pp") or 0)
        hours_to_expiry = compute_hours_to_expiry(alert)
        volume_at_alert = compute_volume_at_alert(alert)
        if metrics.should_suppress_settlement_snap(
            move_pp=move_pp,
            hours_to_expiry=hours_to_expiry,
        ):
            continue
        rows.append(
            {
                "Time": alert.get("created_at"),
                "Market": alert.get("market_title"),
                "Outcome": alert.get("outcome"),
                "Move (pp)": move_pp,
                "Threshold (pp)": float(alert.get("threshold_pp") or 0),
                "Window (sec)": int(alert.get("window_seconds") or 0),
                "Hours to Expiry": hours_to_expiry,
                "Volume at Alert ($)": volume_at_alert,
                "Severity": metrics.classify_alert_severity(
                    move_pp=move_pp,
                    hours_to_expiry=hours_to_expiry,
                    volume=volume_at_alert,
                ),
                "Source": (alert.get("source") or "unknown").upper(),
                "Reason": alert.get("reason"),
                "Symbol": alert.get("symbol"),
                "Acknowledged": alert.get("acknowledged_at"),
            }
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["Time"] = pd.to_datetime(df["Time"], utc=True, errors="coerce")
    df["Acknowledged"] = pd.to_datetime(
        df["Acknowledged"],
        utc=True,
        errors="coerce",
    )
    df["Hours to Expiry"] = pd.to_numeric(df["Hours to Expiry"], errors="coerce")
    df["Volume at Alert ($)"] = pd.to_numeric(df["Volume at Alert ($)"], errors="coerce")
    df = df[df["Hours to Expiry"].isna() | (df["Hours to Expiry"] > 0)]
    return df


def apply_filters(df: pd.DataFrame, min_severity: str, hours) -> pd.DataFrame:
    if df.empty:
        return df
    min_level = SEVERITY_ORDER.get(min_severity, 1)
    df = df[df["Severity"].map(lambda value: SEVERITY_ORDER.get(value, -1)) >= min_level]
    if hours != "All":
        cutoff = pd.Timestamp.now(tz="UTC") - timedelta(hours=int(hours))
        df = df[df["Time"] >= cutoff]
    return df


def render_source_filter(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    sources = sorted(df["Source"].dropna().unique().tolist())
    selected = st.multiselect(
        "Source",
        options=sources,
        default=sources,
    )
    if not selected:
        return df.iloc[0:0]
    return df[df["Source"].isin(selected)]


def render_alerts_table(df: pd.DataFrame):
    if df.empty:
        st.info("No alerts matched these filters.")
        return
    st.dataframe(
        df[
            [
                "Time",
                "Market",
                "Outcome",
                "Move (pp)",
                "Threshold (pp)",
                "Window (sec)",
                "Hours to Expiry",
                "Volume at Alert ($)",
                "Severity",
                "Source",
                "Reason",
                "Symbol",
                "Acknowledged",
            ]
        ],
        column_config={
            "Time": st.column_config.DatetimeColumn(format="D MMM, HH:mm:ss"),
            "Move (pp)": st.column_config.NumberColumn(format="%.2f"),
            "Threshold (pp)": st.column_config.NumberColumn(format="%.2f"),
            "Hours to Expiry": st.column_config.NumberColumn(format="%.1f h"),
            "Volume at Alert ($)": st.column_config.NumberColumn(format="$%.0f"),
            "Acknowledged": st.column_config.DatetimeColumn(
                format="D MMM, HH:mm:ss",
            ),
        },
        width="stretch",
        hide_index=True,
    )


filters = render_filters()
alerts = get_alerts_data(filters["limit"], filters["unack_only"])

if not alerts:
    st.info("No alerts found.")
else:
    df = normalize_alerts(alerts)
    df = apply_filters(df, filters["min_severity"], filters["hours"])
    df = render_source_filter(df)
    render_alerts_table(df)

with st.expander("Next to implement"):
    st.markdown(
        "- Acknowledge alerts in the dashboard\n"
        "- Link to market detail pages\n"
        "- Show volume context alongside move alerts\n"
        "- Persist user filter preferences"
    )
