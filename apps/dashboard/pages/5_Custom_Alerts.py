"""
Custom Alerts Page - Create and manage personalized price alerts.
"""

import streamlit as st
import pandas as pd
from datetime import datetime
import uuid

from packages.core.storage.queries import MarketQueries, UserAlertsQueries
from packages.core.storage import get_db_pool

st.set_page_config(
    page_title="Custom Alerts | PM Movers",
    page_icon="🔔",
    layout="wide",
)

# Initialize session ID for this user
if 'user_session_id' not in st.session_state:
    st.session_state.user_session_id = str(uuid.uuid4())

SESSION_ID = st.session_state.user_session_id

# Custom styling
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Space+Grotesk:wght@400;500;700&display=swap');

    .page-title {
        font-family: 'Space Grotesk', sans-serif;
        font-size: 2rem;
        font-weight: 600;
        color: #e4e4e7;
        margin-bottom: 1rem;
    }

    .alert-card {
        background: linear-gradient(135deg, #12121a 0%, #1a1a24 100%);
        border: 1px solid #2a2a3a;
        border-radius: 12px;
        padding: 1.25rem;
        margin-bottom: 1rem;
    }

    .alert-card.active {
        border-color: #00d4aa;
    }

    .alert-card.inactive {
        opacity: 0.6;
    }

    .notification-card {
        background: linear-gradient(135deg, #1a1a24 0%, #12121a 100%);
        border: 1px solid #fbbf24;
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 0.5rem;
    }

    .condition-tag {
        display: inline-block;
        padding: 0.25rem 0.5rem;
        border-radius: 4px;
        font-size: 0.75rem;
        font-weight: 600;
    }

    .condition-above {
        background: rgba(0, 212, 170, 0.2);
        color: #00d4aa;
    }

    .condition-below {
        background: rgba(255, 71, 87, 0.2);
        color: #ff4757;
    }

    .condition-change {
        background: rgba(88, 101, 242, 0.2);
        color: #5865f2;
    }
</style>
""", unsafe_allow_html=True)


def get_session_id():
    """Get or create a session ID for the current user."""
    return SESSION_ID


def main():
    st.markdown('<h1 class="page-title">🔔 Custom Alerts</h1>', unsafe_allow_html=True)
    st.markdown("Set up personalized price alerts for any market.")

    # Create tabs
    tab_create, tab_manage, tab_notifications = st.tabs(["Create Alert", "My Alerts", "Notifications"])

    with tab_create:
        create_alert_form()

    with tab_manage:
        manage_alerts()

    with tab_notifications:
        show_notifications()


def create_alert_form():
    """Form to create a new custom alert."""
    st.subheader("Create New Alert")

    # Fetch available markets
    db = get_db_pool()
    try:
        markets = db.execute("""
            SELECT m.market_id, m.title, m.source,
                   json_agg(json_build_object(
                       'token_id', mt.token_id,
                       'outcome', mt.outcome
                   )) as tokens
            FROM markets m
            JOIN market_tokens mt ON m.market_id = mt.market_id
            WHERE m.status = 'active'
            GROUP BY m.market_id
            ORDER BY m.updated_at DESC
            LIMIT 100
        """, fetch=True) or []
    except Exception as e:
        st.error(f"Error fetching markets: {e}")
        return

    if not markets:
        st.info("No markets available. Wait for the collector to sync data.")
        return

    # Market selector
    market_options = {
        f"{m['title'][:50]}... ({m['source']})" if len(m['title']) > 50 else f"{m['title']} ({m['source']})": m
        for m in markets
    }

    selected_label = st.selectbox("Select Market", options=list(market_options.keys()))

    if not selected_label:
        return

    selected_market = market_options[selected_label]
    tokens = selected_market.get('tokens', [])

    # Token selector (YES/NO)
    if tokens:
        token_options = {t['outcome']: t['token_id'] for t in tokens if t}
        selected_outcome = st.radio("Select Outcome", options=list(token_options.keys()), horizontal=True)
        selected_token_id = token_options[selected_outcome]
    else:
        st.warning("No tokens found for this market.")
        return

    # Alert condition
    st.markdown("---")
    st.markdown("**Alert Condition**")

    condition_type = st.selectbox(
        "Alert when price...",
        options=["above", "below", "change_pct"],
        format_func=lambda x: {
            "above": "Goes ABOVE threshold",
            "below": "Goes BELOW threshold",
            "change_pct": "Changes by % in time window"
        }[x]
    )

    col1, col2 = st.columns(2)

    with col1:
        if condition_type in ["above", "below"]:
            threshold = st.slider(
                "Price Threshold ($)",
                min_value=0.01,
                max_value=0.99,
                value=0.50,
                step=0.01,
                format="$%.2f"
            )
            window_seconds = None
        else:
            threshold = st.slider(
                "Change Threshold (%)",
                min_value=1.0,
                max_value=50.0,
                value=10.0,
                step=1.0,
                format="%.0f%%"
            )

    with col2:
        if condition_type == "change_pct":
            window_options = {
                "5 minutes": 300,
                "15 minutes": 900,
                "1 hour": 3600,
                "24 hours": 86400
            }
            window_label = st.selectbox("Time Window", options=list(window_options.keys()))
            window_seconds = window_options[window_label]
        else:
            window_seconds = None

    # Notification options
    notify_once = st.checkbox("Notify only once (then deactivate alert)", value=False)

    # Preview
    st.markdown("---")
    st.markdown("**Alert Preview**")

    if condition_type == "above":
        preview = f"Alert when **{selected_outcome}** price goes **above ${threshold:.2f}**"
    elif condition_type == "below":
        preview = f"Alert when **{selected_outcome}** price goes **below ${threshold:.2f}**"
    else:
        preview = f"Alert when **{selected_outcome}** price changes by **{threshold:.0f}%** in **{window_label}**"

    st.info(preview)

    # Submit
    if st.button("Create Alert", type="primary", width="stretch"):
        try:
            result = UserAlertsQueries.create_user_alert(
                session_id=get_session_id(),
                market_id=selected_market['market_id'],
                token_id=selected_token_id,
                condition_type=condition_type,
                threshold=threshold,
                window_seconds=window_seconds,
                notify_once=notify_once
            )
            if result:
                st.success("Alert created successfully!")
                st.rerun()
            else:
                st.error("Failed to create alert.")
        except Exception as e:
            st.error(f"Error creating alert: {e}")


def manage_alerts():
    """Display and manage existing alerts."""
    st.subheader("My Alerts")

    alerts = UserAlertsQueries.get_user_alerts(get_session_id(), active_only=False)

    if not alerts:
        st.info("You haven't created any alerts yet. Go to 'Create Alert' to get started.")
        return

    active_count = len([a for a in alerts if a['is_active']])
    st.markdown(f"**{active_count} active alert(s)** out of {len(alerts)} total")
    st.markdown("---")

    for alert in alerts:
        render_alert_card(alert)


def render_alert_card(alert: dict):
    """Render a single alert card."""
    is_active = alert.get('is_active', False)
    condition_type = alert.get('condition_type', 'unknown')
    threshold = float(alert.get('threshold', 0))
    current_price = float(alert.get('current_price', 0) or 0)

    # Condition display
    if condition_type == "above":
        condition_class = "condition-above"
        condition_text = f"Above ${threshold:.2f}"
    elif condition_type == "below":
        condition_class = "condition-below"
        condition_text = f"Below ${threshold:.2f}"
    else:
        condition_class = "condition-change"
        window_mins = (alert.get('window_seconds') or 3600) // 60
        if window_mins >= 60:
            window_str = f"{window_mins // 60}h"
        else:
            window_str = f"{window_mins}m"
        condition_text = f"{threshold:.0f}% in {window_str}"

    status_class = "active" if is_active else "inactive"
    status_text = "ACTIVE" if is_active else "INACTIVE"
    status_color = "#00d4aa" if is_active else "#71717a"

    st.markdown(f"""
    <div class="alert-card {status_class}">
        <div style="display: flex; justify-content: space-between; align-items: flex-start;">
            <div>
                <span class="condition-tag {condition_class}">{condition_text}</span>
                <span style="color: {status_color}; font-size: 0.7rem; margin-left: 0.5rem; font-weight: 600;">{status_text}</span>
            </div>
            <span style="color: #71717a; font-size: 0.75rem;">
                Triggered {alert.get('trigger_count', 0)}x
            </span>
        </div>
        <p style="color: #e4e4e7; margin-top: 0.5rem; font-weight: 500;">
            {alert.get('market_title', 'Unknown Market')[:60]}
        </p>
        <p style="color: #71717a; font-size: 0.85rem;">
            {alert.get('outcome', 'YES')} Token |
            Current: <span style="color: #5865f2;">${current_price:.2f}</span> |
            Created: {str(alert.get('created_at', ''))[:10]}
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Delete button
    if st.button("🗑️ Delete", key=f"delete_{alert['alert_id']}", width="stretch"):
        UserAlertsQueries.delete_user_alert(alert['alert_id'], get_session_id())
        st.rerun()


def show_notifications():
    """Display triggered alert notifications."""
    st.subheader("Notifications")

    col1, col2 = st.columns([3, 1])
    with col1:
        show_all = st.checkbox("Show acknowledged notifications", value=False)
    with col2:
        if st.button("Acknowledge All"):
            UserAlertsQueries.acknowledge_all_notifications(get_session_id())
            st.rerun()

    notifications = UserAlertsQueries.get_user_notifications(
        get_session_id(),
        unacknowledged_only=not show_all
    )

    if not notifications:
        st.info("No notifications yet. Your alerts will appear here when triggered.")
        return

    for notif in notifications:
        render_notification(notif)


def render_notification(notif: dict):
    """Render a notification card."""
    acknowledged = notif.get('acknowledged', False)
    opacity = "0.6" if acknowledged else "1"

    condition_type = notif.get('condition_type', 'unknown')
    if condition_type == "above":
        icon = "📈"
    elif condition_type == "below":
        icon = "📉"
    else:
        icon = "🔄"

    st.markdown(f"""
    <div class="notification-card" style="opacity: {opacity};">
        <div style="display: flex; justify-content: space-between;">
            <span style="font-size: 1.25rem;">{icon}</span>
            <span style="color: #71717a; font-size: 0.75rem;">
                {str(notif.get('triggered_at', ''))[:19]}
            </span>
        </div>
        <p style="color: #e4e4e7; margin-top: 0.5rem; font-weight: 500;">
            {notif.get('market_title', 'Unknown')} ({notif.get('outcome', 'YES')})
        </p>
        <p style="color: #a1a1aa; font-size: 0.9rem;">
            {notif.get('message', 'Alert triggered')}
        </p>
        <p style="color: #71717a; font-size: 0.85rem;">
            Price: ${float(notif.get('current_price', 0)):.2f} | Threshold: ${float(notif.get('threshold_price', 0)):.2f}
        </p>
    </div>
    """, unsafe_allow_html=True)

    if not acknowledged:
        if st.button("✓ Acknowledge", key=f"ack_{notif['notification_id']}", width="stretch"):
            UserAlertsQueries.acknowledge_notification(notif['notification_id'])
            st.rerun()


if __name__ == "__main__":
    main()
