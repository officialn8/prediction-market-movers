"""
Prediction Market Movers - Streamlit Dashboard

Main entry point for the dashboard application.
"""

import streamlit as st

from packages.core.settings import settings
from packages.core.storage import get_db_pool

# Page configuration
st.set_page_config(
    page_title="Prediction Market Movers",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for styling
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Space+Grotesk:wght@400;500;700&display=swap');
    
    :root {
        --pm-bg-dark: #0a0a0f;
        --pm-surface: #12121a;
        --pm-border: #1e1e2e;
        --pm-green: #00d4aa;
        --pm-red: #ff4757;
        --pm-blue: #5865f2;
        --pm-purple: #a855f7;
        --pm-text: #e4e4e7;
        --pm-muted: #71717a;
    }
    
    .stApp {
        background: linear-gradient(135deg, var(--pm-bg-dark) 0%, #0d0d14 100%);
    }
    
    .main-header {
        font-family: 'Space Grotesk', sans-serif;
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(135deg, var(--pm-green) 0%, var(--pm-blue) 50%, var(--pm-purple) 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin-bottom: 0.5rem;
    }
    
    .sub-header {
        font-family: 'JetBrains Mono', monospace;
        color: var(--pm-muted);
        font-size: 0.9rem;
        margin-bottom: 2rem;
    }
    
    .metric-card {
        background: var(--pm-surface);
        border: 1px solid var(--pm-border);
        border-radius: 12px;
        padding: 1.5rem;
        margin: 0.5rem 0;
    }
    
    .gain {
        color: var(--pm-green) !important;
    }
    
    .loss {
        color: var(--pm-red) !important;
    }
    
    .stMetric {
        background: var(--pm-surface);
        border: 1px solid var(--pm-border);
        border-radius: 8px;
        padding: 1rem;
    }
    
    .status-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 600;
        font-family: 'JetBrains Mono', monospace;
    }
    
    .status-healthy {
        background: rgba(0, 212, 170, 0.15);
        color: var(--pm-green);
        border: 1px solid var(--pm-green);
    }
    
    .status-error {
        background: rgba(255, 71, 87, 0.15);
        color: var(--pm-red);
        border: 1px solid var(--pm-red);
    }
</style>
""", unsafe_allow_html=True)


def check_database_connection() -> tuple[bool, str]:
    """Check if database is accessible."""
    try:
        db = get_db_pool()
        if db.health_check():
            stats = db.get_pool_stats()
            return True, f"Connected (pool: {stats.get('size', 0)}/{stats.get('max_size', 0)})"
        return False, "Health check failed"
    except Exception as e:
        return False, str(e)


def main():
    """Main dashboard page."""
    # Header
    st.markdown('<h1 class="main-header">📈 Prediction Market Movers</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Real-time tracking of Polymarket & Kalshi price movements</p>', unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.markdown("### ⚙️ System Status")
        
        # Database status
        db_healthy, db_status = check_database_connection()
        status_class = "status-healthy" if db_healthy else "status-error"
        status_icon = "✓" if db_healthy else "✗"
        st.markdown(
            f'<span class="status-badge {status_class}">{status_icon} Database</span>',
            unsafe_allow_html=True
        )
        st.caption(db_status)
        
        st.markdown("---")
        st.markdown("### 🔧 Settings")
        st.caption(f"Sync interval: {settings.sync_interval_seconds}s")
        st.caption(f"Log level: {settings.log_level}")
        
        st.markdown("---")
        st.markdown("### 📊 Sources")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Polymarket**")
            st.caption("Active" if settings.polymarket_api_key else "No API key")
        with col2:
            st.markdown("**Kalshi**")
            st.caption("Active" if settings.kalshi_api_key else "No API key")
    
    # Main content
    if not db_healthy:
        st.error("⚠️ Database connection failed. Please check your configuration.")
        st.code(f"DATABASE_URL: {settings.database_url[:50]}...")
        return

    # Check for alerts using Toast
    try:
        from packages.core.storage.queries import AnalyticsQueries
        from datetime import datetime, timezone
        
        if "last_alert_check" not in st.session_state:
            # First load: set to now so we don't spam old alerts
            st.session_state.last_alert_check = datetime.now(timezone.utc)
            
        # Get recent alerts 
        recent_alerts = AnalyticsQueries.get_recent_alerts(limit=5)
        
        # New alerts detected
        if recent_alerts:
            current_check_time = datetime.now(timezone.utc)
            
            # Show toasts for alerts created after our last check
            # Iterate in reverse to show oldest of the new ones first
            for alert in reversed(recent_alerts):
                # Ensure timezone awareness compatibility
                alert_ts = alert['created_at']
                if alert_ts.tzinfo is None:
                    alert_ts = alert_ts.replace(tzinfo=timezone.utc)
                
                if alert_ts > st.session_state.last_alert_check:
                    st.toast(
                        f"🚨 **{alert['market_title']}**\n\n"
                        f"{float(alert['move_pp']):.2f}% move ({alert['outcome']})", 
                        icon="🔥"
                    )
            
            st.session_state.last_alert_check = current_check_time

    except Exception as e:
        # Log to console but don't break UI
        print(f"Alert toast error: {e}")

    
    # Quick stats
    st.markdown("### 📊 Quick Stats")
    col1, col2, col3, col4 = st.columns(4)
    
    # Fetch stats from database
    db = get_db_pool()
    
    try:
        market_count = db.execute(
            "SELECT COUNT(*) as count FROM markets WHERE status = 'active'",
            fetch=True
        )
        token_count = db.execute(
            "SELECT COUNT(*) as count FROM market_tokens",
            fetch=True
        )
        snapshot_count = db.execute(
            "SELECT COUNT(*) as count FROM snapshots",
            fetch=True
        )
        latest_snapshot = db.execute(
            "SELECT MAX(ts) as latest FROM snapshots",
            fetch=True
        )
        
        with col1:
            st.metric("Active Markets", market_count[0]["count"] if market_count else 0)
        with col2:
            st.metric("Tokens Tracked", token_count[0]["count"] if token_count else 0)
        with col3:
            st.metric("Price Snapshots", snapshot_count[0]["count"] if snapshot_count else 0)
        with col4:
            latest = latest_snapshot[0]["latest"] if latest_snapshot and latest_snapshot[0]["latest"] else "No data"
            st.metric("Last Update", str(latest)[:19] if latest != "No data" else latest)
            
    except Exception as e:
        st.warning(f"Could not fetch stats: {e}")
    
    st.markdown("---")
    
    # Navigation hint
    st.info("""
    👈 **Navigate using the sidebar pages:**
    - **Top Movers** - See biggest price changes
    - **Market Detail** - Explore individual markets
    
    ⏳ *Data will appear once the collector service starts syncing markets.*
    """)
    
    # Footer
    st.markdown("---")
    st.markdown(
        '<p style="text-align: center; color: #71717a; font-size: 0.8rem;">'
        'Built with Streamlit • Data from Polymarket & Kalshi'
        '</p>',
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()

