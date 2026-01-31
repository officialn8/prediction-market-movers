"""
Prediction Market Movers - Combined Dashboard
Real-time tracking of Polymarket & Kalshi price movements
"""

import streamlit as st
from datetime import datetime, timezone

from packages.core.settings import settings
from packages.core.storage import get_db_pool
from packages.core.storage.queries import MarketQueries, AnalyticsQueries
from packages.core.wss import WSSMetrics

# Page configuration
st.set_page_config(
    page_title="Prediction Market Movers",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)


def init_theme():
    """Initialize theme in session state."""
    if "dark_mode" not in st.session_state:
        st.session_state.dark_mode = False


def get_theme_css():
    """Generate CSS based on current theme."""
    dark_mode = st.session_state.get("dark_mode", False)
    
    if dark_mode:
        return """
        <style>
            @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap');
            
            :root {
                --pm-bg: #0c0c10;
                --pm-surface: #14141a;
                --pm-surface-2: #1c1c24;
                --pm-border: #2a2a36;
                --pm-accent: #6366f1;
                --pm-accent-light: #818cf8;
                --pm-green: #10b981;
                --pm-green-bg: rgba(16, 185, 129, 0.12);
                --pm-red: #ef4444;
                --pm-red-bg: rgba(239, 68, 68, 0.12);
                --pm-orange: #f59e0b;
                --pm-orange-bg: rgba(245, 158, 11, 0.12);
                --pm-text: #f4f4f5;
                --pm-text-secondary: #a1a1aa;
                --pm-text-muted: #71717a;
            }
            
            .stApp {
                background: var(--pm-bg) !important;
            }
            
            .stApp > header {
                background: transparent !important;
            }
            
            section[data-testid="stSidebar"] {
                background: var(--pm-surface) !important;
                border-right: 1px solid var(--pm-border) !important;
            }
            
            .main .block-container {
                padding-top: 2rem;
                max-width: 1400px;
            }
        </style>
        """
    else:
        return """
        <style>
            @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap');
            
            :root {
                --pm-bg: #fafafa;
                --pm-surface: #ffffff;
                --pm-surface-2: #f4f4f5;
                --pm-border: #e4e4e7;
                --pm-accent: #4f46e5;
                --pm-accent-light: #6366f1;
                --pm-green: #059669;
                --pm-green-bg: rgba(5, 150, 105, 0.08);
                --pm-red: #dc2626;
                --pm-red-bg: rgba(220, 38, 38, 0.08);
                --pm-orange: #d97706;
                --pm-orange-bg: rgba(217, 119, 6, 0.08);
                --pm-text: #18181b;
                --pm-text-secondary: #52525b;
                --pm-text-muted: #a1a1aa;
            }
            
            .stApp {
                background: var(--pm-bg) !important;
            }
            
            .stApp > header {
                background: transparent !important;
            }
            
            section[data-testid="stSidebar"] {
                background: var(--pm-surface) !important;
                border-right: 1px solid var(--pm-border) !important;
            }
            
            .main .block-container {
                padding-top: 2rem;
                max-width: 1400px;
            }
        </style>
        """


def get_component_css():
    """Get component styles that work with both themes."""
    return """
    <style>
        .header-container {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1.5rem;
            padding-bottom: 1rem;
            border-bottom: 1px solid var(--pm-border);
        }
        
        .logo-section {
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }
        
        .logo-icon {
            width: 40px;
            height: 40px;
            background: linear-gradient(135deg, var(--pm-accent) 0%, #8b5cf6 100%);
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.25rem;
        }
        
        .logo-text {
            font-family: 'DM Sans', sans-serif;
            font-size: 1.5rem;
            font-weight: 700;
            color: var(--pm-text);
            margin: 0;
        }
        
        .logo-subtitle {
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.75rem;
            color: var(--pm-text-muted);
            margin: 0;
        }
        
        .status-row {
            display: flex;
            align-items: center;
            gap: 1rem;
        }
        
        .status-badge {
            display: inline-flex;
            align-items: center;
            gap: 0.375rem;
            padding: 0.375rem 0.75rem;
            border-radius: 9999px;
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.75rem;
            font-weight: 600;
        }
        
        .status-connected {
            background: var(--pm-green-bg);
            color: var(--pm-green);
            border: 1px solid var(--pm-green);
        }
        
        .status-polling {
            background: var(--pm-orange-bg);
            color: var(--pm-orange);
            border: 1px solid var(--pm-orange);
        }
        
        .status-disconnected {
            background: var(--pm-red-bg);
            color: var(--pm-red);
            border: 1px solid var(--pm-red);
        }
        
        .status-dot {
            width: 6px;
            height: 6px;
            border-radius: 50%;
            animation: pulse 2s infinite;
        }
        
        .status-dot.connected { background: var(--pm-green); }
        .status-dot.polling { background: var(--pm-orange); }
        .status-dot.disconnected { background: var(--pm-red); }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 1rem;
            margin-bottom: 1.5rem;
        }
        
        @media (max-width: 768px) {
            .stats-grid {
                grid-template-columns: repeat(2, 1fr);
            }
        }
        
        .stat-card {
            background: var(--pm-surface);
            border: 1px solid var(--pm-border);
            border-radius: 12px;
            padding: 1.25rem;
        }
        
        .stat-label {
            font-family: 'DM Sans', sans-serif;
            font-size: 0.8rem;
            color: var(--pm-text-muted);
            margin-bottom: 0.375rem;
            text-transform: uppercase;
            letter-spacing: 0.025em;
        }
        
        .stat-value {
            font-family: 'IBM Plex Mono', monospace;
            font-size: 1.5rem;
            font-weight: 600;
            color: var(--pm-text);
        }
        
        .section-header {
            font-family: 'DM Sans', sans-serif;
            font-size: 1.125rem;
            font-weight: 600;
            color: var(--pm-text);
            margin: 1.5rem 0 1rem 0;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }
        
        .mover-card {
            background: var(--pm-surface);
            border: 1px solid var(--pm-border);
            border-radius: 12px;
            padding: 1rem 1.25rem;
            margin-bottom: 0.625rem;
            transition: border-color 0.15s ease;
        }
        
        .mover-card:hover {
            border-color: var(--pm-accent);
        }
        
        .mover-header {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 0.5rem;
        }
        
        .mover-tags {
            display: flex;
            gap: 0.375rem;
            flex-wrap: wrap;
            margin-bottom: 0.5rem;
        }
        
        .tag {
            display: inline-flex;
            align-items: center;
            padding: 0.2rem 0.5rem;
            border-radius: 4px;
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.65rem;
            font-weight: 600;
            text-transform: uppercase;
        }
        
        .tag-source {
            background: rgba(99, 102, 241, 0.12);
            color: var(--pm-accent);
        }
        
        .tag-yes {
            background: var(--pm-green-bg);
            color: var(--pm-green);
        }
        
        .tag-no {
            background: var(--pm-red-bg);
            color: var(--pm-red);
        }
        
        .tag-category {
            background: var(--pm-surface-2);
            color: var(--pm-text-secondary);
        }
        
        .mover-title {
            font-family: 'DM Sans', sans-serif;
            font-size: 0.95rem;
            font-weight: 500;
            color: var(--pm-text);
            line-height: 1.4;
            margin: 0;
        }
        
        .mover-price {
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.8rem;
            color: var(--pm-text-muted);
            margin-top: 0.25rem;
        }
        
        .mover-change {
            font-family: 'IBM Plex Mono', monospace;
            font-size: 1.375rem;
            font-weight: 600;
            text-align: right;
        }
        
        .mover-change.positive { color: var(--pm-green); }
        .mover-change.negative { color: var(--pm-red); }
        
        .mover-reason {
            font-family: 'DM Sans', sans-serif;
            font-size: 0.8rem;
            color: var(--pm-text-secondary);
            margin-top: 0.5rem;
        }
        
        .filter-section {
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 0.75rem;
            margin-bottom: 1rem;
            padding: 0.75rem 1rem;
            background: var(--pm-surface);
            border: 1px solid var(--pm-border);
            border-radius: 10px;
        }
        
        .empty-state {
            text-align: center;
            padding: 3rem 1rem;
            color: var(--pm-text-muted);
        }
        
        .empty-state-icon {
            font-size: 2.5rem;
            margin-bottom: 0.75rem;
        }
        
        .wss-metrics {
            display: flex;
            gap: 1rem;
            margin-top: 0.5rem;
        }
        
        .wss-metric {
            font-family: 'IBM Plex Mono', monospace;
            font-size: 0.7rem;
            color: var(--pm-text-muted);
        }
        
        .wss-metric strong {
            color: var(--pm-text-secondary);
        }
    </style>
    """


def check_database_connection() -> tuple[bool, str]:
    """Check if database is accessible."""
    try:
        db = get_db_pool()
        if db.health_check():
            stats = db.get_pool_stats()
            return True, f"pool {stats.get('size', 0)}/{stats.get('max_size', 0)}"
        return False, "Health check failed"
    except Exception as e:
        return False, str(e)


def get_wss_status() -> dict:
    """Get accurate WSS connection status by checking actual data flow."""
    # Use activity-based check for accurate status
    metrics = WSSMetrics.load_with_activity_check()
    
    # Determine actual status based on metrics
    status = {
        "mode": metrics.mode,
        "display_mode": metrics.mode.upper(),
        "messages_per_second": metrics.messages_per_second,
        "subscriptions": metrics.current_subscriptions,
        "last_message_age": metrics.last_message_age_seconds,
        "reconnections": metrics.reconnection_count,
    }
    
    # Update last message age based on stored time
    if metrics.last_message_time > 0:
        import time
        status["last_message_age"] = time.time() - metrics.last_message_time
    
    # Check if DB has recent data (within 5 minutes) even if WSS is "disconnected"
    db_has_recent_data = False
    try:
        db = get_db_pool()
        result = db.execute("""
            SELECT COUNT(*) as cnt FROM snapshots 
            WHERE ts > NOW() - INTERVAL '5 minutes'
        """, fetch=True)
        db_has_recent_data = result and result[0]['cnt'] > 0
    except Exception:
        pass
    
    # Determine status class
    if metrics.mode == "wss":
        status["class"] = "connected"
        status["icon"] = "●"
        status["display_mode"] = "LIVE"
    elif metrics.mode == "polling":
        status["class"] = "polling"
        status["icon"] = "◐"
        status["display_mode"] = "POLLING"
    elif db_has_recent_data:
        # Data exists but collector might be between syncs
        status["class"] = "polling"
        status["icon"] = "◐"
        status["display_mode"] = "SYNCING"
    else:
        status["class"] = "disconnected"
        status["icon"] = "○"
        status["display_mode"] = "OFFLINE"
    
    return status


def get_stats() -> dict:
    """Fetch dashboard stats from database."""
    db = get_db_pool()
    stats = {
        "markets": 0,
        "tokens": 0,
        "snapshots": 0,
        "last_update": "—",
    }
    
    try:
        market_count = db.execute(
            "SELECT COUNT(*) as count FROM markets WHERE status = 'active'",
            fetch=True
        )
        stats["markets"] = market_count[0]["count"] if market_count else 0
        
        token_count = db.execute(
            "SELECT COUNT(*) as count FROM market_tokens",
            fetch=True
        )
        stats["tokens"] = token_count[0]["count"] if token_count else 0
        
        snapshot_count = db.execute(
            "SELECT COUNT(*) as count FROM snapshots",
            fetch=True
        )
        stats["snapshots"] = snapshot_count[0]["count"] if snapshot_count else 0
        
        latest_snapshot = db.execute(
            "SELECT MAX(ts) as latest FROM snapshots",
            fetch=True
        )
        if latest_snapshot and latest_snapshot[0]["latest"]:
            latest = latest_snapshot[0]["latest"]
            stats["last_update"] = str(latest)[11:19]  # Just time portion
    except Exception:
        pass
    
    return stats


def format_volume(volume: float) -> str:
    """Format volume as human-readable string."""
    if volume >= 1_000_000:
        return f"${volume/1_000_000:.1f}M"
    elif volume >= 1_000:
        return f"${volume/1_000:.1f}k"
    else:
        return f"${volume:.0f}"


def render_header():
    """Render the header with logo, status, and theme toggle."""
    wss_status = get_wss_status()
    db_healthy, db_info = check_database_connection()
    
    # Build status HTML
    wss_class = wss_status["class"]
    wss_display = wss_status["display_mode"]
    
    # WSS metrics for connected state
    wss_metrics_html = ""
    if wss_status["mode"] == "wss":
        wss_metrics_html = f"""
        <div class="wss-metrics">
            <span class="wss-metric"><strong>{wss_status['messages_per_second']:.1f}</strong> msg/s</span>
            <span class="wss-metric"><strong>{wss_status['subscriptions']}</strong> subs</span>
            <span class="wss-metric">Last: <strong>{wss_status['last_message_age']:.0f}s</strong> ago</span>
        </div>
        """
    
    # DB status badge
    db_class = "connected" if db_healthy else "disconnected"
    db_icon = "●" if db_healthy else "○"
    
    st.markdown(f"""
    <div class="header-container">
        <div class="logo-section">
            <div class="logo-icon">📈</div>
            <div>
                <p class="logo-text">Prediction Market Movers</p>
                <p class="logo-subtitle">Polymarket & Kalshi • Live</p>
            </div>
        </div>
        <div>
            <div class="status-row">
                <div class="status-badge status-{wss_class}">
                    <span class="status-dot {wss_class}"></span>
                    {wss_display}
                </div>
                <div class="status-badge status-{db_class}">
                    {db_icon} DB {db_info}
                </div>
            </div>
            {wss_metrics_html}
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_stats():
    """Render quick stats cards."""
    stats = get_stats()
    
    st.markdown(f"""
    <div class="stats-grid">
        <div class="stat-card">
            <div class="stat-label">Active Markets</div>
            <div class="stat-value">{stats['markets']:,}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Tokens Tracked</div>
            <div class="stat-value">{stats['tokens']:,}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Price Snapshots</div>
            <div class="stat-value">{stats['snapshots']:,}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Last Update</div>
            <div class="stat-value">{stats['last_update']}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_mover_card(mover: dict):
    """Render a single mover card."""
    pct_change = float(mover.get("pct_change") or mover.get("move_pp") or 0)
    change_class = "positive" if pct_change > 0 else "negative"
    change_sign = "+" if pct_change > 0 else ""
    
    source = mover.get("source", "unknown").upper()
    outcome = mover.get("outcome", "YES")
    outcome_class = "yes" if outcome == "YES" else "no"
    
    latest_price = float(mover.get("latest_price") or mover.get("price_now") or 0)
    old_price = float(mover.get("old_price") or mover.get("price_then") or 0)
    volume = float(mover.get("latest_volume") or mover.get("current_volume") or mover.get("volume_24h") or 0)
    
    title = mover.get('title', 'Unknown Market')
    category = mover.get('category', '')
    
    # Generate reason
    direction = "spiked" if pct_change > 0 else "dropped"
    vol_str = format_volume(volume)
    reason = f"{outcome} {direction} {abs(pct_change):.1f}pp on {vol_str} volume"
    
    # Category tag
    category_html = f'<span class="tag tag-category">{category}</span>' if category else ''
    
    html_content = f"""<div class="mover-card"><div class="mover-header"><div style="flex: 1;"><div class="mover-tags"><span class="tag tag-source">{source}</span><span class="tag tag-{outcome_class}">{outcome}</span>{category_html}</div><p class="mover-title">{title}</p><p class="mover-price">${old_price:.2f} → ${latest_price:.2f}</p></div><div class="mover-change {change_class}">{change_sign}{pct_change:.1f}pp</div></div><div class="mover-reason">📊 {reason}</div></div>"""
    
    st.markdown(html_content, unsafe_allow_html=True)


def main():
    """Main dashboard - combined landing page."""
    init_theme()
    
    # Apply theme CSS
    st.markdown(get_theme_css(), unsafe_allow_html=True)
    st.markdown(get_component_css(), unsafe_allow_html=True)
    
    # Theme toggle in sidebar
    with st.sidebar:
        st.markdown("### ⚙️ Settings")
        
        dark_mode = st.toggle(
            "Dark Mode",
            value=st.session_state.dark_mode,
            key="theme_toggle"
        )
        if dark_mode != st.session_state.dark_mode:
            st.session_state.dark_mode = dark_mode
            st.rerun()
        
        st.markdown("---")
        
        # Additional settings
        st.caption(f"**Sync Interval:** {settings.sync_interval_seconds}s")
        st.caption(f"**Log Level:** {settings.log_level}")
        
        st.markdown("---")
        st.markdown("### 📊 Data Sources")
        
        poly_status = "✓ Active" if settings.polymarket_api_key else "○ No key"
        kalshi_status = "✓ Active" if settings.kalshi_api_key else "○ No key"
        
        st.caption(f"**Polymarket:** {poly_status}")
        st.caption(f"**Kalshi:** {kalshi_status}")
    
    # Check database
    db_healthy, _ = check_database_connection()
    if not db_healthy:
        st.error("⚠️ Database connection failed. Please check your configuration.")
        return
    
    # Header with status
    render_header()
    
    # Stats row
    render_stats()
    
    # Search bar
    search_query = st.text_input(
        "Search markets",
        placeholder="Search: trump, bitcoin, fed, elections...",
        label_visibility="collapsed",
    )
    
    if search_query:
        results = MarketQueries.search_markets(search_query)
        if not results:
            st.info(f"No markets found for '{search_query}'")
        else:
            st.markdown(f'<div class="section-header">🔍 Results for "{search_query}"</div>', unsafe_allow_html=True)
            ids = [str(r['market_id']) for r in results[:20]]
            full_data = MarketQueries.get_markets_batch_with_prices(ids)
            for market in full_data:
                tokens = market.get('tokens', [])
                if not tokens:
                    continue
                top_token = tokens[0]
                mover_wrapper = {
                    'market_id': market['market_id'],
                    'token_id': top_token.get('token_id'),
                    'title': market['title'],
                    'source': market['source'],
                    'category': market['category'],
                    'outcome': top_token.get('outcome'),
                    'latest_price': top_token.get('latest_price', 0),
                    'latest_volume': top_token.get('latest_volume', 0),
                    'pct_change': 0,
                    'old_price': 0
                }
                render_mover_card(mover_wrapper)
        return
    
    # Section header
    st.markdown('<div class="section-header">🔥 What\'s Moving Now</div>', unsafe_allow_html=True)
    
    # Filters row
    col1, col2 = st.columns([1, 3])
    
    with col1:
        tf_map = {"5min": 5, "1hr": 60, "24hr": 1440, "7day": 10080}
        selected_tf = st.selectbox(
            "Timeframe",
            options=list(tf_map.keys()),
            index=1,
            label_visibility="collapsed"
        )
        window_minutes = tf_map[selected_tf]
    
    with col2:
        CATEGORIES = [
            "All", "Politics", "Sports", "Crypto", "Finance", "Geopolitics",
            "Tech", "Culture", "World", "Economy", "Climate & Science", "Elections"
        ]
        selected_category = st.selectbox(
            "Category",
            options=CATEGORIES,
            index=0,
            label_visibility="collapsed"
        )
    
    category_filter = None if selected_category == "All" else selected_category
    
    # Fetch movers
    try:
        window_seconds = window_minutes * 60
        movers = []
        
        # Try cached first for standard windows
        cached_windows = [300, 3600, 86400]
        if window_seconds in cached_windows:
            movers = AnalyticsQueries.get_cached_movers(
                window_seconds=window_seconds,
                limit=30,
                category=category_filter,
                direction="both"
            )
        
        if not movers:
            movers = MarketQueries.get_movers_window(
                window_seconds=window_seconds,
                limit=30,
                category=category_filter,
                direction="both"
            )
        
        if not movers:
            st.markdown("""
            <div class="empty-state">
                <div class="empty-state-icon">📭</div>
                <p>No movers found for this timeframe and category.</p>
                <p style="font-size: 0.85rem;">Try a different filter or wait for more data.</p>
            </div>
            """, unsafe_allow_html=True)
            return
        
        # Render mover cards
        for mover in movers:
            render_mover_card(mover)
            
    except Exception as e:
        st.error(f"Error loading movers: {e}")


if __name__ == "__main__":
    main()
