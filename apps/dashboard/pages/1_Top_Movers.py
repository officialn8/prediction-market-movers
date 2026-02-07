"""
Advanced Movers - Extended filtering and analysis view
"""

import streamlit as st

from packages.core.storage import get_db_pool
from packages.core.storage.queries import MarketQueries, AnalyticsQueries
from apps.dashboard.components import render_mover_card, init_watchlist

st.set_page_config(
    page_title="Advanced Movers | PM Movers",
    page_icon="📊",
    layout="wide",
)

# Inherit theme from main app
def get_theme_css():
    """Get theme CSS based on session state."""
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
                --pm-green: #10b981;
                --pm-green-bg: rgba(16, 185, 129, 0.12);
                --pm-red: #ef4444;
                --pm-red-bg: rgba(239, 68, 68, 0.12);
                --pm-text: #f4f4f5;
                --pm-text-secondary: #a1a1aa;
                --pm-text-muted: #71717a;
            }
            
            .stApp { background: var(--pm-bg) !important; }
            section[data-testid="stSidebar"] { background: var(--pm-surface) !important; }
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
                --pm-green: #059669;
                --pm-green-bg: rgba(5, 150, 105, 0.08);
                --pm-red: #dc2626;
                --pm-red-bg: rgba(220, 38, 38, 0.08);
                --pm-text: #18181b;
                --pm-text-secondary: #52525b;
                --pm-text-muted: #a1a1aa;
            }
            
            .stApp { background: var(--pm-bg) !important; }
            section[data-testid="stSidebar"] { background: var(--pm-surface) !important; }
        </style>
        """


def _hydrate_market_context(movers: list[dict]) -> list[dict]:
    """Hydrate mover market fields from markets table, always refreshing canonical URL."""
    if not movers:
        return movers

    missing_ids = {
        str(m.get("market_id"))
        for m in movers
        if m.get("market_id")
    }

    if not missing_ids:
        return movers

    db = get_db_pool()
    rows = db.execute(
        """
        SELECT market_id, title, source, source_id, category, url
        FROM markets
        WHERE market_id = ANY(%s::uuid[])
        """,
        (list(missing_ids),),
        fetch=True,
    ) or []

    lookup = {str(row["market_id"]): row for row in rows}
    for mover in movers:
        market_id = str(mover.get("market_id") or "")
        if market_id in lookup:
            record = lookup[market_id]
            for key in ("title", "source", "source_id", "category"):
                if not mover.get(key) and record.get(key):
                    mover[key] = record.get(key)
            # Always trust canonical URL from markets table over cached mover payload.
            mover["url"] = record.get("url") or ""
    return movers


def _summarize_missing_fields(movers: list[dict]) -> dict:
    """Count movers missing key fields for diagnostics."""
    missing = {"market_id": 0, "title": 0, "url": 0}
    for mover in movers:
        if not mover.get("market_id"):
            missing["market_id"] += 1
        if not mover.get("title"):
            missing["title"] += 1
        if not mover.get("url"):
            missing["url"] += 1
    return missing


def _safe_float(value, default: float = 0.0) -> float:
    """Safely parse values used in numeric filters/labels."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _volume_for_display(mover: dict) -> float:
    """Prefer fallback display volume when available."""
    display_volume = mover.get("display_volume")
    if display_volume is not None:
        return _safe_float(display_volume)
    return _safe_float(
        mover.get("latest_volume")
        or mover.get("current_volume")
        or mover.get("volume_24h")
        or 0
    )


def _apply_stale_volume_fallback(
    movers: list[dict],
    *,
    enabled: bool,
) -> list[dict]:
    """
    Display-only fallback for movers.

    Keeps freshness gating for ranking/scoring untouched, but optionally surfaces
    last-known volume in the card UI when fresh volume is unavailable.
    """
    if not movers or not enabled:
        return movers

    token_ids: list[str] = []
    for mover in movers:
        token_id = mover.get("token_id")
        if not token_id:
            continue
        if _volume_for_display(mover) > 0:
            continue
        token_ids.append(str(token_id))

    if not token_ids:
        return movers

    db = get_db_pool()
    rows = db.execute(
        """
        SELECT
            token_id,
            volume_24h,
            volume_source,
            volume_age_seconds,
            is_volume_fresh
        FROM v_latest_volumes
        WHERE token_id = ANY(%s::uuid[])
          AND has_volume_data = true
          AND volume_24h IS NOT NULL
          AND volume_24h > 0
        """,
        (list(set(token_ids)),),
        fetch=True,
    ) or []

    volume_lookup = {str(row["token_id"]): row for row in rows}
    for mover in movers:
        token_id = str(mover.get("token_id") or "")
        row = volume_lookup.get(token_id)
        if not row:
            continue
        mover["display_volume"] = _safe_float(row.get("volume_24h"))
        mover["display_volume_source"] = row.get("volume_source")
        mover["display_volume_age_seconds"] = _safe_float(
            row.get("volume_age_seconds"),
            default=0.0,
        )
        mover["display_volume_is_stale"] = not bool(row.get("is_volume_fresh"))

    return movers


def main():
    init_watchlist()
    
    # Apply theme
    st.markdown(get_theme_css(), unsafe_allow_html=True)
    
    st.title("📊 Advanced Movers")
    st.caption("Extended filtering and analysis for power users")
    
    # Advanced filters in columns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        tf_options = {
            "5 minutes": 5,
            "15 minutes": 15,
            "30 minutes": 30,
            "1 hour": 60,
            "4 hours": 240,
            "12 hours": 720,
            "24 hours": 1440,
            "7 days": 10080,
        }
        selected_tf = st.selectbox("Timeframe", options=list(tf_options.keys()), index=3)
        window_minutes = tf_options[selected_tf]
    
    with col2:
        CATEGORIES = [
            "All Categories", "Politics", "Sports", "Crypto", "Finance", 
            "Geopolitics", "Earnings", "Tech", "Culture", "World", 
            "Economy", "Climate & Science", "Elections"
        ]
        selected_category = st.selectbox("Category", options=CATEGORIES)
        category_filter = None if selected_category == "All Categories" else selected_category
    
    with col3:
        direction_options = {
            "Both": "both",
            "Gainers Only": "up",
            "Losers Only": "down",
        }
        selected_direction = st.selectbox("Direction", options=list(direction_options.keys()))
        direction = direction_options[selected_direction]
    
    with col4:
        source_options = ["All Sources", "Polymarket", "Kalshi"]
        selected_source = st.selectbox("Source", options=source_options)
        source_filter = None if selected_source == "All Sources" else selected_source.lower()
    
    # Additional filters
    with st.expander("🔧 More Filters"):
        col_a, col_b, col_c, col_d, col_e = st.columns(5)
        
        with col_a:
            min_change = st.number_input("Min % Change", value=0.0, step=1.0)
        
        with col_b:
            min_volume = st.number_input("Min Volume ($)", value=0, step=1000)
        
        with col_c:
            limit = st.slider("Results Limit", min_value=10, max_value=100, value=50)
        
        with col_d:
            show_stale_volume_fallback = st.toggle(
                "Show stale volume",
                value=True,
                help="Display last-known volume when fresh volume is unavailable. Ranking still uses strict freshness gating.",
            )

        with col_e:
            hide_zero_volume = st.toggle(
                "Hide zero volume",
                value=True,
                help="Hide movers with zero displayed volume.",
            )
    
    st.markdown("---")
    
    # Fetch and display movers
    try:
        db = get_db_pool()
        db_healthy = db.health_check()
        with st.expander("Connection status", expanded=False):
            if db_healthy:
                st.success("Database connection healthy")
            else:
                st.error("Database connection failed")
            st.write(db.get_pool_stats())

        if not db_healthy:
            st.error("Unable to load movers until the database is healthy.")
            return

        window_seconds = window_minutes * 60
        movers = []
        
        # Try cached first for standard windows
        cached_windows = [300, 3600, 86400]
        if window_seconds in cached_windows:
            movers = AnalyticsQueries.get_cached_movers(
                window_seconds=window_seconds,
                limit=limit,
                category=category_filter,
                direction=direction
            )
        
        if not movers:
            movers = MarketQueries.get_movers_window(
                window_seconds=window_seconds,
                limit=limit,
                category=category_filter,
                direction=direction
            )
        
        # Ensure market context fields are present for rendering/links
        movers = _hydrate_market_context(movers)
        movers = _apply_stale_volume_fallback(
            movers,
            enabled=show_stale_volume_fallback,
        )

        # Apply additional filters
        if source_filter:
            movers = [m for m in movers if m.get('source', '').lower() == source_filter]
        
        if min_change > 0:
            movers = [m for m in movers if abs(float(m.get('pct_change') or m.get('move_pp') or 0)) >= min_change]
        
        if min_volume > 0:
            movers = [m for m in movers if _volume_for_display(m) >= min_volume]

        hidden_zero_count = 0
        if hide_zero_volume:
            before_count = len(movers)
            movers = [m for m in movers if _volume_for_display(m) > 0]
            hidden_zero_count = before_count - len(movers)
        
        # Stats summary
        if movers:
            gainers = [m for m in movers if float(m.get('pct_change') or m.get('move_pp') or 0) > 0]
            losers = [m for m in movers if float(m.get('pct_change') or m.get('move_pp') or 0) < 0]
            
            col_s1, col_s2, col_s3 = st.columns(3)
            col_s1.metric("Total Results", len(movers))
            col_s2.metric("Gainers", len(gainers), delta=None)
            col_s3.metric("Losers", len(losers), delta=None)
            
            st.markdown("---")
        
        if not movers:
            st.info("No movers found with these filters. Try adjusting your criteria.")
            return

        missing_fields = _summarize_missing_fields(movers)
        if missing_fields["market_id"] or missing_fields["title"]:
            st.warning("Some movers are missing market metadata. Data may be incomplete.")
        if missing_fields["url"]:
            st.caption(f"Links available for {len(movers) - missing_fields['url']} of {len(movers)} movers.")
        stale_volume_count = sum(1 for mover in movers if mover.get("display_volume_is_stale"))
        if show_stale_volume_fallback and stale_volume_count:
            st.caption(
                f"Showing stale fallback volume for {stale_volume_count} movers "
                "(display-only, not used for ranking)."
            )
        if hidden_zero_count:
            st.caption(f"Hidden {hidden_zero_count} zero-volume movers.")
        
        # Display as cards
        for mover in movers:
            render_mover_card(mover, show_watchlist=True)
            
    except Exception as e:
        st.error(f"Error loading data: {e}")


if __name__ == "__main__":
    main()
