"""
Advanced Movers - Extended filtering and analysis view
"""

import streamlit as st
import pandas as pd
from datetime import datetime

from packages.core.storage import get_db_pool
from packages.core.storage.queries import MarketQueries, AnalyticsQueries
from apps.dashboard.components import render_mover_card, init_watchlist, format_volume

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
        col_a, col_b, col_c = st.columns(3)
        
        with col_a:
            min_change = st.number_input("Min % Change", value=0.0, step=1.0)
        
        with col_b:
            min_volume = st.number_input("Min Volume ($)", value=0, step=1000)
        
        with col_c:
            limit = st.slider("Results Limit", min_value=10, max_value=100, value=50)
    
    st.markdown("---")
    
    # Fetch and display movers
    try:
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
        
        # Apply additional filters
        if source_filter:
            movers = [m for m in movers if m.get('source', '').lower() == source_filter]
        
        if min_change > 0:
            movers = [m for m in movers if abs(float(m.get('pct_change') or m.get('move_pp') or 0)) >= min_change]
        
        if min_volume > 0:
            movers = [m for m in movers if float(m.get('latest_volume') or m.get('current_volume') or 0) >= min_volume]
        
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
        
        # Display as cards
        for mover in movers:
            render_mover_card(mover, show_watchlist=True)
            
    except Exception as e:
        st.error(f"Error loading data: {e}")


if __name__ == "__main__":
    main()
