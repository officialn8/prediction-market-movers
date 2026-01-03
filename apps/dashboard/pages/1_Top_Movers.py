"""
Top Movers Page - 'The Simplest, Most Informative Display'
"""

import streamlit as st
import pandas as pd
from datetime import datetime

from packages.core.storage import get_db_pool
from packages.core.storage.queries import MarketQueries, AnalyticsQueries
from apps.dashboard.components import render_mover_card, init_watchlist, to_user_tz

st.set_page_config(
    page_title="What's Moving Now | PM Movers",
    page_icon="🔥",
    layout="wide",
)

# Custom styling for simplified list view
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    .stApp {
        font-family: 'Inter', sans-serif;
    }

    h1 {
        font-family: 'Inter', sans-serif;
        font-weight: 700;
        font-size: 1.5rem !important;
        margin-bottom: 0rem !important;
    }
    
    .stRadio > div {
        display: flex;
        flex-direction: row;
        gap: 1rem;
    }
    
</style>
""", unsafe_allow_html=True)


def main():
    # Initialize watchlist
    init_watchlist()

    # Search Bar (Top)
    search_query = st.text_input("🔍 Search Markets", placeholder="trump, bitcoin, fed...", label_visibility="collapsed")
    if search_query:
        st.caption(f"Searching for '{search_query}'...")
        results = MarketQueries.search_markets(search_query)
        if not results:
            st.info("No matching markets found.")
        else:
            ids = [str(r['market_id']) for r in results]
            full_data = MarketQueries.get_markets_batch_with_prices(ids)
            for market in full_data:
                 tokens = market.get('tokens', [])
                 if not tokens: continue
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
                    'pct_change': 0, 'old_price': 0
                }
                 render_mover_card(mover_wrapper)
        if st.button("Clear Search"): st.rerun()
        return

    # Header & Time Toggle
    col_header, col_toggle = st.columns([1, 1])
    with col_header:
        st.markdown("# What's Moving Now")
    
    with col_toggle:
        # Timeframe toggle (Horizontal Radio)
        # Options: 5m, 1h, 24h, 7d
        # Default: 1h
        tf_map = {"5m": 5, "1h": 60, "24h": 1440, "7d": 10080}
        display_map = {"5m": "⏱️ 5min", "1h": "1hr", "24h": "24hr", "7d": "7day"}
        
        selected_label = st.radio(
            "Timeframe",
            options=["5m", "1h", "24h", "7d"],
            index=1,
            format_func=lambda x: display_map[x],
            horizontal=True,
            label_visibility="collapsed"
        )
        window_minutes = tf_map[selected_label]

    # Category Filter
    CATEGORIES = [
        "Politics", "Sports", "Crypto", "Finance", "Geopolitics", 
        "Earnings", "Tech", "Culture", "World", "Economy", 
        "Climate & Science", "Elections"
    ]
    
    # Use pills if available (Streamlit 1.40+), otherwise fallback to selectbox
    selected_category = "All"
    if hasattr(st, "pills"):
        selected_category = st.pills(
            "Category",
            options=["All"] + CATEGORIES,
            default="All",
            selection_mode="single",
            label_visibility="collapsed"
        )
    else:
        selected_category = st.selectbox(
            "Category",
            options=["All"] + CATEGORIES,
            index=0,
            label_visibility="visible" # Make it visible so they know what it is if it's a dropdown
        )
        
    category_filter = None if selected_category == "All" else selected_category

    st.markdown("---")

    # Fetch Data
    try:
        window_seconds = window_minutes * 60
        movers = []
        
        # Use Cached or Raw (Standard Windows)
        cached_windows = [300, 3600, 86400]
        used_cache = False
        
        if window_seconds in cached_windows:
            movers = AnalyticsQueries.get_cached_movers(
                window_seconds=window_seconds,
                limit=50, # Show more for list view
                category=category_filter,
                direction="both"
            )
            used_cache = True
            
        if not movers:
            movers = MarketQueries.get_movers_window(
                window_seconds=window_seconds,
                limit=50,
                category=category_filter,
                direction="both"
            )

        if not movers:
            if category_filter:
                st.info(f"No top movers found in '{category_filter}' for this timeframe.")
            else:
                st.info("Waiting for first update... (Ensure collector is running)")
            return

        # List Display (The 'Killer View')
        for mover in movers:
            render_mover_card(mover)
            
        # Footer
        st.markdown("<br><br>", unsafe_allow_html=True)
        with st.expander("Advanced Filters"):
             st.write("Source: All")
             st.write("Sort: Absolute Change")
    
    except Exception as e:
        st.error(f"Error: {e}")

if __name__ == "__main__":
    main()
