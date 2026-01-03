"""
Top Movers Page - Displays markets with highest price changes.
"""

import streamlit as st
import pandas as pd
from datetime import datetime

from packages.core.storage import get_db_pool
from packages.core.storage.queries import MarketQueries, AnalyticsQueries
from apps.dashboard.components import render_mover_card, init_watchlist

st.set_page_config(
    page_title="Top Movers | PM Movers",
    page_icon="🚀",
    layout="wide",
)

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
    
    .empty-state {
        text-align: center;
        padding: 4rem 2rem;
        color: #71717a;
    }
    
    .empty-state-icon {
        font-size: 4rem;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)


def main():
    # Initialize watchlist (session loading)
    init_watchlist()

    st.markdown('<h1 class="page-title">🚀 Top Movers</h1>', unsafe_allow_html=True)
    
    # Search implementation (Task 5)
    search_query = st.text_input("🔍 Search Markets", placeholder="vance, bitcoin, election...", label_visibility="collapsed")
    
    if search_query:
        st.subheader(f"Search Results for '{search_query}'")
        results = MarketQueries.search_markets(search_query)
        
        if not results:
            st.info("No markets found matching your query.")
        else:
            # We need to fetch prices to show useful info
            ids = [str(r['market_id']) for r in results]
            full_data = MarketQueries.get_markets_batch_with_prices(ids)
            
            for market in full_data:
                # Construct a pseudo-mover object for rendering
                # We'll pick the first token or highest volume token
                tokens = market.get('tokens', [])
                if not tokens:
                    continue
                    
                # Sort tokens by volume if possible to show most relevant
                # But tokens might not have volume stats in this view unless we fetched snapshots.
                # get_markets_batch_with_prices returns 'latest_price' and 'latest_volume' for tokens.
                
                # Let's just create a card for each token? No, too many.
                # Just show the market and its tokens.
                # Since render_mover_card is specific to single token, maybe we iterate?
                # Or we make a Custom "Search Result Card".
                # For consistency, let's use render_mover_card for the first/best token.
                
                top_token = tokens[0] # Default
                
                mover_wrapper = {
                    'market_id': market['market_id'],
                    'token_id': top_token.get('token_id'),
                    'title': market['title'],
                    'source': market['source'],
                    'category': market['category'],
                    'outcome': top_token.get('outcome'),
                    'latest_price': top_token.get('latest_price', 0),
                    'latest_volume': top_token.get('latest_volume', 0),
                    'pct_change': 0, # Search result doesn't imply move
                    'old_price': 0
                }
                render_mover_card(mover_wrapper, show_watchlist=True)
                
        st.markdown("---")
        if st.button("Clear Search"):
             st.rerun()
        return


    # Filters
    col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
    
    with col1:
        # Timeframe options: values are in minutes for sub-hour, hours for 1h+
        timeframe_options = {
            "5m": 5,      # 5 minutes
            "15m": 15,    # 15 minutes
            "1h": 60,     # 1 hour
            "6h": 360,    # 6 hours
            "12h": 720,   # 12 hours
            "24h": 1440,  # 24 hours
            "7d": 10080,  # 7 days
        }
        timeframe_label = st.selectbox(
            "Timeframe",
            options=list(timeframe_options.keys()),
            index=2,  # Default to 1h
        )
        timeframe_minutes = timeframe_options[timeframe_label]
    
    with col2:
        source_filter = st.selectbox(
            "Source",
            options=["All", "polymarket", "kalshi"],
        )
    
    with col3:
        direction = st.selectbox(
            "Direction",
            options=["both", "gainers", "losers"],
            format_func=lambda x: x.title(),
        )
    
    with col4:
        limit = st.number_input("Limit", min_value=5, max_value=100, value=20)
    
    st.markdown("---")
    
    # Fetch data
    try:
        source = source_filter if source_filter != "All" else None

        # Calculate window in seconds
        window_seconds = timeframe_minutes * 60

        movers = []
        used_cache = False

        # Cached windows: 300 (5m), 900 (15m), 3600 (1h), 86400 (24h)
        cached_windows = [300, 900, 3600, 86400]
        if window_seconds in cached_windows:
            movers = AnalyticsQueries.get_cached_movers(
                window_seconds=window_seconds,
                limit=limit,
                source=source,
                direction=direction
            )
            if movers:
                used_cache = True

        # Fallback to raw SQL if cache miss or non-standard window
        if not movers:
            # Fallback to raw SQL if cache miss or non-standard window
            # We now use the exact window seconds for the query
            movers = MarketQueries.get_movers_window(
                window_seconds=window_seconds,
                limit=limit,
                source=source,
                direction=direction,
            )
        
        if used_cache:
            st.caption(f"⚡ Data from cache • Last updated: {movers[0]['as_of_ts'].strftime('%H:%M:%S')}")

        
        if not movers:
            st.markdown("""
            <div class="empty-state">
                <div class="empty-state-icon">📭</div>
                <h3>No Data Yet</h3>
                <p>Price data will appear here once the collector starts syncing markets.</p>
                <p>Make sure the collector service is running and has captured at least two snapshots.</p>
            </div>
            """, unsafe_allow_html=True)
            return
        
        # Display stats
        gainers = len([m for m in movers if float(m.get("pct_change", 0)) > 0])
        losers = len([m for m in movers if float(m.get("pct_change", 0)) < 0])
        
        stat_col1, stat_col2, stat_col3 = st.columns(3)
        with stat_col1:
            st.metric("Total Movers", len(movers))
        with stat_col2:
            st.metric("Gainers 📈", gainers)
        with stat_col3:
            st.metric("Losers 📉", losers)
        
        st.markdown("---")
        
        # Display movers
        col_left, col_right = st.columns(2)
        
        for i, mover in enumerate(movers):
            with col_left if i % 2 == 0 else col_right:
                render_mover_card(mover)
        
        # Export option
        if st.button("📊 Export to CSV"):
            df = pd.DataFrame(movers)
            csv = df.to_csv(index=False)
            st.download_button(
                "Download CSV",
                csv,
                f"top_movers_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                "text/csv",
            )
            
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        st.info("Make sure the database is running and migrations have been applied.")


if __name__ == "__main__":
    main()
