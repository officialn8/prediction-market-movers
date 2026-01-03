"""
Watchlist Page - View and manage your watched markets.
"""

import streamlit as st
import pandas as pd
from datetime import datetime

from apps.dashboard.components import get_watchlist, toggle_watchlist, init_watchlist, to_user_tz
from packages.core.storage.queries import MarketQueries

st.set_page_config(
    page_title="Watchlist | PM Movers",
    page_icon="★",
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

    .watchlist-card {
        background: linear-gradient(135deg, #12121a 0%, #1a1a24 100%);
        border: 1px solid #2a2a3a;
        border-radius: 12px;
        padding: 1.25rem;
        margin-bottom: 1rem;
    }

    .watchlist-card:hover {
        border-color: #fbbf24;
    }

    .market-title {
        font-family: 'Space Grotesk', sans-serif;
        font-size: 1rem;
        font-weight: 500;
        color: #e4e4e7;
        margin-bottom: 0.5rem;
    }

    .price-display {
        font-family: 'JetBrains Mono', monospace;
        font-size: 1.5rem;
        font-weight: 600;
    }

    .price-yes { color: #00d4aa; }
    .price-no { color: #ff4757; }

    .source-tag {
        display: inline-block;
        padding: 0.15rem 0.5rem;
        border-radius: 4px;
        font-size: 0.7rem;
        font-weight: 600;
        text-transform: uppercase;
        background: rgba(168, 85, 247, 0.2);
        color: #a855f7;
    }

    .empty-state {
        text-align: center;
        padding: 4rem 2rem;
        color: #71717a;
    }
</style>
""", unsafe_allow_html=True)


def main():
    st.markdown('<h1 class="page-title">★ My Watchlist</h1>', unsafe_allow_html=True)
    st.markdown("Track your favorite markets in one place.")

    # Initialize watchlist
    init_watchlist()
    watchlist = get_watchlist()

    if not watchlist:
        st.markdown("""
        <div class="empty-state">
            <div style="font-size: 4rem; margin-bottom: 1rem;">☆</div>
            <h3>Your watchlist is empty</h3>
            <p>Add markets from the Top Movers page by clicking the star button.</p>
        </div>
        """, unsafe_allow_html=True)
        return

    st.markdown(f"**{len(watchlist)} market(s) in watchlist**")
    st.markdown("---")

    # Clear all button
    if st.button("🗑️ Clear Watchlist", type="secondary"):
        st.session_state.watchlist = {}
        st.rerun()

    # Display each watched market with current prices
    col_left, col_right = st.columns(2)
    
    # Batch fetch all markets to avoid N+1 queries
    market_ids = list(watchlist.keys())
    markets_data = MarketQueries.get_markets_batch_with_prices(market_ids)
    
    # Convert list to dict for lookup
    markets_map = {str(m['market_id']): m for m in markets_data}

    for i, (market_id, info) in enumerate(watchlist.items()):
        with col_left if i % 2 == 0 else col_right:
            # Fetch current market data from batch
            market = markets_map.get(market_id)

            if not market:
                # Market might have been deleted or closed
                st.markdown(f"""
                <div class="watchlist-card" style="opacity: 0.5;">
                    <p class="market-title">{info.get('title', 'Unknown')}</p>
                    <p style="color: #71717a;">Market no longer available</p>
                </div>
                """, unsafe_allow_html=True)
                if st.button("Remove", key=f"remove_{market_id}"):
                    toggle_watchlist(market_id, "", "")
                    st.rerun()
                continue

            tokens = market.get("tokens", [])

            # Build price display
            price_html = ""
            for token in tokens:
                if token:
                    price = float(token.get("latest_price", 0) or 0)
                    outcome = token.get("outcome", "YES")
                    price_class = "price-yes" if outcome == "YES" else "price-no"
                    price_html += f'<span class="{price_class}" style="margin-right: 1rem;">{outcome}: ${price:.2f}</span>'

            st.markdown(f"""
            <div class="watchlist-card">
                <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                    <div>
                        <span class="source-tag">{market.get('source', 'unknown')}</span>
                        <span style="color: #71717a; font-size: 0.75rem; margin-left: 0.5rem;">
                            Added {to_user_tz(datetime.fromisoformat(info.get('added_at', ''))).strftime('%Y-%m-%d %H:%M') if info.get('added_at') else ''}
                        </span>
                    </div>
                    <span style="color: #fbbf24; font-size: 1.25rem;">★</span>
                </div>
                <p class="market-title" style="margin-top: 0.5rem;">{market.get('title', 'Unknown')}</p>
                <div class="price-display" style="margin-top: 0.5rem;">
                    {price_html}
                </div>
                <p style="color: #71717a; font-size: 0.8rem; margin-top: 0.5rem;">
                    Category: {market.get('category', 'Uncategorized')} |
                    Status: {market.get('status', 'unknown').upper()}
                </p>
            </div>
            """, unsafe_allow_html=True)

            # Action buttons
            btn_col1, btn_col2 = st.columns(2)
            with btn_col1:
                if st.button("📊 View Details", key=f"view_{market_id}", width="stretch"):
                    st.switch_page("pages/2_Market_Detail.py")
            with btn_col2:
                if st.button("✕ Remove", key=f"remove_{market_id}", width="stretch"):
                    toggle_watchlist(market_id, "", "")
                    st.rerun()


if __name__ == "__main__":
    main()
