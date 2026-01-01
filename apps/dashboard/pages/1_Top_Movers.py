"""
Top Movers Page - Displays markets with highest price changes.
"""

import streamlit as st
import pandas as pd
from datetime import datetime

from packages.core.storage import get_db_pool
from packages.core.storage.queries import MarketQueries

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
    
    .mover-card {
        background: linear-gradient(135deg, #12121a 0%, #1a1a24 100%);
        border: 1px solid #2a2a3a;
        border-radius: 12px;
        padding: 1.25rem;
        margin-bottom: 1rem;
        transition: all 0.2s ease;
    }
    
    .mover-card:hover {
        border-color: #5865f2;
        transform: translateY(-2px);
    }
    
    .market-title {
        font-family: 'Space Grotesk', sans-serif;
        font-size: 1rem;
        font-weight: 500;
        color: #e4e4e7;
        margin-bottom: 0.5rem;
        line-height: 1.4;
    }
    
    .price-change {
        font-family: 'JetBrains Mono', monospace;
        font-size: 1.5rem;
        font-weight: 600;
    }
    
    .price-change.positive {
        color: #00d4aa;
    }
    
    .price-change.negative {
        color: #ff4757;
    }
    
    .price-info {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.85rem;
        color: #71717a;
    }
    
    .source-tag {
        display: inline-block;
        padding: 0.15rem 0.5rem;
        border-radius: 4px;
        font-size: 0.7rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    .source-polymarket {
        background: rgba(168, 85, 247, 0.2);
        color: #a855f7;
    }
    
    .source-kalshi {
        background: rgba(88, 101, 242, 0.2);
        color: #5865f2;
    }
    
    .outcome-tag {
        display: inline-block;
        padding: 0.15rem 0.5rem;
        border-radius: 4px;
        font-size: 0.7rem;
        font-weight: 600;
        margin-left: 0.5rem;
    }
    
    .outcome-yes {
        background: rgba(0, 212, 170, 0.15);
        color: #00d4aa;
    }
    
    .outcome-no {
        background: rgba(255, 71, 87, 0.15);
        color: #ff4757;
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


def generate_reason(pct_change: float, volume: float, outcome: str) -> str:
    """Generate a readable reason for the move."""
    direction = "spiked" if pct_change > 0 else "dropped"
    abs_pct = abs(pct_change)
    
    # Simplify volume
    if volume >= 1_000_000:
        vol_str = f"${volume/1_000_000:.1f}M"
    elif volume >= 1_000:
        vol_str = f"${volume/1_000:.1f}k"
    else:
        vol_str = f"${volume:.0f}"
        
    return f"**{outcome}** {direction} **{abs_pct:.1f}%** on {vol_str} vol"


def render_mover_card(mover: dict) -> None:
    """Render a single mover card."""
    pct_change = float(mover.get("pct_change", 0))
    change_class = "positive" if pct_change > 0 else "negative"
    change_sign = "+" if pct_change > 0 else ""
    
    source = mover.get("source", "unknown")
    source_class = f"source-{source}"
    
    outcome = mover.get("outcome", "YES")
    outcome_class = "outcome-yes" if outcome == "YES" else "outcome-no"
    
    latest_price = float(mover.get("latest_price", 0))
    old_price = float(mover.get("old_price", 0))
    
    # Try to get volume from various keys (cache vs raw SQL might differ)
    volume = float(mover.get("latest_volume") or mover.get("volume_24h") or 0)
    
    reason = generate_reason(pct_change, volume, outcome)
    
    st.markdown(f"""
    <div class="mover-card">
        <div style="display: flex; justify-content: space-between; align-items: flex-start;">
            <div style="flex: 1;">
                <div>
                    <span class="source-tag {source_class}">{source}</span>
                    <span class="outcome-tag {outcome_class}">{outcome}</span>
                </div>
                <p class="market-title">{mover.get('title', 'Unknown Market')}</p>
                <p class="price-info">
                    ${old_price:.2f} → ${latest_price:.2f}
                </p>
                <div style="margin-top: 0.5rem; font-size: 0.85rem; color: #a1a1aa;">
                    ℹ️ {reason}
                </div>
            </div>
            <div style="text-align: right;">
                <p class="price-change {change_class}">{change_sign}{pct_change:.1f}%</p>
                <p class="price-info">{mover.get('category', 'Uncategorized')}</p>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def main():
    st.markdown('<h1 class="page-title">🚀 Top Movers</h1>', unsafe_allow_html=True)
    
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

        # Try to fetch from cache first for supported windows (5m, 15m, 1h, 24h)
        from packages.core.storage.queries import AnalyticsQueries

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
            # Convert to hours for the query (minimum 1 hour)
            hours = max(1, timeframe_minutes // 60)
            movers = MarketQueries.get_top_movers(
                hours=hours,
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

