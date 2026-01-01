"""
Market Detail Page - Deep dive into individual markets.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta

from packages.core.storage import get_db_pool
from packages.core.storage.queries import MarketQueries

st.set_page_config(
    page_title="Market Detail | PM Movers",
    page_icon="🔍",
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
    
    .market-header {
        background: linear-gradient(135deg, #12121a 0%, #1a1a24 100%);
        border: 1px solid #2a2a3a;
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1.5rem;
    }
    
    .market-title-large {
        font-family: 'Space Grotesk', sans-serif;
        font-size: 1.5rem;
        font-weight: 600;
        color: #e4e4e7;
        margin-bottom: 0.5rem;
    }
    
    .token-price {
        font-family: 'JetBrains Mono', monospace;
        font-size: 2rem;
        font-weight: 600;
    }
    
    .price-yes {
        color: #00d4aa;
    }
    
    .price-no {
        color: #ff4757;
    }
</style>
""", unsafe_allow_html=True)


def create_price_chart(snapshots: list[dict], token_outcome: str) -> go.Figure:
    """Create a price chart for a token."""
    if not snapshots:
        return None
    
    df = pd.DataFrame(snapshots)
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.sort_values("ts")
    
    color = "#00d4aa" if token_outcome == "YES" else "#ff4757"
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df["ts"],
        y=df["price"],
        mode="lines",
        name=f"{token_outcome} Price",
        line=dict(color=color, width=2),
        fill="tozeroy",
        fillcolor=f"rgba({','.join(str(int(color[i:i+2], 16)) for i in (1, 3, 5))}, 0.1)",
    ))
    
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(18, 18, 26, 1)",
        margin=dict(l=0, r=0, t=40, b=0),
        xaxis=dict(
            showgrid=True,
            gridcolor="rgba(255,255,255,0.05)",
            title="",
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="rgba(255,255,255,0.05)",
            title="Price ($)",
            range=[0, 1],
        ),
        hovermode="x unified",
        showlegend=False,
    )
    
    return fig


def main():
    st.markdown('<h1 class="page-title">🔍 Market Detail</h1>', unsafe_allow_html=True)
    
    db = get_db_pool()
    
    # Fetch available markets
    try:
        markets = db.execute("""
            SELECT market_id, title, source, category 
            FROM markets 
            WHERE status = 'active'
            ORDER BY updated_at DESC
            LIMIT 100
        """, fetch=True)
    except Exception as e:
        st.error(f"Error fetching markets: {e}")
        markets = []
    
    if not markets:
        st.info("""
        📭 **No markets available yet.**
        
        Markets will appear here once the collector service starts syncing data.
        """)
        return
    
    # Market selector
    market_options = {
        f"{m['title'][:60]}... ({m['source']})" if len(m['title']) > 60 else f"{m['title']} ({m['source']})": m['market_id']
        for m in markets
    }
    
    selected_label = st.selectbox(
        "Select a Market",
        options=list(market_options.keys()),
    )
    
    if not selected_label:
        return
    
    market_id = market_options[selected_label]
    
    # Fetch market details
    market = MarketQueries.get_market_with_tokens_and_latest_prices(market_id)
    
    if not market:
        st.error("Market not found")
        return
    
    # Market header
    st.markdown(f"""
    <div class="market-header">
        <h2 class="market-title-large">{market['title']}</h2>
        <p style="color: #71717a; margin-bottom: 0.5rem;">
            <strong>Source:</strong> {market['source'].upper()} | 
            <strong>Category:</strong> {market.get('category', 'Uncategorized')} |
            <strong>Status:</strong> {market['status'].upper()}
        </p>
        {f'<a href="{market["url"]}" target="_blank" style="color: #5865f2;">View on {market["source"].title()} →</a>' if market.get('url') else ''}
    </div>
    """, unsafe_allow_html=True)
    
    # Token prices
    tokens = market.get("tokens", [])
    if tokens and tokens[0]:  # Check if tokens exist and aren't null
        st.markdown("### Current Prices")
        
        cols = st.columns(len(tokens))
        for i, token in enumerate(tokens):
            if token:
                price = float(token.get("latest_price", 0) or 0)
                outcome = token.get("outcome", "YES")
                price_class = "price-yes" if outcome == "YES" else "price-no"
                
                with cols[i]:
                    st.markdown(f"""
                    <div style="text-align: center; padding: 1rem; background: #12121a; border-radius: 8px;">
                        <p style="color: #71717a; margin-bottom: 0.5rem;">{outcome}</p>
                        <p class="token-price {price_class}">${price:.2f}</p>
                        <p style="color: #71717a; font-size: 0.8rem;">
                            Vol: ${float(token.get('latest_volume', 0) or 0):,.0f}
                        </p>
                    </div>
                    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Price history chart
    st.markdown("### Price History")
    
    timeframe = st.select_slider(
        "Timeframe",
        options=["1H", "6H", "24H", "7D", "30D"],
        value="24H",
    )
    
    # Map timeframe to hours
    timeframe_hours = {
        "1H": 1,
        "6H": 6,
        "24H": 24,
        "7D": 168,
        "30D": 720,
    }
    hours = timeframe_hours.get(timeframe, 24)
    start_ts = datetime.utcnow() - timedelta(hours=hours)
    
    # Fetch and display charts for each token
    if tokens and tokens[0]:
        for token in tokens:
            if token and token.get("token_id"):
                st.markdown(f"**{token.get('outcome', 'Unknown')} Token**")
                
                # Use OHLC for longer timeframes to improve performance
                # 7 days (168h) or more -> use OHLC
                if hours >= 168:
                    # Choose resolution based on timeframe
                    # 7d -> 1h candles is plenty (168 points)
                    # 30d -> 1h candles (720 points)
                    resolution = "1h"
                    
                    # Need to import new class (lazy import to avoid circular dep issues in theory, mainly just scope)
                    from packages.core.storage.queries import OHLCQueries
                    
                    snapshots = OHLCQueries.get_candles(
                        token_id=token["token_id"],
                        start_ts=start_ts,
                        resolution=resolution
                    )
                    
                    # Adapt OHLC data for the chart function which expects 'price'
                    # We'll use 'close' as price for line chart
                    if snapshots:
                        for s in snapshots:
                            s['price'] = s['close']
                            
                else:
                    # High resolution for short windows
                    snapshots = MarketQueries.get_snapshots_range(
                        token_id=token["token_id"],
                        start_ts=start_ts,
                    )
                
                if snapshots:
                    fig = create_price_chart(snapshots, token.get("outcome", "YES"))
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info(f"No price history available for {token.get('outcome')} token in this timeframe.")
    else:
        st.info("No token data available for this market yet.")


if __name__ == "__main__":
    main()

