"""
Market Detail Page - Deep dive into individual markets.
Supports both Polymarket and Kalshi with source-specific displays.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta

from packages.core.storage import get_db_pool
from packages.core.storage.queries import MarketQueries, OHLCQueries
from apps.dashboard.components import to_user_tz

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
    
    .price-yes { color: #00d4aa; }
    .price-no { color: #ff4757; }
    
    .source-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        margin-right: 0.5rem;
    }
    
    .source-polymarket {
        background: rgba(130, 71, 229, 0.2);
        color: #a78bfa;
        border: 1px solid #8b5cf6;
    }
    
    .source-kalshi {
        background: rgba(59, 130, 246, 0.2);
        color: #60a5fa;
        border: 1px solid #3b82f6;
    }
    
    .metric-card {
        background: #12121a;
        border: 1px solid #2a2a3a;
        border-radius: 8px;
        padding: 1rem;
        text-align: center;
    }
    
    .metric-label {
        font-size: 0.75rem;
        color: #71717a;
        text-transform: uppercase;
        margin-bottom: 0.25rem;
    }
    
    .metric-value {
        font-family: 'JetBrains Mono', monospace;
        font-size: 1.25rem;
        font-weight: 600;
        color: #e4e4e7;
    }
    
    .spread-indicator {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.85rem;
        color: #71717a;
    }
    
    .bid-ask-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 0.5rem;
        background: rgba(255,255,255,0.02);
        border-radius: 4px;
        margin-top: 0.5rem;
    }
    
    .bid { color: #00d4aa; }
    .ask { color: #ff4757; }
</style>
""", unsafe_allow_html=True)


def get_source_badge(source: str) -> str:
    """Generate HTML badge for market source."""
    source_lower = source.lower()
    if source_lower == 'kalshi':
        return '<span class="source-badge source-kalshi">KALSHI</span>'
    else:
        return '<span class="source-badge source-polymarket">POLYMARKET</span>'


def create_price_chart(data: list[dict], token_outcome: str, use_ohlc: bool = False, show_spread: bool = False) -> go.Figure:
    """
    Create a price chart for a token.
    Supports both raw snapshots and OHLC candles.
    Optionally shows bid/ask spread.
    """
    if not data:
        return None

    df = pd.DataFrame(data)
    df["ts"] = pd.to_datetime(df["ts"])
    df["ts"] = df["ts"].apply(to_user_tz)
    df = df.sort_values("ts")

    color = "#00d4aa" if token_outcome == "YES" else "#ff4757"

    fig = go.Figure()

    # Determine price column based on data type
    if use_ohlc and "close" in df.columns:
        # OHLC candle data
        fig.add_trace(go.Scatter(
            x=df["ts"],
            y=df["high"],
            mode="lines",
            name="High",
            line=dict(color=color, width=1, dash="dot"),
            opacity=0.3,
            showlegend=False,
        ))
        fig.add_trace(go.Scatter(
            x=df["ts"],
            y=df["low"],
            mode="lines",
            name="Low",
            line=dict(color=color, width=1, dash="dot"),
            fill="tonexty",
            fillcolor=f"rgba({','.join(str(int(color[i:i+2], 16)) for i in (1, 3, 5))}, 0.1)",
            opacity=0.3,
            showlegend=False,
        ))
        fig.add_trace(go.Scatter(
            x=df["ts"],
            y=df["close"],
            mode="lines",
            name=f"{token_outcome} Close",
            line=dict(color=color, width=2),
        ))
    else:
        # Raw snapshot data
        price_col = "price" if "price" in df.columns else "close"
        fig.add_trace(go.Scatter(
            x=df["ts"],
            y=df[price_col],
            mode="lines",
            name=f"{token_outcome} Price",
            line=dict(color=color, width=2),
            fill="tozeroy",
            fillcolor=f"rgba({','.join(str(int(color[i:i+2], 16)) for i in (1, 3, 5))}, 0.1)",
        ))
        
        # Add spread visualization if available
        if show_spread and "spread" in df.columns and df["spread"].notna().any():
            # Show spread as shaded region around price
            spread_half = df["spread"] / 2
            fig.add_trace(go.Scatter(
                x=df["ts"],
                y=df[price_col] + spread_half,
                mode="lines",
                name="Ask",
                line=dict(color="#ff4757", width=1, dash="dot"),
                opacity=0.5,
                showlegend=True,
            ))
            fig.add_trace(go.Scatter(
                x=df["ts"],
                y=df[price_col] - spread_half,
                mode="lines",
                name="Bid",
                line=dict(color="#00d4aa", width=1, dash="dot"),
                fill="tonexty",
                fillcolor="rgba(255,255,255,0.03)",
                opacity=0.5,
                showlegend=True,
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
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
    )

    return fig


def get_kalshi_specific_data(market_id: str) -> dict:
    """Fetch Kalshi-specific data like spread and open interest."""
    db = get_db_pool()
    try:
        # Get latest snapshot with spread
        result = db.execute("""
            SELECT 
                s.spread,
                s.price,
                s.volume_24h,
                mt.outcome
            FROM snapshots s
            JOIN market_tokens mt ON s.token_id = mt.token_id
            WHERE mt.market_id = %s
            ORDER BY s.ts DESC
            LIMIT 2
        """, (market_id,), fetch=True)
        
        if result:
            return {
                "spread": float(result[0].get("spread") or 0),
                "has_spread": result[0].get("spread") is not None,
            }
    except Exception:
        pass
    return {"spread": 0, "has_spread": False}


def main():
    st.markdown('<h1 class="page-title">🔍 Market Detail</h1>', unsafe_allow_html=True)
    
    db = get_db_pool()
    
    # Source filter
    col1, col2 = st.columns([1, 3])
    with col1:
        source_filter = st.selectbox(
            "Source",
            options=["All", "Polymarket", "Kalshi"],
            index=0,
        )
    
    # Build query based on filter
    source_condition = ""
    params = []
    if source_filter == "Polymarket":
        source_condition = "AND source = 'polymarket'"
    elif source_filter == "Kalshi":
        source_condition = "AND source = 'kalshi'"
    
    # Fetch available markets
    try:
        markets = db.execute(f"""
            SELECT market_id, title, source, category 
            FROM markets 
            WHERE status = 'active' {source_condition}
            ORDER BY updated_at DESC
            LIMIT 200
        """, fetch=True)
    except Exception as e:
        st.error(f"Error fetching markets: {e}")
        markets = []
    
    if not markets:
        if source_filter != "All":
            st.info(f"""
            📭 **No {source_filter} markets available yet.**
            
            Markets will appear here once the collector service starts syncing data.
            Try selecting "All" to see markets from other sources.
            """)
        else:
            st.info("""
            📭 **No markets available yet.**
            
            Markets will appear here once the collector service starts syncing data.
            """)
        return
    
    # Show market counts by source (always show total counts, not filtered)
    try:
        all_markets_count = db.execute("""
            SELECT source, COUNT(*) as cnt 
            FROM markets 
            WHERE status = 'active' 
            GROUP BY source
        """, fetch=True)
        poly_total = sum(m['cnt'] for m in all_markets_count if m['source'] == 'polymarket')
        kalshi_total = sum(m['cnt'] for m in all_markets_count if m['source'] == 'kalshi')
    except Exception:
        poly_total = sum(1 for m in markets if m['source'] == 'polymarket')
        kalshi_total = sum(1 for m in markets if m['source'] == 'kalshi')
    
    with col2:
        filter_note = f" (showing {source_filter})" if source_filter != "All" else ""
        st.caption(f"📊 {poly_total} Polymarket | {kalshi_total} Kalshi total{filter_note}")
    
    # Market selector with source indicator
    market_options = {}
    for m in markets:
        source_tag = "🟣" if m['source'] == 'polymarket' else "🔵"
        title = m['title'][:55] + "..." if len(m['title']) > 55 else m['title']
        label = f"{source_tag} {title}"
        market_options[label] = m['market_id']
    
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
    
    source = market.get('source', 'unknown')
    is_kalshi = source.lower() == 'kalshi'
    
    # Get Kalshi-specific data if applicable
    kalshi_data = get_kalshi_specific_data(market_id) if is_kalshi else {}
    
    # Market header with source badge
    source_badge = get_source_badge(source)
    
    # Generate proper URL - ensure we build from scratch to avoid HTML in URL field
    if is_kalshi and market.get('source_id'):
        # Kalshi URL format
        ticker = market.get('source_id', '')
        series = ticker.split('-')[0].lower() if '-' in ticker else ticker.lower()
        market_url = f"https://kalshi.com/markets/{series}"
    elif source.lower() == 'polymarket':
        # Polymarket URL from slug or source_id
        slug = market.get('slug') or market.get('source_id', '')
        if slug:
            # Clean slug - remove any URL prefix if present
            if 'polymarket.com' in str(slug):
                slug = slug.split('/')[-1]
            market_url = f"https://polymarket.com/event/{slug}"
        else:
            market_url = ''
    else:
        market_url = ''
    
    # Get category - for Kalshi, try to infer from title/ticker
    category = market.get('category', '')
    if not category or category.lower() == 'kalshi':
        if is_kalshi:
            ticker = market.get('source_id', '').upper()
            if 'HOUSERACE' in ticker:
                category = 'Politics'
            elif 'SENATE' in ticker:
                category = 'Politics'
            elif 'PRESIDENT' in ticker:
                category = 'Politics'
            elif 'FED' in ticker or 'FOMC' in ticker:
                category = 'Economics'
            elif 'BTC' in ticker or 'ETH' in ticker:
                category = 'Crypto'
            else:
                category = 'Politics'  # Default for Kalshi
        else:
            category = 'Uncategorized'
    
    st.markdown(f"""
    <div class="market-header">
        <div style="margin-bottom: 0.75rem;">
            {source_badge}
            <span style="color: #71717a; font-size: 0.85rem;">{category}</span>
        </div>
        <h2 class="market-title-large">{market['title']}</h2>
        <p style="color: #71717a; margin-bottom: 0.5rem; font-size: 0.85rem;">
            <strong>Status:</strong> {market['status'].upper()}
            {f' | <strong>Ticker:</strong> {market.get("source_id", "N/A")}' if is_kalshi else ''}
        </p>
        {'<a href="' + market_url + '" target="_blank" style="color: #5865f2;">View on ' + source.title() + ' →</a>' if market_url else ''}
    </div>
    """, unsafe_allow_html=True)
    
    # Token prices and metrics
    tokens = market.get("tokens", [])
    if tokens and tokens[0]:
        st.markdown("### Current Prices")
        
        # For Kalshi, show additional metrics
        if is_kalshi and kalshi_data.get('has_spread'):
            spread_cents = kalshi_data.get('spread', 0) * 100
            st.markdown(f"""
            <div style="background: #12121a; border: 1px solid #2a2a3a; border-radius: 8px; padding: 0.75rem; margin-bottom: 1rem;">
                <span style="color: #71717a; font-size: 0.8rem;">BID/ASK SPREAD: </span>
                <span style="font-family: 'JetBrains Mono', monospace; color: #fbbf24;">{spread_cents:.1f}¢</span>
            </div>
            """, unsafe_allow_html=True)
        
        cols = st.columns(len(tokens))
        for i, token in enumerate(tokens):
            if token:
                price = float(token.get("latest_price", 0) or 0)
                outcome = token.get("outcome", "YES")
                price_class = "price-yes" if outcome == "YES" else "price-no"
                volume = float(token.get('latest_volume', 0) or 0)
                
                # Format volume
                if volume >= 1_000_000:
                    vol_str = f"${volume/1_000_000:.1f}M"
                elif volume >= 1_000:
                    vol_str = f"${volume/1_000:.1f}K"
                else:
                    vol_str = f"${volume:.0f}"
                
                # Price in cents for display
                price_cents = price * 100
                
                with cols[i]:
                    st.markdown(f"""
                    <div class="metric-card">
                        <p class="metric-label">{outcome}</p>
                        <p class="token-price {price_class}">{price_cents:.0f}¢</p>
                        <p style="color: #71717a; font-size: 0.8rem; margin-top: 0.25rem;">
                            Vol: {vol_str}
                        </p>
                    </div>
                    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Price history chart
    st.markdown("### Price History")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        timeframe = st.select_slider(
            "Timeframe",
            options=["1H", "6H", "24H", "7D", "30D"],
            value="24H",
        )
    
    with col2:
        show_spread = st.checkbox("Show Spread", value=is_kalshi, disabled=not is_kalshi)
    
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
        # Show data source info
        if hours >= 48:
            st.caption("📊 Using 1-hour OHLC candles for faster loading")
        elif hours >= 6:
            st.caption("📊 Using 1-minute OHLC candles")
        else:
            st.caption("📊 Using raw price snapshots")

        for token in tokens:
            if token and token.get("token_id"):
                outcome = token.get('outcome', 'Unknown')
                outcome_color = "#00d4aa" if outcome == "YES" else "#ff4757"
                st.markdown(f'<span style="color: {outcome_color}; font-weight: 600;">● {outcome} Token</span>', unsafe_allow_html=True)

                use_ohlc = hours >= 6

                if use_ohlc:
                    data = OHLCQueries.get_candles_for_timeframe(
                        token_id=token["token_id"],
                        start_ts=start_ts,
                        hours=hours,
                    )
                else:
                    data = MarketQueries.get_snapshots_range(
                        token_id=token["token_id"],
                        start_ts=start_ts,
                    )

                if data:
                    fig = create_price_chart(
                        data, 
                        outcome, 
                        use_ohlc=use_ohlc,
                        show_spread=show_spread and not use_ohlc
                    )
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info(f"No price history available for {outcome} token in this timeframe.")
    else:
        st.info("No token data available for this market yet.")
    
    # Additional market info
    st.markdown("---")
    st.markdown("### Market Metadata")
    
    meta_col1, meta_col2, meta_col3 = st.columns(3)
    
    with meta_col1:
        st.metric("Source", source.upper())
    with meta_col2:
        st.metric("Market ID", str(market.get('market_id', 'N/A'))[:8] + "...")
    with meta_col3:
        st.metric("Source ID", str(market.get('source_id', 'N/A'))[:20] + ("..." if len(str(market.get('source_id', ''))) > 20 else ""))


if __name__ == "__main__":
    main()
