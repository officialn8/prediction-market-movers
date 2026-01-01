"""
Category Trends - Aggregated views of market movements.
"""

import streamlit as st
import pandas as pd
import altair as alt
from packages.core.storage.queries import MarketQueries

st.set_page_config(
    page_title="Category Trends | PM Movers",
    page_icon="📊",
    layout="wide",
)

# Custom styling (reused from App)
st.markdown("""
<style>
    .stApp {
        background: linear-gradient(135deg, #0a0a0f 0%, #0d0d14 100%);
    }
    h1, h2, h3 {
        font-family: 'Space Grotesk', sans-serif;
    }
</style>
""", unsafe_allow_html=True)

def main():
    st.title("📊 Category Trends")
    st.markdown("Analyze market volatility and volume across different categories.")
    
    # Controls
    col1, col2 = st.columns([1, 3])
    with col1:
        timeframe = st.selectbox(
            "Timeframe",
            options=[6, 12, 24, 72, 168],
            format_func=lambda x: f"{x}h" if x < 24 else f"{x//24}d",
            index=2
        )
    
    # Fetch Data
    with st.spinner("Crunching numbers..."):
        stats = MarketQueries.get_category_stats(hours=timeframe)
        
    if not stats:
        st.info("No category data available yet. Wait for more data collection.")
        return
        
    df = pd.DataFrame(stats)
    
    # Process numeric columns
    df["avg_abs_move"] = pd.to_numeric(df["avg_abs_move"])
    df["total_volume"] = pd.to_numeric(df["total_volume"])
    
    # 1. Bar Chart: Volatility (Avg Abs Move)
    st.subheader("🔥 Volatility by Category")
    st.caption(f"Average absolute price change over last {timeframe}h")
    
    chart_volatility = alt.Chart(df).mark_bar().encode(
        x=alt.X('avg_abs_move', title='Avg |Move| (%)'),
        y=alt.Y('category', sort='-x', title='Category'),
        color=alt.Color('avg_abs_move', scale=alt.Scale(scheme='magma'), legend=None),
        tooltip=['category', alt.Tooltip('avg_abs_move', format='.2f'), 'market_count']
    ).properties(height=400)
    
    st.altair_chart(chart_volatility, use_container_width=True)
    
    # 2. Volume Distribution
    st.subheader("💰 Volume Distribution")
    
    chart_volume = alt.Chart(df).mark_arc(innerRadius=50).encode(
        theta=alt.Theta(field="total_volume", type="quantitative"),
        color=alt.Color(field="category", type="nominal"),
        tooltip=['category', 'total_volume', 'market_count']
    ).properties(height=400)
    
    st.altair_chart(chart_volume, use_container_width=True)

    # 3. Drill Down
    st.divider()
    st.subheader("🔬 Drill Down")
    
    selected_category = st.selectbox(
        "Select Category to View Top Movers",
        options=df["category"].unique()
    )
    
    if selected_category:
        from apps.dashboard.components import render_mover_card
        
        # Now we filter by category in the SQL query
        movers = MarketQueries.get_top_movers(
            hours=timeframe,
            limit=10,
            category=selected_category,
            direction="both"
        )
        
        if not movers:
            st.warning(f"No significant movers found in {selected_category} for this timeframe.")
        else:
            col_left, col_right = st.columns(2)
            for i, mover in enumerate(movers):
                with col_left if i % 2 == 0 else col_right:
                    render_mover_card(mover)

if __name__ == "__main__":
    main()
