import streamlit as st
import pandas as pd
from packages.core.storage.queries import AnalyticsQueries

st.set_page_config(
    page_title="Alerts Log",
    page_icon="🚨",
    layout="wide"
)

st.title("🚨 Alerts Log")
st.markdown("History of significant market movements.")

# Fetch alerts
limit = st.slider("Number of alerts to show", 10, 200, 50)
alerts = AnalyticsQueries.get_recent_alerts(limit=limit)

if not alerts:
    st.info("No alerts found.")
else:
    # Convert to DataFrame for display
    data = []
    for a in alerts:
        data.append({
            "Time": a["created_at"],
            "Market": a["market_title"],
            "Outcome": a["outcome"],
            "Move": f"{float(a['move_pp']):.2f}pp",
            "Reason": a["reason"],
            "Symbol": a["symbol"]
        })
    
    df = pd.DataFrame(data)
    
    # Show as table
    st.dataframe(
        df,
        column_config={
            "Time": st.column_config.DatetimeColumn(format="D MMM, HH:mm:ss"),
        },
        width="stretch",
        hide_index=True
    )
