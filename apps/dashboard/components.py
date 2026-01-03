from datetime import datetime, timedelta
from typing import Optional
import uuid
import pandas as pd
import streamlit as st
import altair as alt
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

try:
    from streamlit_javascript import st_javascript
except ImportError:
    # Fallback to avoid crash if dependency is missing
    def st_javascript(js_code, key=None):
        return None

from packages.core.storage.queries import WatchlistQueries, MarketQueries


def get_session_id() -> str:
    """Get persistent session ID using localStorage or generate new."""
    if 'user_session_id' in st.session_state:
        return st.session_state.user_session_id
    
    # Attempt to retrieve from localStorage
    try:
        uid_from_js = st_javascript("localStorage.getItem('pm_movers_uid')", key="get_uid_js")
    except Exception:
        uid_from_js = None

    if uid_from_js:
        st.session_state.user_session_id = uid_from_js
        return uid_from_js
        
    # If not found or not yet returned, generate new one to use immediately
    if 'temp_uid' not in st.session_state:
        st.session_state.temp_uid = str(uuid.uuid4())
    
    # We return the temp UID but also try to save it. 
    # If the user refreshes, they might get the saved one.
    uid = st.session_state.temp_uid
    st.session_state.user_session_id = uid
    
    try:
        st_javascript(f"localStorage.setItem('pm_movers_uid', '{uid}')", key="set_uid_js")
    except Exception:
        pass
        
    return uid


def get_user_timezone() -> str:
    """Get the user's timezone from the browser."""
    if 'user_timezone' in st.session_state:
        return st.session_state.user_timezone
        
    try:
        tz = st_javascript("Intl.DateTimeFormat().resolvedOptions().timeZone", key="get_tz_js")
        if tz:
            st.session_state.user_timezone = tz
            return tz
    except Exception:
        pass
        
    return "UTC"


def to_user_tz(dt: datetime) -> datetime:
    """Convert a datetime to the user's timezone."""
    if dt is None:
        return None
        
    # Ensure dt is timezone-aware and in UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    
    user_tz = get_user_timezone()
    try:
        return dt.astimezone(ZoneInfo(user_tz))
    except Exception:
        return dt # Fallback


def init_watchlist():
    """Initialize watchlist from database using session ID."""
    if 'watchlist_initialized' not in st.session_state:
        uid = get_session_id()
        # Trigger timezone fetch early
        get_user_timezone() 
        
        # Load from DB
        items = WatchlistQueries.get_all(uid)
        st.session_state.watchlist = {
            str(item['market_id']): item for item in items
        }
        st.session_state.watchlist_initialized = True


def toggle_watchlist(market_id: str, title: str, source: str) -> bool:
    """Toggle a market in/out of the watchlist backed by DB."""
    init_watchlist()
    uid = get_session_id()
    
    if market_id in st.session_state.watchlist:
        # Remove
        WatchlistQueries.remove(uid, market_id)
        if market_id in st.session_state.watchlist:
            del st.session_state.watchlist[market_id]
        return False
    else:
        # Add
        WatchlistQueries.add(uid, market_id)
        st.session_state.watchlist[market_id] = {
            'title': title,
            'source': source,
            'added_at': datetime.now().isoformat()
        }
        return True


def is_in_watchlist(market_id: str) -> bool:
    """Check if a market is in the watchlist."""
    try:
        init_watchlist()
        return market_id in st.session_state.watchlist
    except Exception:
        return False


def get_watchlist() -> dict:
    """Get the current watchlist."""
    try:
        init_watchlist()
        return st.session_state.watchlist
    except Exception:
        return {}


def render_mover_list_item(mover: dict, show_watchlist: bool = True) -> None:
    """
    Render a simplified list item for a mover (The 'Killer View').
    Format: [Change Badge] [Title] [Price Flow] [Actions]
    """
    market_id = str(mover.get("market_id", ""))
    token_id = mover.get("token_id")
    title = mover.get('title', 'Unknown Market')
    
    # Extract Data
    pct_change = float(mover.get("pct_change", 0))
    latest_price = float(mover.get("latest_price", 0))
    old_price = float(mover.get("old_price", 0))
    
    # Colors & Sign
    if pct_change > 0:
        color = "🟢"
        sign = "+"
        change_class = "positive"
    elif pct_change < 0:
        color = "🔴"
        sign = ""
        change_class = "negative"
    else:
        color = "⚪"
        sign = ""
        change_class = "neutral"
        
    change_text = f"{color} {sign}{pct_change:.0f}%" # Rounded per design
    prices_text = f"{int(old_price*100)}¢→{int(latest_price*100)}¢" # Cents per design
    
    # Check Watchlist
    in_watchlist = is_in_watchlist(market_id) if market_id else False
    
    # Render Container
    with st.container():
        # Adjust column ratios for the list view
        c1, c2, c3, c4 = st.columns([0.15, 0.50, 0.20, 0.15])
        
        with c1:
            st.markdown(f"**{change_text}**")
            
        with c2:
            st.markdown(f"{title}")
            
        with c3:
            st.markdown(f"**{prices_text}**")
            
        with c4:
            pass

    # The expander must be outside the columns to span full width
    with st.expander("Expand Details", expanded=False):
        c_chart, c_actions = st.columns([3, 1])
        with c_chart:
             if token_id:
                try:
                    hist = MarketQueries.get_snapshots_range(
                        token_id, 
                        start_ts=datetime.now() - timedelta(hours=24)
                    )
                    if hist:
                        df = pd.DataFrame(hist)
                        
                        # Convert to user timezone
                        df['ts'] = pd.to_datetime(df['ts'])
                        df['ts'] = df['ts'].apply(lambda x: to_user_tz(x))
                        
                        df['price'] = df['price'].astype(float)
                        
                        # Create Altair Sparkline
                        # Minimal tooltip, no axes
                        chart = alt.Chart(df).mark_line().encode(
                            x=alt.X('ts', axis=None),
                            y=alt.Y('price', axis=None, scale=alt.Scale(zero=False)),
                            tooltip=[
                                alt.Tooltip('ts', title='Date', format='%b %d %H:%M'),
                                alt.Tooltip('price', title='Price', format='$.3f')
                            ]
                        ).properties(
                            height=150
                        ).configure_view(
                            strokeWidth=0  # Remove border
                        )
                        
                        st.altair_chart(chart, use_container_width=True)
                        
                except Exception:
                    st.caption("Chart unavailable")
        
        with c_actions:
             if show_watchlist:
                lbl = "Remove Watchlist" if in_watchlist else "Add Watchlist"
                if st.button(lbl, key=f"wl_{market_id}_{token_id}"):
                    toggle_watchlist(market_id, title, mover.get("source", "unknown"))
                    st.rerun()
             
             if st.button("Full Page", key=f"fp_{market_id}_{token_id}"):
                 st.session_state["selected_market_id"] = market_id
                 st.switch_page("pages/2_Market_Detail.py")
                 
    st.markdown("---") # Divider between rows
