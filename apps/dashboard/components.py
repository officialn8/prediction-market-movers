from datetime import datetime, timedelta
from typing import Optional
import uuid
import pandas as pd
import streamlit as st

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


def init_watchlist():
    """Initialize watchlist from database using session ID."""
    if 'watchlist_initialized' not in st.session_state:
        uid = get_session_id()
        
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


def render_mover_card(mover: dict, show_watchlist: bool = True) -> None:
    """Render a single mover card with toggleable details."""
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

    market_id = str(mover.get("market_id", ""))
    title = mover.get('title', 'Unknown Market')
    token_id = mover.get("token_id")

    # Check if in watchlist
    in_watchlist = is_in_watchlist(market_id) if market_id else False
    star_icon = "★" if in_watchlist else "☆"

    # Render the card HTML
    st.markdown(f"""
    <div class="mover-card" style="background: linear-gradient(135deg, #12121a 0%, #1a1a24 100%); border: 1px solid #2a2a3a; border-radius: 12px; padding: 1.25rem; margin-bottom: 0.5rem;">
        <div style="display: flex; justify-content: space-between; align-items: flex-start;">
            <div style="flex: 1;">
                <div>
                    <span class="source-tag {source_class}" style="display: inline-block; padding: 0.15rem 0.5rem; border-radius: 4px; font-size: 0.7rem; font-weight: 600; text-transform: uppercase; background: rgba(168, 85, 247, 0.2); color: #a855f7;">{source}</span>
                    <span class="outcome-tag {outcome_class}" style="display: inline-block; padding: 0.15rem 0.5rem; border-radius: 4px; font-size: 0.7rem; font-weight: 600; margin-left: 0.5rem; background: {'rgba(0, 212, 170, 0.15); color: #00d4aa;' if outcome == 'YES' else 'rgba(255, 71, 87, 0.15); color: #ff4757;'}">{outcome}</span>
                </div>
                <p class="market-title" style="font-family: 'Space Grotesk', sans-serif; font-size: 1rem; font-weight: 500; color: #e4e4e7; margin-bottom: 0.5rem; line-height: 1.4;">{title}</p>
                <p class="price-info" style="font-family: 'JetBrains Mono', monospace; font-size: 0.85rem; color: #71717a;">
                    ${old_price:.2f} → ${latest_price:.2f}
                </p>
                <div style="margin-top: 0.5rem; font-size: 0.85rem; color: #a1a1aa;">
                    ℹ️ {reason}
                </div>
            </div>
            <div style="text-align: right;">
                <p class="price-change {change_class}" style="font-family: 'JetBrains Mono', monospace; font-size: 1.5rem; font-weight: 600; color: {'#00d4aa' if pct_change > 0 else '#ff4757'};">{change_sign}{pct_change:.1f}%</p>
                <p class="price-info" style="margin-top: 0;">{mover.get('category', 'Uncategorized')}</p>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Actions & Details Expander (Task 2)
    # Using a unique key for the expander to avoid conflicts
    with st.expander("📊 View Details & Actions"):
        c1, c2 = st.columns([2, 1])
        
        with c1:
            if token_id:
                # Fetch history for mini-chart
                try:
                    hist = MarketQueries.get_snapshots_range(
                        token_id, 
                        start_ts=datetime.now() - timedelta(hours=24)
                    )
                    if hist:
                        df = pd.DataFrame(hist)
                        df['ts'] = pd.to_datetime(df['ts'])
                        # Simple line chart
                        st.line_chart(df.set_index('ts')['price'], height=150)
                    else:
                        st.caption("No recent history available.")
                except Exception:
                    st.caption("Error loading history.")
            else:
                st.caption("Token ID missing.")
                
        with c2:
            st.write("Actions")
            if show_watchlist and market_id:
                btn_len = "Remove Watchlist" if in_watchlist else "Add Watchlist"
                if st.button(btn_len, key=f"mini_watch_{market_id}_{outcome}", use_container_width=True):
                    toggle_watchlist(market_id, title, source)
                    st.rerun()
            
            if st.button("Full Market Page", key=f"mini_view_{market_id}", use_container_width=True):
                # We can't easily pass state via switch_page until latest Streamlit, but we can set session state
                st.session_state["selected_market_id"] = market_id
                st.switch_page("pages/2_Market_Detail.py")
