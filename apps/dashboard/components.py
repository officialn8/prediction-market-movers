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
    init_watchlist()
    return st.session_state.watchlist


def format_volume(volume: float) -> str:
    """Format volume as human-readable string."""
    if volume >= 1_000_000:
        return f"${volume/1_000_000:.1f}M"
    elif volume >= 1_000:
        return f"${volume/1_000:.1f}k"
    else:
        return f"${volume:.0f}"


def get_spike_badge(spike_ratio: Optional[float]) -> str:
    """Generate HTML badge for volume spike indicator."""
    if spike_ratio is None or spike_ratio < 1.5:
        return ""

    if spike_ratio >= 10:
        color = "#ff4757"  # Red - extreme
        label = f"🔥 {spike_ratio:.1f}x VOL"
    elif spike_ratio >= 5:
        color = "#ffa502"  # Orange - high
        label = f"🔥 {spike_ratio:.1f}x VOL"
    elif spike_ratio >= 3:
        color = "#fbbf24"  # Yellow - medium
        label = f"📈 {spike_ratio:.1f}x VOL"
    else:
        color = "#71717a"  # Gray - low
        label = f"↑ {spike_ratio:.1f}x vol"

    return f'<span style="display: inline-block; padding: 0.15rem 0.5rem; border-radius: 4px; font-size: 0.7rem; font-weight: 600; margin-left: 0.5rem; background: rgba({_hex_to_rgb(color)}, 0.2); color: {color};">{label}</span>'


def _hex_to_rgb(hex_color: str) -> str:
    """Convert hex color to RGB string for rgba()."""
    hex_color = hex_color.lstrip('#')
    r, g, b = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
    return f"{r}, {g}, {b}"


def generate_reason(pct_change: float, volume: float, outcome: str, spike_ratio: Optional[float] = None) -> str:
    """Generate a readable reason for the move."""
    direction = "spiked" if pct_change > 0 else "dropped"
    abs_pct = abs(pct_change)

    vol_str = format_volume(volume)

    reason = f"**{outcome}** {direction} **{abs_pct:.1f}%** on {vol_str} vol"

    # Add spike context if significant
    if spike_ratio and spike_ratio >= 2.0:
        reason += f" (**{spike_ratio:.1f}x** normal)"

    return reason


def render_mover_card(mover: dict, show_watchlist: bool = True) -> None:
    """Render a single mover card with optional watchlist button and volume spike indicator."""
    pct_change = float(mover.get("pct_change") or mover.get("move_pp") or 0)
    change_class = "positive" if pct_change > 0 else "negative"
    change_sign = "+" if pct_change > 0 else ""

    source = mover.get("source", "unknown")
    source_class = f"source-{source}"

    outcome = mover.get("outcome", "YES")
    outcome_class = "outcome-yes" if outcome == "YES" else "outcome-no"

    latest_price = float(mover.get("latest_price") or mover.get("price_now") or 0)
    old_price = float(mover.get("old_price") or mover.get("price_then") or 0)

    # Try to get volume from various keys (cache vs raw SQL might differ)
    volume = float(mover.get("latest_volume") or mover.get("current_volume") or mover.get("volume_24h") or 0)

    # Get volume spike ratio if available
    spike_ratio = mover.get("volume_spike_ratio")
    if spike_ratio is not None:
        try:
            spike_ratio = float(spike_ratio)
        except (ValueError, TypeError):
            spike_ratio = None

    # Generate reason with spike context
    reason = generate_reason(pct_change, volume, outcome, spike_ratio)

    # Get spike badge HTML
    spike_badge = get_spike_badge(spike_ratio)

    market_id = str(mover.get("market_id", ""))
    title = mover.get('title', 'Unknown Market')

    # Quality score for display (if available)
    quality_score = mover.get("quality_score")
    quality_str = ""
    if quality_score:
        try:
            quality_str = f'<span style="font-size: 0.7rem; color: #71717a; margin-left: 0.5rem;">Score: {float(quality_score):.1f}</span>'
        except:
            pass

    # Check if in watchlist
    in_watchlist = is_in_watchlist(market_id) if market_id else False
    star_icon = "★" if in_watchlist else "☆"
    star_color = "#fbbf24" if in_watchlist else "#71717a"

    # Render the card HTML
    st.html(f"""
    <div class="mover-card" style="background: linear-gradient(135deg, #12121a 0%, #1a1a24 100%); border: 1px solid #2a2a3a; border-radius: 12px; padding: 1.25rem; margin-bottom: 0.5rem;">
        <div style="display: flex; justify-content: space-between; align-items: flex-start;">
            <div style="flex: 1;">
                <div>
                    <span class="source-tag {source_class}" style="display: inline-block; padding: 0.15rem 0.5rem; border-radius: 4px; font-size: 0.7rem; font-weight: 600; text-transform: uppercase; background: rgba(168, 85, 247, 0.2); color: #a855f7;">{source}</span>
                    <span class="outcome-tag {outcome_class}" style="display: inline-block; padding: 0.15rem 0.5rem; border-radius: 4px; font-size: 0.7rem; font-weight: 600; margin-left: 0.5rem; background: {'rgba(0, 212, 170, 0.15); color: #00d4aa;' if outcome == 'YES' else 'rgba(255, 71, 87, 0.15); color: #ff4757;'}">{outcome}</span>
                    {spike_badge}
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
    """)

    # Render watchlist button below the card (Streamlit button)
    if show_watchlist and market_id:
        btn_label = f"{star_icon} {'Remove from' if in_watchlist else 'Add to'} Watchlist"
        if st.button(btn_label, key=f"watch_{market_id}_{outcome}", width="stretch"):
            toggle_watchlist(market_id, title, source)
            st.rerun()


def render_volume_spike_alert(spike: dict) -> None:
    """Render a volume spike alert card."""
    title = spike.get("title", "Unknown Market")
    outcome = spike.get("outcome", "")
    spike_ratio = float(spike.get("spike_ratio", 0))
    current_volume = float(spike.get("current_volume", 0))
    avg_volume = float(spike.get("avg_volume", 0))
    severity = spike.get("severity", "medium")
    current_price = spike.get("current_price")
    price_change = spike.get("price_change_1h")

    # Color by severity
    severity_colors = {
        "low": "#71717a",
        "medium": "#fbbf24",
        "high": "#ffa502",
        "extreme": "#ff4757",
    }
    color = severity_colors.get(severity, "#fbbf24")

    # Price change display
    price_str = ""
    if current_price:
        price_str = f"${float(current_price):.2f}"
    if price_change:
        pc = float(price_change)
        sign = "+" if pc > 0 else ""
        price_str += f" ({sign}{pc:.1f}%)"

    st.html(f"""
    <div style="background: linear-gradient(135deg, #12121a 0%, #1a1a24 100%); border: 1px solid {color}; border-radius: 12px; padding: 1rem; margin-bottom: 0.5rem;">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <div>
                <span style="display: inline-block; padding: 0.2rem 0.5rem; border-radius: 4px; font-size: 0.7rem; font-weight: 600; text-transform: uppercase; background: rgba({_hex_to_rgb(color)}, 0.2); color: {color};">
                    🔥 {severity.upper()} VOLUME SPIKE
                </span>
                <p style="font-family: 'Space Grotesk', sans-serif; font-size: 0.95rem; font-weight: 500; color: #e4e4e7; margin: 0.5rem 0 0.25rem 0;">
                    {title} {f'({outcome})' if outcome else ''}
                </p>
                <p style="font-family: 'JetBrains Mono', monospace; font-size: 0.8rem; color: #71717a;">
                    {format_volume(current_volume)} volume (avg {format_volume(avg_volume)})
                    {f' | {price_str}' if price_str else ''}
                </p>
            </div>
            <div style="text-align: right;">
                <p style="font-family: 'JetBrains Mono', monospace; font-size: 1.5rem; font-weight: 600; color: {color};">
                    {spike_ratio:.1f}x
                </p>
                <p style="font-size: 0.75rem; color: #71717a;">normal volume</p>
            </div>
        </div>
    </div>
    """)
