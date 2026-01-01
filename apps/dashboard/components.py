from datetime import datetime
import streamlit as st

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
    
    # CSS constants we expect to exist in the parent page
    # (page-title, mover-card, etc are assumed defined in global css or copied)
    
    st.markdown(f"""
    <div class="mover-card" style="background: linear-gradient(135deg, #12121a 0%, #1a1a24 100%); border: 1px solid #2a2a3a; border-radius: 12px; padding: 1.25rem; margin-bottom: 1rem;">
        <div style="display: flex; justify-content: space-between; align-items: flex-start;">
            <div style="flex: 1;">
                <div>
                    <span class="source-tag {source_class}" style="display: inline-block; padding: 0.15rem 0.5rem; border-radius: 4px; font-size: 0.7rem; font-weight: 600; text-transform: uppercase; background: rgba(168, 85, 247, 0.2); color: #a855f7;">{source}</span>
                    <span class="outcome-tag {outcome_class}" style="display: inline-block; padding: 0.15rem 0.5rem; border-radius: 4px; font-size: 0.7rem; font-weight: 600; margin-left: 0.5rem; background: {'rgba(0, 212, 170, 0.15); color: #00d4aa;' if outcome == 'YES' else 'rgba(255, 71, 87, 0.15); color: #ff4757;'}">{outcome}</span>
                </div>
                <p class="market-title" style="font-family: 'Space Grotesk', sans-serif; font-size: 1rem; font-weight: 500; color: #e4e4e7; margin-bottom: 0.5rem; line-height: 1.4;">{mover.get('title', 'Unknown Market')}</p>
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
