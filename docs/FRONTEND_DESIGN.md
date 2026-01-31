# PMM Frontend Design Architecture
## Maximum FOMO & Appeal for Traders

### Core Principle
**Every element should answer: "What am I missing right now?"**

---

## 🎯 Hero Section (Above Fold)

### Live Ticker Banner (Always Visible)
```
┌─────────────────────────────────────────────────────────────────┐
│ 🔴 LIVE │ TRUMP 2028: +8.2pp (2min) │ BTC $100K: -3.1pp │ ...   │
└─────────────────────────────────────────────────────────────────┘
```
- Scrolling marquee of biggest moves in last 15 minutes
- Red pulse animation on extreme moves (>5pp)
- Click to jump to market

### "Right Now" Stats (Social Proof)
```
┌──────────────┬──────────────┬──────────────┬──────────────┐
│  $2.4M       │  847         │  23          │  4,291       │
│  Volume/hr   │  Active      │  Alerts      │  Watching    │
│              │  Markets     │  Triggered   │  Now         │
└──────────────┴──────────────┴──────────────┴──────────────┘
```
- "Watching Now" = active sessions (creates urgency)
- "Alerts Triggered" in last hour (proof system works)

---

## 🔥 Top Movers Section

### Card Design - Urgency First
```
┌─────────────────────────────────────────────────────────────┐
│ POLYMARKET │ YES │ Politics                                 │
│                                                     +12.4pp │
│ Will Trump win 2028 election?                        ↗ 🔥   │
│                                                             │
│ 34¢ → 46¢  │  $847K vol  │  ⏱️ Moved 3 min ago            │
│                                                             │
│ 🔔 142 alerts set │ 👀 1.2K watching │ 📈 5th consecutive ↑ │
│                                                             │
│ [🔔 Set Alert]  [📊 Deep Dive]                             │
└─────────────────────────────────────────────────────────────┘
```

### Visual Urgency Cues
- **Pulsing border** on moves >5pp in last 5min
- **Fire emoji 🔥** for top 3 movers
- **Streak indicator** - "5th consecutive move up"
- **Time since move** - "3 min ago" (not timestamp)
- **Social counters** - alerts set, people watching

### Color System
```css
--extreme-up: #00ff88;     /* >10pp gains - neon green */
--strong-up: #10b981;      /* 5-10pp gains */
--mild-up: #6ee7b7;        /* 1-5pp gains */
--mild-down: #fca5a5;      /* 1-5pp losses */
--strong-down: #ef4444;    /* 5-10pp losses */
--extreme-down: #ff3366;   /* >10pp losses - neon red */
```

---

## 📊 "What You Missed" Section

### Missed Opportunities (FOMO Trigger)
```
┌─────────────────────────────────────────────────────────────┐
│ 💸 WHAT YOU MISSED TODAY                                    │
├─────────────────────────────────────────────────────────────┤
│ If you caught "Fed Rate Cut March" at 8:42 AM:              │
│ → Entry: 23¢  → Peak: 67¢  → +191% in 4 hours              │
│                                                             │
│ [🔔 Never miss again - Set smart alerts]                   │
└─────────────────────────────────────────────────────────────┘
```
- Show 2-3 biggest moves of the day
- Calculate hypothetical returns
- CTA to set alerts

---

## ⚡ Real-Time Feed (WebSocket)

### Live Activity Stream
```
┌─────────────────────────────────────────────────────────────┐
│ 🔴 LIVE ACTIVITY                              [Pause] [🔊]  │
├─────────────────────────────────────────────────────────────┤
│ ⏱️ 2s   BTC $100K JAN: YES spiked +2.1pp ($45K vol)        │
│ ⏱️ 8s   Trump 2028: Alert triggered for @trader_mike       │
│ ⏱️ 15s  Fed March Cut: Large buy detected ($120K)          │
│ ⏱️ 23s  NASDAQ Record: YES dropped -1.8pp                  │
│ ...                                                         │
└─────────────────────────────────────────────────────────────┘
```
- New items slide in from top
- Sound option for big moves
- Shows OTHER users' alerts triggering (social proof)

---

## 🔔 Alert Configuration (Conversion Point)

### Smart Alert Builder
```
┌─────────────────────────────────────────────────────────────┐
│ 🔔 CREATE SMART ALERT                                       │
├─────────────────────────────────────────────────────────────┤
│ Market: [Trump 2028 ▼]                                      │
│                                                             │
│ Alert me when:                                              │
│ ○ Price moves ±[5]pp in [1 hour]                           │
│ ○ Price crosses [50]¢                                       │
│ ○ Volume spikes [3x] normal                                 │
│ ○ Z-score exceeds [2.0] (unusual move)                     │
│                                                             │
│ Notify via:                                                 │
│ ☑ Email  ☑ Browser  ☐ Webhook  ☐ SMS (Pro)                 │
│                                                             │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ ⚠️ You have 2/3 free alerts remaining                   │ │
│ │ [Upgrade to Pro for 25 alerts →]                        │ │
│ └─────────────────────────────────────────────────────────┘ │
│                                                             │
│                              [Cancel]  [Create Alert 🔔]    │
└─────────────────────────────────────────────────────────────┘
```

---

## 📱 Mobile-First Considerations

### Swipe Actions
- Swipe right on mover → Quick add to watchlist
- Swipe left → Set alert
- Pull down → Refresh

### Notification Priority
- **Critical**: Alerts YOU set
- **High**: Extreme moves (>10pp)
- **Medium**: Watchlist moves
- **Low**: Market opens/closes

---

## 🎨 Visual Hierarchy

### Typography
```
Headings:    DM Sans 700
Numbers:     IBM Plex Mono 600 (prices, percentages)
Body:        DM Sans 400
Labels:      DM Sans 500, 0.75rem, uppercase
```

### Animation Guidelines
- **New movers**: Slide in + brief glow
- **Price updates**: Number flip animation
- **Extreme moves**: Pulse border 3x then settle
- **Alerts**: Toast notification + optional sound

---

## 💰 Conversion Triggers

### Free → Pro Upgrade Points
1. **Alert limit hit**: "You've used all 3 free alerts"
2. **API access tease**: "Export this data? [Pro feature]"
3. **Advanced filters locked**: Volume spike filter is Pro
4. **Watchlist limit**: "Add more with Pro"

### Urgency Copy
- "847 traders set alerts on this market"
- "This moved 12pp while you were away"
- "Pro users got notified 3 min before you saw this"

---

## 🔌 Technical Requirements

### WebSocket Events
```typescript
interface MoverEvent {
  type: 'price_update' | 'alert_triggered' | 'volume_spike';
  market_id: string;
  token_id: string;
  data: {
    price_now: number;
    price_then: number;
    move_pp: number;
    volume: number;
    z_score?: number;
    watching_count: number;
    alerts_set: number;
  };
  timestamp: number;
}
```

### State Management
- **Zustand** for global state (auth, preferences)
- **TanStack Query** for server state (movers, markets)
- **WebSocket** for real-time updates

### Key Metrics to Track
- Time on page
- Alert creation funnel
- Upgrade click-through
- Most-watched markets
- Alert trigger → user return rate

---

## 🚀 Launch Priorities

### MVP (Week 1)
1. Live top movers with source tags (Polymarket/Kalshi)
2. Basic alert creation (price threshold)
3. Stripe checkout integration

### V1.1 (Week 2)
4. "What You Missed" section
5. WebSocket live feed
6. Social counters (watching, alerts set)

### V1.2 (Week 3)
7. Advanced alerts (Z-score, volume spike)
8. Browser notifications
9. Mobile PWA

---

*Design principle: Show them what they're missing, not what they have.*
