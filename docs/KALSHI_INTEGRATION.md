# Kalshi Integration Plan

## API Overview

**Base URL:** `https://api.elections.kalshi.com/trade-api/v2`  
*(Despite "elections" subdomain, serves ALL Kalshi markets)*

**WebSocket:** `wss://api.elections.kalshi.com/trade-api/ws/v2`

**Docs:** https://docs.kalshi.com  
**OpenAPI:** https://docs.kalshi.com/openapi.yaml  
**AsyncAPI:** https://docs.kalshi.com/asyncapi.yaml

---

## Public Endpoints (No Auth Required)

### Markets & Events
```
GET /events                           # List all events (paginated)
GET /events/{event_ticker}           # Single event + nested markets
GET /markets                          # List all markets (filter: series_ticker, status)
GET /markets/{ticker}/orderbook      # Current orderbook
GET /markets/trades                   # Recent trades (paginated)
```

### Series (Categories)
```
GET /series/{series_ticker}          # Series info (title, category, frequency)
```

### Historical Data
```
GET /series/{series_ticker}/markets/{ticker}/candlesticks
  - period_interval: 1 (min), 60 (hour), 1440 (day)
  - start_ts, end_ts: Unix timestamps
```

---

## WebSocket Channels

| Channel | Auth | Description |
|---------|------|-------------|
| `ticker` | No | Full ticker updates (price, volume, OI) |
| `ticker_v2` | No | Incremental ticker deltas |
| `trade` | No | Public trade notifications |
| `orderbook_delta` | Yes | Orderbook changes |
| `market_lifecycle_v2` | No | Market state changes |

### Connection
- Heartbeat: Server sends ping every 10s
- Subscribe: `{"cmd": "subscribe", "params": {"channels": ["ticker"]}}`

---

## Data Mapping: Kalshi → PMM

### Market Fields
| Kalshi | PMM |
|--------|-----|
| `ticker` | `token_id` |
| `event_ticker` | `market_id` |
| `title` | `title` |
| `yes_price` | `price` (÷100 for decimal) |
| `volume` | `volume_24h` |
| `open_interest` | (new field) |

### Key Differences from Polymarket
- Kalshi prices in cents (1-99), Polymarket in decimal (0.01-0.99)
- Kalshi has YES/NO as separate prices, Polymarket has single price
- Kalshi uses tickers (KXHIGHNY-25JAN31), Polymarket uses UUIDs

---

## Implementation Tasks

### Phase 1: REST Collector
1. Add Kalshi adapter in `packages/adapters/kalshi.py`
2. Fetch markets via `GET /markets?status=open`
3. Map to shared schema, store in DB with `source='kalshi'`
4. Run alongside Polymarket collector

### Phase 2: WebSocket Integration
1. Connect to `wss://api.elections.kalshi.com/trade-api/ws/v2`
2. Subscribe to `ticker` channel (no auth needed for public data)
3. Process real-time price updates
4. Share WebSocket manager with Polymarket

### Phase 3: Combined Display
- Add `source` filter to frontend
- Show Kalshi badge on markets
- Cross-platform arbitrage detection?

---

## Rate Limits

**REST:** Not documented for public endpoints (be reasonable)  
**WebSocket:** No message limits documented

---

## Sample Code

### Fetch Markets
```python
import httpx

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

async def get_kalshi_markets():
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{BASE_URL}/markets", params={"status": "open", "limit": 200})
        data = resp.json()
        return data["markets"]
```

### WebSocket Ticker
```python
import websockets
import json

async def kalshi_ticker():
    uri = "wss://api.elections.kalshi.com/trade-api/ws/v2"
    async with websockets.connect(uri) as ws:
        # Subscribe to all tickers
        await ws.send(json.dumps({
            "id": 1,
            "cmd": "subscribe",
            "params": {"channels": ["ticker"]}
        }))
        
        async for msg in ws:
            data = json.loads(msg)
            if data.get("type") == "ticker":
                print(f"{data['msg']['market_ticker']}: {data['msg']['yes_price']}¢")
```

---

## Notes

- Kalshi is CFTC-regulated (US-accessible)
- Polymarket is offshore (not officially US)
- Different user bases = potential price discrepancies = value prop for PMM
