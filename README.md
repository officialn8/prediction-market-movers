# 📈 Prediction Market Movers

Real-time tracking of price movements across Polymarket and Kalshi prediction markets.

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+ (for local development)

### Running with Docker

1. **Create environment file:**
   ```bash
   cp .env.example .env
   # Edit .env with your settings
   ```

2. **Start all services:**
   ```bash
   docker-compose up -d
   ```

3. **Access the dashboard:**
   Open http://localhost:8501 in your browser

4. **Check service status:**
   ```bash
   docker-compose ps
   docker-compose logs -f collector
   ```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | Auto-generated |
| `COLLECTOR_MODE` | `simulated`, `polymarket`, or `live` | `simulated` |
| `COLLECTOR_INTERVAL_SECONDS` | Polling interval (when not using WSS) | `30` |
| `POLYMARKET_USE_WSS` | Enable WebSocket real-time streaming | `false` |
| `WSS_WATCHDOG_TIMEOUT` | Seconds without messages before reconnect | `120` |
| `WSS_RECONNECT_DELAY` | Seconds to wait before reconnecting | `5` |
| `WSS_MAX_RECONNECT_ATTEMPTS` | Max failures before fallback to polling | `10` |
| `POLYMARKET_API_KEY` | Polymarket API key (optional) | - |
| `KALSHI_API_KEY` | Kalshi API key | - |
| `KALSHI_API_SECRET` | Kalshi API secret | - |

## 📦 Project Structure

```
prediction-market-movers/
├── docker-compose.yml       # Service orchestration
├── requirements.txt         # Python dependencies
├── apps/
│   ├── collector/          # Data ingestion service
│   │   ├── main.py         # Entry point & background jobs
│   │   ├── jobs/           # Scheduled tasks
│   │   │   ├── polymarket_sync.py      # REST API polling
│   │   │   ├── polymarket_wss_sync.py  # WebSocket real-time
│   │   │   ├── movers_cache.py         # Top movers precompute
│   │   │   ├── rollups.py              # OHLC aggregation
│   │   │   ├── alerts.py               # System alerts
│   │   │   ├── user_alerts.py          # Custom user alerts
│   │   │   └── volume_spikes.py        # Volume anomaly detection
│   │   └── adapters/       # API clients
│   │       ├── polymarket.py           # REST adapter
│   │       └── polymarket_wss.py       # WebSocket adapter
│   └── dashboard/          # Streamlit frontend
│       ├── app.py          # Main dashboard
│       └── pages/          # Dashboard pages
│           ├── 1_Top_Movers.py
│           ├── 2_Market_Detail.py
│           ├── 2_Category_Trends.py
│           ├── 3_Alerts_Log.py
│           ├── 4_Watchlist.py
│           └── 5_Custom_Alerts.py
├── packages/
│   ├── core/
│   │   ├── models.py       # Pydantic data models
│   │   ├── settings.py     # Configuration
│   │   ├── wss.py          # WebSocket metrics
│   │   ├── analytics/      # Scoring & metrics
│   │   └── storage/        # Database layer
│   │       ├── db.py       # Connection pooling
│   │       └── queries.py  # SQL queries
│   └── utils/
└── migrations/             # Auto-applied SQL migrations
    ├── 001_init.sql
    ├── 002_analytics_and_alerts.sql
    └── ...
```

## 🗄️ Database Schema

### Tables

- **`markets`**: Canonical market information (id, source, title, category, status)
- **`market_tokens`**: Tradeable outcomes per market (YES/NO tokens)
- **`snapshots`**: Append-only price time series
- **`ohlc_5m` / `ohlc_1h`**: Aggregated candlestick data
- **`movers_cache`**: Precomputed top movers with composite scores
- **`alerts`**: System-generated alerts
- **`user_alerts`**: Custom user-defined price alerts
- **`volume_spikes`**: Detected volume anomalies
- **`watchlist`**: User watchlist items

### Key Indexes
- BRIN index on `snapshots.ts` for efficient time-range queries
- Composite index on `(token_id, ts DESC)` for latest price lookups

## 🔧 Development

### Local Setup

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Start only Postgres
docker-compose up -d postgres

# Run collector locally
COLLECTOR_MODE=polymarket POLYMARKET_USE_WSS=true python -m apps.collector.main

# Run dashboard locally
streamlit run apps/dashboard/app.py
```

### Database Migrations

Migrations run automatically on collector startup. They are tracked in the `schema_migrations` table and only run once.

## 📊 Dashboard Pages

1. **Home** - System status, WSS health, and quick stats
2. **Top Movers** - Markets with highest price changes (composite scored)
3. **Market Detail** - Deep dive with price charts
4. **Category Trends** - Market activity by category
5. **Alerts Log** - System-generated alerts history
6. **Watchlist** - Track specific markets
7. **Custom Alerts** - Create price threshold alerts

## ⚡ Real-Time Mode (WebSocket)

When `POLYMARKET_USE_WSS=true`, the collector uses Polymarket's WebSocket API for sub-second price updates:

- **~26k+ messages/minute** throughput
- **Instant mover detection** (5+ percentage point moves)
- **Automatic reconnection** with watchdog timeout
- **Health logging** every 60 seconds
- **Fallback to polling** after max reconnect attempts

Monitor WSS health in logs:
```
WSS Health: 26563 msgs in 60s (26563.0/min), subscriptions=3180
```

## 🛣️ Roadmap

- [x] Phase 1: Infrastructure & Database
- [x] Phase 2: Polymarket Integration (REST + WebSocket)
- [x] Phase 3: Real-time Analytics (OHLC, Volume Spikes, Movers)
- [x] Phase 4: Alerts & Notifications
- [x] Phase 5: Dashboard & Visualization
- [ ] Phase 6: Kalshi Integration
- [ ] Phase 7: Advanced ML-based Anomaly Detection

## 📝 License

MIT
