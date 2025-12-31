<<<<<<< HEAD
=======
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
| `POSTGRES_USER` | Database user | `pmuser` |
| `POSTGRES_PASSWORD` | Database password | `pmpass` |
| `POSTGRES_DB` | Database name | `prediction_movers` |
| `DATABASE_URL` | Full connection string | Auto-generated |
| `SYNC_INTERVAL_SECONDS` | Data sync frequency | `300` |
| `POLYMARKET_API_KEY` | Polymarket API key | - |
| `KALSHI_API_KEY` | Kalshi API key | - |
| `KALSHI_API_SECRET` | Kalshi API secret | - |

## 📦 Project Structure

```
prediction-movers/
├── docker-compose.yml       # Service orchestration
├── requirements.txt         # Python dependencies
├── apps/
│   ├── collector/          # Data ingestion service
│   │   ├── main.py         # Entry point
│   │   ├── jobs/           # Sync jobs (polymarket, kalshi)
│   │   └── adapters/       # API clients
│   └── dashboard/          # Streamlit frontend
│       ├── app.py          # Main dashboard
│       └── pages/          # Dashboard pages
├── packages/
│   ├── core/
│   │   ├── models.py       # Pydantic data models
│   │   ├── settings.py     # Configuration
│   │   └── storage/        # Database layer
│   │       ├── db.py       # Connection pooling
│   │       └── queries.py  # SQL queries
│   └── utils/
└── migrations/
    └── 001_init.sql        # Database schema
```

## 🗄️ Database Schema

### Tables

- **`markets`**: Canonical market information (id, source, title, category, status)
- **`market_tokens`**: Tradeable outcomes per market (YES/NO tokens)
- **`snapshots`**: Append-only price time series

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
python -m apps.collector.main

# Run dashboard locally
streamlit run apps/dashboard/app.py
```

### Database Migrations

Migrations run automatically on first Postgres startup. For manual execution:

```bash
docker-compose exec postgres psql -U pmuser -d prediction_movers -f /docker-entrypoint-initdb.d/001_init.sql
```

## 📊 Dashboard Pages

1. **Home** - System status and quick stats
2. **Top Movers** - Markets with highest % price changes
3. **Market Detail** - Deep dive with price charts

## 🛣️ Roadmap

- [x] Phase 1: Infrastructure & Database
- [ ] Phase 2: Polymarket Integration
- [ ] Phase 3: Kalshi Integration
- [ ] Phase 4: Advanced Analytics
- [ ] Phase 5: Alerts & Notifications

## 📝 License

MIT

>>>>>>> 81d4d73 (Initial commit)
