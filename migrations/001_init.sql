-- Prediction Market Movers - Initial Schema
-- Normalized schema for multi-source prediction market data

BEGIN;

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================================================
-- MARKETS TABLE
-- Stores canonical market information from all sources
-- =============================================================================
CREATE TABLE IF NOT EXISTS markets (
    market_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source          VARCHAR(32) NOT NULL CHECK (source IN ('polymarket', 'kalshi')),
    source_id       VARCHAR(255) NOT NULL,
    title           TEXT NOT NULL,
    category        VARCHAR(128),
    status          VARCHAR(32) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'closed', 'resolved')),
    url             TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Prevent duplicate markets from the same source
    CONSTRAINT uq_source_market UNIQUE (source, source_id)
);

-- Index for filtering by source and status
CREATE INDEX IF NOT EXISTS idx_markets_source ON markets(source);
CREATE INDEX IF NOT EXISTS idx_markets_status ON markets(status);
CREATE INDEX IF NOT EXISTS idx_markets_category ON markets(category);

-- =============================================================================
-- MARKET_TOKENS TABLE
-- Stores individual tradeable outcomes (YES/NO tokens) for each market
-- =============================================================================
CREATE TABLE IF NOT EXISTS market_tokens (
    token_id        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    market_id       UUID NOT NULL REFERENCES markets(market_id) ON DELETE CASCADE,
    outcome         VARCHAR(32) NOT NULL CHECK (outcome IN ('YES', 'NO')),
    symbol          VARCHAR(128),
    source_token_id VARCHAR(255),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Each market should have unique outcomes
    CONSTRAINT uq_market_outcome UNIQUE (market_id, outcome)
);

-- Index for looking up tokens by market
CREATE INDEX IF NOT EXISTS idx_tokens_market_id ON market_tokens(market_id);

-- =============================================================================
-- SNAPSHOTS TABLE
-- Append-only time-series table for price/volume data
-- Optimized for time-range queries and aggregations
-- =============================================================================
CREATE TABLE IF NOT EXISTS snapshots (
    ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    token_id        UUID NOT NULL REFERENCES market_tokens(token_id) ON DELETE CASCADE,
    price           DECIMAL(10, 6) NOT NULL CHECK (price >= 0 AND price <= 1),
    volume_24h      DECIMAL(20, 2),
    spread          DECIMAL(10, 6),
    
    -- Composite primary key for time-series data
    PRIMARY KEY (ts, token_id)
);

-- Critical indexes for time-series queries
-- BRIN index is efficient for append-only time-series data
CREATE INDEX IF NOT EXISTS idx_snapshots_ts_brin ON snapshots USING BRIN(ts);

-- B-tree index for token lookups with time ordering
CREATE INDEX IF NOT EXISTS idx_snapshots_token_ts ON snapshots(token_id, ts DESC);

-- Index for finding latest snapshot per token
CREATE INDEX IF NOT EXISTS idx_snapshots_token_id ON snapshots(token_id);

-- =============================================================================
-- HELPER VIEWS
-- =============================================================================

-- View: Latest snapshot for each token (useful for current prices)
CREATE OR REPLACE VIEW v_latest_snapshots AS
SELECT DISTINCT ON (token_id)
    s.ts,
    s.token_id,
    s.price,
    s.volume_24h,
    s.spread,
    mt.market_id,
    mt.outcome,
    m.title,
    m.source,
    m.category
FROM snapshots s
JOIN market_tokens mt ON s.token_id = mt.token_id
JOIN markets m ON mt.market_id = m.market_id
WHERE m.status = 'active'
ORDER BY token_id, ts DESC;

-- View: Price changes over different time windows
CREATE OR REPLACE VIEW v_price_movers AS
WITH latest AS (
    SELECT DISTINCT ON (token_id)
        token_id,
        ts as latest_ts,
        price as latest_price
    FROM snapshots
    ORDER BY token_id, ts DESC
),
hourly AS (
    SELECT DISTINCT ON (token_id)
        token_id,
        price as price_1h_ago
    FROM snapshots
    WHERE ts <= NOW() - INTERVAL '1 hour'
    ORDER BY token_id, ts DESC
),
daily AS (
    SELECT DISTINCT ON (token_id)
        token_id,
        price as price_24h_ago
    FROM snapshots
    WHERE ts <= NOW() - INTERVAL '24 hours'
    ORDER BY token_id, ts DESC
)
SELECT 
    l.token_id,
    l.latest_ts,
    l.latest_price,
    h.price_1h_ago,
    d.price_24h_ago,
    CASE 
        WHEN h.price_1h_ago > 0 THEN 
            ROUND(((l.latest_price - h.price_1h_ago) / h.price_1h_ago * 100)::numeric, 2)
        ELSE NULL 
    END as pct_change_1h,
    CASE 
        WHEN d.price_24h_ago > 0 THEN 
            ROUND(((l.latest_price - d.price_24h_ago) / d.price_24h_ago * 100)::numeric, 2)
        ELSE NULL 
    END as pct_change_24h
FROM latest l
LEFT JOIN hourly h ON l.token_id = h.token_id
LEFT JOIN daily d ON l.token_id = d.token_id;

-- =============================================================================
-- FUNCTIONS
-- =============================================================================

-- Function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger to auto-update markets.updated_at
CREATE TRIGGER update_markets_updated_at
    BEFORE UPDATE ON markets
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

COMMIT;

