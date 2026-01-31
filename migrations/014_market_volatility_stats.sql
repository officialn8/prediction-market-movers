-- Market Volatility Statistics
-- Tracks historical stats per token for Z-score normalization

BEGIN;

-- =============================================================================
-- MARKET_STATS TABLE
-- Per-token volatility metrics for normalized scoring
-- =============================================================================
CREATE TABLE IF NOT EXISTS market_stats (
    token_id        UUID PRIMARY KEY REFERENCES market_tokens(token_id) ON DELETE CASCADE,
    
    -- Price movement stats (based on 1h windows)
    avg_move_pp     DECIMAL(10, 4),       -- Mean absolute move in pp
    stddev_move_pp  DECIMAL(10, 4),       -- Std dev of moves
    max_move_pp     DECIMAL(10, 4),       -- Max observed move
    
    -- Log-odds stats (for information-theoretic scoring)
    avg_log_odds    DECIMAL(10, 6),       -- Mean log-odds change
    stddev_log_odds DECIMAL(10, 6),       -- Std dev of log-odds
    
    -- Volume stats
    avg_volume      DECIMAL(20, 2),       -- Mean 24h volume
    stddev_volume   DECIMAL(20, 2),       -- Std dev of volume
    
    -- Metadata
    sample_count    INTEGER DEFAULT 0,    -- Number of samples used
    last_updated    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Quality flag
    has_sufficient_data BOOLEAN DEFAULT false  -- True if sample_count >= 10
);

-- Index for quick lookups
CREATE INDEX IF NOT EXISTS idx_market_stats_updated 
ON market_stats(last_updated DESC);

-- =============================================================================
-- VIEW: Market stats with market context
-- =============================================================================
CREATE OR REPLACE VIEW v_market_stats AS
SELECT 
    ms.*,
    mt.market_id,
    mt.outcome,
    m.title,
    m.source,
    m.category
FROM market_stats ms
JOIN market_tokens mt ON ms.token_id = mt.token_id
JOIN markets m ON mt.market_id = m.market_id;

COMMIT;
