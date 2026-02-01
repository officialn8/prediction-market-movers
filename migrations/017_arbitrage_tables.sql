-- =============================================================================
-- Arbitrage Detection Tables
-- Cross-platform arbitrage opportunities between Polymarket and Kalshi
-- =============================================================================

BEGIN;

-- =============================================================================
-- MARKET_PAIRS TABLE
-- Stores matched markets between different platforms for arbitrage detection
-- =============================================================================
CREATE TABLE IF NOT EXISTS market_pairs (
    pair_id                 UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    polymarket_market_id    UUID NOT NULL REFERENCES markets(market_id) ON DELETE CASCADE,
    kalshi_market_id        UUID NOT NULL REFERENCES markets(market_id) ON DELETE CASCADE,
    matching_method         VARCHAR(32) NOT NULL CHECK (matching_method IN ('manual', 'fuzzy', 'exact')),
    similarity_score        DECIMAL(5, 4),  -- 0 to 1 for fuzzy matching
    notes                   TEXT,
    active                  BOOLEAN NOT NULL DEFAULT true,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Ensure each market can only be paired once
    CONSTRAINT uq_polymarket_market UNIQUE (polymarket_market_id),
    CONSTRAINT uq_kalshi_market UNIQUE (kalshi_market_id),

    -- Ensure markets are from different sources
    CONSTRAINT chk_different_sources CHECK (polymarket_market_id != kalshi_market_id)
);

-- Indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_market_pairs_polymarket ON market_pairs(polymarket_market_id);
CREATE INDEX IF NOT EXISTS idx_market_pairs_kalshi ON market_pairs(kalshi_market_id);
CREATE INDEX IF NOT EXISTS idx_market_pairs_active ON market_pairs(active) WHERE active = true;

-- =============================================================================
-- ARBITRAGE_OPPORTUNITIES TABLE
-- Log detected arbitrage opportunities with profit margins and liquidity info
-- =============================================================================
CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
    opportunity_id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pair_id                 UUID NOT NULL REFERENCES market_pairs(pair_id) ON DELETE CASCADE,
    detected_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Arbitrage type: 'YES_NO' (buy YES on one platform, NO on other) or 'NO_YES'
    arbitrage_type          VARCHAR(8) NOT NULL CHECK (arbitrage_type IN ('YES_NO', 'NO_YES')),

    -- Prices at detection time
    polymarket_yes_price    DECIMAL(10, 6) NOT NULL CHECK (polymarket_yes_price >= 0 AND polymarket_yes_price <= 1),
    polymarket_no_price     DECIMAL(10, 6) NOT NULL CHECK (polymarket_no_price >= 0 AND polymarket_no_price <= 1),
    kalshi_yes_price        DECIMAL(10, 6) NOT NULL CHECK (kalshi_yes_price >= 0 AND kalshi_yes_price <= 1),
    kalshi_no_price         DECIMAL(10, 6) NOT NULL CHECK (kalshi_no_price >= 0 AND kalshi_no_price <= 1),

    -- Total cost of buying both positions (should be < 1 for arbitrage)
    total_cost              DECIMAL(10, 6) NOT NULL,
    profit_margin           DECIMAL(10, 6) NOT NULL,  -- 1 - total_cost
    profit_percentage       DECIMAL(10, 4) NOT NULL,  -- (profit_margin / total_cost) * 100

    -- Volume/liquidity information
    polymarket_volume_24h   DECIMAL(20, 2),
    kalshi_volume_24h       DECIMAL(20, 2),
    min_volume_24h          DECIMAL(20, 2),  -- min of the two platforms

    -- Spreads at detection time
    polymarket_spread       DECIMAL(10, 6),
    kalshi_spread           DECIMAL(10, 6),

    -- Status tracking
    status                  VARCHAR(32) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'expired', 'executed')),
    expires_at              TIMESTAMPTZ,  -- When opportunity is no longer valid

    CONSTRAINT chk_valid_arbitrage CHECK (total_cost < 1 AND profit_margin > 0)
);

-- Indexes for querying opportunities
CREATE INDEX IF NOT EXISTS idx_arb_opportunities_detected ON arbitrage_opportunities(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_arb_opportunities_pair ON arbitrage_opportunities(pair_id);
CREATE INDEX IF NOT EXISTS idx_arb_opportunities_status ON arbitrage_opportunities(status);
CREATE INDEX IF NOT EXISTS idx_arb_opportunities_profit ON arbitrage_opportunities(profit_percentage DESC) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_arb_opportunities_volume ON arbitrage_opportunities(min_volume_24h DESC) WHERE status = 'active';

-- =============================================================================
-- HELPER VIEW: Active arbitrage opportunities with full market details
-- =============================================================================
CREATE OR REPLACE VIEW v_active_arbitrage AS
SELECT
    ao.opportunity_id,
    ao.detected_at,
    ao.arbitrage_type,

    -- Market details
    mp_poly.title as polymarket_title,
    mp_kalshi.title as kalshi_title,
    mp_poly.url as polymarket_url,
    mp_kalshi.url as kalshi_url,
    mp_poly.category as category,

    -- Prices
    ao.polymarket_yes_price,
    ao.polymarket_no_price,
    ao.kalshi_yes_price,
    ao.kalshi_no_price,

    -- Profitability
    ao.total_cost,
    ao.profit_margin,
    ao.profit_percentage,

    -- Volume/liquidity
    ao.polymarket_volume_24h,
    ao.kalshi_volume_24h,
    ao.min_volume_24h,

    -- Spreads
    ao.polymarket_spread,
    ao.kalshi_spread,

    -- Trade recommendation
    CASE
        WHEN ao.arbitrage_type = 'YES_NO' THEN
            'Buy YES on Polymarket @ $' || ROUND(ao.polymarket_yes_price, 3)::TEXT ||
            ', Buy NO on Kalshi @ $' || ROUND(ao.kalshi_no_price, 3)::TEXT
        WHEN ao.arbitrage_type = 'NO_YES' THEN
            'Buy NO on Polymarket @ $' || ROUND(ao.polymarket_no_price, 3)::TEXT ||
            ', Buy YES on Kalshi @ $' || ROUND(ao.kalshi_yes_price, 3)::TEXT
    END as trade_recommendation,

    -- Expiry
    ao.expires_at,
    CASE
        WHEN ao.expires_at IS NOT NULL THEN
            EXTRACT(EPOCH FROM (ao.expires_at - NOW()))::INTEGER
    END as seconds_until_expiry

FROM arbitrage_opportunities ao
JOIN market_pairs mp ON ao.pair_id = mp.pair_id
JOIN markets mp_poly ON mp.polymarket_market_id = mp_poly.market_id
JOIN markets mp_kalshi ON mp.kalshi_market_id = mp_kalshi.market_id
WHERE ao.status = 'active'
    AND (ao.expires_at IS NULL OR ao.expires_at > NOW())
ORDER BY ao.profit_percentage DESC, ao.min_volume_24h DESC;

-- =============================================================================
-- SAMPLE DATA: Manually matched high-volume markets for initial testing
-- =============================================================================

-- Note: These market pairs would be inserted after actual markets exist in the system
-- Example (commented out, to be run manually when markets are available):
/*
INSERT INTO market_pairs (polymarket_market_id, kalshi_market_id, matching_method, notes)
VALUES
    -- Example: 2024 Presidential Election
    ('poly-market-uuid', 'kalshi-market-uuid', 'manual', '2024 US Presidential Election Winner'),
    -- Example: Bitcoin price markets
    ('poly-btc-uuid', 'kalshi-btc-uuid', 'manual', 'Bitcoin above $50k by end of 2024');
*/

COMMIT;