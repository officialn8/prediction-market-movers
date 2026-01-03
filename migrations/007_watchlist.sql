CREATE TABLE IF NOT EXISTS user_watchlist (
    user_session_id TEXT NOT NULL,
    market_id UUID NOT NULL REFERENCES markets(market_id) ON DELETE CASCADE,
    added_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (user_session_id, market_id)
);

CREATE INDEX idx_watchlist_user ON user_watchlist(user_session_id);
