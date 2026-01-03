-- 004_user_alerts.sql
-- Custom user-defined price alerts

CREATE TABLE IF NOT EXISTS user_alerts (
    alert_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id VARCHAR(255) NOT NULL,  -- Session-based identification
    market_id UUID REFERENCES markets(market_id) ON DELETE CASCADE,
    token_id UUID REFERENCES market_tokens(token_id) ON DELETE CASCADE,

    -- Alert configuration
    condition_type VARCHAR(32) NOT NULL CHECK (condition_type IN ('above', 'below', 'change_pct')),
    threshold DECIMAL(10, 6) NOT NULL,
    window_seconds INT,  -- For change_pct alerts

    -- State
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_triggered TIMESTAMPTZ,
    trigger_count INT DEFAULT 0,

    -- Notification settings
    notify_once BOOLEAN DEFAULT false  -- If true, deactivate after first trigger
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_user_alerts_session ON user_alerts(session_id);
CREATE INDEX IF NOT EXISTS idx_user_alerts_active ON user_alerts(is_active) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_user_alerts_token ON user_alerts(token_id);

-- Table to store triggered user alert notifications
CREATE TABLE IF NOT EXISTS user_alert_notifications (
    notification_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_alert_id UUID REFERENCES user_alerts(alert_id) ON DELETE CASCADE,
    triggered_at TIMESTAMPTZ DEFAULT NOW(),
    current_price DECIMAL(10, 6),
    threshold_price DECIMAL(10, 6),
    message TEXT,
    acknowledged BOOLEAN DEFAULT false
);

CREATE INDEX IF NOT EXISTS idx_user_notifications_alert ON user_alert_notifications(user_alert_id);
CREATE INDEX IF NOT EXISTS idx_user_notifications_unacked ON user_alert_notifications(acknowledged) WHERE acknowledged = false;
