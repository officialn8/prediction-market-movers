-- One-time archive-first cleanup for historical settlement-noise alerts.
--
-- What it removes:
-- 1) Alerts that were created at/after market expiry or resolution.
-- 2) Mirror YES/NO duplicate rows for the same market event bucket.
--
-- This script archives rows into alerts_suppressed_archive before deletion.
-- Review counts with SELECT-only variants before running in production.

BEGIN;

CREATE TABLE IF NOT EXISTS alerts_suppressed_archive (
    archive_id BIGSERIAL PRIMARY KEY,
    alert_id UUID NOT NULL,
    suppression_reason TEXT NOT NULL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_table TEXT NOT NULL DEFAULT 'alerts',
    alert_row JSONB NOT NULL,
    UNIQUE (alert_id, suppression_reason)
);

-- ---------------------------------------------------------------------------
-- 1) Purge alerts created at/after expiry/resolution.
-- ---------------------------------------------------------------------------
WITH to_delete AS (
    SELECT a.alert_id
    FROM alerts a
    JOIN market_tokens mt ON a.token_id = mt.token_id
    JOIN markets m ON mt.market_id = m.market_id
    WHERE (
        (m.end_date IS NOT NULL AND a.created_at >= m.end_date)
        OR (
            COALESCE(m.status, '') <> 'active'
            AND COALESCE(m.resolved_at, m.end_date) IS NOT NULL
            AND a.created_at >= COALESCE(m.resolved_at, m.end_date)
        )
    )
),
archived AS (
    INSERT INTO alerts_suppressed_archive (
        alert_id,
        suppression_reason,
        alert_row
    )
    SELECT
        a.alert_id,
        'expired_or_resolved_at_alert_time',
        to_jsonb(a)
    FROM alerts a
    JOIN to_delete d ON d.alert_id = a.alert_id
    ON CONFLICT (alert_id, suppression_reason) DO NOTHING
    RETURNING alert_id
),
deleted AS (
    DELETE FROM alerts a
    USING to_delete d
    WHERE a.alert_id = d.alert_id
    RETURNING a.alert_id
)
SELECT
    (SELECT COUNT(*) FROM archived) AS archived_expired_or_resolved,
    (SELECT COUNT(*) FROM deleted) AS deleted_expired_or_resolved;

-- ---------------------------------------------------------------------------
-- 2) Purge mirror YES/NO duplicates (keep best row per market event bucket).
-- ---------------------------------------------------------------------------
WITH scoped AS (
    SELECT
        a.alert_id,
        a.window_seconds,
        COALESCE(a.alert_type, 'price_move') AS alert_type,
        DATE_TRUNC('minute', a.created_at) AS minute_bucket,
        mt.market_id,
        mt.outcome,
        a.move_pp,
        a.created_at
    FROM alerts a
    JOIN market_tokens mt ON a.token_id = mt.token_id
),
mirror_groups AS (
    SELECT
        market_id,
        window_seconds,
        alert_type,
        minute_bucket
    FROM scoped
    GROUP BY market_id, window_seconds, alert_type, minute_bucket
    HAVING BOOL_OR(UPPER(COALESCE(outcome, '')) IN ('YES', 'Y'))
       AND BOOL_OR(UPPER(COALESCE(outcome, '')) IN ('NO', 'N'))
),
ranked AS (
    SELECT
        s.alert_id,
        ROW_NUMBER() OVER (
            PARTITION BY s.market_id, s.window_seconds, s.alert_type, s.minute_bucket
            ORDER BY
                ABS(s.move_pp) DESC,
                CASE
                    WHEN UPPER(COALESCE(s.outcome, '')) IN ('YES', 'Y') THEN 0
                    ELSE 1
                END,
                s.created_at DESC
        ) AS rn
    FROM scoped s
    JOIN mirror_groups g
      ON g.market_id = s.market_id
     AND g.window_seconds = s.window_seconds
     AND g.alert_type = s.alert_type
     AND g.minute_bucket = s.minute_bucket
),
to_delete AS (
    SELECT alert_id
    FROM ranked
    WHERE rn > 1
),
archived AS (
    INSERT INTO alerts_suppressed_archive (
        alert_id,
        suppression_reason,
        alert_row
    )
    SELECT
        a.alert_id,
        'mirror_yes_no_duplicates',
        to_jsonb(a)
    FROM alerts a
    JOIN to_delete d ON d.alert_id = a.alert_id
    ON CONFLICT (alert_id, suppression_reason) DO NOTHING
    RETURNING alert_id
),
deleted AS (
    DELETE FROM alerts a
    USING to_delete d
    WHERE a.alert_id = d.alert_id
    RETURNING a.alert_id
)
SELECT
    (SELECT COUNT(*) FROM archived) AS archived_mirror_duplicates,
    (SELECT COUNT(*) FROM deleted) AS deleted_mirror_duplicates;

COMMIT;
