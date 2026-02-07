-- Storage containment updates
-- - Drop redundant snapshot write index
-- - Move volume baseline view to retained hourly candles

BEGIN;

-- idx_snapshots_token_ts already covers token lookup patterns used by latest-price queries.
DROP INDEX IF EXISTS idx_snapshots_token_id;

-- Use retained hourly candles for baseline volume estimates so analytics do not
-- depend on long raw-snapshot retention windows.
CREATE OR REPLACE VIEW volume_averages AS
SELECT
    h.token_id,
    AVG(h.volume) as avg_volume_7d,
    STDDEV(h.volume) as stddev_volume_7d,
    MAX(h.volume) as max_volume_7d,
    MIN(h.volume) as min_volume_7d,
    COUNT(*) as sample_count
FROM (
    SELECT DISTINCT ON (token_id, DATE(bucket_ts))
        token_id,
        volume,
        bucket_ts
    FROM ohlc_1h
    WHERE bucket_ts > NOW() - INTERVAL '14 days'
      AND volume IS NOT NULL
      AND volume > 0
    ORDER BY token_id, DATE(bucket_ts), bucket_ts DESC
) h
GROUP BY h.token_id
HAVING COUNT(*) >= 2;

COMMIT;
