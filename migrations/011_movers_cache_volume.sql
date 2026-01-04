-- Add volume and spike_ratio columns to movers_cache
-- These were previously calculated but discarded before persistence,
-- causing dashboard display inconsistencies with the ranking logic.

BEGIN;

-- Add volume_24h column (stores the volume used in scoring)
ALTER TABLE movers_cache
ADD COLUMN IF NOT EXISTS volume_24h DECIMAL(20, 2);

-- Add spike_ratio column (stores the volume spike ratio used in scoring)
ALTER TABLE movers_cache
ADD COLUMN IF NOT EXISTS spike_ratio DECIMAL(10, 4);

-- Add index for volume-based queries
CREATE INDEX IF NOT EXISTS idx_movers_cache_volume 
ON movers_cache(window_seconds, volume_24h DESC NULLS LAST)
WHERE volume_24h IS NOT NULL;

COMMIT;


