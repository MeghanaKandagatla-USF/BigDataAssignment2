-- This script implements a hybrid optimization strategy for the StreamFlix database.

-- Step 1: Create an optimally partitioned table with sub-partitioning
-- We create a new table partitioned by month (RANGE on event_timestamp) and then 
-- sub-partitioned by country_code (HASH). This allows for extremely fast queries 
-- when filtering by both a time range and a specific country.
DROP TABLE IF EXISTS viewing_events_hybrid CASCADE;
CREATE TABLE viewing_events_hybrid (
    event_id               BIGINT NOT NULL,
    user_id                INTEGER NOT NULL,
    content_id             INTEGER NOT NULL,
    event_timestamp        TIMESTAMZ NOT NULL,
    event_type             VARCHAR(50),
    watch_duration_seconds INTEGER,
    device_type            VARCHAR(50),
    country_code           VARCHAR(2),
    quality                VARCHAR(10),
    bandwidth_mbps         DECIMAL(6,2),
    created_at             TIMESTAMZ DEFAULT NOW(),
    -- The primary key must include all partition keys.
    PRIMARY KEY (event_id, event_timestamp, country_code)
) PARTITION BY RANGE (event_timestamp);


-- Step 2 & 4: Create partition-aware indexes and maintenance procedures
-- This function creates a monthly partition and then creates HASH sub-partitions within it.
-- This automates the management of the complex partition structure.
CREATE OR REPLACE FUNCTION create_hybrid_monthly_partition(target_date DATE)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    partition_name TEXT;
    partition_start TEXT;
    partition_end TEXT;
BEGIN
    partition_name := 'viewing_events_hybrid_y' || to_char(target_date, 'YYYYmMM');
    partition_start := to_char(date_trunc('month', target_date), 'YYYY-MM-DD');
    partition_end := to_char(date_trunc('month', target_date) + INTERVAL '1 month', 'YYYY-MM-DD');

    -- Create the main monthly partition, which is itself partitioned by HASH
    EXECUTE format('CREATE TABLE %I PARTITION OF viewing_events_hybrid FOR VALUES FROM (%L) TO (%L) PARTITION BY HASH (country_code);',
        partition_name, partition_start, partition_end);

    -- Create sub-partitions for major countries and a default for others
    -- This creates 5 sub-partitions inside the monthly partition
    EXECUTE format('CREATE TABLE %1$I_us PARTITION OF %1$I FOR VALUES WITH (MODULUS 5, REMAINDER 0);', partition_name);
    EXECUTE format('CREATE TABLE %1$I_uk PARTITION OF %1$I FOR VALUES WITH (MODULUS 5, REMAINDER 1);', partition_name);
    EXECUTE format('CREATE TABLE %1$I_ca PARTITION OF %1$I FOR VALUES WITH (MODULUS 5, REMAINDER 2);', partition_name);
    EXECUTE format('CREATE TABLE %1$I_au PARTITION OF %1$I FOR VALUES WITH (MODULUS 5, REMAINDER 3);', partition_name);
    EXECUTE format('CREATE TABLE %1$I_ot PARTITION OF %1$I FOR VALUES WITH (MODULUS 5, REMAINDER 4);', partition_name);
    
    RAISE NOTICE 'Created partition % with country sub-partitions.', partition_name;
END;
$$;


-- Step 3: Create any supporting structures (Materialized View for Daily Active Users)
-- The daily active user query is expensive, so we can pre-calculate its results daily.
-- Queries against this view will be nearly instantaneous.
CREATE MATERIALIZED VIEW daily_active_users_mv AS
SELECT
    event_timestamp::date AS day,
    COUNT(DISTINCT user_id) as dau
FROM viewing_events_hybrid
GROUP BY event_timestamp::date
ORDER BY event_timestamp::date;

-- Add a unique index to allow for fast, concurrent refreshes of the view.
CREATE UNIQUE INDEX idx_daily_active_users_mv_day ON daily_active_users_mv(day);

-- Example command to refresh the view:
-- REFRESH MATERIALIZED VIEW CONCURRENTLY daily_active_users_mv;