-- Create a partitioned version of the viewing_events table
-- We partition by RANGE on event_timestamp, as most queries filter by a time range.
-- This enables highly effective partition pruning. A monthly interval is a good balance
-- between having too many small partitions (daily) and partitions being too large (yearly).
CREATE TABLE viewing_events_partitioned (
    event_id               BIGINT NOT NULL,
    user_id                INTEGER NOT NULL,
    content_id             INTEGER NOT NULL,
    event_timestamp        TIMESTAMPTZ NOT NULL,
    event_type             VARCHAR(50),
    watch_duration_seconds INTEGER,
    device_type            VARCHAR(50),
    country_code           VARCHAR(2),
    quality                VARCHAR(10),
    bandwidth_mbps         DECIMAL(6,2),
    created_at             TIMESTAMPTZ DEFAULT NOW(),
    -- The primary key must include the partition key.
    PRIMARY KEY (event_id, event_timestamp)
) PARTITION BY RANGE (event_timestamp);


-- Create a function to generate partitions and their indexes for a given month.
CREATE OR REPLACE FUNCTION create_partition_and_indexes(target_date DATE)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    partition_name TEXT;
    partition_start TEXT;
    partition_end TEXT;
BEGIN
    -- 1. Calculate partition name and date range for the given month
    partition_name := 'viewing_events_y' || to_char(target_date, 'YYYY') || 'm' || to_char(target_date, 'MM');
    partition_start := to_char(date_trunc('month', target_date), 'YYYY-MM-DD');
    partition_end := to_char(date_trunc('month', target_date) + INTERVAL '1 month', 'YYYY-MM-DD');

    -- 2. Create the partition if it doesn't exist
    IF NOT EXISTS (SELECT FROM pg_class WHERE relname = partition_name) THEN
        RAISE NOTICE 'Creating partition % for month %', partition_name, partition_start;
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF viewing_events_partitioned FOR VALUES FROM (%L) TO (%L);',
            partition_name, partition_start, partition_end
        );

        -- 3. Create appropriate indexes on the new partition
        -- These indexes match the optimal ones we designed in Part 2.
        EXECUTE format('CREATE INDEX ON %I (event_timestamp DESC, user_id);', partition_name);
        EXECUTE format('CREATE INDEX ON %I (event_timestamp DESC, content_id) WHERE event_type = ''start'';', partition_name);
        EXECUTE format('CREATE INDEX ON %I (device_type, event_timestamp DESC);', partition_name);
        EXECUTE format('CREATE INDEX ON %I (user_id);', partition_name);
    ELSE
        RAISE NOTICE 'Partition % already exists, skipping.', partition_name;
    END IF;
END;
$$;