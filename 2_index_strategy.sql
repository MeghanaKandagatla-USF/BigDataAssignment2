-- Week 03 Assignment: StreamFlix Indexing Strategy

-- Index 1: For daily active user queries
CREATE INDEX idx_viewing_events_timestamp_userid ON viewing_events (event_timestamp DESC, user_id);
-- Reason: A composite B-tree index on (event_timestamp, user_id) allows PostgreSQL to
-- efficiently find all the records within the 7-day time range and then use the
-- same index to quickly access the user IDs for the distinct count.

-- Index 2: For content performance queries
CREATE INDEX idx_viewing_events_timestamp_contentid ON viewing_events (event_timestamp DESC, content_id) WHERE event_type = 'start';
-- Reason: This is a partial composite index. It's highly efficient because the WHERE
-- clause means the index only stores entries for 'start' events, making it much smaller.
-- It's perfect for finding the most recent popular content.

-- Index 3: For Regional Analytics
CREATE INDEX idx_viewing_events_country_timestamp ON viewing_events (country_code, event_timestamp DESC);
-- Reason: This composite index is ideal for quickly selecting rows for a specific
-- country and then filtering that smaller subset by the time range.

-- Index 4: For device type analysis
CREATE INDEX idx_viewing_events_device_timestamp ON viewing_events (device_type, event_timestamp DESC);
-- Reason: Similar to the regional index, this lets the database rapidly narrow down
-- the search to a specific device type before filtering by date.

-- Index 5: Any additional indexes you think are necessary
CREATE INDEX idx_viewing_events_userid ON viewing_events (user_id);
-- Reason: A dedicated index on user_id is crucial for efficiently finding all events
-- for a specific user, which is a common requirement for retention and user behavior analysis.