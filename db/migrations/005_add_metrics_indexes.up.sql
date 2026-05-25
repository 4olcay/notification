-- Indexes that speed up the two hot queries in GET /metrics.
--
-- 1. channelStats: WHERE created_at >= NOW() - INTERVAL '24 hours' GROUP BY channel
--    A covering index lets PostgreSQL do an index-only scan for the entire
--    aggregation — no heap fetch needed for channel, status, or delivered_at.
CREATE INDEX IF NOT EXISTS idx_notifications_metrics
    ON notifications (created_at DESC)
    INCLUDE (channel, status, delivered_at);

-- 2. queueDepths: WHERE status IN ('pending','queued','processing') GROUP BY priority
--    A partial index covering only active-status rows eliminates the need to
--    scan delivered / cancelled / dead_letter rows that dominate the table at scale.
CREATE INDEX IF NOT EXISTS idx_notifications_queue_depth
    ON notifications (status, priority)
    WHERE status IN ('pending', 'queued', 'processing');

-- 3. List endpoint: WHERE status=? AND channel=? ORDER BY created_at DESC
--    A composite covering index avoids separate lookups when the caller
--    filters by both status and channel (the two most common list filters).
CREATE INDEX IF NOT EXISTS idx_notifications_list
    ON notifications (status, channel, created_at DESC);
