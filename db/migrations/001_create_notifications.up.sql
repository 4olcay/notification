CREATE TABLE IF NOT EXISTS notification_batches (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    total_count INT NOT NULL,
    pending     INT NOT NULL DEFAULT 0,
    delivered   INT NOT NULL DEFAULT 0,
    failed      INT NOT NULL DEFAULT 0,
    cancelled   INT NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS notifications (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id         UUID REFERENCES notification_batches(id),
    recipient        VARCHAR(255) NOT NULL,
    channel          VARCHAR(20)  NOT NULL,
    content          TEXT         NOT NULL,
    priority         VARCHAR(20)  NOT NULL DEFAULT 'normal',
    status           VARCHAR(30)  NOT NULL DEFAULT 'pending',
    idempotency_key  VARCHAR(255) UNIQUE,
    retry_count      INT          NOT NULL DEFAULT 0,
    max_retries      INT          NOT NULL DEFAULT 5,
    next_retry_at    TIMESTAMPTZ,
    provider_msg_id  VARCHAR(255),
    error_message    TEXT,
    scheduled_at     TIMESTAMPTZ,
    delivered_at     TIMESTAMPTZ,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_notifications_status     ON notifications(status);
CREATE INDEX IF NOT EXISTS idx_notifications_channel    ON notifications(channel);
CREATE INDEX IF NOT EXISTS idx_notifications_priority   ON notifications(priority);
CREATE INDEX IF NOT EXISTS idx_notifications_batch_id   ON notifications(batch_id);
CREATE INDEX IF NOT EXISTS idx_notifications_created_at ON notifications(created_at);
CREATE INDEX IF NOT EXISTS idx_notifications_retry
    ON notifications(next_retry_at)
    WHERE status = 'retrying';
CREATE INDEX IF NOT EXISTS idx_notifications_scheduled
    ON notifications(scheduled_at)
    WHERE status = 'pending' AND scheduled_at IS NOT NULL;
