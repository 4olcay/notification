CREATE TABLE notification_templates (
    id         TEXT        PRIMARY KEY,
    name       TEXT        NOT NULL UNIQUE,
    channel    TEXT        NOT NULL,
    subject    TEXT,
    body       TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_templates_channel ON notification_templates (channel);
