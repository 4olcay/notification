ALTER TABLE notifications
    ADD CONSTRAINT chk_notifications_status
        CHECK (status IN (
            'pending', 'queued', 'processing',
            'delivered', 'retrying', 'failed',
            'dead_letter', 'cancelled'
        )),
    ADD CONSTRAINT chk_notifications_channel
        CHECK (channel IN ('sms', 'email', 'push')),
    ADD CONSTRAINT chk_notifications_priority
        CHECK (priority IN ('high', 'normal', 'low'));

ALTER TABLE notification_templates
    ADD CONSTRAINT chk_templates_channel
        CHECK (channel IN ('sms', 'email', 'push'));
