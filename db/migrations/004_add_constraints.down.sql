ALTER TABLE notification_templates
    DROP CONSTRAINT IF EXISTS chk_templates_channel;

ALTER TABLE notifications
    DROP CONSTRAINT IF EXISTS chk_notifications_priority,
    DROP CONSTRAINT IF EXISTS chk_notifications_channel,
    DROP CONSTRAINT IF EXISTS chk_notifications_status;
