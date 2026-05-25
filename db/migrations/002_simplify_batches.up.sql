ALTER TABLE notification_batches
    DROP COLUMN IF EXISTS pending,
    DROP COLUMN IF EXISTS delivered,
    DROP COLUMN IF EXISTS failed,
    DROP COLUMN IF EXISTS cancelled;
