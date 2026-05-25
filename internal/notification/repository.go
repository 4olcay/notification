package notification

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

type Repository struct {
	db *sqlx.DB
}

func NewRepository(db *sqlx.DB) *Repository {
	return &Repository{db: db}
}

const insertNotificationSQL = `
	INSERT INTO notifications
		(id, batch_id, recipient, channel, content, priority, status,
		 idempotency_key, max_retries, scheduled_at, created_at, updated_at)
	VALUES
		(:id, :batch_id, :recipient, :channel, :content, :priority, :status,
		 :idempotency_key, :max_retries, :scheduled_at, NOW(), NOW())
	RETURNING
		id, batch_id, recipient, channel, content, priority, status,
		idempotency_key, retry_count, max_retries, next_retry_at,
		provider_msg_id, error_message, scheduled_at, delivered_at,
		created_at, updated_at`

func (r *Repository) CreateWithIdempotency(ctx context.Context, n Notification) (Notification, bool, error) {
	query := `
		INSERT INTO notifications
			(id, batch_id, recipient, channel, content, priority, status,
			 idempotency_key, max_retries, scheduled_at, created_at, updated_at)
		VALUES
			(:id, :batch_id, :recipient, :channel, :content, :priority, :status,
			 :idempotency_key, :max_retries, :scheduled_at, NOW(), NOW())
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING
			id, batch_id, recipient, channel, content, priority, status,
			idempotency_key, retry_count, max_retries, next_retry_at,
			provider_msg_id, error_message, scheduled_at, delivered_at,
			created_at, updated_at`

	rows, err := r.db.NamedQueryContext(ctx, query, n)
	if err != nil {
		return Notification{}, false, fmt.Errorf("insert with idempotency: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		var created Notification
		if err := rows.StructScan(&created); err != nil {
			return Notification{}, false, fmt.Errorf("scan created notification: %w", err)
		}
		return created, true, nil
	}

	existing, err := r.FindByIdempotencyKey(ctx, *n.IdempotencyKey)
	if err != nil {
		return Notification{}, false, err
	}
	return existing, false, nil
}

func (r *Repository) Create(ctx context.Context, n Notification) (Notification, error) {
	rows, err := r.db.NamedQueryContext(ctx, insertNotificationSQL, n)
	if err != nil {
		return Notification{}, fmt.Errorf("insert notification: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return Notification{}, errors.New("no rows returned after insert")
	}

	var created Notification
	if err := rows.StructScan(&created); err != nil {
		return Notification{}, fmt.Errorf("scan notification: %w", err)
	}
	return created, nil
}

func (r *Repository) CreateBatchAtomic(ctx context.Context, b Batch, notifications []Notification) (Batch, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return Batch{}, fmt.Errorf("begin batch transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	batchStmt, err := tx.PrepareNamedContext(ctx, `
		INSERT INTO notification_batches (id, total_count, created_at, updated_at)
		VALUES (:id, :total_count, NOW(), NOW())
		RETURNING id, total_count, created_at, updated_at`)
	if err != nil {
		return Batch{}, fmt.Errorf("prepare batch header insert: %w", err)
	}
	defer batchStmt.Close()

	rows, err := batchStmt.QueryxContext(ctx, b)
	if err != nil {
		return Batch{}, fmt.Errorf("insert batch header: %w", err)
	}
	if !rows.Next() {
		rows.Close()
		return Batch{}, errors.New("no rows returned after batch header insert")
	}
	var created Batch
	if err := rows.StructScan(&created); err != nil {
		rows.Close()
		return Batch{}, fmt.Errorf("scan batch header: %w", err)
	}
	rows.Close()

	stmt, err := tx.PrepareNamedContext(ctx, `
		INSERT INTO notifications
			(id, batch_id, recipient, channel, content, priority, status,
			 idempotency_key, max_retries, scheduled_at, created_at, updated_at)
		VALUES
			(:id, :batch_id, :recipient, :channel, :content, :priority, :status,
			 :idempotency_key, :max_retries, :scheduled_at, NOW(), NOW())`)
	if err != nil {
		return Batch{}, fmt.Errorf("prepare notification insert: %w", err)
	}
	defer stmt.Close()

	for i := range notifications {
		if _, err := stmt.ExecContext(ctx, notifications[i]); err != nil {
			return Batch{}, fmt.Errorf("insert notification[%d]: %w", i, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return Batch{}, fmt.Errorf("commit batch transaction: %w", err)
	}
	return created, nil
}

func (r *Repository) FindBatchWithLiveCounts(ctx context.Context, id string) (Batch, error) {
	var b Batch
	err := r.db.GetContext(ctx, &b, `
		SELECT
			b.id,
			b.total_count,
			b.created_at,
			b.updated_at,
			COALESCE(SUM(CASE WHEN n.status = 'pending'  THEN 1 ELSE 0 END), 0)::int AS pending,
			COALESCE(SUM(CASE WHEN n.status IN ('queued','processing','retrying') THEN 1 ELSE 0 END), 0)::int AS processing,
			COALESCE(SUM(CASE WHEN n.status = 'delivered' THEN 1 ELSE 0 END), 0)::int AS delivered,
			COALESCE(SUM(CASE WHEN n.status IN ('failed','dead_letter') THEN 1 ELSE 0 END), 0)::int AS failed,
			COALESCE(SUM(CASE WHEN n.status = 'cancelled' THEN 1 ELSE 0 END), 0)::int AS cancelled
		FROM notification_batches b
		LEFT JOIN notifications n ON n.batch_id = b.id
		WHERE b.id = $1
		GROUP BY b.id, b.total_count, b.created_at, b.updated_at`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return Batch{}, ErrNotFound
	}
	if err != nil {
		return Batch{}, fmt.Errorf("find batch with live counts: %w", err)
	}
	return b, nil
}

func (r *Repository) FindByID(ctx context.Context, id string) (Notification, error) {
	var n Notification
	err := r.db.GetContext(ctx, &n, `
		SELECT id, batch_id, recipient, channel, content, priority, status,
		       idempotency_key, retry_count, max_retries, next_retry_at,
		       provider_msg_id, error_message, scheduled_at, delivered_at,
		       created_at, updated_at
		FROM notifications
		WHERE id = $1`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return Notification{}, ErrNotFound
	}
	if err != nil {
		return Notification{}, fmt.Errorf("find notification by id: %w", err)
	}
	return n, nil
}

func (r *Repository) FindByIdempotencyKey(ctx context.Context, key string) (Notification, error) {
	var n Notification
	err := r.db.GetContext(ctx, &n, `
		SELECT id, batch_id, recipient, channel, content, priority, status,
		       idempotency_key, retry_count, max_retries, next_retry_at,
		       provider_msg_id, error_message, scheduled_at, delivered_at,
		       created_at, updated_at
		FROM notifications
		WHERE idempotency_key = $1`, key)
	if errors.Is(err, sql.ErrNoRows) {
		return Notification{}, ErrNotFound
	}
	if err != nil {
		return Notification{}, fmt.Errorf("find notification by idempotency key: %w", err)
	}
	return n, nil
}

func (r *Repository) TryMarkProcessing(ctx context.Context, id string) (bool, error) {
	result, err := r.db.ExecContext(ctx, `
		UPDATE notifications
		SET status='processing', error_message=NULL, updated_at=NOW()
		WHERE id=$1
		  AND status IN ('pending', 'queued')`,
		id)
	if err != nil {
		return false, fmt.Errorf("try mark processing [%s]: %w", id, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("try mark processing rows affected [%s]: %w", id, err)
	}
	return n > 0, nil
}

func (r *Repository) UpdateStatus(ctx context.Context, id string, status Status, errMsg string) error {
	var msg *string
	if errMsg != "" {
		msg = &errMsg
	}
	_, err := r.db.ExecContext(ctx,
		"UPDATE notifications SET status=$1, error_message=$2, updated_at=NOW() WHERE id=$3",
		status, msg, id)
	if err != nil {
		return fmt.Errorf("update notification status [%s → %s]: %w", id, status, err)
	}
	return nil
}

func (r *Repository) MarkDelivered(ctx context.Context, id, providerMsgID string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE notifications
		SET status=$1, provider_msg_id=$2, delivered_at=NOW(), updated_at=NOW()
		WHERE id=$3`,
		StatusDelivered, providerMsgID, id)
	if err != nil {
		return fmt.Errorf("mark delivered [%s]: %w", id, err)
	}
	return nil
}

func (r *Repository) MarkRetrying(ctx context.Context, id string, retryCount int, nextRetryAt time.Time, errMsg string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE notifications
		SET status=$1, retry_count=$2, next_retry_at=$3, error_message=$4, updated_at=NOW()
		WHERE id=$5`,
		StatusRetrying, retryCount, nextRetryAt, errMsg, id)
	if err != nil {
		return fmt.Errorf("mark retrying [%s] attempt %d: %w", id, retryCount, err)
	}
	return nil
}

func (r *Repository) MarkDeadLetter(ctx context.Context, id, errMsg string) error {
	_, err := r.db.ExecContext(ctx,
		"UPDATE notifications SET status=$1, error_message=$2, updated_at=NOW() WHERE id=$3",
		StatusDeadLetter, errMsg, id)
	if err != nil {
		return fmt.Errorf("mark dead-letter [%s]: %w", id, err)
	}
	return nil
}

func (r *Repository) Cancel(ctx context.Context, id string) error {
	result, err := r.db.ExecContext(ctx,
		"UPDATE notifications SET status=$1, updated_at=NOW() WHERE id=$2 AND status='pending'",
		StatusCancelled, id)
	if err != nil {
		return fmt.Errorf("cancel notification: %w", err)
	}

	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("cancel: rows affected: %w", err)
	}
	if n == 1 {
		return nil
	}

	var exists bool
	if err := r.db.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM notifications WHERE id=$1)", id).Scan(&exists); err != nil {
		return fmt.Errorf("cancel: existence check: %w", err)
	}
	if !exists {
		return ErrNotFound
	}
	return ErrCannotCancel
}

func (r *Repository) List(ctx context.Context, f ListFilter) ([]Notification, int, error) {
	conds := []string{"1=1"}
	args := []any{}
	i := 1

	if f.Status != "" {
		conds = append(conds, fmt.Sprintf("status=$%d", i))
		args = append(args, f.Status)
		i++
	}
	if f.Channel != "" {
		conds = append(conds, fmt.Sprintf("channel=$%d", i))
		args = append(args, f.Channel)
		i++
	}
	if f.FromTime != nil {
		conds = append(conds, fmt.Sprintf("created_at>=$%d", i))
		args = append(args, *f.FromTime)
		i++
	}
	if f.ToTime != nil {
		conds = append(conds, fmt.Sprintf("created_at<=$%d", i))
		args = append(args, *f.ToTime)
		i++
	}

	where := strings.Join(conds, " AND ")

	var total int
	if err := r.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM notifications WHERE "+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count notifications: %w", err)
	}

	limitArgs := make([]any, 0, len(args)+2)
	limitArgs = append(limitArgs, args...)
	limitArgs = append(limitArgs, f.Limit, f.Offset)
	query := fmt.Sprintf(`
		SELECT id, batch_id, recipient, channel, content, priority, status,
		       idempotency_key, retry_count, max_retries, next_retry_at,
		       provider_msg_id, error_message, scheduled_at, delivered_at,
		       created_at, updated_at
		FROM notifications
		WHERE %s
		ORDER BY created_at DESC
		LIMIT $%d OFFSET $%d`,
		where, i, i+1,
	)

	var notifications []Notification
	if err := r.db.SelectContext(ctx, &notifications, query, limitArgs...); err != nil {
		return nil, 0, fmt.Errorf("list notifications: %w", err)
	}

	return notifications, total, nil
}

func (r *Repository) FindDueStuckPending(ctx context.Context, stuckSince time.Time, limit int) ([]Notification, error) {
	var notifications []Notification
	err := r.db.SelectContext(ctx, &notifications, `
		UPDATE notifications
		SET status = 'queued', updated_at = NOW()
		WHERE id IN (
		    SELECT id FROM notifications
		    WHERE (
		        (status = 'pending' AND scheduled_at IS NULL)
		        OR status = 'queued'
		    ) AND updated_at < $1
		    ORDER BY updated_at ASC
		    LIMIT $2
		    FOR UPDATE SKIP LOCKED
		)
		RETURNING id, batch_id, recipient, channel, content, priority, status,
		          idempotency_key, retry_count, max_retries, next_retry_at,
		          provider_msg_id, error_message, scheduled_at, delivered_at,
		          created_at, updated_at`,
		stuckSince, limit)
	if err != nil {
		return nil, fmt.Errorf("find stuck pending: %w", err)
	}
	return notifications, nil
}

func (r *Repository) FindDueRetries(ctx context.Context, limit int) ([]Notification, error) {
	var notifications []Notification
	err := r.db.SelectContext(ctx, &notifications, `
		UPDATE notifications
		SET status = 'queued', updated_at = NOW()
		WHERE id IN (
		    SELECT id FROM notifications
		    WHERE status = $1 AND next_retry_at <= NOW()
		    ORDER BY next_retry_at ASC
		    LIMIT $2
		    FOR UPDATE SKIP LOCKED
		)
		RETURNING id, batch_id, recipient, channel, content, priority, status,
		          idempotency_key, retry_count, max_retries, next_retry_at,
		          provider_msg_id, error_message, scheduled_at, delivered_at,
		          created_at, updated_at`,
		StatusRetrying, limit)
	if err != nil {
		return nil, fmt.Errorf("find due retries: %w", err)
	}
	return notifications, nil
}

func (r *Repository) FindDueScheduled(ctx context.Context, limit int) ([]Notification, error) {
	var notifications []Notification
	err := r.db.SelectContext(ctx, &notifications, `
		UPDATE notifications
		SET status = 'queued', updated_at = NOW()
		WHERE id IN (
		    SELECT id FROM notifications
		    WHERE status = $1 AND scheduled_at IS NOT NULL AND scheduled_at <= NOW()
		    ORDER BY scheduled_at ASC
		    LIMIT $2
		    FOR UPDATE SKIP LOCKED
		)
		RETURNING id, batch_id, recipient, channel, content, priority, status,
		          idempotency_key, retry_count, max_retries, next_retry_at,
		          provider_msg_id, error_message, scheduled_at, delivered_at,
		          created_at, updated_at`,
		StatusPending, limit)
	if err != nil {
		return nil, fmt.Errorf("find due scheduled: %w", err)
	}
	return notifications, nil
}
