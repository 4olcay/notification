package notification

import (
	"errors"
	"time"
)

type Status string
type Channel string
type Priority string

const (
	StatusPending    Status = "pending"
	StatusQueued     Status = "queued"
	StatusProcessing Status = "processing"
	StatusDelivered  Status = "delivered"
	StatusFailed     Status = "failed"
	StatusRetrying   Status = "retrying"
	StatusDeadLetter Status = "dead_letter"
	StatusCancelled  Status = "cancelled"
)

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

var (
	ErrNotFound     = errors.New("notification not found")
	ErrCannotCancel = errors.New("only pending notifications can be cancelled")
)

type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

type Notification struct {
	ID             string     `db:"id"              json:"id"`
	BatchID        *string    `db:"batch_id"        json:"batch_id,omitempty"`
	Recipient      string     `db:"recipient"       json:"recipient"`
	Channel        Channel    `db:"channel"         json:"channel"`
	Content        string     `db:"content"         json:"content"`
	Priority       Priority   `db:"priority"        json:"priority"`
	Status         Status     `db:"status"          json:"status"`
	IdempotencyKey *string    `db:"idempotency_key" json:"idempotency_key,omitempty"`
	RetryCount     int        `db:"retry_count"     json:"retry_count"`
	MaxRetries     int        `db:"max_retries"     json:"max_retries"`
	NextRetryAt    *time.Time `db:"next_retry_at"   json:"next_retry_at,omitempty"`
	ProviderMsgID  *string    `db:"provider_msg_id" json:"provider_msg_id,omitempty"`
	ErrorMessage   *string    `db:"error_message"   json:"error_message,omitempty"`
	ScheduledAt    *time.Time `db:"scheduled_at"    json:"scheduled_at,omitempty"`
	DeliveredAt    *time.Time `db:"delivered_at"    json:"delivered_at,omitempty"`
	CreatedAt      time.Time  `db:"created_at"      json:"created_at"`
	UpdatedAt      time.Time  `db:"updated_at"      json:"updated_at"`
}

type Batch struct {
	ID         string    `db:"id"          json:"id"`
	Total      int       `db:"total_count" json:"total"`
	Pending    int       `db:"pending"     json:"pending"`
	Processing int       `db:"processing"  json:"processing"`
	Delivered  int       `db:"delivered"   json:"delivered"`
	Failed     int       `db:"failed"      json:"failed"`
	Cancelled  int       `db:"cancelled"   json:"cancelled"`
	CreatedAt  time.Time `db:"created_at"  json:"created_at"`
	UpdatedAt  time.Time `db:"updated_at"  json:"updated_at"`
}

type ListFilter struct {
	Status   Status
	Channel  Channel
	FromTime *time.Time
	ToTime   *time.Time
	Limit    int
	Offset   int
}

type CreateRequest struct {
	Recipient      string
	Channel        string
	Content        string
	Priority       string
	IdempotencyKey *string
	ScheduledAt    *time.Time
}

type CreateBatchRequest struct {
	Notifications []CreateRequest
}
