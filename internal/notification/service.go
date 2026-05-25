package notification

import (
	"context"
	"fmt"
	"log/slog"
	"net/mail"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/4olcay/notification/internal/queue"
)

type queuePublisher interface {
	Publish(ctx context.Context, msg queue.Message) error
}

type notifStore interface {
	CreateWithIdempotency(ctx context.Context, n Notification) (Notification, bool, error)
	Create(ctx context.Context, n Notification) (Notification, error)
	CreateBatchAtomic(ctx context.Context, b Batch, notifications []Notification) (Batch, error)
	FindByID(ctx context.Context, id string) (Notification, error)
	FindBatchWithLiveCounts(ctx context.Context, id string) (Batch, error)
	UpdateStatus(ctx context.Context, id string, status Status, errMsg string) error
	Cancel(ctx context.Context, id string) error
	List(ctx context.Context, f ListFilter) ([]Notification, int, error)
}

type Service struct {
	repo       notifStore
	publisher  queuePublisher
	maxRetries int
}

func NewService(repo notifStore, publisher queuePublisher, maxRetries int) *Service {
	return &Service{repo: repo, publisher: publisher, maxRetries: maxRetries}
}

func (s *Service) Create(ctx context.Context, req CreateRequest) (Notification, bool, error) {
	if err := validateRequest(req); err != nil {
		return Notification{}, false, err
	}

	priority := Priority(req.Priority)
	if priority == "" {
		priority = PriorityNormal
	}

	n := Notification{
		ID:             uuid.New().String(),
		Recipient:      req.Recipient,
		Channel:        Channel(req.Channel),
		Content:        req.Content,
		Priority:       priority,
		Status:         StatusPending,
		IdempotencyKey: req.IdempotencyKey,
		MaxRetries:     s.maxRetries,
		ScheduledAt:    req.ScheduledAt,
	}

	var (
		created Notification
		isNew   bool
		err     error
	)

	if req.IdempotencyKey != nil {
		created, isNew, err = s.repo.CreateWithIdempotency(ctx, n)
	} else {
		created, err = s.repo.Create(ctx, n)
		isNew = true
	}
	if err != nil {
		return Notification{}, false, fmt.Errorf("persist notification: %w", err)
	}

	if isNew && req.ScheduledAt == nil {
		if err := s.enqueue(ctx, created); err != nil {
			slog.WarnContext(ctx, "initial enqueue failed; stuck-pending recovery will re-enqueue",
				"notification_id", created.ID, "error", err)
			return created, true, nil
		}
	}

	return created, isNew, nil
}

func (s *Service) CreateBatch(ctx context.Context, req CreateBatchRequest) (Batch, error) {
	seen := make(map[string]int, len(req.Notifications))
	for i, r := range req.Notifications {
		if err := validateRequest(r); err != nil {
			return Batch{}, fmt.Errorf("notification[%d]: %w", i, err)
		}
		if r.IdempotencyKey != nil {
			if prev, dup := seen[*r.IdempotencyKey]; dup {
				return Batch{}, fmt.Errorf("notification[%d]: duplicate idempotency_key %q already used at index %d", i, *r.IdempotencyKey, prev)
			}
			seen[*r.IdempotencyKey] = i
		}
	}

	batchID := uuid.New().String()
	notifications := make([]Notification, len(req.Notifications))
	for i, r := range req.Notifications {
		priority := Priority(r.Priority)
		if priority == "" {
			priority = PriorityNormal
		}
		notifications[i] = Notification{
			ID:             uuid.New().String(),
			BatchID:        &batchID,
			Recipient:      r.Recipient,
			Channel:        Channel(r.Channel),
			Content:        r.Content,
			Priority:       priority,
			Status:         StatusPending,
			IdempotencyKey: r.IdempotencyKey,
			MaxRetries:     s.maxRetries,
			ScheduledAt:    r.ScheduledAt,
		}
	}

	batch := Batch{
		ID:    batchID,
		Total: len(req.Notifications),
	}
	createdBatch, err := s.repo.CreateBatchAtomic(ctx, batch, notifications)
	if err != nil {
		return Batch{}, fmt.Errorf("create batch: %w", err)
	}

	for _, n := range notifications {
		if n.ScheduledAt == nil {
			if err := s.enqueue(ctx, n); err != nil {
				slog.WarnContext(ctx, "batch enqueue failed; stuck-pending recovery will re-enqueue",
					"notification_id", n.ID, "batch_id", createdBatch.ID, "error", err)
			}
		}
	}

	return s.repo.FindBatchWithLiveCounts(ctx, createdBatch.ID)
}

func (s *Service) GetByID(ctx context.Context, id string) (Notification, error) {
	return s.repo.FindByID(ctx, id)
}

func (s *Service) GetBatchByID(ctx context.Context, id string) (Batch, error) {
	return s.repo.FindBatchWithLiveCounts(ctx, id)
}

func (s *Service) Cancel(ctx context.Context, id string) error {
	return s.repo.Cancel(ctx, id)
}

func (s *Service) List(ctx context.Context, f ListFilter) ([]Notification, int, error) {
	return s.repo.List(ctx, f)
}

func (s *Service) enqueue(ctx context.Context, n Notification) error {
	msg := queue.Message{
		NotificationID: n.ID,
		Recipient:      n.Recipient,
		Channel:        string(n.Channel),
		Content:        n.Content,
		Priority:       string(n.Priority),
		RetryCount:     0,
	}
	if err := s.publisher.Publish(ctx, msg); err != nil {
		return err
	}
	return s.repo.UpdateStatus(ctx, n.ID, StatusQueued, "")
}

func validateRequest(req CreateRequest) error {
	if req.IdempotencyKey != nil {
		if len(*req.IdempotencyKey) == 0 {
			return &ValidationError{Message: "idempotency_key must not be empty"}
		}
		if len(*req.IdempotencyKey) > 255 {
			return &ValidationError{Message: "idempotency_key exceeds 255 characters"}
		}
	}

	if req.Priority != "" {
		switch req.Priority {
		case "high", "normal", "low":
			// valid
		default:
			return &ValidationError{Message: fmt.Sprintf("unsupported priority %q: must be one of high, normal, low", req.Priority)}
		}
	}

	switch req.Channel {
	case "sms":
		if utf8.RuneCountInString(req.Content) > 1520 {
			return &ValidationError{Message: "SMS content exceeds 1520 characters"}
		}
	case "email":
		if _, err := mail.ParseAddress(req.Recipient); err != nil {
			return &ValidationError{Message: "invalid email address"}
		}
		if utf8.RuneCountInString(req.Content) > 10000 {
			return &ValidationError{Message: "email content exceeds 10000 characters"}
		}
	case "push":
		if utf8.RuneCountInString(req.Content) > 256 {
			return &ValidationError{Message: "push content exceeds 256 characters"}
		}
	default:
		return &ValidationError{Message: fmt.Sprintf("unsupported channel %q: must be one of sms, email, push", req.Channel)}
	}
	return nil
}
