package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/4olcay/notification/internal/delivery"
	"github.com/4olcay/notification/internal/notification"
	"github.com/4olcay/notification/internal/queue"
)

type mockRepo struct {
	tryMarkProcessingFn func(ctx context.Context, id string) (bool, error)
	updateStatusFn      func(ctx context.Context, id string, status notification.Status, errMsg string) error
	markDeliveredFn     func(ctx context.Context, id, providerMsgID string) error
	markRetryingFn      func(ctx context.Context, id string, retryCount int, nextRetryAt time.Time, errMsg string) error
	markDeadLetterFn    func(ctx context.Context, id, errMsg string) error
	findByIDFn          func(ctx context.Context, id string) (notification.Notification, error)
}

type mockDeadLetterPublisher struct {
	publishFn func(ctx context.Context, msg queue.Message) error
}

func (m *mockRepo) TryMarkProcessing(ctx context.Context, id string) (bool, error) {
	if m.tryMarkProcessingFn != nil {
		return m.tryMarkProcessingFn(ctx, id)
	}
	return true, nil
}
func (m *mockRepo) UpdateStatus(ctx context.Context, id string, status notification.Status, errMsg string) error {
	return m.updateStatusFn(ctx, id, status, errMsg)
}
func (m *mockRepo) MarkDelivered(ctx context.Context, id, providerMsgID string) error {
	return m.markDeliveredFn(ctx, id, providerMsgID)
}
func (m *mockRepo) MarkRetrying(ctx context.Context, id string, retryCount int, nextRetryAt time.Time, errMsg string) error {
	return m.markRetryingFn(ctx, id, retryCount, nextRetryAt, errMsg)
}
func (m *mockRepo) MarkDeadLetter(ctx context.Context, id, errMsg string) error {
	return m.markDeadLetterFn(ctx, id, errMsg)
}
func (m *mockRepo) FindByID(ctx context.Context, id string) (notification.Notification, error) {
	return m.findByIDFn(ctx, id)
}

func (m *mockDeadLetterPublisher) PublishDeadLetter(ctx context.Context, msg queue.Message) error {
	return m.publishFn(ctx, msg)
}

type mockProvider struct {
	deliverFn func(ctx context.Context, req delivery.Request) (delivery.Response, error)
}

func (m *mockProvider) Deliver(ctx context.Context, req delivery.Request) (delivery.Response, error) {
	return m.deliverFn(ctx, req)
}

func noopUpdateStatus(_ context.Context, _ string, _ notification.Status, _ string) error {
	return nil
}

func newMsg(retryCount int) queue.Message {
	return queue.Message{
		NotificationID: "notif-1",
		Recipient:      "user@example.com",
		Channel:        "email",
		Content:        "body",
		Priority:       "normal",
		RetryCount:     retryCount,
	}
}

func TestProcess_DeliverySuccess(t *testing.T) {
	deliverCalled := false
	markedDelivered := false

	repo := &mockRepo{
		updateStatusFn: noopUpdateStatus,
		markDeliveredFn: func(_ context.Context, id, msgID string) error {
			markedDelivered = true
			return nil
		},
	}
	provider := &mockProvider{
		deliverFn: func(_ context.Context, _ delivery.Request) (delivery.Response, error) {
			deliverCalled = true
			return delivery.Response{MessageID: "ext-123"}, nil
		},
	}

	rl := NewChannelRateLimiter(1000)
	w := NewWorker(repo, provider, rl, 5, nil, nil)
	_ = w.Process(context.Background(), newMsg(0))

	if !deliverCalled {
		t.Error("expected provider.Deliver to be called")
	}
	if !markedDelivered {
		t.Error("expected MarkDelivered to be called on success")
	}
}

func TestProcess_DeliveryFailure_SchedulesRetry(t *testing.T) {
	markedRetrying := false

	repo := &mockRepo{
		updateStatusFn: noopUpdateStatus,
		markRetryingFn: func(_ context.Context, id string, retryCount int, nextRetryAt time.Time, errMsg string) error {
			markedRetrying = true
			if retryCount != 1 {
				t.Errorf("expected retryCount=1, got %d", retryCount)
			}
			if nextRetryAt.Before(time.Now()) {
				t.Error("nextRetryAt should be in the future")
			}
			return nil
		},
	}
	provider := &mockProvider{
		deliverFn: func(_ context.Context, _ delivery.Request) (delivery.Response, error) {
			return delivery.Response{}, errors.New("provider unavailable")
		},
	}

	rl := NewChannelRateLimiter(1000)
	w := NewWorker(repo, provider, rl, 5, nil, nil)
	_ = w.Process(context.Background(), newMsg(0))

	if !markedRetrying {
		t.Error("expected MarkRetrying to be called on first failure")
	}
}

func TestProcess_DeliveryFailure_MaxRetries_DeadLetter(t *testing.T) {
	markedDeadLetter := false

	repo := &mockRepo{
		updateStatusFn: noopUpdateStatus,
		markDeadLetterFn: func(_ context.Context, id, errMsg string) error {
			markedDeadLetter = true
			return nil
		},
	}
	provider := &mockProvider{
		deliverFn: func(_ context.Context, _ delivery.Request) (delivery.Response, error) {
			return delivery.Response{}, errors.New("still failing")
		},
	}

	rl := NewChannelRateLimiter(1000)
	w := NewWorker(repo, provider, rl, 5, nil, nil)
	_ = w.Process(context.Background(), newMsg(5))

	if !markedDeadLetter {
		t.Error("expected MarkDeadLetter when retry count equals maxRetries")
	}
}

func TestProcess_DeadLetter_PublishesToKafka(t *testing.T) {
	dlPublished := false

	repo := &mockRepo{
		updateStatusFn: noopUpdateStatus,
		markDeadLetterFn: func(_ context.Context, _ string, _ string) error {
			return nil
		},
	}
	provider := &mockProvider{
		deliverFn: func(_ context.Context, _ delivery.Request) (delivery.Response, error) {
			return delivery.Response{}, errors.New("still failing")
		},
	}
	dl := &mockDeadLetterPublisher{
		publishFn: func(_ context.Context, msg queue.Message) error {
			dlPublished = true
			return nil
		},
	}

	rl := NewChannelRateLimiter(1000)
	w := NewWorker(repo, provider, rl, 5, dl, nil)
	_ = w.Process(context.Background(), newMsg(5))

	if !dlPublished {
		t.Error("expected dead-letter publisher to be called when max retries exceeded")
	}
}

func TestProcess_RateLimiterCancelledContext(t *testing.T) {
	markedBackToQueued := false

	repo := &mockRepo{
		updateStatusFn: func(_ context.Context, id string, status notification.Status, _ string) error {
			if status == notification.StatusQueued {
				markedBackToQueued = true
			}
			return nil
		},
	}
	provider := &mockProvider{
		deliverFn: func(_ context.Context, _ delivery.Request) (delivery.Response, error) {
			t.Error("provider should not be called when context is cancelled")
			return delivery.Response{}, nil
		},
	}

	rl := NewChannelRateLimiter(1000)
	w := NewWorker(repo, provider, rl, 5, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_ = w.Process(ctx, newMsg(0))

	if !markedBackToQueued {
		t.Error("expected status reverted to queued when context cancelled during rate limit")
	}
}
