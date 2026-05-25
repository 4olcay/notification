package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/4olcay/notification/internal/notification"
	"github.com/4olcay/notification/internal/queue"
)

const (
	retryWorkerMinInterval  = 5 * time.Second
	retryWorkerMaxInterval  = 5 * time.Minute
	stuckPendingAge         = 2 * time.Minute
	retryWorkerDefaultBatch = 100
)

type retryRepository interface {
	FindDueRetries(ctx context.Context, limit int) ([]notification.Notification, error)
	FindDueScheduled(ctx context.Context, limit int) ([]notification.Notification, error)
	FindDueStuckPending(ctx context.Context, stuckSince time.Time, limit int) ([]notification.Notification, error)
}

type retryPublisher interface {
	Publish(ctx context.Context, msg queue.Message) error
}

type RetryWorker struct {
	repo      retryRepository
	publisher retryPublisher
	interval  time.Duration
	batchSize int
}

func NewRetryWorker(repo retryRepository, publisher retryPublisher) *RetryWorker {
	return &RetryWorker{
		repo:      repo,
		publisher: publisher,
		interval:  retryWorkerMinInterval,
		batchSize: retryWorkerDefaultBatch,
	}
}

func (w *RetryWorker) WithBatchSize(n int) *RetryWorker {
	w.batchSize = n
	return w
}

func (w *RetryWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			hadErrors := w.runCycle(ctx)

			newInterval := retryWorkerMinInterval
			if hadErrors {
				newInterval = w.interval * 2
				if newInterval > retryWorkerMaxInterval {
					newInterval = retryWorkerMaxInterval
				}
			}
			if newInterval != w.interval {
				w.interval = newInterval
				ticker.Stop()
				select {
				case <-ticker.C:
				default:
				}
				ticker.Reset(newInterval)
			}
		}
	}
}

func (w *RetryWorker) runCycle(ctx context.Context) (hadErrors bool) {
	e1 := w.processRetries(ctx)
	e2 := w.processScheduled(ctx)
	e3 := w.processStuckPending(ctx)
	return e1 || e2 || e3
}

func (w *RetryWorker) processRetries(ctx context.Context) (failed bool) {
	notifications, err := w.repo.FindDueRetries(ctx, w.batchSize)
	if err != nil {
		slog.Error("find due retries", "error", err)
		return true
	}

	for _, n := range notifications {
		msg := queue.Message{
			NotificationID: n.ID,
			Recipient:      n.Recipient,
			Channel:        string(n.Channel),
			Content:        n.Content,
			Priority:       string(n.Priority),
			RetryCount:     n.RetryCount,
		}

		if err := w.publisher.Publish(ctx, msg); err != nil {
			slog.Error("re-enqueue retry", "notification_id", n.ID, "error", err)
			failed = true
		}
	}
	return failed
}

func (w *RetryWorker) processScheduled(ctx context.Context) (failed bool) {
	notifications, err := w.repo.FindDueScheduled(ctx, w.batchSize)
	if err != nil {
		slog.Error("find due scheduled", "error", err)
		return true
	}

	for _, n := range notifications {
		msg := queue.Message{
			NotificationID: n.ID,
			Recipient:      n.Recipient,
			Channel:        string(n.Channel),
			Content:        n.Content,
			Priority:       string(n.Priority),
			RetryCount:     0,
		}

		if err := w.publisher.Publish(ctx, msg); err != nil {
			slog.Error("enqueue scheduled", "notification_id", n.ID, "error", err)
			failed = true
		}
	}
	return failed
}

func (w *RetryWorker) processStuckPending(ctx context.Context) (failed bool) {
	stuckSince := time.Now().Add(-stuckPendingAge)
	notifications, err := w.repo.FindDueStuckPending(ctx, stuckSince, w.batchSize)
	if err != nil {
		slog.Error("find stuck pending", "error", err)
		return true
	}

	for _, n := range notifications {
		msg := queue.Message{
			NotificationID: n.ID,
			Recipient:      n.Recipient,
			Channel:        string(n.Channel),
			Content:        n.Content,
			Priority:       string(n.Priority),
			RetryCount:     0,
		}

		if err := w.publisher.Publish(ctx, msg); err != nil {
			slog.Error("re-enqueue stuck pending", "notification_id", n.ID, "error", err)
			failed = true
		}
	}
	return failed
}
