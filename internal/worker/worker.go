package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/4olcay/notification/internal/delivery"
	"github.com/4olcay/notification/internal/notification"
	"github.com/4olcay/notification/internal/queue"
)

var retryBackoff = []time.Duration{
	10 * time.Second,
	30 * time.Second,
	1 * time.Minute,
	5 * time.Minute,
	15 * time.Minute,
}

type notifRepository interface {
	FindByID(ctx context.Context, id string) (notification.Notification, error)
	TryMarkProcessing(ctx context.Context, id string) (bool, error)
	UpdateStatus(ctx context.Context, id string, status notification.Status, errMsg string) error
	MarkDelivered(ctx context.Context, id, providerMsgID string) error
	MarkRetrying(ctx context.Context, id string, retryCount int, nextRetryAt time.Time, errMsg string) error
	MarkDeadLetter(ctx context.Context, id, errMsg string) error
}

type deadLetterPublisher interface {
	PublishDeadLetter(ctx context.Context, msg queue.Message) error
}

type statusNotifier interface {
	Broadcast(notifID string, status string)
}

type Worker struct {
	repo        notifRepository
	provider    delivery.Provider
	rateLimiter *ChannelRateLimiter
	maxRetries  int
	dlPublisher deadLetterPublisher
	notifier    statusNotifier
}

func NewWorker(repo notifRepository, provider delivery.Provider, rl *ChannelRateLimiter, maxRetries int, dl deadLetterPublisher, notifier statusNotifier) *Worker {
	return &Worker{
		repo:        repo,
		provider:    provider,
		rateLimiter: rl,
		maxRetries:  maxRetries,
		dlPublisher: dl,
		notifier:    notifier,
	}
}

func (w *Worker) Process(ctx context.Context, msg queue.Message) error {
	log := slog.With("notification_id", msg.NotificationID, "channel", msg.Channel)

	started, err := w.repo.TryMarkProcessing(ctx, msg.NotificationID)
	if err != nil {
		log.Error("failed to mark processing", "error", err)
		return fmt.Errorf("mark processing: %w", err)
	}
	if !started {
		log.Info("skipping delivery: notification already in terminal state")
		return nil
	}

	if err := w.rateLimiter.Wait(ctx, msg.Channel); err != nil {
		log.Warn("rate limiter cancelled", "error", err)
		revertCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if revertErr := w.repo.UpdateStatus(revertCtx, msg.NotificationID, notification.StatusQueued, ""); revertErr != nil {
			log.Error("failed to revert processing status to queued; stuck-pending recovery will fix",
				"revert_error", revertErr)
		}
		return nil
	}

	resp, err := w.provider.Deliver(ctx, delivery.Request{
		To:      msg.Recipient,
		Channel: msg.Channel,
		Content: msg.Content,
	})

	if err == nil {
		if markErr := w.repo.MarkDelivered(ctx, msg.NotificationID, resp.MessageID); markErr != nil {
			log.Error("failed to mark delivered, not committing offset for redelivery", "error", markErr)
			return fmt.Errorf("mark delivered: %w", markErr)
		}
		log.Info("notification delivered", "provider_msg_id", resp.MessageID)
		w.broadcast(msg.NotificationID, "delivered")
		return nil
	}

	log.Warn("delivery failed", "error", err, "retry_count", msg.RetryCount)
	return w.handleFailure(ctx, msg, err)
}

func (w *Worker) handleFailure(ctx context.Context, msg queue.Message, deliveryErr error) error {
	if msg.RetryCount >= w.maxRetries {
		if err := w.repo.MarkDeadLetter(ctx, msg.NotificationID, deliveryErr.Error()); err != nil {
			slog.Error("failed to mark dead-letter, not committing offset for redelivery",
				"notification_id", msg.NotificationID, "error", err)
			return fmt.Errorf("mark dead-letter: %w", err)
		}
		slog.Warn("notification moved to dead-letter", "notification_id", msg.NotificationID)
		w.broadcast(msg.NotificationID, "dead_letter")
		if w.dlPublisher != nil {
			if err := w.dlPublisher.PublishDeadLetter(ctx, msg); err != nil {
				slog.Error("failed to publish dead-letter event", "notification_id", msg.NotificationID, "error", err)
			}
		}
		return nil
	}

	backoff := retryBackoff[min(msg.RetryCount, len(retryBackoff)-1)]
	nextRetry := time.Now().Add(backoff)

	if err := w.repo.MarkRetrying(ctx, msg.NotificationID, msg.RetryCount+1, nextRetry,
		fmt.Sprintf("attempt %d: %s", msg.RetryCount+1, deliveryErr)); err != nil {
		slog.Error("failed to mark retrying, not committing offset for redelivery",
			"notification_id", msg.NotificationID, "error", err)
		return fmt.Errorf("mark retrying: %w", err)
	}
	w.broadcast(msg.NotificationID, "retrying")
	return nil
}

func (w *Worker) broadcast(notifID string, status string) {
	if w.notifier != nil {
		w.notifier.Broadcast(notifID, status)
	}
}
