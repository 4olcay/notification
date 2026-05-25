package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/4olcay/notification/config"
	"github.com/4olcay/notification/internal/queue"
)

type Dispatcher struct {
	cfg         config.KafkaConfig
	concurrency int
	worker      *Worker
	retryWorker *RetryWorker
	sem         chan struct{}
	wg          sync.WaitGroup
}

func NewDispatcher(cfg config.KafkaConfig, concurrency int, w *Worker, rw *RetryWorker) *Dispatcher {
	if concurrency <= 0 {
		concurrency = 10
	}
	return &Dispatcher{
		cfg:         cfg,
		concurrency: concurrency,
		worker:      w,
		retryWorker: rw,
		sem:         make(chan struct{}, concurrency),
	}
}

func (d *Dispatcher) Start(ctx context.Context) {
	topics := []string{d.cfg.TopicHigh, d.cfg.TopicNormal, d.cfg.TopicLow}
	d.wg.Add(len(topics) + 1)

	for _, topic := range topics {
		go func(t string) {
			defer d.wg.Done()
			d.runTopic(ctx, t)
		}(topic)
	}

	go func() {
		defer d.wg.Done()
		d.retryWorker.Run(ctx)
	}()
}

func (d *Dispatcher) Wait() {
	d.wg.Wait()
}

func (d *Dispatcher) runTopic(ctx context.Context, topic string) {
	groupID := d.cfg.GroupID + "." + topic
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        d.cfg.Brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})
	defer reader.Close()

	slog.Info("consumer started", "topic", topic)

	for {
		kafkaMsg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				slog.Info("consumer stopped", "topic", topic)
				return
			}
			slog.Error("kafka fetch error", "topic", topic, "error", err)
			continue
		}

		var msg queue.Message
		if err := json.Unmarshal(kafkaMsg.Value, &msg); err != nil {
			slog.Error("unmarshal kafka message", "error", err)
			_ = reader.CommitMessages(ctx, kafkaMsg)
			continue
		}

		d.wg.Add(1)
		select {
		case d.sem <- struct{}{}:
		case <-ctx.Done():
			d.wg.Done()
			slog.Info("consumer stopping before semaphore acquire", "topic", topic)
			return
		}
		go func(km kafkago.Message, qm queue.Message) {
			defer d.wg.Done()
			defer func() { <-d.sem }()
			if err := d.worker.Process(ctx, qm); err != nil {
				slog.Error("process failed, skipping commit for redelivery",
					"notification_id", qm.NotificationID, "error", err)
				return
			}
			if err := reader.CommitMessages(ctx, km); err != nil && ctx.Err() == nil {
				slog.Error("kafka commit error", "error", err)
			}
		}(kafkaMsg, msg)
	}
}
