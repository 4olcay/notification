package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/4olcay/notification/config"
)

type Message struct {
	NotificationID string `json:"notification_id"`
	Recipient      string `json:"recipient"`
	Channel        string `json:"channel"`
	Content        string `json:"content"`
	Priority       string `json:"priority"`
	RetryCount     int    `json:"retry_count"`
}

type Producer struct {
	writers         map[string]*kafkago.Writer
	brokers         []string
	priorityTopics  map[string]string
	deadLetterTopic string
}

func NewProducer(cfg config.KafkaConfig) (*Producer, error) {
	topics := []string{cfg.TopicHigh, cfg.TopicNormal, cfg.TopicLow, cfg.TopicDeadLetter}
	writers := make(map[string]*kafkago.Writer, len(topics))

	for _, topic := range topics {
		writers[topic] = &kafkago.Writer{
			Addr:                   kafkago.TCP(cfg.Brokers...),
			Topic:                  topic,
			Balancer:               &kafkago.LeastBytes{},
			RequiredAcks:           kafkago.RequireAll,
			BatchSize:              100,
			BatchTimeout:           10 * time.Millisecond,
			Compression:            kafkago.Snappy,
			AllowAutoTopicCreation: true,
		}
	}

	return &Producer{
		writers: writers,
		brokers: cfg.Brokers,
		priorityTopics: map[string]string{
			"high":   cfg.TopicHigh,
			"normal": cfg.TopicNormal,
			"low":    cfg.TopicLow,
		},
		deadLetterTopic: cfg.TopicDeadLetter,
	}, nil
}

func (p *Producer) Publish(ctx context.Context, msg Message) error {
	topic, ok := p.priorityTopics[msg.Priority]
	if !ok {
		topic = p.priorityTopics["normal"]
	}
	w, ok := p.writers[topic]
	if !ok {
		return fmt.Errorf("no writer for topic %q (priority %q)", topic, msg.Priority)
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	return w.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(msg.NotificationID),
		Value: data,
	})
}

func (p *Producer) PublishDeadLetter(ctx context.Context, msg Message) error {
	w, ok := p.writers[p.deadLetterTopic]
	if !ok {
		return fmt.Errorf("no writer for dead-letter topic %s", p.deadLetterTopic)
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal dead-letter message: %w", err)
	}

	return w.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(msg.NotificationID),
		Value: data,
	})
}

func (p *Producer) Ping(ctx context.Context) error {
	if len(p.brokers) == 0 {
		return fmt.Errorf("no kafka brokers configured")
	}
	var lastErr error
	for _, broker := range p.brokers {
		conn, err := kafkago.DialContext(ctx, "tcp", broker)
		if err != nil {
			lastErr = fmt.Errorf("broker %s: %w", broker, err)
			continue
		}
		_, err = conn.Brokers()
		conn.Close()
		if err == nil {
			return nil
		}
		lastErr = fmt.Errorf("broker %s: %w", broker, err)
	}
	return fmt.Errorf("no kafka broker reachable: %w", lastErr)
}

func (p *Producer) Close() {
	for _, w := range p.writers {
		if err := w.Close(); err != nil {
			slog.Error("kafka writer close", "topic", w.Topic, "error", err)
		}
	}
}
