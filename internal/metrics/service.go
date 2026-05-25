package metrics

import (
	"context"
	"fmt"
	"time"
)

type metricsStore interface {
	queueDepths(ctx context.Context) ([]queueDepth, error)
	channelStats(ctx context.Context) ([]channelStat, error)
}

type channelStat struct {
	Channel      string  `db:"channel"`
	Delivered    int64   `db:"delivered"`
	Failed       int64   `db:"failed"`
	Total        int64   `db:"total"`
	SuccessRate  float64 `db:"success_rate"`
	AvgLatencyMs float64 `db:"avg_latency_ms"`
}

type queueDepth struct {
	Priority string `db:"priority"`
	Count    int64  `db:"cnt"`
}

type Summary struct {
	QueueDepth map[string]int64       `json:"queue_depth"`
	Channels   map[string]ChannelStat `json:"channels"`
}

type ChannelStat struct {
	Delivered24h int64   `json:"delivered_24h"`
	Failed24h    int64   `json:"failed_24h"`
	SuccessRate  float64 `json:"success_rate"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
}

type Service struct {
	repo metricsStore
}

func NewService(repo metricsStore) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetSummary(ctx context.Context) (Summary, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	depths, err := s.repo.queueDepths(ctx)
	if err != nil {
		return Summary{}, fmt.Errorf("queue depths: %w", err)
	}

	stats, err := s.repo.channelStats(ctx)
	if err != nil {
		return Summary{}, fmt.Errorf("channel stats: %w", err)
	}

	depthMap := make(map[string]int64)
	for _, d := range depths {
		depthMap[d.Priority] = d.Count
	}

	statMap := make(map[string]ChannelStat)
	for _, cs := range stats {
		statMap[cs.Channel] = ChannelStat{
			Delivered24h: cs.Delivered,
			Failed24h:    cs.Failed,
			SuccessRate:  cs.SuccessRate,
			AvgLatencyMs: cs.AvgLatencyMs,
		}
	}

	return Summary{QueueDepth: depthMap, Channels: statMap}, nil
}
