package metrics

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type Repository struct {
	db *sqlx.DB
}

func NewRepository(db *sqlx.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) queueDepths(ctx context.Context) ([]queueDepth, error) {
	var rows []queueDepth
	err := r.db.SelectContext(ctx, &rows, `
		SELECT priority, COUNT(*) AS cnt
		FROM notifications
		WHERE status IN ('pending','queued','processing')
		GROUP BY priority`)
	if err != nil {
		return nil, fmt.Errorf("query queue depths: %w", err)
	}
	return rows, nil
}

func (r *Repository) channelStats(ctx context.Context) ([]channelStat, error) {
	var rows []channelStat
	err := r.db.SelectContext(ctx, &rows, `
		SELECT
			channel,
			COALESCE(SUM(CASE WHEN status='delivered' THEN 1 ELSE 0 END), 0)   AS delivered,
			COALESCE(SUM(CASE WHEN status IN ('failed','dead_letter') THEN 1 ELSE 0 END), 0) AS failed,
			COUNT(*) AS total,
			CASE WHEN COUNT(*) > 0
				THEN ROUND(
					SUM(CASE WHEN status='delivered' THEN 1 ELSE 0 END)::numeric / COUNT(*), 4
				)
				ELSE 0
			END AS success_rate,
			COALESCE(
				AVG(CASE WHEN status='delivered' AND delivered_at IS NOT NULL
					THEN EXTRACT(EPOCH FROM (delivered_at - created_at)) * 1000
				END),
				0
			) AS avg_latency_ms
		FROM notifications
		WHERE created_at >= NOW() - INTERVAL '24 hours'
		GROUP BY channel`)
	if err != nil {
		return nil, fmt.Errorf("query channel stats: %w", err)
	}
	return rows, nil
}
