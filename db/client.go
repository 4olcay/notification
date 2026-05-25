package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/4olcay/notification/config"
)

func NewClient(cfg config.DBConfig) (*sqlx.DB, error) {
	database, err := sqlx.Open("postgres", cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	database.SetMaxOpenConns(25)
	database.SetMaxIdleConns(10)
	database.SetConnMaxLifetime(5 * time.Minute)
	database.SetConnMaxIdleTime(2 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := database.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping db: %w", err)
	}

	return database, nil
}
