package template

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

const uniqueViolation = pq.ErrorCode("23505")

type Repository struct {
	db *sqlx.DB
}

func NewRepository(db *sqlx.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) Create(ctx context.Context, t Template) (Template, error) {
	rows, err := r.db.NamedQueryContext(ctx, `
		INSERT INTO notification_templates (id, name, channel, subject, body, created_at, updated_at)
		VALUES (:id, :name, :channel, :subject, :body, NOW(), NOW())
		RETURNING id, name, channel, subject, body, created_at, updated_at`, t)
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == uniqueViolation {
			return Template{}, ErrAlreadyExists
		}
		return Template{}, fmt.Errorf("insert template: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return Template{}, errors.New("no rows returned after insert")
	}
	var created Template
	if err := rows.StructScan(&created); err != nil {
		return Template{}, fmt.Errorf("scan template: %w", err)
	}
	return created, nil
}

func (r *Repository) FindByName(ctx context.Context, name string) (Template, error) {
	var t Template
	err := r.db.GetContext(ctx, &t, `
		SELECT id, name, channel, subject, body, created_at, updated_at
		FROM notification_templates
		WHERE name = $1`, name)
	if errors.Is(err, sql.ErrNoRows) {
		return Template{}, ErrNotFound
	}
	if err != nil {
		return Template{}, fmt.Errorf("find template by name: %w", err)
	}
	return t, nil
}

func (r *Repository) List(ctx context.Context) ([]Template, error) {
	var templates []Template
	err := r.db.SelectContext(ctx, &templates, `
		SELECT id, name, channel, subject, body, created_at, updated_at
		FROM notification_templates
		ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list templates: %w", err)
	}
	return templates, nil
}

func (r *Repository) Delete(ctx context.Context, name string) error {
	result, err := r.db.ExecContext(ctx,
		"DELETE FROM notification_templates WHERE name = $1", name)
	if err != nil {
		return fmt.Errorf("delete template: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete template rows affected: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}
