package template

import (
	"errors"
	"time"
)

var (
	ErrNotFound      = errors.New("template not found")
	ErrAlreadyExists = errors.New("template name already exists")
)

type Template struct {
	ID        string    `db:"id"         json:"id"`
	Name      string    `db:"name"       json:"name"`
	Channel   string    `db:"channel"    json:"channel"`
	Subject   *string   `db:"subject"    json:"subject,omitempty"`
	Body      string    `db:"body"       json:"body"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`
}

type CreateRequest struct {
	Name    string  `json:"name" binding:"required,min=1,max=100"`
	Channel string  `json:"channel" binding:"required,oneof=sms email push"`
	Subject *string `json:"subject"`
	Body    string  `json:"body" binding:"required,min=1"`
}

type RenderRequest struct {
	Variables map[string]string `json:"variables"`
}

type RenderResponse struct {
	Subject *string `json:"subject,omitempty"`
	Body    string  `json:"body"`
}
