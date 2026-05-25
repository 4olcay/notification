package delivery

import "context"

type Request struct {
	To      string
	Channel string
	Content string
}

type Response struct {
	MessageID string
	Status    string
	Timestamp string
}

type Provider interface {
	Deliver(ctx context.Context, req Request) (Response, error)
}
