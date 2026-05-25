package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/4olcay/notification/config"
)

type WebhookProvider struct {
	client     *http.Client
	webhookURL string
}

func NewWebhookProvider(cfg config.ProviderConfig) *WebhookProvider {
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	return &WebhookProvider{
		client:     &http.Client{Timeout: timeout},
		webhookURL: cfg.WebhookURL,
	}
}

type webhookPayload struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type webhookResponse struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

func (p *WebhookProvider) Deliver(ctx context.Context, req Request) (Response, error) {
	payload := webhookPayload(req)

	data, err := json.Marshal(payload)
	if err != nil {
		return Response{}, fmt.Errorf("failed to marshal payload: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.webhookURL, bytes.NewReader(data))
	if err != nil {
		return Response{}, fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return Response{}, fmt.Errorf("delivery request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return Response{}, fmt.Errorf("provider returned status %d: %s", resp.StatusCode, bytes.TrimSpace(body))
	}

	var result webhookResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return Response{}, fmt.Errorf("failed to decode provider response: %w", err)
	}

	return Response(result), nil
}
