package ws

import (
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type conn struct {
	ws        *websocket.Conn
	mu        sync.Mutex
	closeOnce sync.Once
}

func (c *conn) writeJSON(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ws.WriteMessage(websocket.TextMessage, data)
}

func (c *conn) close() {
	c.closeOnce.Do(func() {
		c.ws.Close()
	})
}

type StatusEvent struct {
	NotificationID string    `json:"notification_id"`
	Status         string    `json:"status"`
	Timestamp      time.Time `json:"timestamp"`
}

type Hub struct {
	mu          sync.RWMutex
	subscribers map[string]map[*conn]struct{}
}

func NewHub() *Hub {
	return &Hub{
		subscribers: make(map[string]map[*conn]struct{}),
	}
}

func (h *Hub) Subscribe(notifID string, c *conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subscribers[notifID] == nil {
		h.subscribers[notifID] = make(map[*conn]struct{})
	}
	h.subscribers[notifID][c] = struct{}{}
}

func (h *Hub) Unsubscribe(notifID string, c *conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if subs, ok := h.subscribers[notifID]; ok {
		delete(subs, c)
		if len(subs) == 0 {
			delete(h.subscribers, notifID)
		}
	}
}

func (h *Hub) Broadcast(notifID string, status string) {
	event := StatusEvent{
		NotificationID: notifID,
		Status:         status,
		Timestamp:      time.Now().UTC(),
	}
	data, err := json.Marshal(event)
	if err != nil {
		slog.Error("ws: marshal status event", "error", err)
		return
	}

	h.mu.RLock()
	conns := make([]*conn, 0, len(h.subscribers[notifID]))
	for c := range h.subscribers[notifID] {
		conns = append(conns, c)
	}
	h.mu.RUnlock()

	for _, c := range conns {
		if err := c.writeJSON(data); err != nil {
			slog.Warn("ws: write failed, removing subscriber",
				"notification_id", notifID, "error", err)
			h.Unsubscribe(notifID, c)
			c.close()
		}
	}
}
