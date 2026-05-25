package ws

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

const (
	wsPongWait     = 60 * time.Second
	wsPingInterval = 50 * time.Second
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// CheckOrigin accepts all origins. In production this should be replaced
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Handler struct {
	hub *Hub
}

func NewHandler(hub *Hub) *Handler {
	return &Handler{hub: hub}
}

func (h *Handler) RegisterRoutes(r *gin.Engine) {
	r.GET("/ws/notifications/:id", h.Subscribe)
}

// Subscribe godoc
// @Summary     Subscribe to real-time status updates for a notification
// @Description Upgrades the HTTP connection to WebSocket. The server pushes a
//
//	StatusEvent JSON object every time the notification changes status.
//	The connection stays open until the client disconnects.
//
// @Tags        websocket
// @Param       id   path string true "Notification ID"
// @Success     101  "Switching Protocols"
// @Router      /ws/notifications/{id} [get]
func (h *Handler) Subscribe(c *gin.Context) {
	notifID := c.Param("id")

	wsConn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		slog.Error("ws: upgrade failed", "notification_id", notifID, "error", err)
		return
	}

	wrapped := &conn{ws: wsConn}
	h.hub.Subscribe(notifID, wrapped)
	slog.Info("ws: client subscribed", "notification_id", notifID,
		"remote_addr", c.Request.RemoteAddr)

	defer func() {
		h.hub.Unsubscribe(notifID, wrapped)
		wrapped.close()
		slog.Info("ws: client disconnected", "notification_id", notifID)
	}()

	wsConn.SetReadDeadline(time.Now().Add(wsPongWait)) //nolint:errcheck
	wsConn.SetPongHandler(func(string) error {
		wsConn.SetReadDeadline(time.Now().Add(wsPongWait)) //nolint:errcheck
		return nil
	})

	pingTicker := time.NewTicker(wsPingInterval)
	done := make(chan struct{})
	defer func() {
		close(done)
		pingTicker.Stop()
	}()

	go func() {
		for {
			select {
			case <-done:
				return
			case <-pingTicker.C:
				if err := wsConn.WriteControl(
					websocket.PingMessage, nil, time.Now().Add(5*time.Second),
				); err != nil {
					return
				}
			}
		}
	}()

	for {
		if _, _, err := wsConn.ReadMessage(); err != nil {
			return
		}
	}
}
