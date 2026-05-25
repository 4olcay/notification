package notification

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/4olcay/notification/internal/apiresponse"
)

type notificationService interface {
	Create(ctx context.Context, req CreateRequest) (Notification, bool, error)
	CreateBatch(ctx context.Context, req CreateBatchRequest) (Batch, error)
	GetByID(ctx context.Context, id string) (Notification, error)
	GetBatchByID(ctx context.Context, id string) (Batch, error)
	Cancel(ctx context.Context, id string) error
	List(ctx context.Context, f ListFilter) ([]Notification, int, error)
}

type Handler struct {
	svc notificationService
}

func NewHandler(svc notificationService) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) RegisterRoutes(r *gin.Engine) {
	g := r.Group("/notifications")
	g.POST("", h.Create)
	g.POST("/batch", h.CreateBatch)
	g.GET("", h.List)
	g.GET("/:id", h.GetByID)
	g.DELETE("/:id", h.Cancel)

	r.GET("/batches/:batch_id", h.GetBatchByID)
}

type createRequest struct {
	Recipient      string     `json:"recipient"       binding:"required"`
	Channel        string     `json:"channel"         binding:"required,oneof=sms email push"`
	Content        string     `json:"content"         binding:"required"`
	Priority       string     `json:"priority"        binding:"omitempty,oneof=high normal low"`
	IdempotencyKey *string    `json:"idempotency_key"`
	ScheduledAt    *time.Time `json:"scheduled_at"`
}

type createBatchRequest struct {
	Notifications []createRequest `json:"notifications" binding:"required,min=1,max=1000,dive"`
}

type listQuery struct {
	Status  string `form:"status"   binding:"omitempty,oneof=pending queued processing delivered failed retrying dead_letter cancelled"`
	Channel string `form:"channel"  binding:"omitempty,oneof=sms email push"`
	From    int64  `form:"from"`
	To      int64  `form:"to"`
	Limit   int    `form:"limit"`
	Offset  int    `form:"offset"`
}

// Create godoc
// @Summary      Create a notification
// @Description  Persists and enqueues a single notification. Returns 202 for new, 200 for idempotent duplicate.
// @Tags         notifications
// @Accept       json
// @Produce      json
// @Param        request  body      createRequest  true  "Notification payload"
// @Success      202      {object}  Notification
// @Success      200      {object}  Notification
// @Failure      400      {object}  map[string]string
// @Router       /notifications [post]
func (h *Handler) Create(c *gin.Context) {
	var req createRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresponse.BadRequest(c, err.Error())
		return
	}

	svcReq := CreateRequest(req)

	notification, isNew, err := h.svc.Create(c.Request.Context(), svcReq)
	if err != nil {
		handleServiceError(c, err)
		return
	}

	status := http.StatusAccepted
	if !isNew {
		status = http.StatusOK // idempotent: return existing
	}
	apiresponse.Success(c, status, notification)
}

// CreateBatch godoc
// @Summary      Create a batch of notifications
// @Description  Persists up to 1000 notifications under a shared batch ID and enqueues them for processing.
// @Tags         notifications
// @Accept       json
// @Produce      json
// @Param        request  body      createBatchRequest  true  "Batch payload"
// @Success      202      {object}  Batch
// @Failure      400      {object}  map[string]string
// @Router       /notifications/batch [post]
func (h *Handler) CreateBatch(c *gin.Context) {
	var req createBatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresponse.BadRequest(c, err.Error())
		return
	}

	svcReqs := make([]CreateRequest, len(req.Notifications))
	for i, r := range req.Notifications {
		svcReqs[i] = CreateRequest(r)
	}

	batch, err := h.svc.CreateBatch(c.Request.Context(), CreateBatchRequest{Notifications: svcReqs})
	if err != nil {
		handleServiceError(c, err)
		return
	}

	apiresponse.Accepted(c, batch)
}

// GetByID godoc
// @Summary      Get a notification by ID
// @Tags         notifications
// @Produce      json
// @Param        id   path      string  true  "Notification UUID"
// @Success      200  {object}  Notification
// @Failure      404  {object}  map[string]string
// @Failure      500  {object}  map[string]string
// @Router       /notifications/{id} [get]
func (h *Handler) GetByID(c *gin.Context) {
	id := c.Param("id")
	n, err := h.svc.GetByID(c.Request.Context(), id)
	if err != nil {
		handleServiceError(c, err)
		return
	}
	apiresponse.OK(c, n)
}

// GetBatchByID godoc
// @Summary      Get batch status with live counts
// @Description  Returns batch metadata with notification counts computed live from the notifications table.
// @Tags         notifications
// @Produce      json
// @Param        batch_id  path      string  true  "Batch UUID"
// @Success      200       {object}  Batch
// @Failure      404       {object}  map[string]string
// @Failure      500       {object}  map[string]string
// @Router       /batches/{batch_id} [get]
func (h *Handler) GetBatchByID(c *gin.Context) {
	id := c.Param("batch_id")
	batch, err := h.svc.GetBatchByID(c.Request.Context(), id)
	if err != nil {
		handleServiceError(c, err)
		return
	}
	apiresponse.OK(c, batch)
}

// Cancel godoc
// @Summary      Cancel a pending notification
// @Tags         notifications
// @Produce      json
// @Param        id   path      string  true  "Notification UUID"
// @Success      200  {object}  map[string]string
// @Failure      404  {object}  map[string]string
// @Failure      409  {object}  map[string]string  "Notification is no longer pending"
// @Failure      500  {object}  map[string]string
// @Router       /notifications/{id} [delete]
func (h *Handler) Cancel(c *gin.Context) {
	id := c.Param("id")
	err := h.svc.Cancel(c.Request.Context(), id)
	if err != nil {
		handleServiceError(c, err)
		return
	}
	apiresponse.OK(c, gin.H{"status": "cancelled"})
}

// List godoc
// @Summary      List notifications
// @Tags         notifications
// @Produce      json
// @Param        status   query     string  false  "Filter by status (pending|queued|processing|delivered|failed|retrying|dead_letter|cancelled)"
// @Param        channel  query     string  false  "Filter by channel (sms|email|push)"
// @Param        from     query     integer false  "Created after Unix timestamp"
// @Param        to       query     integer false  "Created before Unix timestamp"
// @Param        limit    query     integer false  "Page size (default 20, max 100)"
// @Param        offset   query     integer false  "Page offset"
// @Success      200      {object}  apiresponse.PageMeta
// @Failure      400      {object}  map[string]string
// @Failure      500      {object}  map[string]string
// @Router       /notifications [get]
func (h *Handler) List(c *gin.Context) {
	var q listQuery
	if err := c.ShouldBindQuery(&q); err != nil {
		apiresponse.BadRequest(c, err.Error())
		return
	}

	limit := q.Limit
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	filter := ListFilter{
		Status:  Status(q.Status),
		Channel: Channel(q.Channel),
		Limit:   limit,
		Offset:  q.Offset,
	}
	if q.From > 0 {
		t := time.Unix(q.From, 0)
		filter.FromTime = &t
	}
	if q.To > 0 {
		t := time.Unix(q.To, 0)
		filter.ToTime = &t
	}

	notifications, total, err := h.svc.List(c.Request.Context(), filter)
	if err != nil {
		handleServiceError(c, err)
		return
	}

	apiresponse.SuccessWithMeta(c, http.StatusOK, notifications, apiresponse.PageMeta{
		Total:  total,
		Limit:  limit,
		Offset: q.Offset,
	})
}

func handleServiceError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		apiresponse.NotFound(c, "resource not found")
	case errors.Is(err, ErrCannotCancel):
		apiresponse.Conflict(c, "only pending notifications can be cancelled")
	default:
		var valErr *ValidationError
		if errors.As(err, &valErr) {
			apiresponse.Validation(c, valErr.Message)
			return
		}
		apiresponse.Internal(c, "internal server error", err)
	}
}
