package metrics

import (
	"context"

	"github.com/gin-gonic/gin"

	"github.com/4olcay/notification/internal/apiresponse"
)

type metricsService interface {
	GetSummary(ctx context.Context) (Summary, error)
}

type Handler struct {
	svc metricsService
}

func NewHandler(svc metricsService) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) RegisterRoutes(r *gin.Engine) {
	r.GET("/metrics", h.GetMetrics)
}

func (h *Handler) GetMetrics(c *gin.Context) {
	summary, err := h.svc.GetSummary(c.Request.Context())
	if err != nil {
		apiresponse.Internal(c, "failed to fetch metrics", err)
		return
	}
	apiresponse.OK(c, summary)
}
