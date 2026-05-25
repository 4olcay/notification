package template

import (
	"context"
	"errors"

	"github.com/gin-gonic/gin"

	"github.com/4olcay/notification/internal/apiresponse"
)

// templateService is the interface the handler depends on, keeping it
// decoupled from the concrete Service and testable with mocks.
type templateService interface {
	Create(ctx context.Context, req CreateRequest) (Template, error)
	Render(ctx context.Context, name string, req RenderRequest) (RenderResponse, error)
	List(ctx context.Context) ([]Template, error)
	Delete(ctx context.Context, name string) error
}

type Handler struct {
	svc templateService
}

func NewHandler(svc templateService) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) RegisterRoutes(r *gin.Engine) {
	g := r.Group("/templates")
	g.POST("", h.Create)
	g.GET("", h.List)
	g.POST("/:name/render", h.Render)
	g.DELETE("/:name", h.Delete)
}

// Create godoc
// @Summary     Create a notification template
// @Tags        templates
// @Accept      json
// @Produce     json
// @Param       body body     CreateRequest true "Template definition"
// @Success     201  {object} Template
// @Failure     400  {object} map[string]string
// @Failure     409  {object} map[string]string
// @Router      /templates [post]
func (h *Handler) Create(c *gin.Context) {
	var req CreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresponse.BadRequest(c, err.Error())
		return
	}

	t, err := h.svc.Create(c.Request.Context(), req)
	if err != nil {
		handleServiceError(c, err)
		return
	}
	apiresponse.Created(c, t)
}

// List godoc
// @Summary     List all templates
// @Tags        templates
// @Produce     json
// @Success     200 {array}  Template
// @Router      /templates [get]
func (h *Handler) List(c *gin.Context) {
	templates, err := h.svc.List(c.Request.Context())
	if err != nil {
		handleServiceError(c, err)
		return
	}
	apiresponse.OK(c, templates)
}

// Render godoc
// @Summary     Render a template with variable substitution
// @Tags        templates
// @Accept      json
// @Produce     json
// @Param       name path     string        true "Template name"
// @Param       body body     RenderRequest true "Variable map"
// @Success     200  {object} RenderResponse
// @Failure     404  {object} map[string]string
// @Router      /templates/{name}/render [post]
func (h *Handler) Render(c *gin.Context) {
	name := c.Param("name")
	var req RenderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		apiresponse.BadRequest(c, err.Error())
		return
	}

	resp, err := h.svc.Render(c.Request.Context(), name, req)
	if err != nil {
		handleServiceError(c, err)
		return
	}
	apiresponse.OK(c, resp)
}

// Delete godoc
// @Summary     Delete a template
// @Tags        templates
// @Param       name path string true "Template name"
// @Success     204
// @Failure     404 {object} map[string]string
// @Router      /templates/{name} [delete]
func (h *Handler) Delete(c *gin.Context) {
	name := c.Param("name")
	if err := h.svc.Delete(c.Request.Context(), name); err != nil {
		handleServiceError(c, err)
		return
	}
	apiresponse.NoContent(c)
}

func handleServiceError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		apiresponse.NotFound(c, "template not found")
	case errors.Is(err, ErrAlreadyExists):
		apiresponse.Conflict(c, "template name already exists")
	default:
		apiresponse.Internal(c, "internal server error", err)
	}
}
