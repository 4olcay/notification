package apiresponse

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
)

type Envelope struct {
	Success bool       `json:"success"`
	Data    any        `json:"data,omitempty"`
	Meta    any        `json:"meta,omitempty"`
	Error   *ErrorBody `json:"error,omitempty"`
}

type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type PageMeta struct {
	Total  int `json:"total"`
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
}

func OK(c *gin.Context, data any) {
	Success(c, http.StatusOK, data)
}

func Created(c *gin.Context, data any) {
	Success(c, http.StatusCreated, data)
}

func Accepted(c *gin.Context, data any) {
	Success(c, http.StatusAccepted, data)
}

func Success(c *gin.Context, status int, data any) {
	c.JSON(status, Envelope{
		Success: true,
		Data:    data,
	})
}

func SuccessWithMeta(c *gin.Context, status int, data any, meta any) {
	c.JSON(status, Envelope{
		Success: true,
		Data:    data,
		Meta:    meta,
	})
}

func NoContent(c *gin.Context) {
	c.Status(http.StatusNoContent)
}

func BadRequest(c *gin.Context, message string) {
	Error(c, http.StatusBadRequest, "BAD_REQUEST", message, nil)
}

func Validation(c *gin.Context, message string) {
	Error(c, http.StatusBadRequest, "VALIDATION_ERROR", message, nil)
}

func NotFound(c *gin.Context, message string) {
	Error(c, http.StatusNotFound, "NOT_FOUND", message, nil)
}

func Conflict(c *gin.Context, message string) {
	Error(c, http.StatusConflict, "CONFLICT", message, nil)
}

func Internal(c *gin.Context, message string, err error) {
	Error(c, http.StatusInternalServerError, "INTERNAL_ERROR", message, err)
}

func Error(c *gin.Context, status int, code string, message string, err error) {
	cid := correlationID(c)
	if status >= http.StatusInternalServerError {
		slog.Error("api error",
			"status", status,
			"code", code,
			"message", message,
			"error", err,
			"correlation_id", cid,
		)
	}
	c.JSON(status, Envelope{
		Success: false,
		Error: &ErrorBody{
			Code:    code,
			Message: message,
		},
	})
}

func correlationID(c *gin.Context) string {
	if id := c.GetString("correlation_id"); id != "" {
		return id
	}
	return c.GetHeader("X-Correlation-ID")
}
