// @title           Notification System API
// @version         1.0
// @description     Event-driven multi-channel notification service (SMS, Email, Push) with priority queuing, idempotency, scheduled delivery, template rendering, and automatic retry support.
// @host            localhost:8080
// @BasePath        /
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"

	"github.com/4olcay/notification/config"
	"github.com/4olcay/notification/db"
	_ "github.com/4olcay/notification/docs"
	"github.com/4olcay/notification/internal/apiresponse"
	"github.com/4olcay/notification/internal/delivery"
	"github.com/4olcay/notification/internal/metrics"
	"github.com/4olcay/notification/internal/notification"
	"github.com/4olcay/notification/internal/queue"
	tmpl "github.com/4olcay/notification/internal/template"
	"github.com/4olcay/notification/internal/tracing"
	"github.com/4olcay/notification/internal/worker"
	"github.com/4olcay/notification/internal/ws"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()
	shutdownTracing, err := tracing.Init(ctx, "notification-system",
		os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if err != nil {
		slog.Warn("tracing init failed; running without distributed tracing", "error", err)
	} else {
		defer func() {
			flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := shutdownTracing(flushCtx); err != nil {
				slog.Error("tracing shutdown error", "error", err)
			}
		}()
	}

	database, err := db.NewClient(cfg.DB)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer database.Close()

	if err := db.RunMigrations(cfg.DB); err != nil {
		slog.Error("failed to run migrations", "error", err)
		os.Exit(1)
	}

	producer, err := queue.NewProducer(cfg.Kafka)
	if err != nil {
		slog.Error("failed to create queue producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	webhookProvider := delivery.NewWebhookProvider(cfg.Provider)
	provider := delivery.NewCircuitBreaker(
		webhookProvider,
		cfg.Provider.CircuitBreakerThreshold,
		cfg.Provider.CircuitBreakerReset,
	)

	notifRepo := notification.NewRepository(database)
	notifSvc := notification.NewService(notifRepo, producer, cfg.Worker.MaxRetries)
	notifHandler := notification.NewHandler(notifSvc)

	metricsRepo := metrics.NewRepository(database)
	metricsSvc := metrics.NewService(metricsRepo)
	metricsHandler := metrics.NewHandler(metricsSvc)

	tmplRepo := tmpl.NewRepository(database)
	tmplSvc := tmpl.NewService(tmplRepo)
	tmplHandler := tmpl.NewHandler(tmplSvc)

	hub := ws.NewHub()
	wsHandler := ws.NewHandler(hub)

	rateLimiter := worker.NewChannelRateLimiter(cfg.Worker.RateLimitPerSec)
	w := worker.NewWorker(notifRepo, provider, rateLimiter, cfg.Worker.MaxRetries, producer, hub)
	retryWorker := worker.NewRetryWorker(notifRepo, producer)
	dispatcher := worker.NewDispatcher(cfg.Kafka, cfg.Worker.Concurrency, w, retryWorker)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.HandleMethodNotAllowed = true
	r.Use(gin.Recovery())
	r.Use(maxBodySizeMiddleware(10 << 20))
	r.Use(otelgin.Middleware("notification-system"))
	r.Use(correlationIDMiddleware())
	r.Use(requestLoggerMiddleware())

	r.GET("/health", func(c *gin.Context) {
		apiresponse.OK(c, gin.H{"status": "healthy"})
	})
	r.GET("/ready", func(c *gin.Context) {
		rCtx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		if err := database.PingContext(rCtx); err != nil {
			apiresponse.Error(c, http.StatusServiceUnavailable, "DEPENDENCY_UNAVAILABLE", "database unavailable", err)
			return
		}
		if err := producer.Ping(rCtx); err != nil {
			apiresponse.Error(c, http.StatusServiceUnavailable, "DEPENDENCY_UNAVAILABLE", "kafka unavailable", err)
			return
		}
		apiresponse.OK(c, gin.H{"status": "ready"})
	})

	notifHandler.RegisterRoutes(r)
	metricsHandler.RegisterRoutes(r)
	tmplHandler.RegisterRoutes(r)
	wsHandler.RegisterRoutes(r)

	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	r.NoRoute(func(c *gin.Context) {
		apiresponse.NotFound(c, "route not found")
	})
	r.NoMethod(func(c *gin.Context) {
		apiresponse.Error(c, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed", nil)
	})

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:      r,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	appCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dispatcher.Start(appCtx)

	go func() {
		slog.Info("server starting", "port", cfg.Server.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("graceful shutdown failed", "error", err)
	}

	dispatcher.Wait()
	slog.Info("server stopped")
}

func maxBodySizeMiddleware(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
		c.Next()
	}
}

func correlationIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader("X-Correlation-ID")
		if id == "" {
			id = uuid.New().String()
		}
		c.Set("correlation_id", id)
		c.Header("X-Correlation-ID", id)
		c.Next()
	}
}

func requestLoggerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		slog.Info("request",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"latency_ms", time.Since(start).Milliseconds(),
			"correlation_id", c.GetString("correlation_id"),
		)
	}
}
