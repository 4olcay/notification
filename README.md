# Event-Driven Notification System

A production-ready notification service that delivers messages across **SMS, Email, and Push** channels with reliable delivery, intelligent retry logic, real-time WebSocket updates, and full observability.

---

## Quick Start

```bash
# Clone and enter the project
cd notification

# Start all services (PostgreSQL, Redpanda, Jaeger, App)
docker compose up --build -d

# Wait ~20s for health checks to pass, then verify
curl http://localhost:8080/health   # {"status":"healthy"}
curl http://localhost:8080/ready    # {"status":"ready"}

# Send a notification
curl -X POST http://localhost:8080/notifications \
  -H "Content-Type: application/json" \
  -d '{"recipient":"+905551234567","channel":"sms","content":"Hello!","priority":"high"}'

# Check Swagger UI
open http://localhost:8080/swagger/index.html

# Check Jaeger traces
open http://localhost:16686

# Check Redpanda console
open http://localhost:8888
```

> **No configuration needed.** The external provider is pre-configured to `https://webhook.site/uuid` which responds with HTTP 202 + accepted JSON.

---

## Architecture

```
                        ┌─────────────────────────────────────────────┐
                        │              REST API (Gin)                  │
                        │  POST /notifications    GET /metrics         │
                        │  POST /notifications/batch                   │
                        │  GET  /notifications/:id  DELETE /…/:id      │
                        │  POST /templates  POST /templates/:n/render  │
                        │  GET  /ws/notifications/:id  (WebSocket)     │
                        └────────────┬───────────────────┬────────────┘
                                     │ publish            │ write state
                                     ▼                    ▼
            ┌────────────────────────────┐   ┌─────────────────────────┐
            │      Redpanda / Kafka      │   │       PostgreSQL 16      │
            │  notifications.high        │   │                          │
            │  notifications.normal      │   │  notifications           │
            │  notifications.low         │   │  notification_batches    │
            │  notifications.dead_letter │   │  notification_templates  │
            └────────────┬───────────────┘   └────────────┬────────────┘
                         │ consume                         │ read/write
                         ▼                                 │
            ┌────────────────────────────┐                 │
            │       Worker Pool          │─────────────────┘
            │  ─────────────────────     │
            │  Rate limiter (token       │
            │  bucket, 100 msg/s/chan)   │
            │  Circuit breaker           │     ┌──────────────────┐
            │  ─────────────────────     │────▶│  webhook.site    │
            │  RetryWorker (5s poll)     │     │  (external mock) │
            │  ScheduledWorker           │     └──────────────────┘
            │  StuckPendingRecovery      │
            └────────────────────────────┘
                         │ status change
                         ▼
            ┌────────────────────────────┐
            │     WebSocket Hub          │
            │  subscribers[notif_id]     │──▶ Browser / Client
            └────────────────────────────┘

            ┌────────────────────────────┐
            │         Jaeger             │  ◀── OTLP traces from every request
            │   http://localhost:16686   │
            └────────────────────────────┘
```

### Tech Stack

| Component | Technology | Reason |
|---|---|---|
| HTTP framework | Gin v1.12 | Fast, binding/validation, middleware chain |
| Database | PostgreSQL 16 | ACID transactions, partial indexes, LISTEN/NOTIFY |
| Message broker | Redpanda | Kafka-compatible, zero-dependency local dev |
| Migrations | golang-migrate | Versioned, reproducible schema |
| Config | Viper | Env-var override, 12-factor |
| Rate limiting | `golang.org/x/time/rate` | Token bucket **per channel** (100 msg/s × 3 channels = 300 msg/s ceiling); channel isolation prevents a burst on one channel from starving others |
| Tracing | OpenTelemetry + Jaeger | Vendor-neutral distributed traces |
| WebSocket | gorilla/websocket | Real-time status push |
| API docs | swaggo/swag | Auto-generated OpenAPI 2.0 |
| CI/CD | GitHub Actions | Build, test, lint, Docker build on every push |

---

## API Reference

### Notifications

#### `POST /notifications` — Create
```bash
curl -X POST http://localhost:8080/notifications \
  -H "Content-Type: application/json" \
  -d '{
    "recipient":       "+905551234567",
    "channel":         "sms",
    "content":         "Flash sale başladı!",
    "priority":        "high",
    "idempotency_key": "order-123-sms",
    "scheduled_at":    "2026-06-01T09:00:00Z"
  }'
```

| Field | Required | Values / Limits |
|---|---|---|
| `recipient` | ✅ | Phone / email address / device token |
| `channel` | ✅ | `sms` · `email` · `push` |
| `content` | ✅ | SMS ≤ 1520 chars · Email ≤ 10 000 chars · Push ≤ 256 chars |
| `priority` | | `high` · `normal` (default) · `low` |
| `idempotency_key` | | 1–255 char unique string |
| `scheduled_at` | | ISO 8601 future timestamp |

**Responses:** `202` new · `200` idempotent duplicate

---

#### `POST /notifications/batch` — Batch create (up to 1000)
```bash
curl -X POST http://localhost:8080/notifications/batch \
  -H "Content-Type: application/json" \
  -d '{
    "notifications": [
      {"recipient": "+905551234567", "channel": "sms",   "content": "Msg 1"},
      {"recipient": "u@example.com", "channel": "email", "content": "Msg 2"},
      {"recipient": "token-xyz",     "channel": "push",  "content": "Msg 3"}
    ]
  }'
```

---

#### `GET /notifications/:id` — Get by ID
```bash
curl http://localhost:8080/notifications/3f2a1b00-...
```

#### `GET /batches/:batch_id` — Batch status with live counts
```bash
curl http://localhost:8080/batches/8e4c2d11-...
```

#### `DELETE /notifications/:id` — Cancel (pending only)
```bash
curl -X DELETE http://localhost:8080/notifications/3f2a1b00-...
# 204 → cancelled  |  409 → not pending  |  404 → not found
```

#### `GET /notifications` — List with filtering + pagination
```bash
curl "http://localhost:8080/notifications?status=delivered&channel=sms&limit=20&offset=0"
# also: from=<unix_ts>&to=<unix_ts>
```

---

### Templates

#### `POST /templates` — Create a template
```bash
curl -X POST http://localhost:8080/templates \
  -H "Content-Type: application/json" \
  -d '{
    "name":    "order_confirmed",
    "channel": "sms",
    "body":    "Merhaba {{name}}, {{order_id}} nolu siparişin onaylandı."
  }'
```

Use `{{variable_name}}` placeholders in `body` (and `subject` for email).

#### `POST /templates/:name/render` — Render with variables
```bash
curl -X POST http://localhost:8080/templates/order_confirmed/render \
  -H "Content-Type: application/json" \
  -d '{"variables": {"name": "Ahmet", "order_id": "ORD-42"}}'

# Response:
# {"body": "Merhaba Ahmet, ORD-42 nolu siparişin onaylandı."}
```

#### `GET /templates` — List all templates
#### `DELETE /templates/:name` — Delete a template

---

### WebSocket — Real-time status updates

```javascript
// Connect to receive status events for a specific notification
const ws = new WebSocket("ws://localhost:8080/ws/notifications/<notification-id>");

ws.onmessage = (e) => {
  const event = JSON.parse(e.data);
  // {"notification_id":"abc-123","status":"delivered","timestamp":"2026-05-24T17:10:05Z"}
  console.log(event.status); // pending → queued → processing → delivered
};
```

Status transitions pushed in real time: `queued` → `processing` → `delivered` / `retrying` / `dead_letter`

---

### Observability

#### `GET /metrics` — Real-time delivery stats (last 24 h)
```json
{
  "queue_depth": { "high": 5, "normal": 120, "low": 30 },
  "channels": {
    "sms":   { "delivered_24h": 8400, "failed_24h": 42,  "success_rate": 0.9950, "avg_latency_ms": 240 },
    "email": { "delivered_24h": 3200, "failed_24h": 16,  "success_rate": 0.9950, "avg_latency_ms": 180 },
    "push":  { "delivered_24h": 12000, "failed_24h": 24, "success_rate": 0.9980, "avg_latency_ms": 95  }
  }
}
```

#### `GET /health` / `GET /ready`
```bash
curl http://localhost:8080/health  # always 200 {"status":"healthy"}
curl http://localhost:8080/ready   # 200 or 503 — checks DB + Kafka liveness
```

#### Distributed Tracing — Jaeger UI: `http://localhost:16686`
Every HTTP request is traced via `otelgin` middleware. Select service `notification` in Jaeger to view spans.

#### Structured Logging
Every request log line includes `correlation_id` (from `X-Correlation-ID` header or auto-generated UUID):
```json
{"time":"...","level":"INFO","msg":"request","method":"POST","path":"/notifications",
 "status":202,"latency_ms":4,"correlation_id":"550e8400-e29b-41d4-..."}
```

---

## Notification Status Flow

```
created ──▶ pending ──▶ queued ──▶ processing ──▶ delivered
                │                       │
                │                       └──▶ retrying (up to max_retries)
                │                                  │
                │                                  └──▶ dead_letter
                │
                └──▶ cancelled (only while still pending)
```

## Retry Backoff

| Attempt | Wait |
|---|---|
| 1st retry | 10 s |
| 2nd retry | 30 s |
| 3rd retry | 1 min |
| 4th retry | 5 min |
| 5th retry | 15 min |
| > max_retries | → `dead_letter` + Kafka DL topic |

---

## Database Migrations

Migrations run automatically on startup via `golang-migrate`.

All migrations have a corresponding `.down.sql` for safe rollback (`migrate down`).

| File | Description |
|---|---|
| `001_create_notifications.up.sql` | `notifications` + `notification_batches` tables, all indexes |
| `002_simplify_batches.up.sql` | Drop stale counter columns from batches (counts computed live via JOIN) |
| `003_templates.up.sql` | `notification_templates` table |
| `004_add_constraints.up.sql` | DB-level CHECK constraints for status / channel / priority enums |
| `005_add_metrics_indexes.up.sql` | Covering indexes for `/metrics` and list queries (channel, status, created_at) |

---

## Running Tests

```bash
# Unit tests (no external dependencies required)
go test ./...

# With race detector
go test -race -count=1 ./...

# Via Makefile
make test
```

All tests are fully in-process with mock stores and publishers — no Docker required.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `SERVER_PORT` | `8080` | HTTP listening port |
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_USER` | `postgres` | DB user |
| `DB_PASSWORD` | `postgres` | DB password |
| `DB_DATABASE` | `notifications_db` | DB name |
| `DB_SSLMODE` | `disable` | SSL mode |
| `KAFKA_BROKERS` | `localhost:19092` | Comma-separated Kafka brokers |
| `KAFKA_TOPIC_HIGH` | `notifications.high` | High-priority topic |
| `KAFKA_TOPIC_NORMAL` | `notifications.normal` | Normal-priority topic |
| `KAFKA_TOPIC_LOW` | `notifications.low` | Low-priority topic |
| `KAFKA_TOPIC_DEAD_LETTER` | `notifications.dead_letter` | Dead-letter topic |
| `KAFKA_GROUP_ID` | `notification-workers` | Consumer group ID |
| `PROVIDER_WEBHOOK_URL` | *(pre-configured)* | External notification provider URL |
| `PROVIDER_TIMEOUT` | `10s` | HTTP timeout for provider calls |
| `PROVIDER_CIRCUIT_BREAKER_THRESHOLD` | `5` | Failures before circuit opens |
| `PROVIDER_CIRCUIT_BREAKER_RESET` | `30s` | How long circuit stays open |
| `WORKER_CONCURRENCY` | `10` | Max concurrent deliveries |
| `WORKER_RATE_LIMIT_PER_SEC` | `100` | Token bucket rate per channel |
| `WORKER_MAX_RETRIES` | `5` | Max delivery attempts before dead-letter |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | Jaeger OTLP endpoint |

---

## Load Testing

```bash
make load-test
```

Runs a k6 script that ramps 50 → 200 → 500 virtual users hitting `POST /notifications`.  
**Thresholds:** p95 < 500 ms · p99 < 1000 ms · error rate < 1 %

---

## Services Summary

| Service | URL | Description |
|---|---|---|
| **API** | http://localhost:8080 | Notification REST API |
| **Swagger UI** | http://localhost:8080/swagger/index.html | Interactive API docs |
| **Jaeger** | http://localhost:16686 | Distributed traces |
| **Redpanda Console** | http://localhost:8888 | Kafka topic browser |
| **PostgreSQL** | localhost:5432 | Direct DB access |
