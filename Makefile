.PHONY: build run clean docker-up docker-down docker-logs load-test \
        test test-health test-ready test-create test-create-idempotent \
        test-batch test-list test-metrics test-cancel \
        test-template-create test-template-render test-template-list \
        test-ws-connect swagger

build:
	go build -o bin/server ./cmd/server/main.go

run:
	go run ./cmd/server/main.go

clean:
	rm -rf bin/
	rm -f loadtest/results.json

test:
	go test ./... -v -count=1

docker-up:
	WEBHOOK_URL=$(WEBHOOK_URL) docker compose up --build -d

docker-down:
	docker compose down -v

docker-logs:
	docker compose logs -f app

load-test:
	docker compose --profile test up k6

test-health:
	@curl -s http://localhost:8080/health | jq .

test-ready:
	@curl -s http://localhost:8080/ready | jq .

test-create:
	@curl -s -X POST http://localhost:8080/notifications \
		-H "Content-Type: application/json" \
		-d '{"recipient":"+905551234567","channel":"sms","content":"Hello from Insider!","priority":"high"}' | jq .

test-create-idempotent:
	@curl -s -X POST http://localhost:8080/notifications \
		-H "Content-Type: application/json" \
		-d '{"recipient":"+905551234567","channel":"sms","content":"Idempotent message","priority":"normal","idempotency_key":"test-idem-001"}' | jq .

test-batch:
	@curl -s -X POST http://localhost:8080/notifications/batch \
		-H "Content-Type: application/json" \
		-d '{"notifications":[{"recipient":"+905551234567","channel":"sms","content":"Batch msg 1","priority":"normal"},{"recipient":"user@example.com","channel":"email","content":"Batch email","priority":"low"},{"recipient":"device-token-xyz","channel":"push","content":"Push!","priority":"high"}]}' | jq .

test-cancel:
	@echo "Usage: make test-cancel ID=<notification-uuid>"
	@curl -s -X DELETE http://localhost:8080/notifications/$(ID) | jq .

test-list:
	@curl -s "http://localhost:8080/notifications?limit=5" | jq .

test-metrics:
	@curl -s http://localhost:8080/metrics | jq .

test-template-create:
	@curl -s -X POST http://localhost:8080/templates \
		-H "Content-Type: application/json" \
		-d '{"name":"welcome_sms","channel":"sms","body":"Merhaba {{name}}, siparişin {{order_id}} onaylandı."}' | jq .

test-template-render:
	@curl -s -X POST http://localhost:8080/templates/welcome_sms/render \
		-H "Content-Type: application/json" \
		-d '{"variables":{"name":"Ahmet","order_id":"ORD-42"}}' | jq .

test-template-list:
	@curl -s http://localhost:8080/templates | jq .

test-ws-connect:
	@echo "Usage: make test-ws-connect ID=<notification-uuid>"
	@wscat -c ws://localhost:8080/ws/notifications/$(ID)

swagger:
	swag init -g cmd/server/main.go -o docs
