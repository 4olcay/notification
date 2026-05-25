FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/server ./cmd/server/main.go

FROM alpine:3.19

RUN apk add --no-cache ca-certificates wget

WORKDIR /app

COPY --from=builder /app/server .
COPY --from=builder /app/db/migrations ./db/migrations

EXPOSE 8080

ENTRYPOINT ["./server"]
