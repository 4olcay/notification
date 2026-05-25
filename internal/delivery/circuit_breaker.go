package delivery

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrCircuitOpen = errors.New("circuit breaker open: provider unavailable")

type cbState int

const (
	cbClosed cbState = iota
	cbOpen
	cbHalfOpen
)

type CircuitBreaker struct {
	mu         sync.Mutex
	inner      Provider
	failures   int
	threshold  int
	state      cbState
	openedAt   time.Time
	resetAfter time.Duration
	probing    bool
}

func NewCircuitBreaker(inner Provider, threshold int, resetAfter time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		inner:      inner,
		threshold:  threshold,
		resetAfter: resetAfter,
	}
}

func (cb *CircuitBreaker) Deliver(ctx context.Context, req Request) (Response, error) {
	cb.mu.Lock()
	switch cb.state {
	case cbOpen:
		if time.Since(cb.openedAt) < cb.resetAfter {
			cb.mu.Unlock()
			return Response{}, ErrCircuitOpen
		}
		if cb.probing {
			cb.mu.Unlock()
			return Response{}, ErrCircuitOpen
		}
		cb.state = cbHalfOpen
		cb.probing = true
	case cbHalfOpen:
		if cb.probing {
			cb.mu.Unlock()
			return Response{}, ErrCircuitOpen
		}
		cb.probing = true
	}
	cb.mu.Unlock()

	resp, err := cb.inner.Deliver(ctx, req)

	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.probing = false

	if err != nil {
		cb.failures++
		if cb.failures >= cb.threshold {
			cb.state = cbOpen
			cb.openedAt = time.Now()
		}
		return Response{}, err
	}

	cb.failures = 0
	cb.state = cbClosed
	return resp, nil
}
