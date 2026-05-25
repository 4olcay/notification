package worker

import (
	"context"
	"math"
	"sync"

	"golang.org/x/time/rate"
)

type ChannelRateLimiter struct {
	limiters map[string]*rate.Limiter
	mu       sync.RWMutex
	rps      float64
}

func NewChannelRateLimiter(rps float64) *ChannelRateLimiter {
	return &ChannelRateLimiter{
		limiters: make(map[string]*rate.Limiter),
		rps:      rps,
	}
}

func (c *ChannelRateLimiter) Wait(ctx context.Context, channel string) error {
	return c.limiterFor(channel).Wait(ctx)
}

func (c *ChannelRateLimiter) limiterFor(channel string) *rate.Limiter {
	c.mu.RLock()
	l, ok := c.limiters[channel]
	c.mu.RUnlock()
	if ok {
		return l
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if l, ok = c.limiters[channel]; ok {
		return l
	}

	burst := int(math.Ceil(c.rps))
	if burst < 1 {
		burst = 1
	}
	l = rate.NewLimiter(rate.Limit(c.rps), burst)
	c.limiters[channel] = l
	return l
}
