package api

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
)

type ipLimiter struct {
	limiter          *rate.Limiter
	lastSeenUnixNano atomic.Int64
}

func (il *ipLimiter) touch(now time.Time) {
	il.lastSeenUnixNano.Store(now.UnixNano())
}

func (il *ipLimiter) lastSeen() time.Time {
	return time.Unix(0, il.lastSeenUnixNano.Load())
}

type IPRateLimiter struct {
	ips             sync.Map
	rate            rate.Limit
	burst           int
	stopCh          chan struct{}
	cleanupInterval time.Duration
}

func NewIPRateLimiter(r float64, burst int) *IPRateLimiter {
	return newIPRateLimiter(r, burst, rateLimitCleanupInterval)
}

func newIPRateLimiter(r float64, burst int, cleanupInterval time.Duration) *IPRateLimiter {
	rl := &IPRateLimiter{
		rate:            rate.Limit(r),
		burst:           burst,
		stopCh:          make(chan struct{}),
		cleanupInterval: cleanupInterval,
	}
	go rl.cleanupLoop()
	return rl
}

func (rl *IPRateLimiter) GetLimiter(ip string) *rate.Limiter {
	if v, ok := rl.ips.Load(ip); ok {
		il := v.(*ipLimiter)
		il.touch(time.Now())
		return il.limiter
	}

	limiter := rate.NewLimiter(rl.rate, rl.burst)
	il := &ipLimiter{limiter: limiter}
	il.touch(time.Now())
	actual, loaded := rl.ips.LoadOrStore(ip, il)
	actualLimiter := actual.(*ipLimiter)
	if loaded {
		actualLimiter.touch(time.Now())
	}
	return actualLimiter.limiter
}

const rateLimitCleanupInterval = 10 * time.Minute

func (rl *IPRateLimiter) cleanupLoop() {
	ticker := time.NewTicker(rl.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rl.purgeStale()
		case <-rl.stopCh:
			return
		}
	}
}

func (rl *IPRateLimiter) purgeStale() {
	now := time.Now()
	rl.ips.Range(func(key, value any) bool {
		il := value.(*ipLimiter)
		if now.Sub(il.lastSeen()) > 30*time.Minute {
			rl.ips.Delete(key)
		}
		return true
	})
}

func (rl *IPRateLimiter) Stop() {
	close(rl.stopCh)
}

func RateLimitMiddleware(rl *IPRateLimiter) gin.HandlerFunc {
	return func(c *gin.Context) {
		ip := c.ClientIP()
		limiter := rl.GetLimiter(ip)
		if !limiter.Allow() {
			c.JSON(http.StatusTooManyRequests, gin.H{
				"status":  "error",
				"message": "rate limit exceeded, try again later",
			})
			c.Abort()
			return
		}
		c.Next()
	}
}
