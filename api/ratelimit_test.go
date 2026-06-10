package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func TestIPRateLimiter_AllowsUnderLimit(t *testing.T) {
	rl := NewIPRateLimiter(10, 5) // 10 req/s, burst 5
	defer rl.Stop()

	limiter := rl.GetLimiter("192.168.1.1")
	for i := 0; i < 5; i++ {
		assert.True(t, limiter.Allow(), "request %d should be allowed", i)
	}
}

func TestIPRateLimiter_RejectsOverBurst(t *testing.T) {
	rl := NewIPRateLimiter(1, 2) // 1 req/s, burst 2
	defer rl.Stop()

	limiter := rl.GetLimiter("10.0.0.1")
	assert.True(t, limiter.Allow())
	assert.True(t, limiter.Allow())
	assert.False(t, limiter.Allow(), "should reject after burst exhausted")
}

func TestIPRateLimiter_DifferentIPsAreIndependent(t *testing.T) {
	rl := NewIPRateLimiter(1, 1) // 1 req/s, burst 1
	defer rl.Stop()

	l1 := rl.GetLimiter("1.1.1.1")
	l2 := rl.GetLimiter("2.2.2.2")

	assert.True(t, l1.Allow())
	assert.True(t, l2.Allow()) // different IP, should be independent
	assert.False(t, l1.Allow())
	assert.False(t, l2.Allow())
}

func TestIPRateLimiter_SameIPReturnsSameLimiter(t *testing.T) {
	rl := NewIPRateLimiter(10, 5)
	defer rl.Stop()

	l1 := rl.GetLimiter("10.0.0.1")
	l2 := rl.GetLimiter("10.0.0.1")
	assert.Same(t, l1, l2, "same IP should return same limiter instance")
}

func TestRateLimitMiddleware_AllowsUnderLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rl := NewIPRateLimiter(100, 10)
	defer rl.Stop()

	router := gin.New()
	router.Use(RateLimitMiddleware(rl))
	router.GET("/test", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/test", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestRateLimitMiddleware_RejectsOverLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rl := NewIPRateLimiter(0.001, 1) // extremely low rate, burst 1
	defer rl.Stop()

	router := gin.New()
	router.Use(RateLimitMiddleware(rl))
	router.GET("/test", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// First request should pass
	w1 := httptest.NewRecorder()
	req1, _ := http.NewRequest("GET", "/test", nil)
	router.ServeHTTP(w1, req1)
	assert.Equal(t, http.StatusOK, w1.Code)

	// Second request should be rate limited
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest("GET", "/test", nil)
	router.ServeHTTP(w2, req2)
	assert.Equal(t, http.StatusTooManyRequests, w2.Code)

	var resp map[string]interface{}
	json.Unmarshal(w2.Body.Bytes(), &resp)
	assert.Equal(t, "error", resp["status"])
	assert.Contains(t, resp["message"], "rate limit")
}

func TestRateLimitMiddleware_ReturnsCorrectJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rl := NewIPRateLimiter(0.001, 1)
	defer rl.Stop()

	router := gin.New()
	router.Use(RateLimitMiddleware(rl))
	router.GET("/test", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// Exhaust burst
	w1 := httptest.NewRecorder()
	req1, _ := http.NewRequest("GET", "/test", nil)
	router.ServeHTTP(w1, req1)

	// This should be rate limited
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest("GET", "/test", nil)
	router.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusTooManyRequests, w2.Code)
	assert.Equal(t, "application/json; charset=utf-8", w2.Header().Get("Content-Type"))

	var resp map[string]interface{}
	err := json.Unmarshal(w2.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, "error", resp["status"])
}

func TestPurgeStale(t *testing.T) {
	rl := &IPRateLimiter{
		rate:   1,
		burst:  5,
		stopCh: make(chan struct{}),
	}

	rl.GetLimiter("10.0.0.1")
	rl.GetLimiter("10.0.0.2")
	rl.GetLimiter("10.0.0.3")

	// Simulate old entries by backdating lastSeen
	rl.ips.Range(func(key, value any) bool {
		il := value.(*ipLimiter)
		il.lastSeen = time.Now().Add(-40 * time.Minute)
		return true
	})
	// Make one entry recent
	if v, ok := rl.ips.Load("10.0.0.2"); ok {
		il := v.(*ipLimiter)
		il.lastSeen = time.Now()
	}

	rl.purgeStale()

	_, exists1 := rl.ips.Load("10.0.0.1")
	_, exists2 := rl.ips.Load("10.0.0.2")
	_, exists3 := rl.ips.Load("10.0.0.3")

	assert.False(t, exists1, "stale entry should be purged")
	assert.True(t, exists2, "recent entry should be kept")
	assert.False(t, exists3, "stale entry should be purged")
}

func TestCleanupLoop_PurgesStaleEntries(t *testing.T) {
	oldInterval := rateLimitCleanupInterval
	rateLimitCleanupInterval = 50 * time.Millisecond
	defer func() { rateLimitCleanupInterval = oldInterval }()

	rl := NewIPRateLimiter(10, 5)
	defer rl.Stop()

	// Get a limiter and backdate it
	rl.GetLimiter("10.0.0.1")
	rl.ips.Range(func(key, value any) bool {
		il := value.(*ipLimiter)
		il.lastSeen = time.Now().Add(-40 * time.Minute)
		return true
	})

	// Wait for cleanupLoop to fire
	time.Sleep(100 * time.Millisecond)

	_, exists := rl.ips.Load("10.0.0.1")
	assert.False(t, exists, "stale entry should be purged by cleanup loop")
}
