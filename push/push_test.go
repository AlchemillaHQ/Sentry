package push

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDispatcher_SendAsync(t *testing.T) {
	d := &Dispatcher{
		queue:       make(chan pushRequest, 10),
		workers:     0,
		cancelFuncs: make(map[string]context.CancelFunc),
	}

	ctx := context.Background()
	err := d.Send(ctx, "android", "token", "call-1", "sip:caller", "Name")
	assert.NoError(t, err)

	assert.Equal(t, 1, len(d.queue))
	req := <-d.queue
	assert.Equal(t, "call-1", req.callID)
	assert.Equal(t, "android", req.platform)
}

func TestDispatcher_CancelPush(t *testing.T) {
	d := &Dispatcher{
		queue:       make(chan pushRequest, 10),
		workers:     0,
		cancelFuncs: make(map[string]context.CancelFunc),
	}

	called := false
	d.cancelMu.Lock()
	_, cancel := context.WithCancel(context.Background())
	d.cancelFuncs["call-1"] = func() {
		cancel()
		called = true
	}
	d.cancelMu.Unlock()

	d.CancelPush("call-1")

	d.cancelMu.Lock()
	_, exists := d.cancelFuncs["call-1"]
	d.cancelMu.Unlock()

	assert.True(t, called)
	assert.False(t, exists)
}

func TestDispatcher_CancelPush_NotFound(t *testing.T) {
	d := &Dispatcher{
		cancelFuncs: make(map[string]context.CancelFunc),
	}

	d.CancelPush("nonexistent")
}

func TestDispatcher_OnDeadToken(t *testing.T) {
	d := &Dispatcher{
		queue:       make(chan pushRequest, 10),
		workers:     0,
		cancelFuncs: make(map[string]context.CancelFunc),
	}

	var mu sync.Mutex
	var calls [][3]string

	d.OnDeadToken(func(platform, token, callID string) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, [3]string{platform, token, callID})
	})

	assert.NotNil(t, d.deadTokenHandler)

	d.deadTokenHandler("android", "dead-token", "call-1")
	d.deadTokenHandler("ios", "bad-token", "call-2")

	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, calls, 2)
	assert.Equal(t, [3]string{"android", "dead-token", "call-1"}, calls[0])
	assert.Equal(t, [3]string{"ios", "bad-token", "call-2"}, calls[1])
}

func TestDispatcher_SendQueueFull(t *testing.T) {
	d := &Dispatcher{
		queue:       make(chan pushRequest, 1),
		workers:     0,
		cancelFuncs: make(map[string]context.CancelFunc),
	}

	ctx := context.Background()

	err := d.Send(ctx, "android", "token", "call-1", "sip:caller", "Name")
	assert.NoError(t, err)

	err = d.Send(ctx, "android", "token", "call-2", "sip:caller", "Name")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "queue full")
}

func TestDispatcher_CancelPush_StopsRetryLoop(t *testing.T) {
	d := &Dispatcher{
		queue:       make(chan pushRequest, 10),
		workers:     0,
		cancelFuncs: make(map[string]context.CancelFunc),
	}

	var wg sync.WaitGroup
	wg.Add(1)

	start := time.Now()
	go func() {
		defer wg.Done()
		d.sendWithRetry(pushRequest{
			platform:  "android",
			token:     "token",
			callID:    "call-cancel",
			callerURI: "sip:caller",
		})
	}()

	time.Sleep(50 * time.Millisecond)
	d.CancelPush("call-cancel")

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		elapsed := time.Since(start)
		assert.Less(t, elapsed, 1*time.Second,
			"retry loop should stop quickly after CancelPush, but took %v", elapsed)
	case <-time.After(3 * time.Second):
		t.Fatal("retry loop did not stop within 3s after CancelPush")
	}

	d.cancelMu.Lock()
	_, exists := d.cancelFuncs["call-cancel"]
	d.cancelMu.Unlock()

	assert.False(t, exists, "cancel func should be cleaned up after retry loop exits")
}
