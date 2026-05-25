package push

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDispatcher_SendAsync(t *testing.T) {
	d := &Dispatcher{
		queue:   make(chan pushRequest, 10),
		workers: 0, // Don't start workers, we want to inspect the queue
	}

	ctx := context.Background()
	err := d.Send(ctx, "android", "token", "call-1", "sip:caller", "Name")
	assert.NoError(t, err)

	assert.Equal(t, 1, len(d.queue))
	req := <-d.queue
	assert.Equal(t, "call-1", req.callID)
	assert.Equal(t, "android", req.platform)
}
