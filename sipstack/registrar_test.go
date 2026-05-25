package sipstack

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpstreamRegistrar_State(t *testing.T) {
	ur := NewUpstreamRegistrar(nil) // Stack is nil for state tests

	deviceID := "dev-123"
	reg := &UpstreamReg{
		DeviceID: deviceID,
		User:     "user",
		Host:     "pbx.com",
	}

	// Manual insert to test getters without network
	ur.mu.Lock()
	ur.regs[deviceID] = reg
	ur.mu.Unlock()

	assert.True(t, ur.IsRegistered(deviceID))
	assert.Equal(t, reg, ur.GetReg(deviceID))

	ur.mu.Lock()
	delete(ur.regs, deviceID)
	ur.mu.Unlock()

	assert.False(t, ur.IsRegistered(deviceID))
}
