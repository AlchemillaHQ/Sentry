package sipstack

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/AlchemillaHQ/Sentry/config"
	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testSIPReply struct {
	status         int
	reason         string
	err            error
	expires        int
	contactExpires int
	minExpires     int
	retryAfter     int
}

type supervisorTestClient struct {
	mu sync.Mutex

	optionsUp       bool
	registerUp      bool
	registerReplies []testSIPReply
	registerCalls   int
	optionCalls     int
	registerExpires []int
}

type blockingSIPClient struct {
	started chan struct{}
	once    sync.Once
}

func (c *blockingSIPClient) Do(ctx context.Context, _ *sip.Request, _ ...sipgo.ClientRequestOption) (*sip.Response, error) {
	c.once.Do(func() { close(c.started) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func (c *blockingSIPClient) DoDigestAuth(ctx context.Context, req *sip.Request, _ *sip.Response, _ sipgo.DigestAuth) (*sip.Response, error) {
	return c.Do(ctx, req)
}

func newSupervisorTestClient() *supervisorTestClient {
	return &supervisorTestClient{optionsUp: true, registerUp: true}
}

func (c *supervisorTestClient) Do(_ context.Context, req *sip.Request, _ ...sipgo.ClientRequestOption) (*sip.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if req.Method == sip.OPTIONS {
		c.optionCalls++
		if !c.optionsUp {
			return nil, errors.New("gateway unavailable")
		}
		return sip.NewResponse(sip.StatusOK, "OK"), nil
	}

	c.registerCalls++
	if !c.registerUp {
		return nil, errors.New("registration transport unavailable")
	}
	if header := req.GetHeader("Expires"); header != nil {
		if expires, err := strconv.Atoi(header.Value()); err == nil {
			c.registerExpires = append(c.registerExpires, expires)
		}
	}
	reply := testSIPReply{status: sip.StatusOK, reason: "OK"}
	if len(c.registerReplies) > 0 {
		reply = c.registerReplies[0]
		c.registerReplies = c.registerReplies[1:]
	}
	return buildTestResponse(reply)
}

func (c *supervisorTestClient) DoDigestAuth(ctx context.Context, req *sip.Request, _ *sip.Response, _ sipgo.DigestAuth) (*sip.Response, error) {
	return c.Do(ctx, req)
}

func buildTestResponse(reply testSIPReply) (*sip.Response, error) {
	if reply.err != nil {
		return nil, reply.err
	}
	if reply.status == 0 {
		reply.status = sip.StatusOK
	}
	res := sip.NewResponse(reply.status, reply.reason)
	if reply.expires > 0 {
		res.AppendHeader(sip.NewHeader("Expires", strconv.Itoa(reply.expires)))
	}
	if reply.contactExpires > 0 {
		params := sip.NewParams()
		params.Add("expires", strconv.Itoa(reply.contactExpires))
		res.AppendHeader(&sip.ContactHeader{
			Address: sip.Uri{User: "contact", Host: "example.com"},
			Params:  params,
		})
	}
	if reply.minExpires > 0 {
		res.AppendHeader(sip.NewHeader("Min-Expires", strconv.Itoa(reply.minExpires)))
	}
	if reply.retryAfter > 0 {
		res.AppendHeader(sip.NewHeader("Retry-After", strconv.Itoa(reply.retryAfter)))
	}
	return res, nil
}

func (c *supervisorTestClient) setOptionsUp(up bool) {
	c.mu.Lock()
	c.optionsUp = up
	c.mu.Unlock()
}

func (c *supervisorTestClient) setRegisterUp(up bool) {
	c.mu.Lock()
	c.registerUp = up
	c.mu.Unlock()
}

func (c *supervisorTestClient) counts() (registers, options int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.registerCalls, c.optionCalls
}

func (c *supervisorTestClient) expiries() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int(nil), c.registerExpires...)
}

func aggressiveTestRegistrarConfig() config.RegistrarConfig {
	cfg := config.DefaultRegistrarConfig()
	cfg.ProbeEnabled = true
	cfg.ProbeIntervalMilliseconds = 20
	cfg.ProbeTimeoutMilliseconds = 10
	cfg.ProbeFailureThreshold = 2
	cfg.DownProbeIntervalMillis = 10
	cfg.RegisterCanaryIntervalMillis = 20
	cfg.RecoveryWorkersPerGateway = 4
	cfg.RecoveryInitialRate = 1000
	cfg.RecoveryMaxRate = 1000
	cfg.RecoveryGlobalWorkers = 8
	cfg.RecoveryGlobalMaxRate = 2000
	return cfg
}

func TestGatewaySupervisorSharesHealthAndRecoversMembers(t *testing.T) {
	client := newSupervisorTestClient()
	ur := newUpstreamRegistrarWithClientConfig(testStack(), client, aggressiveTestRegistrarConfig())
	t.Cleanup(ur.StopAll)

	first := &UpstreamReg{DeviceID: "device-11111111", User: "1001", Host: "pbx.example.com", Port: 5060, Transport: "udp"}
	second := &UpstreamReg{DeviceID: "device-22222222", User: "1002", Host: "PBX.EXAMPLE.COM.", Port: 5060, Transport: "UDP"}
	require.NoError(t, ur.Register(context.Background(), first))
	require.NoError(t, ur.Register(context.Background(), second))
	require.Eventually(t, func() bool {
		_, options := client.counts()
		return options >= 1
	}, time.Second, 5*time.Millisecond)

	summary := ur.HealthSummary()
	assert.Equal(t, 1, summary.Gateways)
	assert.Equal(t, 2, summary.HealthyRegistrations)

	client.setOptionsUp(false)
	require.Eventually(t, func() bool {
		return ur.HealthSummary().UnavailableGateways == 1
	}, time.Second, 5*time.Millisecond)
	assert.False(t, ur.IsRegistered(first.DeviceID))
	assert.False(t, ur.IsRegistered(second.DeviceID))

	registersBeforeRecovery, _ := client.counts()
	client.setOptionsUp(true)
	require.Eventually(t, func() bool {
		health := ur.HealthSummary()
		registers, _ := client.counts()
		return health.UnavailableGateways == 0 && health.HealthyRegistrations == 2 && registers >= registersBeforeRecovery+2
	}, time.Second, 5*time.Millisecond)
}

func TestManageQueuesWithoutWaitingForUpstream(t *testing.T) {
	client := &blockingSIPClient{started: make(chan struct{})}
	cfg := config.DefaultRegistrarConfig()
	cfg.ProbeEnabled = false
	ur := newUpstreamRegistrarWithClientConfig(testStack(), client, cfg)

	started := time.Now()
	err := ur.Manage(&UpstreamReg{
		DeviceID: "device-11111111",
		User:     "1001",
		Host:     "pbx.example.com",
		Port:     5060,
	})
	require.NoError(t, err)
	assert.Less(t, time.Since(started), 100*time.Millisecond)
	select {
	case <-client.started:
	case <-time.After(time.Second):
		t.Fatal("managed registration was not dispatched")
	}
	ur.StopAll()
}

func TestUnvalidatedOptionsUsesOneSharedCanaryWithoutFanout(t *testing.T) {
	client := newSupervisorTestClient()
	client.setOptionsUp(false)
	cfg := aggressiveTestRegistrarConfig()
	cfg.RegisterCanaryIntervalMillis = 500
	ur := newUpstreamRegistrarWithClientConfig(testStack(), client, cfg)
	t.Cleanup(ur.StopAll)

	first := &UpstreamReg{DeviceID: "device-11111111", User: "1001", Host: "pbx.example.com", Port: 5060}
	second := &UpstreamReg{DeviceID: "device-22222222", User: "1002", Host: "pbx.example.com", Port: 5060}
	require.NoError(t, ur.Register(context.Background(), first))
	require.NoError(t, ur.Register(context.Background(), second))
	require.Eventually(t, func() bool {
		registers, options := client.counts()
		health := ur.HealthSummary()
		ur.mu.RLock()
		var usingCanary bool
		for _, gateway := range ur.gateways {
			usingCanary = gateway.probeUnsupported
		}
		ur.mu.RUnlock()
		return options >= 2 && registers == 3 && usingCanary && health.SuspectGateways == 0
	}, time.Second, 5*time.Millisecond)

	// Resolving a merely-suspect gateway must not re-register every member.
	time.Sleep(100 * time.Millisecond)
	registers, _ := client.counts()
	assert.Equal(t, 3, registers)
	assert.True(t, ur.IsRegistered(first.DeviceID))
	assert.True(t, ur.IsRegistered(second.DeviceID))
	assert.Equal(t, 1, ur.HealthSummary().CanaryGateways)
	assert.Equal(t, 0, ur.HealthSummary().UnavailableGateways)
}

func TestRejectedCanaryStillProvesGatewayReachability(t *testing.T) {
	client := newSupervisorTestClient()
	client.setOptionsUp(false)
	client.registerReplies = []testSIPReply{
		{status: sip.StatusOK, reason: "OK"},
		{status: sip.StatusForbidden, reason: "Forbidden"},
	}
	cfg := aggressiveTestRegistrarConfig()
	cfg.RegisterCanaryIntervalMillis = 500
	ur := newUpstreamRegistrarWithClientConfig(testStack(), client, cfg)
	t.Cleanup(ur.StopAll)

	reg := &UpstreamReg{DeviceID: "device-11111111", User: "1001", Host: "pbx.example.com", Port: 5060}
	require.NoError(t, ur.Register(context.Background(), reg))
	require.Eventually(t, func() bool {
		registers, options := client.counts()
		health := ur.HealthSummary()
		return registers == 2 && options >= 2 && health.CanaryGateways == 1 && health.SuspectGateways == 0
	}, time.Second, 5*time.Millisecond)

	assert.True(t, ur.IsRegistered(reg.DeviceID))
	assert.Equal(t, 0, ur.HealthSummary().UnavailableGateways)
}

func TestUnvalidatedGatewayOpensSharedCircuitAndRecoversThroughCanary(t *testing.T) {
	client := newSupervisorTestClient()
	client.setOptionsUp(false)
	client.setRegisterUp(false)
	cfg := aggressiveTestRegistrarConfig()
	cfg.RegisterCanaryIntervalMillis = 20
	ur := newUpstreamRegistrarWithClientConfig(testStack(), client, cfg)
	t.Cleanup(ur.StopAll)

	const registrations = 20
	for index := 0; index < registrations; index++ {
		require.NoError(t, ur.Manage(&UpstreamReg{
			DeviceID: "device-" + strconv.Itoa(index),
			User:     "10" + strconv.Itoa(index),
			Host:     "pbx.example.com",
			Port:     5060,
		}))
	}

	require.Eventually(t, func() bool {
		ur.mu.RLock()
		defer ur.mu.RUnlock()
		down := false
		for _, gateway := range ur.gateways {
			down = !gateway.reachable
		}
		if !down {
			return false
		}
		for _, state := range ur.regs {
			if state.inFlight || state.timer != nil {
				return false
			}
		}
		return true
	}, 2*time.Second, 5*time.Millisecond)
	registersAtDown, _ := client.counts()
	time.Sleep(80 * time.Millisecond)
	registersStillDown, _ := client.counts()

	// Only the gateway probe loop may send while the circuit is open. The
	// number of attempts is therefore time-bound, not account-count-bound.
	assert.Greater(t, registersStillDown, registersAtDown)
	assert.LessOrEqual(t, registersStillDown-registersAtDown, 12)
	assert.Equal(t, registrations, ur.HealthSummary().PendingRegistrations)

	client.setRegisterUp(true)
	require.Eventually(t, func() bool {
		health := ur.HealthSummary()
		return health.UnavailableGateways == 0 && health.HealthyRegistrations == registrations
	}, 2*time.Second, 5*time.Millisecond)
}

func TestPreviouslyValidatedOptionsFallsBackToCanary(t *testing.T) {
	client := newSupervisorTestClient()
	cfg := aggressiveTestRegistrarConfig()
	cfg.RegisterCanaryIntervalMillis = 500
	ur := newUpstreamRegistrarWithClientConfig(testStack(), client, cfg)
	t.Cleanup(ur.StopAll)

	reg := &UpstreamReg{DeviceID: "device-11111111", User: "1001", Host: "pbx.example.com", Port: 5060}
	require.NoError(t, ur.Register(context.Background(), reg))
	require.Eventually(t, func() bool {
		ur.mu.RLock()
		defer ur.mu.RUnlock()
		for _, gateway := range ur.gateways {
			if gateway.probeValidated {
				return true
			}
		}
		return false
	}, time.Second, 5*time.Millisecond)

	client.setOptionsUp(false)
	require.Eventually(t, func() bool {
		ur.mu.RLock()
		usingCanary := false
		for _, gateway := range ur.gateways {
			if gateway.probeUnsupported && gateway.reachable {
				usingCanary = true
			}
		}
		ur.mu.RUnlock()
		return usingCanary && ur.IsRegistered(reg.DeviceID)
	}, 2*time.Second, 5*time.Millisecond)
	assert.True(t, ur.IsRegistered(reg.DeviceID))
}

func TestRegisterUsesNegotiatedContactExpiry(t *testing.T) {
	client := newSupervisorTestClient()
	client.registerReplies = []testSIPReply{{
		status:         sip.StatusOK,
		reason:         "OK",
		expires:        300,
		contactExpires: 120,
	}}
	ur := newUpstreamRegistrarWithClientConfig(testStack(), client, aggressiveTestRegistrarConfig())
	t.Cleanup(ur.StopAll)

	outcome := ur.performRegister(context.Background(), &UpstreamReg{
		DeviceID: "device-11111111",
		User:     "1001",
		Host:     "pbx.example.com",
		Port:     5060,
	}, 600)
	require.NoError(t, outcome.err)
	assert.Equal(t, 120*time.Second, outcome.expires)
}

func TestRegisterRetriesWithMinimumExpiry(t *testing.T) {
	client := newSupervisorTestClient()
	client.registerReplies = []testSIPReply{
		{status: sip.StatusIntervalToBrief, reason: "Interval Too Brief", minExpires: 900},
		{status: sip.StatusOK, reason: "OK", expires: 900},
	}
	ur := newUpstreamRegistrarWithClientConfig(testStack(), client, aggressiveTestRegistrarConfig())
	t.Cleanup(ur.StopAll)

	outcome := ur.performRegister(context.Background(), &UpstreamReg{
		DeviceID: "device-11111111",
		User:     "1001",
		Host:     "pbx.example.com",
		Port:     5060,
	}, 600)
	require.NoError(t, outcome.err)
	assert.Equal(t, 900*time.Second, outcome.expires)
	assert.Equal(t, []int{600, 900}, client.expiries())
}

func TestGatewayOverloadAppliesBackpressure(t *testing.T) {
	cfg := aggressiveTestRegistrarConfig()
	cfg.RecoveryInitialRate = 100
	cfg.RecoveryMaxRate = 400
	client := newSupervisorTestClient()
	ur := newUpstreamRegistrarWithClientConfig(testStack(), client, cfg)
	t.Cleanup(ur.StopAll)

	reg := &UpstreamReg{DeviceID: "device-11111111", User: "1001", Host: "pbx.example.com", Port: 5060}
	ur.mu.Lock()
	gateway := ur.getOrCreateGatewayLocked(gatewayKeyFor(reg), reg)
	gateway.currentRate = 200
	gateway.learnedRate = 200
	gateway.limiter.SetLimitAt(time.Now(), 200)
	ur.noteGatewayOverloadLocked(gateway, 3*time.Second)
	currentRate := gateway.currentRate
	pauseUntil := gateway.pauseUntil
	ur.mu.Unlock()

	assert.Equal(t, float64(100), currentRate)
	assert.WithinDuration(t, time.Now().Add(3*time.Second), pauseUntil, 200*time.Millisecond)
}

func TestGatewayLatencySpikeAppliesBackpressure(t *testing.T) {
	cfg := aggressiveTestRegistrarConfig()
	cfg.RecoveryInitialRate = 100
	cfg.RecoveryMaxRate = 400
	client := newSupervisorTestClient()
	ur := newUpstreamRegistrarWithClientConfig(testStack(), client, cfg)
	t.Cleanup(ur.StopAll)

	reg := &UpstreamReg{DeviceID: "device-11111111", User: "1001", Host: "pbx.example.com", Port: 5060}
	ur.mu.Lock()
	gateway := ur.getOrCreateGatewayLocked(gatewayKeyFor(reg), reg)
	gateway.currentRate = 200
	gateway.learnedRate = 200
	gateway.registerRTT = 100 * time.Millisecond
	gateway.limiter.SetLimitAt(time.Now(), 200)
	gateway.workers <- struct{}{}
	gateway.workers <- struct{}{}
	ur.noteRegistrationSuccessLocked(gateway, 3*time.Second)
	currentRate := gateway.currentRate
	<-gateway.workers
	<-gateway.workers
	ur.mu.Unlock()

	assert.Equal(t, float64(100), currentRate)
}

func TestRegistrationHealthExpiresWithoutARefresh(t *testing.T) {
	ur := NewUpstreamRegistrar(nil)
	t.Cleanup(ur.StopAll)
	state := &registrationState{
		reg:        &UpstreamReg{DeviceID: "device-11111111"},
		registered: true,
		expiresAt:  time.Now().Add(-time.Second),
	}
	ur.mu.Lock()
	ur.regs[state.reg.DeviceID] = state
	ur.mu.Unlock()

	assert.False(t, ur.IsRegistered(state.reg.DeviceID))
	assert.Equal(t, 0, ur.HealthSummary().HealthyRegistrations)
}
