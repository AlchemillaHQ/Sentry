package sipstack

import (
	"context"
	"crypto/tls"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlchemillaHQ/Sentry/config"
	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestUpstreamRegistrar_State(t *testing.T) {
	ur := NewUpstreamRegistrar(nil)
	deviceID := "dev-123"
	reg := &UpstreamReg{DeviceID: deviceID, User: "user", Host: "pbx.com"}

	ur.mu.Lock()
	ur.regs[deviceID] = &registrationState{
		reg:        reg,
		registered: true,
		expiresAt:  time.Now().Add(time.Minute),
	}
	ur.mu.Unlock()

	assert.True(t, ur.IsRegistered(deviceID))
	assert.Equal(t, reg, ur.GetReg(deviceID))

	ur.mu.Lock()
	delete(ur.regs, deviceID)
	ur.mu.Unlock()

	assert.False(t, ur.IsRegistered(deviceID))
}

func TestIsRegistered_False(t *testing.T) {
	ur := NewUpstreamRegistrar(nil)
	assert.False(t, ur.IsRegistered("nonexistent"))
}

func TestGetReg_Nil(t *testing.T) {
	ur := NewUpstreamRegistrar(nil)
	assert.Nil(t, ur.GetReg("nonexistent"))
}

func TestNewUpstreamRegistrar(t *testing.T) {
	stack := &Stack{cfg: config.SIPConfig{ExternalIP: "1.2.3.4", ExternalSIPPort: 5060}}
	ur := NewUpstreamRegistrar(stack)
	assert.Same(t, stack, ur.stack)
	assert.NotNil(t, ur.regs)
	assert.Empty(t, ur.regs)
}

func TestBuildRegisterRequest(t *testing.T) {
	ur := NewUpstreamRegistrar(&Stack{cfg: config.SIPConfig{ExternalIP: "10.0.0.1", ExternalSIPPort: 5060}})
	reg := &UpstreamReg{DeviceID: "device-abcdefgh-xxxx", User: "testuser", Host: "pbx.example.com", Port: 5061, Transport: "tls"}
	req := ur.buildRegisterRequest(reg, 600)

	assert.Equal(t, sip.REGISTER, req.Method)
	reqURI := req.Recipient
	assert.Equal(t, "pbx.example.com", reqURI.Host)
	assert.Equal(t, 5061, reqURI.Port)
	assert.True(t, reqURI.UriParams != nil && reqURI.UriParams.Has("transport"))
	from := req.From()
	assert.Equal(t, "testuser", from.Address.User)
	assert.Equal(t, "pbx.example.com", from.Address.Host)
	_, hasTag := from.Params.Get("tag")
	assert.True(t, hasTag)
	to := req.To()
	assert.Equal(t, "testuser", to.Address.User)
	contact := req.Contact()
	assert.Equal(t, "testuser_device-a", contact.Address.User)
	assert.Equal(t, "10.0.0.1", contact.Address.Host)
	assert.Equal(t, 5060, contact.Address.Port)
	assert.NotEmpty(t, req.CallID().Value())
	assert.Equal(t, "600", req.GetHeader("Expires").Value())
	assert.Equal(t, uint32(1), req.CSeq().SeqNo)
	assert.Equal(t, "70", req.GetHeader("Max-Forwards").Value())
}

func TestBuildRegisterRequest_DefaultTransport(t *testing.T) {
	ur := NewUpstreamRegistrar(&Stack{cfg: config.SIPConfig{ExternalIP: "192.168.1.1", ExternalSIPPort: 5062}})
	reg := &UpstreamReg{DeviceID: "dev-1abcdefgh", User: "user1", Host: "sip.example.com", Port: 5060}
	req := ur.buildRegisterRequest(reg, 0)
	assert.False(t, req.Recipient.UriParams != nil && req.Recipient.UriParams.Has("transport"))
	assert.Equal(t, "0", req.GetHeader("Expires").Value())
	assert.Equal(t, "user1_dev-1abc", req.Contact().Address.User)
}

func TestBuildRegisterRequest_ContactUser(t *testing.T) {
	ur := NewUpstreamRegistrar(&Stack{cfg: config.SIPConfig{ExternalIP: "10.0.0.1", ExternalSIPPort: 5060}})
	reg := &UpstreamReg{DeviceID: "abcdefgh", User: "7337", Host: "pbx.com", Port: 5060}
	req := ur.buildRegisterRequest(reg, 600)
	assert.Equal(t, "7337_abcdefgh", req.Contact().Address.User)
}

func TestBuildRegisterRequest_PreservesIdentityAndIncreasesCSeq(t *testing.T) {
	ur := NewUpstreamRegistrar(&Stack{cfg: config.SIPConfig{ExternalIP: "10.0.0.1", ExternalSIPPort: 5060}})
	t.Cleanup(ur.StopAll)
	reg := &UpstreamReg{DeviceID: "device-abcdefgh", User: "7337", Host: "pbx.com", Port: 5060}

	first := ur.buildRegisterRequest(reg, 600)
	second := ur.buildRegisterRequest(reg, 0)

	assert.Equal(t, first.CallID().Value(), second.CallID().Value())
	assert.Equal(t, uint32(1), first.CSeq().SeqNo)
	assert.Equal(t, uint32(2), second.CSeq().SeqNo)
}

func TestStopAll(t *testing.T) {
	ur := NewUpstreamRegistrar(nil)
	var cancelled []string
	ur.regs["d1"] = &registrationState{reg: &UpstreamReg{DeviceID: "d1"}, cancelAttempt: context.CancelFunc(func() { cancelled = append(cancelled, "d1") })}
	ur.regs["d2"] = &registrationState{reg: &UpstreamReg{DeviceID: "d2"}, cancelAttempt: context.CancelFunc(func() { cancelled = append(cancelled, "d2") })}
	ur.regs["d3"] = &registrationState{reg: &UpstreamReg{DeviceID: "d3"}}
	ur.StopAll()
	assert.ElementsMatch(t, []string{"d1", "d2"}, cancelled)
	assert.Empty(t, ur.regs)
}

func TestStackGetters(t *testing.T) {
	ua, _ := sipgo.NewUA(sipgo.WithUserAgent("test"))
	defer ua.Close()
	srv, _ := sipgo.NewServer(ua)
	cli, _ := sipgo.NewClient(ua)
	s := &Stack{cfg: config.SIPConfig{ExternalIP: "1.2.3.4", ExternalSIPPort: 5070, ExternalSIPTransport: "tcp"}, ua: ua, server: srv, client: cli}
	assert.Same(t, ua, s.UA())
	assert.Same(t, srv, s.Server())
	assert.Same(t, cli, s.Client())
}

func TestExternalIP(t *testing.T) {
	s := &Stack{cfg: config.SIPConfig{ExternalIP: "10.0.0.1"}}
	assert.Equal(t, "10.0.0.1", s.ExternalIP())
}

func TestExternalSIPPort(t *testing.T) {
	s := &Stack{cfg: config.SIPConfig{ExternalSIPPort: 5090}}
	assert.Equal(t, 5090, s.ExternalSIPPort())
}

func TestExternalSIPTransport(t *testing.T) {
	s := &Stack{cfg: config.SIPConfig{ExternalSIPTransport: "tls"}}
	assert.Equal(t, "tls", s.ExternalSIPTransport())
}

func TestSetOnRegister(t *testing.T) {
	s := &Stack{cfg: config.SIPConfig{ExternalIP: "10.0.0.1", ExternalSIPPort: 5060}}
	called := false
	s.SetOnRegister(func(req *sip.Request, tx sip.ServerTransaction) { called = true })
	s.onRegister(nil, nil)
	assert.True(t, called)
}

func TestSetOnInvite(t *testing.T) {
	s := &Stack{cfg: config.SIPConfig{ExternalIP: "10.0.0.1", ExternalSIPPort: 5060}}
	called := false
	s.SetOnInvite(func(req *sip.Request, tx sip.ServerTransaction) { called = true })
	s.onInvite(nil, nil)
	assert.True(t, called)
}

func TestSetOnAck(t *testing.T) {
	s := &Stack{cfg: config.SIPConfig{ExternalIP: "10.0.0.1", ExternalSIPPort: 5060}}
	called := false
	s.SetOnAck(func(req *sip.Request, tx sip.ServerTransaction) { called = true })
	s.onAck(nil, nil)
	assert.True(t, called)
}

func TestSetOnBye(t *testing.T) {
	s := &Stack{cfg: config.SIPConfig{ExternalIP: "10.0.0.1", ExternalSIPPort: 5060}}
	called := false
	s.SetOnBye(func(req *sip.Request, tx sip.ServerTransaction) { called = true })
	s.onBye(nil, nil)
	assert.True(t, called)
}

func TestSetOnCancel(t *testing.T) {
	s := &Stack{cfg: config.SIPConfig{ExternalIP: "10.0.0.1", ExternalSIPPort: 5060}}
	called := false
	s.SetOnCancel(func(req *sip.Request, tx sip.ServerTransaction) { called = true })
	s.onCancel(nil, nil)
	assert.True(t, called)
}

type mockServerTx struct {
	mock.Mock
}

func (m *mockServerTx) Origin() *sip.Request                  { return nil }
func (m *mockServerTx) String() string                        { return "mock" }
func (m *mockServerTx) Errors() <-chan error                  { return nil }
func (m *mockServerTx) Done() <-chan struct{}                 { return nil }
func (m *mockServerTx) Terminate()                            {}
func (m *mockServerTx) OnTerminate(fn sip.FnTxTerminate) bool { return false }
func (m *mockServerTx) OnCancel(fn sip.FnTxCancel) bool       { return false }
func (m *mockServerTx) Err() error                            { return nil }
func (m *mockServerTx) Respond(res *sip.Response) error       { return m.Called(res).Error(0) }
func (m *mockServerTx) Acks() <-chan *sip.Request             { return nil }
func (m *mockServerTx) Cancels() <-chan *sip.Request          { return nil }

func testStack() *Stack {
	return &Stack{cfg: config.SIPConfig{ExternalIP: "10.0.0.1", ExternalSIPPort: 5060, ExternalSIPTransport: "udp"}}
}

func TestHandleRegister_WithCallback(t *testing.T) {
	s := testStack()
	var received *sip.Request
	s.onRegister = func(req *sip.Request, tx sip.ServerTransaction) { received = req }
	req := sip.NewRequest(sip.REGISTER, sip.Uri{Host: "test"})
	tx := new(mockServerTx)
	s.handleRegister(req, tx)
	assert.Same(t, req, received)
}

func TestHandleRegister_NoCallback(t *testing.T) {
	s := testStack()
	req := sip.NewRequest(sip.REGISTER, sip.Uri{Host: "test"})
	tx := new(mockServerTx)
	tx.On("Respond", mock.Anything).Return(nil)
	s.handleRegister(req, tx)
	tx.AssertCalled(t, "Respond", mock.Anything)
}

func TestHandleInvite_WithCallback(t *testing.T) {
	s := testStack()
	var received *sip.Request
	s.onInvite = func(req *sip.Request, tx sip.ServerTransaction) { received = req }
	req := sip.NewRequest(sip.INVITE, sip.Uri{Host: "test"})
	tx := new(mockServerTx)
	s.handleInvite(req, tx)
	assert.Same(t, req, received)
}

func TestHandleInvite_NoCallback(t *testing.T) {
	s := testStack()
	req := sip.NewRequest(sip.INVITE, sip.Uri{Host: "test"})
	tx := new(mockServerTx)
	tx.On("Respond", mock.Anything).Return(nil)
	s.handleInvite(req, tx)
	tx.AssertCalled(t, "Respond", mock.Anything)
}

func TestHandleAck_WithCallback(t *testing.T) {
	s := testStack()
	var received *sip.Request
	s.onAck = func(req *sip.Request, tx sip.ServerTransaction) { received = req }
	req := sip.NewRequest(sip.ACK, sip.Uri{Host: "test"})
	tx := new(mockServerTx)
	s.handleAck(req, tx)
	assert.Same(t, req, received)
}

func TestHandleAck_NoCallback(t *testing.T) {
	s := testStack()
	req := sip.NewRequest(sip.ACK, sip.Uri{Host: "test"})
	tx := new(mockServerTx)
	s.handleAck(req, tx)
	tx.AssertNotCalled(t, "Respond")
}

func TestHandleBye_WithCallback(t *testing.T) {
	s := testStack()
	var received *sip.Request
	s.onBye = func(req *sip.Request, tx sip.ServerTransaction) { received = req }
	req := sip.NewRequest(sip.BYE, sip.Uri{Host: "test"})
	tx := new(mockServerTx)
	s.handleBye(req, tx)
	assert.Same(t, req, received)
}

func TestHandleBye_NoCallback(t *testing.T) {
	s := testStack()
	req := sip.NewRequest(sip.BYE, sip.Uri{Host: "test"})
	tx := new(mockServerTx)
	tx.On("Respond", mock.Anything).Return(nil)
	s.handleBye(req, tx)
	tx.AssertCalled(t, "Respond", mock.Anything)
}

func TestHandleCancel_WithCallback(t *testing.T) {
	s := testStack()
	var received *sip.Request
	s.onCancel = func(req *sip.Request, tx sip.ServerTransaction) { received = req }
	req := sip.NewRequest(sip.CANCEL, sip.Uri{Host: "test"})
	tx := new(mockServerTx)
	s.handleCancel(req, tx)
	assert.Same(t, req, received)
}

func TestHandleCancel_NoCallback(t *testing.T) {
	s := testStack()
	req := sip.NewRequest(sip.CANCEL, sip.Uri{Host: "test"})
	tx := new(mockServerTx)
	s.handleCancel(req, tx)
	tx.AssertNotCalled(t, "Respond")
}

func TestStackClose(t *testing.T) {
	ua, _ := sipgo.NewUA(sipgo.WithUserAgent("test"))
	s := &Stack{ua: ua}
	s.Close()
}

type mockSipClient struct {
	mock.Mock
	registerCalls atomic.Int64
	optionCalls   atomic.Int64
}

func (m *mockSipClient) Do(ctx context.Context, req *sip.Request, opts ...sipgo.ClientRequestOption) (*sip.Response, error) {
	if req.Method == sip.REGISTER {
		m.registerCalls.Add(1)
	} else if req.Method == sip.OPTIONS {
		m.optionCalls.Add(1)
	}
	args := m.Called(ctx, req, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*sip.Response), args.Error(1)
}

func (m *mockSipClient) DoDigestAuth(ctx context.Context, req *sip.Request, res *sip.Response, auth sipgo.DigestAuth) (*sip.Response, error) {
	if req.Method == sip.REGISTER {
		m.registerCalls.Add(1)
	}
	args := m.Called(ctx, req, res, auth)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*sip.Response), args.Error(1)
}

func newURWithMock(t *testing.T, stack *Stack) (*UpstreamRegistrar, *mockSipClient) {
	mc := new(mockSipClient)
	ur := newUpstreamRegistrarWithClient(stack, mc)
	t.Cleanup(ur.StopAll)
	return ur, mc
}

func methodCallCount(mc *mockSipClient, method sip.RequestMethod) int {
	switch method {
	case sip.REGISTER:
		return int(mc.registerCalls.Load())
	case sip.OPTIONS:
		return int(mc.optionCalls.Load())
	default:
		return 0
	}
}

func TestSendRegister_Success(t *testing.T) {
	ur, mc := newURWithMock(t, testStack())
	reg := &UpstreamReg{DeviceID: "dev-12345678", User: "user", Host: "pbx.com", Port: 5060}
	mc.On("Do", mock.Anything, mock.Anything, mock.Anything).Return(
		sip.NewResponseFromRequest(sip.NewRequest(sip.REGISTER, sip.Uri{Host: "pbx.com"}), 200, "OK", nil), nil)
	err := ur.sendRegister(context.Background(), reg, 600)
	assert.NoError(t, err)
}

func TestSendRegister_AuthChallenge(t *testing.T) {
	ur, mc := newURWithMock(t, testStack())
	reg := &UpstreamReg{DeviceID: "dev-12345678", User: "user", Host: "pbx.com", Port: 5060, Password: "secret"}
	challenge := sip.NewResponseFromRequest(sip.NewRequest(sip.REGISTER, sip.Uri{Host: "pbx.com"}), 401, "Unauthorized", nil)
	mc.On("Do", mock.Anything, mock.Anything, mock.Anything).Return(challenge, nil)
	mc.On("DoDigestAuth", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		sip.NewResponseFromRequest(sip.NewRequest(sip.REGISTER, sip.Uri{Host: "pbx.com"}), 200, "OK", nil), nil)
	err := ur.sendRegister(context.Background(), reg, 600)
	assert.NoError(t, err)
	mc.AssertCalled(t, "DoDigestAuth", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestSendRegister_DoError(t *testing.T) {
	ur, mc := newURWithMock(t, testStack())
	reg := &UpstreamReg{DeviceID: "dev-12345678", User: "user", Host: "pbx.com", Port: 5060}
	mc.On("Do", mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)
	err := ur.sendRegister(context.Background(), reg, 600)
	assert.Error(t, err)
}

func TestSendRegister_Rejected(t *testing.T) {
	ur, mc := newURWithMock(t, testStack())
	reg := &UpstreamReg{DeviceID: "dev-12345678", User: "user", Host: "pbx.com", Port: 5060}
	mc.On("Do", mock.Anything, mock.Anything, mock.Anything).Return(
		sip.NewResponseFromRequest(sip.NewRequest(sip.REGISTER, sip.Uri{Host: "pbx.com"}), 403, "Forbidden", nil), nil)
	err := ur.sendRegister(context.Background(), reg, 600)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "REGISTER rejected")
}

func TestResponseDelay_ParsesRetryAfterComment(t *testing.T) {
	res := sip.NewResponse(sip.StatusServiceUnavailable, "Unavailable")
	res.AppendHeader(sip.NewHeader("Retry-After", "12 (maintenance);duration=5"))
	assert.Equal(t, 12*time.Second, responseDelay(res, "Retry-After"))
}

func TestRegistrationFailureClassification(t *testing.T) {
	assert.True(t, isPermanentRegistrationStatus(sip.StatusForbidden))
	assert.True(t, isPermanentRegistrationStatus(sip.StatusNotImplemented))
	assert.True(t, isPermanentRegistrationStatus(sip.StatusGlobalDecline))
	assert.False(t, isPermanentRegistrationStatus(sip.StatusServiceUnavailable))
	assert.False(t, isPermanentRegistrationStatus(sip.StatusRequestTimeout))
	assert.True(t, isOverloadStatus(sip.StatusRequestTimeout))
	assert.True(t, isOverloadStatus(sip.StatusServiceUnavailable))
}

func TestSendRegister_DoDigestAuthError(t *testing.T) {
	ur, mc := newURWithMock(t, testStack())
	reg := &UpstreamReg{DeviceID: "dev-12345678", User: "user", Host: "pbx.com", Port: 5060, Password: "secret"}
	challenge := sip.NewResponseFromRequest(sip.NewRequest(sip.REGISTER, sip.Uri{Host: "pbx.com"}), 401, "Unauthorized", nil)
	mc.On("Do", mock.Anything, mock.Anything, mock.Anything).Return(challenge, nil)
	mc.On("DoDigestAuth", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)
	err := ur.sendRegister(context.Background(), reg, 600)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "digest auth")
}

func TestRegister_Success(t *testing.T) {
	ur, mc := newURWithMock(t, testStack())
	reg := &UpstreamReg{DeviceID: "dev-12345678", User: "user", Host: "pbx.com", Port: 5060}
	mc.On("Do", mock.Anything, mock.Anything, mock.Anything).Return(
		sip.NewResponseFromRequest(sip.NewRequest(sip.REGISTER, sip.Uri{Host: "pbx.com"}), 200, "OK", nil), nil)
	err := ur.Register(context.Background(), reg)
	assert.NoError(t, err)
	assert.True(t, ur.IsRegistered("dev-12345678"))
}

func TestRegister_Failure(t *testing.T) {
	ur, mc := newURWithMock(t, testStack())
	reg := &UpstreamReg{DeviceID: "dev-12345678", User: "user", Host: "pbx.com", Port: 5060}
	mc.On("Do", mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)
	err := ur.Register(context.Background(), reg)
	assert.Error(t, err)
	assert.False(t, ur.IsRegistered("dev-12345678"))
	assert.NotNil(t, ur.GetReg("dev-12345678"), "failed enabled registrations remain managed for recovery")
	assert.Equal(t, 1, ur.HealthSummary().PendingRegistrations)
	ur.StopAll()
}

func TestRegister_ReplacesExisting(t *testing.T) {
	ur, mc := newURWithMock(t, testStack())
	mc.On("Do", mock.Anything, mock.Anything, mock.Anything).Return(
		sip.NewResponseFromRequest(sip.NewRequest(sip.REGISTER, sip.Uri{Host: "pbx.com"}), 200, "OK", nil), nil)
	first := &UpstreamReg{DeviceID: "dev-12345678", User: "user", Host: "pbx.com", Port: 5060}
	assert.NoError(t, ur.Register(context.Background(), first))
	second := &UpstreamReg{DeviceID: "dev-12345678", User: "updated", Host: "pbx.com", Port: 5060}
	assert.NoError(t, ur.Register(context.Background(), second))
	assert.Equal(t, "updated", ur.GetReg("dev-12345678").User)
	mc.AssertNumberOfCalls(t, "Do", 2)
	ur.StopAll()
}

func TestUnregister_Success(t *testing.T) {
	ur, mc := newURWithMock(t, testStack())
	reg := &UpstreamReg{DeviceID: "dev-12345678", User: "user", Host: "pbx.com", Port: 5060}
	mc.On("Do", mock.Anything, mock.Anything, mock.Anything).Return(
		sip.NewResponseFromRequest(sip.NewRequest(sip.REGISTER, sip.Uri{Host: "pbx.com"}), 200, "OK", nil), nil)
	assert.NoError(t, ur.Register(context.Background(), reg))
	err := ur.Unregister(context.Background(), "dev-12345678")
	assert.NoError(t, err)
	assert.False(t, ur.IsRegistered("dev-12345678"))
	mc.AssertNumberOfCalls(t, "Do", 2)
}

func TestUnregister_NotFound(t *testing.T) {
	ur, _ := newURWithMock(t, testStack())
	err := ur.Unregister(context.Background(), "nonexistent")
	assert.NoError(t, err)
}

func TestManagedRegistrationRefreshes(t *testing.T) {
	cfg := config.DefaultRegistrarConfig()
	cfg.ProbeEnabled = false
	cfg.ExpiresSeconds = 1
	cfg.RefreshPercent = 50
	cfg.RecoveryInitialRate = 1000
	cfg.RecoveryMaxRate = 1000
	ur, mc := newURWithMock(t, testStack())
	ur.cfg = cfg
	reg := &UpstreamReg{DeviceID: "dev-12345678", User: "user", Host: "pbx.com", Port: 5060}
	mc.On("Do", mock.Anything, mock.Anything, mock.Anything).Return(
		sip.NewResponseFromRequest(sip.NewRequest(sip.REGISTER, sip.Uri{Host: "pbx.com"}), 200, "OK", nil), nil)
	assert.NoError(t, ur.Register(context.Background(), reg))
	assert.Eventually(t, func() bool { return methodCallCount(mc, sip.REGISTER) >= 2 }, 750*time.Millisecond, 10*time.Millisecond)
	ur.StopAll()
}

func TestManagedRegistrationRetriesFailure(t *testing.T) {
	oldDelay := transientRetryDelays[0]
	transientRetryDelays[0] = 20 * time.Millisecond
	defer func() { transientRetryDelays[0] = oldDelay }()
	ur, mc := newURWithMock(t, testStack())
	reg := &UpstreamReg{DeviceID: "dev-12345678", User: "user", Host: "pbx.com", Port: 5060}
	mc.On("Do", mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)
	assert.Error(t, ur.Register(context.Background(), reg))
	assert.Eventually(t, func() bool { return methodCallCount(mc, sip.REGISTER) >= 2 }, 200*time.Millisecond, 5*time.Millisecond)
	ur.StopAll()
}

func TestUnregisterAll(t *testing.T) {
	ur, mc := newURWithMock(t, testStack())
	var cancelled []string
	ur.regs["d1"] = &registrationState{reg: &UpstreamReg{DeviceID: "d1abcdefgh", User: "u1", Host: "h1", Port: 5060}, cancelAttempt: context.CancelFunc(func() { cancelled = append(cancelled, "d1") })}
	ur.regs["d2"] = &registrationState{reg: &UpstreamReg{DeviceID: "d2abcdefgh", User: "u2", Host: "h2", Port: 5060}, cancelAttempt: context.CancelFunc(func() { cancelled = append(cancelled, "d2") })}
	mc.On("Do", mock.Anything, mock.Anything, mock.Anything).Return(
		sip.NewResponseFromRequest(sip.NewRequest(sip.REGISTER, sip.Uri{Host: "test"}), 200, "OK", nil), nil)
	ur.UnregisterAll(context.Background())
	assert.ElementsMatch(t, []string{"d1", "d2"}, cancelled)
	assert.Empty(t, ur.regs)
	mc.AssertNumberOfCalls(t, "Do", 2)
}

func TestUnregisterAll_SendError(t *testing.T) {
	ur, mc := newURWithMock(t, testStack())
	ur.regs["d1"] = &registrationState{reg: &UpstreamReg{DeviceID: "d1abcdefgh", User: "u1", Host: "h1", Port: 5060}}
	mc.On("Do", mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)
	ur.UnregisterAll(context.Background())
	assert.Empty(t, ur.regs)
}

func TestNew(t *testing.T) {
	cfg := config.SIPConfig{ExternalIP: "10.0.0.1", ExternalSIPPort: 5060, ExternalSIPTransport: "udp", UserAgent: "Test/1.0"}
	s, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, s)
	assert.Equal(t, "10.0.0.1", s.ExternalIP())
	assert.Equal(t, 5060, s.ExternalSIPPort())
	assert.NotNil(t, s.ua)
	assert.NotNil(t, s.server)
	assert.NotNil(t, s.client)
	s.Close()
}

func TestNew_WithTLSAndLogSIP(t *testing.T) {
	certPath := "/tmp/test-cert.pem"
	keyPath := "/tmp/test-key.pem"
	if _, err := os.Stat(certPath); os.IsNotExist(err) {
		t.Skip("test cert files not found")
	}
	cfg := config.SIPConfig{ExternalIP: "10.0.0.1", ExternalSIPPort: 5060, ExternalSIPTransport: "tls", UserAgent: "Test/1.0", LogSIP: true, TLSCert: certPath, TLSKey: keyPath}
	s, err := New(cfg)
	assert.NoError(t, err)
	assert.True(t, s.cfg.LogSIP)
	s.Close()
}

func TestNew_TLSCertError(t *testing.T) {
	badCert, _ := os.CreateTemp("", "bad-cert-*.pem")
	badCert.Write([]byte("not a cert"))
	badCert.Close()
	defer os.Remove(badCert.Name())
	badKey, _ := os.CreateTemp("", "bad-key-*.pem")
	badKey.Write([]byte("not a key"))
	badKey.Close()
	defer os.Remove(badKey.Name())
	_, err := New(config.SIPConfig{ExternalIP: "1.2.3.4", TLSCert: badCert.Name(), TLSKey: badKey.Name()})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "load TLS cert")
}

func TestNew_UAError(t *testing.T) {
	oldUA := newUA
	newUA = func(opts ...sipgo.UserAgentOption) (*sipgo.UserAgent, error) { return nil, assert.AnError }
	defer func() { newUA = oldUA }()
	_, err := New(config.SIPConfig{ExternalIP: "1.2.3.4"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create UA")
}

func TestNew_ServerError(t *testing.T) {
	oldUA := newUA
	oldServer := newServer
	defer func() { newUA = oldUA; newServer = oldServer }()
	ua, _ := sipgo.NewUA()
	defer ua.Close()
	newUA = func(opts ...sipgo.UserAgentOption) (*sipgo.UserAgent, error) { return ua, nil }
	newServer = func(ua *sipgo.UserAgent, options ...sipgo.ServerOption) (*sipgo.Server, error) {
		return nil, assert.AnError
	}
	_, err := New(config.SIPConfig{ExternalIP: "1.2.3.4"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create server")
}

func TestNew_ClientError(t *testing.T) {
	oldUA := newUA
	oldServer := newServer
	oldClient := newClient
	defer func() { newUA = oldUA; newServer = oldServer; newClient = oldClient }()
	ua, _ := sipgo.NewUA()
	defer ua.Close()
	srv, _ := sipgo.NewServer(ua)
	newUA = func(opts ...sipgo.UserAgentOption) (*sipgo.UserAgent, error) { return ua, nil }
	newServer = func(ua *sipgo.UserAgent, options ...sipgo.ServerOption) (*sipgo.Server, error) { return srv, nil }
	newClient = func(ua *sipgo.UserAgent, options ...sipgo.ClientOption) (*sipgo.Client, error) {
		return nil, assert.AnError
	}
	_, err := New(config.SIPConfig{ExternalIP: "1.2.3.4"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create client")
}

func TestListenAndServe_UDPError(t *testing.T) {
	oldUDP := serveUDP
	serveUDP = func(s *Stack, errCh chan<- error, ctx context.Context) { errCh <- errors.New("udp error") }
	defer func() { serveUDP = oldUDP }()
	s := &Stack{cfg: config.SIPConfig{UDPAddr: "0.0.0.0:5060"}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := s.ListenAndServe(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "udp error")
}

func TestListenAndServe_TCPError(t *testing.T) {
	oldTCP := serveTCP
	serveTCP = func(s *Stack, errCh chan<- error, ctx context.Context) { errCh <- errors.New("tcp error") }
	defer func() { serveTCP = oldTCP }()
	s := &Stack{cfg: config.SIPConfig{TCPAddr: "0.0.0.0:5060"}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := s.ListenAndServe(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tcp error")
}

func TestListenAndServe_TLSError(t *testing.T) {
	oldTLS := serveTLS
	serveTLS = func(s *Stack, errCh chan<- error, ctx context.Context) { errCh <- errors.New("tls error") }
	defer func() { serveTLS = oldTLS }()
	s := &Stack{cfg: config.SIPConfig{TLSAddr: "0.0.0.0:5061", TLSCert: "/tmp/test-cert.pem", TLSKey: "/tmp/test-key.pem"}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := s.ListenAndServe(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tls error")
}

func TestListenAndServe_ContextCancel(t *testing.T) {
	oldUDP := serveUDP
	serveUDP = func(s *Stack, errCh chan<- error, ctx context.Context) { <-ctx.Done() }
	defer func() { serveUDP = oldUDP }()
	s := &Stack{cfg: config.SIPConfig{UDPAddr: "0.0.0.0:5060"}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.ListenAndServe(ctx) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		assert.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("ListenAndServe did not return after context cancel")
	}
}

func TestListenAndServe_NoAddrs(t *testing.T) {
	s := &Stack{cfg: config.SIPConfig{}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.ListenAndServe(ctx) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		assert.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("ListenAndServe did not return after context cancel")
	}
}

func TestListenAndServe_RealUDP(t *testing.T) {
	oldUDP := serveUDP
	defer func() { serveUDP = oldUDP }()
	ua, _ := sipgo.NewUA(sipgo.WithUserAgent("test"))
	defer ua.Close()
	srv, _ := sipgo.NewServer(ua)
	s := &Stack{cfg: config.SIPConfig{UDPAddr: "127.0.0.1:0"}, server: srv}
	ctx, cancel := context.WithCancel(context.Background())
	go s.ListenAndServe(ctx)
	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)
}

func TestListenAndServe_RealTCP(t *testing.T) {
	oldTCP := serveTCP
	defer func() { serveTCP = oldTCP }()
	ua, _ := sipgo.NewUA(sipgo.WithUserAgent("test"))
	defer ua.Close()
	srv, _ := sipgo.NewServer(ua)
	s := &Stack{cfg: config.SIPConfig{TCPAddr: "127.0.0.1:0"}, server: srv}
	ctx, cancel := context.WithCancel(context.Background())
	go s.ListenAndServe(ctx)
	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)
}

func TestListenAndServe_RealTLS(t *testing.T) {
	certPath := "/tmp/test-cert.pem"
	keyPath := "/tmp/test-key.pem"
	if _, err := os.Stat(certPath); os.IsNotExist(err) {
		t.Skip("test cert files not found")
	}
	oldTLS := serveTLS
	defer func() { serveTLS = oldTLS }()
	ua, _ := sipgo.NewUA(sipgo.WithUserAgent("test"))
	defer ua.Close()
	srv, _ := sipgo.NewServer(ua)
	s := &Stack{cfg: config.SIPConfig{TLSAddr: "127.0.0.1:0", TLSCert: certPath, TLSKey: keyPath}, server: srv, listenTLS: &tls.Config{InsecureSkipVerify: true}}
	ctx, cancel := context.WithCancel(context.Background())
	go s.ListenAndServe(ctx)
	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)
}
