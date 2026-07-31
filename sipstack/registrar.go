package sipstack

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/AlchemillaHQ/Sentry/config"
	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

var (
	errRegistrarStopped     = errors.New("upstream registrar stopped")
	errRegistrationReplaced = errors.New("upstream registration replaced")
	errRegistrationRemoved  = errors.New("upstream registration removed")
)

type UpstreamReg struct {
	DeviceID  string
	User      string
	Host      string
	Port      int
	Transport string
	Password  string
	Realm     string

	identity *registrationIdentity
}

type registrationIdentity struct {
	mu     sync.Mutex
	callID string
	cseq   uint32
}

func newRegistrationIdentity() *registrationIdentity {
	return &registrationIdentity{callID: uuid.New().String()}
}

func (identity *registrationIdentity) next() (string, uint32) {
	identity.mu.Lock()
	defer identity.mu.Unlock()
	identity.cseq++
	return identity.callID, identity.cseq
}

func (identity *registrationIdentity) skip(count uint32) {
	identity.mu.Lock()
	identity.cseq += count
	identity.mu.Unlock()
}

type Registrar interface {
	Register(ctx context.Context, reg *UpstreamReg) error
	Manage(reg *UpstreamReg) error
	Unregister(ctx context.Context, deviceID string) error
	IsRegistered(deviceID string) bool
	GetReg(deviceID string) *UpstreamReg
	StopAll()
	UnregisterAll(ctx context.Context)
}

// RegistrarHealthReporter is optional so lightweight test and alternate
// implementations of Registrar do not need to expose operational state.
type RegistrarHealthReporter interface {
	HealthSummary() RegistrarHealthSummary
}

type RegistrarHealthSummary struct {
	ManagedRegistrations int `json:"managed_registrations"`
	HealthyRegistrations int `json:"healthy_registrations"`
	PendingRegistrations int `json:"pending_registrations"`
	Gateways             int `json:"gateways"`
	CanaryGateways       int `json:"canary_gateways"`
	SuspectGateways      int `json:"suspect_gateways"`
	UnavailableGateways  int `json:"unavailable_gateways"`
}

type sipClient interface {
	Do(ctx context.Context, req *sip.Request, opts ...sipgo.ClientRequestOption) (*sip.Response, error)
	DoDigestAuth(ctx context.Context, req *sip.Request, res *sip.Response, auth sipgo.DigestAuth) (*sip.Response, error)
}

type UpstreamRegistrar struct {
	stack  *Stack
	client sipClient
	cfg    config.RegistrarConfig

	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.RWMutex
	regs     map[string]*registrationState
	gateways map[string]*gatewayState
	closed   bool
	recovery *recoveryController
}

var _ Registrar = (*UpstreamRegistrar)(nil)
var _ RegistrarHealthReporter = (*UpstreamRegistrar)(nil)

func NewUpstreamRegistrar(stack *Stack, configs ...config.RegistrarConfig) *UpstreamRegistrar {
	cfg := config.DefaultRegistrarConfig()
	if len(configs) > 0 {
		cfg = configs[0].WithDefaults()
	}

	var client sipClient
	if stack != nil {
		client = stack.Client()
	}
	return newUpstreamRegistrar(stack, client, cfg)
}

func newUpstreamRegistrar(stack *Stack, client sipClient, cfg config.RegistrarConfig) *UpstreamRegistrar {
	ctx, cancel := context.WithCancel(context.Background())
	cfg = cfg.WithDefaults()
	ur := &UpstreamRegistrar{
		stack:    stack,
		client:   client,
		cfg:      cfg,
		ctx:      ctx,
		cancel:   cancel,
		regs:     make(map[string]*registrationState),
		gateways: make(map[string]*gatewayState),
	}
	ur.recovery = newRecoveryController(cfg)
	return ur
}

// Test helpers use no active probes unless a test explicitly opts into them.
func newUpstreamRegistrarWithClient(stack *Stack, client sipClient) *UpstreamRegistrar {
	cfg := config.DefaultRegistrarConfig()
	cfg.ProbeEnabled = false
	return newUpstreamRegistrar(stack, client, cfg)
}

func newUpstreamRegistrarWithClientConfig(stack *Stack, client sipClient, cfg config.RegistrarConfig) *UpstreamRegistrar {
	return newUpstreamRegistrar(stack, client, cfg)
}

func (ur *UpstreamRegistrar) Register(ctx context.Context, reg *UpstreamReg) error {
	waiter := make(chan error, 1)
	if err := ur.manageRegistration(reg, waiter); err != nil {
		return err
	}

	select {
	case err := <-waiter:
		return err
	case <-ctx.Done():
		ur.removeWaiter(reg.DeviceID, waiter)
		// Desired registration state remains managed. This lets startup and
		// transient API failures recover without waiting for another client call.
		return ctx.Err()
	}
}

func (ur *UpstreamRegistrar) removeWaiter(deviceID string, waiter chan error) {
	ur.mu.Lock()
	defer ur.mu.Unlock()
	state, ok := ur.regs[deviceID]
	if !ok {
		return
	}
	for index, current := range state.waiters {
		if current != waiter {
			continue
		}
		state.waiters = append(state.waiters[:index], state.waiters[index+1:]...)
		return
	}
}

// Manage installs desired registration state and returns after it has been
// queued. It is used for bulk reconciliation where waiting on every upstream
// transaction would prevent later accounts from entering supervision.
func (ur *UpstreamRegistrar) Manage(reg *UpstreamReg) error {
	return ur.manageRegistration(reg, nil)
}

func (ur *UpstreamRegistrar) manageRegistration(reg *UpstreamReg, waiter chan error) error {
	if reg == nil {
		return errors.New("upstream registration is nil")
	}
	if reg.DeviceID == "" || reg.User == "" || reg.Host == "" {
		return errors.New("upstream registration requires device, user, and host")
	}
	if ur.client == nil {
		return errors.New("upstream SIP client is unavailable")
	}

	regCopy := *reg
	if regCopy.Port == 0 {
		regCopy.Port = 5060
	}
	regCopy.Transport = normalizeTransport(regCopy.Transport)

	key := gatewayKeyFor(&regCopy)

	ur.mu.Lock()
	if ur.closed {
		ur.mu.Unlock()
		return errRegistrarStopped
	}

	generation := uint64(1)
	var identity *registrationIdentity
	if existing, ok := ur.regs[regCopy.DeviceID]; ok {
		generation = existing.generation + 1
		if sameRegistrationBinding(existing.reg, &regCopy) {
			identity = existing.reg.identity
		}
		ur.removeRegistrationLocked(existing, errRegistrationReplaced)
	}
	if identity == nil {
		identity = newRegistrationIdentity()
	}
	regCopy.identity = identity

	gateway := ur.getOrCreateGatewayLocked(key, &regCopy)
	state := &registrationState{
		reg:        &regCopy,
		generation: generation,
		gateway:    gateway,
	}
	if waiter != nil {
		state.waiters = []chan error{waiter}
	}
	ur.regs[regCopy.DeviceID] = state
	gateway.members[regCopy.DeviceID] = struct{}{}
	ur.enqueueRegistrationLocked(state)
	ur.mu.Unlock()
	return nil
}

func (ur *UpstreamRegistrar) Unregister(ctx context.Context, deviceID string) error {
	ur.mu.Lock()
	state, ok := ur.regs[deviceID]
	if !ok {
		ur.mu.Unlock()
		return nil
	}
	reg := *state.reg
	ur.removeRegistrationLocked(state, errRegistrationRemoved)
	ur.mu.Unlock()

	return ur.sendRegister(ctx, &reg, 0)
}

func (ur *UpstreamRegistrar) IsRegistered(deviceID string) bool {
	now := time.Now()
	ur.mu.RLock()
	defer ur.mu.RUnlock()
	state, ok := ur.regs[deviceID]
	return ok && state.registered && state.expiresAt.After(now)
}

func (ur *UpstreamRegistrar) GetReg(deviceID string) *UpstreamReg {
	ur.mu.RLock()
	defer ur.mu.RUnlock()
	state, ok := ur.regs[deviceID]
	if !ok {
		return nil
	}
	reg := *state.reg
	return &reg
}

func (ur *UpstreamRegistrar) HealthSummary() RegistrarHealthSummary {
	now := time.Now()
	ur.mu.RLock()
	defer ur.mu.RUnlock()

	summary := RegistrarHealthSummary{
		ManagedRegistrations: len(ur.regs),
		Gateways:             len(ur.gateways),
	}
	for _, state := range ur.regs {
		if state.registered && state.expiresAt.After(now) {
			summary.HealthyRegistrations++
		}
		if state.queued || state.inFlight || state.retryAttempts > 0 {
			summary.PendingRegistrations++
		}
	}
	for _, gateway := range ur.gateways {
		if gateway.probeUnsupported {
			summary.CanaryGateways++
		}
		if gateway.suspect {
			summary.SuspectGateways++
		}
		if !gateway.reachable {
			summary.UnavailableGateways++
		}
	}
	return summary
}

func (ur *UpstreamRegistrar) StopAll() {
	ur.mu.Lock()
	if ur.closed {
		ur.mu.Unlock()
		return
	}
	ur.closed = true
	for _, state := range ur.regs {
		ur.stopRegistrationLocked(state, errRegistrarStopped)
	}
	ur.regs = make(map[string]*registrationState)
	for _, gateway := range ur.gateways {
		gateway.cancel()
	}
	ur.gateways = make(map[string]*gatewayState)
	ur.mu.Unlock()
	ur.cancel()
}

func (ur *UpstreamRegistrar) UnregisterAll(ctx context.Context) {
	ur.mu.Lock()
	if ur.closed && len(ur.regs) == 0 {
		ur.mu.Unlock()
		return
	}
	ur.closed = true
	regs := make([]*UpstreamReg, 0, len(ur.regs))
	for _, state := range ur.regs {
		reg := *state.reg
		regs = append(regs, &reg)
		ur.stopRegistrationLocked(state, errRegistrarStopped)
	}
	ur.regs = make(map[string]*registrationState)
	for _, gateway := range ur.gateways {
		gateway.cancel()
	}
	ur.gateways = make(map[string]*gatewayState)
	ur.mu.Unlock()
	ur.cancel()

	const maxConcurrent = 50
	sem := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup
	for _, reg := range regs {
		wg.Add(1)
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Done()
			continue
		}
		go func(r *UpstreamReg) {
			defer func() { <-sem }()
			defer wg.Done()
			unregisterCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := ur.sendRegister(unregisterCtx, r, 0); err != nil {
				log.Error().Err(err).Str("device", r.DeviceID).Msg("unregister on shutdown failed")
			}
		}(reg)
	}
	wg.Wait()
	log.Info().Int("count", len(regs)).Msg("all upstream registrations cancelled")
}

func (ur *UpstreamRegistrar) stopRegistrationLocked(state *registrationState, waiterErr error) {
	if state.timer != nil {
		state.timer.Stop()
		state.timer = nil
	}
	if state.cancelAttempt != nil {
		state.cancelAttempt()
		state.cancelAttempt = nil
	}
	for _, waiter := range state.waiters {
		select {
		case waiter <- waiterErr:
		default:
		}
	}
	state.waiters = nil
}

func (ur *UpstreamRegistrar) removeRegistrationLocked(state *registrationState, waiterErr error) {
	ur.stopRegistrationLocked(state, waiterErr)
	delete(ur.regs, state.reg.DeviceID)
	delete(state.gateway.members, state.reg.DeviceID)
	if len(state.gateway.members) == 0 {
		delete(ur.gateways, state.gateway.key)
		state.gateway.cancel()
	}
}

func normalizeTransport(transport string) string {
	transport = strings.ToLower(strings.TrimSpace(transport))
	if transport == "" {
		return "udp"
	}
	return transport
}

func sameRegistrationBinding(left, right *UpstreamReg) bool {
	return left.User == right.User && gatewayKeyFor(left) == gatewayKeyFor(right)
}

func (reg *UpstreamReg) nextRegistrationSequence() (string, uint32) {
	if reg.identity == nil {
		reg.identity = newRegistrationIdentity()
	}
	return reg.identity.next()
}

func (ur *UpstreamRegistrar) buildRegisterRequest(reg *UpstreamReg, expires int) *sip.Request {
	target := sip.Uri{Host: reg.Host, Port: reg.Port}
	transport := normalizeTransport(reg.Transport)
	if transport != "udp" {
		target.UriParams = sip.NewParams()
		target.UriParams.Add("transport", transport)
	}

	req := sip.NewRequest(sip.REGISTER, target)
	from := &sip.FromHeader{Address: sip.Uri{User: reg.User, Host: reg.Host}}
	from.Params.Add("tag", sip.GenerateTagN(16))
	req.AppendHeader(from)
	req.AppendHeader(&sip.ToHeader{Address: sip.Uri{User: reg.User, Host: reg.Host}})

	contactHost := "localhost"
	contactPort := 5060
	if ur.stack != nil {
		contactHost = ur.stack.ExternalIP()
		contactPort = ur.stack.ExternalSIPPort()
	}
	deviceSuffix := reg.DeviceID
	if len(deviceSuffix) > 8 {
		deviceSuffix = deviceSuffix[:8]
	}
	b2buaSIPUser := fmt.Sprintf("%s_%s", reg.User, deviceSuffix)
	contactAddr := sip.Uri{User: b2buaSIPUser, Host: contactHost, Port: contactPort}
	if transport != "udp" {
		contactAddr.UriParams = sip.NewParams()
		contactAddr.UriParams.Add("transport", transport)
	}
	req.AppendHeader(&sip.ContactHeader{Address: contactAddr})

	expiresHeader := sip.ExpiresHeader(expires)
	req.AppendHeader(&expiresHeader)
	callIDValue, cseq := reg.nextRegistrationSequence()
	req.AppendHeader(&sip.CSeqHeader{SeqNo: cseq, MethodName: sip.REGISTER})
	callID := sip.CallIDHeader(callIDValue)
	req.AppendHeader(&callID)
	maxForwards := sip.MaxForwardsHeader(70)
	req.AppendHeader(&maxForwards)
	req.SetBody(nil)
	return req
}

type registrationAttempt struct {
	err              error
	statusCode       int
	expires          time.Duration
	retryAfter       time.Duration
	latency          time.Duration
	gatewayReachable bool
	overloaded       bool
	permanent        bool
}

func (ur *UpstreamRegistrar) sendRegister(ctx context.Context, reg *UpstreamReg, expires int) error {
	return ur.performRegister(ctx, reg, expires).err
}

func (ur *UpstreamRegistrar) performRegister(ctx context.Context, reg *UpstreamReg, expires int) registrationAttempt {
	requestedExpires := expires
	for attempt := 0; attempt < 2; attempt++ {
		if err := ctx.Err(); err != nil {
			return registrationAttempt{err: err}
		}
		outcome, minExpires := ur.performRegisterOnce(ctx, reg, requestedExpires)
		if outcome.statusCode != sip.StatusIntervalToBrief || requestedExpires == 0 || minExpires <= requestedExpires {
			return outcome
		}
		requestedExpires = minExpires
		log.Info().
			Str("device", reg.DeviceID).
			Int("min_expires", minExpires).
			Msg("upstream requested a longer registration expiry; retrying")
	}
	return registrationAttempt{err: errors.New("upstream registration retry exhausted")}
}

func (ur *UpstreamRegistrar) performRegisterOnce(ctx context.Context, reg *UpstreamReg, expires int) (registrationAttempt, int) {
	req := ur.buildRegisterRequest(reg, expires)
	log.Debug().
		Str("device", reg.DeviceID).
		Str("target", fmt.Sprintf("%s:%d", reg.Host, reg.Port)).
		Str("transport", normalizeTransport(reg.Transport)).
		Str("user", reg.User).
		Int("expires", expires).
		Msg("sending REGISTER")

	res, err := ur.client.Do(ctx, req)
	if err != nil {
		return registrationAttempt{err: fmt.Errorf("send REGISTER: %w", err)}, 0
	}
	if res == nil {
		return registrationAttempt{err: errors.New("send REGISTER: empty response")}, 0
	}

	outcome := registrationAttempt{statusCode: res.StatusCode, gatewayReachable: true}
	if res.StatusCode == sip.StatusUnauthorized || res.StatusCode == sip.StatusProxyAuthRequired {
		if err := ctx.Err(); err != nil {
			outcome.err = err
			return outcome, 0
		}
		authReq := ur.buildRegisterRequest(reg, expires)
		// sipgo increments the request CSeq once more while applying digest
		// authentication. Reserve that value so the next refresh remains higher.
		reg.identity.skip(1)
		res, err = ur.client.DoDigestAuth(ctx, authReq, res, sipgo.DigestAuth{
			Username: reg.User,
			Password: reg.Password,
		})
		if err != nil {
			outcome.err = fmt.Errorf("digest auth REGISTER: %w", err)
			return outcome, 0
		}
		if res == nil {
			outcome.err = errors.New("digest auth REGISTER: empty response")
			return outcome, 0
		}
		outcome.statusCode = res.StatusCode
	}

	if res.StatusCode >= 200 && res.StatusCode < 300 {
		if expires > 0 {
			negotiated := negotiatedExpirySeconds(res, expires)
			if negotiated <= 0 {
				outcome.err = errors.New("REGISTER succeeded with an invalid expiry")
				return outcome, 0
			}
			outcome.expires = time.Duration(negotiated) * time.Second
		}
		log.Debug().
			Str("device", reg.DeviceID).
			Str("user", reg.User).
			Str("host", reg.Host).
			Int("expires", int(outcome.expires/time.Second)).
			Msg("upstream registration successful")
		return outcome, 0
	}

	outcome.retryAfter = responseDelay(res, "Retry-After")
	outcome.overloaded = isOverloadStatus(res.StatusCode)
	outcome.permanent = isPermanentRegistrationStatus(res.StatusCode)
	outcome.err = fmt.Errorf("REGISTER rejected: %d %s", res.StatusCode, res.Reason)
	if res.StatusCode == sip.StatusIntervalToBrief {
		return outcome, responseSeconds(res, "Min-Expires")
	}
	return outcome, 0
}

func negotiatedExpirySeconds(res *sip.Response, fallback int) int {
	if contact := res.Contact(); contact != nil && contact.Params != nil {
		if value, ok := contact.Params.Get("expires"); ok {
			if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil {
				return seconds
			}
		}
	}
	if seconds := responseSeconds(res, "Expires"); seconds >= 0 {
		return seconds
	}
	return fallback
}

func responseSeconds(res *sip.Response, name string) int {
	header := res.GetHeader(name)
	if header == nil {
		return -1
	}
	value := strings.TrimSpace(header.Value())
	end := 0
	for end < len(value) && value[end] >= '0' && value[end] <= '9' {
		end++
	}
	if end == 0 {
		return -1
	}
	seconds, err := strconv.Atoi(value[:end])
	if err != nil || seconds < 0 {
		return -1
	}
	return seconds
}

func responseDelay(res *sip.Response, name string) time.Duration {
	seconds := responseSeconds(res, name)
	if seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func isOverloadStatus(status int) bool {
	switch status {
	case sip.StatusRequestTimeout, 429, sip.StatusInternalServerError, sip.StatusBadGateway, sip.StatusServiceUnavailable, sip.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func isPermanentRegistrationStatus(status int) bool {
	switch status {
	case sip.StatusNotImplemented, sip.StatusVersionNotSupported, sip.StatusMessageTooLarge:
		return true
	}
	if (status >= 300 && status < 400) || status >= 600 {
		return true
	}
	if status < 400 || status >= 500 {
		return false
	}
	switch status {
	case sip.StatusRequestTimeout, sip.StatusConflict, sip.StatusIntervalToBrief, sip.StatusTemporarilyUnavailable, 429:
		return false
	default:
		return true
	}
}
