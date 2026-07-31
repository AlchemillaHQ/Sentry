package sipstack

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/AlchemillaHQ/Sentry/config"
	"github.com/emiago/sipgo/sip"
	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"
)

const (
	probeConfirmationDelay = 100 * time.Millisecond
	permanentRetryDelay    = 5 * time.Minute
)

var transientRetryDelays = [...]time.Duration{
	500 * time.Millisecond,
	time.Second,
	2 * time.Second,
	5 * time.Second,
	10 * time.Second,
	30 * time.Second,
}

type registrationState struct {
	reg           *UpstreamReg
	generation    uint64
	gateway       *gatewayState
	registered    bool
	expiresAt     time.Time
	lastSuccess   time.Time
	lastError     string
	retryAttempts int
	queued        bool
	inFlight      bool
	timer         *time.Timer
	cancelAttempt context.CancelFunc
	waiters       []chan error
}

type registrationTask struct {
	deviceID   string
	generation uint64
}

type gatewayState struct {
	key       string
	host      string
	port      int
	transport string

	ctx    context.Context
	cancel context.CancelFunc
	wake   chan struct{}
	probe  chan struct{}

	members map[string]struct{}
	pending []registrationTask
	head    int

	reachable      bool
	suspect        bool
	downSince      time.Time
	pauseUntil     time.Time
	probeFailures  int
	probeValidated bool
	probeWarned    bool
	lastProbeAt    time.Time
	lastProbeRTT   time.Duration
	lastSIPAt      time.Time
	registerRTT    time.Duration

	workers chan struct{}
	limiter *rate.Limiter

	currentRate          float64
	learnedRate          float64
	successesSinceAdjust int
	lastRateDecrease     time.Time
}

type recoveryController struct {
	workers      chan struct{}
	limiter      *rate.Limiter
	probeWorkers chan struct{}
	probeLimiter *rate.Limiter
}

func newRecoveryController(cfg config.RegistrarConfig) *recoveryController {
	return &recoveryController{
		workers:      make(chan struct{}, cfg.RecoveryGlobalWorkers),
		limiter:      rate.NewLimiter(rate.Limit(cfg.RecoveryGlobalMaxRate), 1),
		probeWorkers: make(chan struct{}, cfg.ProbeGlobalWorkers),
		probeLimiter: rate.NewLimiter(rate.Limit(cfg.ProbeGlobalMaxRate), 1),
	}
}

func gatewayKeyFor(reg *UpstreamReg) string {
	host := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(reg.Host), "."))
	return fmt.Sprintf("%s|%s|%d", normalizeTransport(reg.Transport), host, reg.Port)
}

func (ur *UpstreamRegistrar) getOrCreateGatewayLocked(key string, reg *UpstreamReg) *gatewayState {
	if gateway, ok := ur.gateways[key]; ok {
		return gateway
	}

	ctx, cancel := context.WithCancel(ur.ctx)
	gateway := &gatewayState{
		key:         key,
		host:        reg.Host,
		port:        reg.Port,
		transport:   normalizeTransport(reg.Transport),
		ctx:         ctx,
		cancel:      cancel,
		wake:        make(chan struct{}, 1),
		probe:       make(chan struct{}, 1),
		members:     make(map[string]struct{}),
		reachable:   true,
		workers:     make(chan struct{}, ur.cfg.RecoveryWorkersPerGateway),
		currentRate: ur.cfg.RecoveryInitialRate,
		learnedRate: ur.cfg.RecoveryInitialRate,
	}
	gateway.limiter = rate.NewLimiter(rate.Limit(gateway.currentRate), 1)
	ur.gateways[key] = gateway

	go ur.dispatchGateway(gateway)
	if ur.cfg.ProbeEnabled && ur.client != nil {
		go ur.probeGateway(gateway)
	}

	log.Info().
		Str("gateway", key).
		Float64("initial_rate", gateway.currentRate).
		Int("workers", ur.cfg.RecoveryWorkersPerGateway).
		Msg("upstream gateway supervisor started")
	return gateway
}

func (ur *UpstreamRegistrar) enqueueRegistrationLocked(state *registrationState) {
	if ur.closed || state.queued || state.inFlight {
		return
	}
	state.queued = true
	state.gateway.pending = append(state.gateway.pending, registrationTask{
		deviceID:   state.reg.DeviceID,
		generation: state.generation,
	})
	signal(state.gateway.wake)
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (ur *UpstreamRegistrar) dispatchGateway(gateway *gatewayState) {
	for {
		if !ur.waitForGatewayWork(gateway) {
			return
		}
		if err := gateway.limiter.Wait(gateway.ctx); err != nil {
			return
		}
		if err := ur.recovery.limiter.Wait(gateway.ctx); err != nil {
			return
		}

		select {
		case gateway.workers <- struct{}{}:
		case <-gateway.ctx.Done():
			return
		}
		select {
		case ur.recovery.workers <- struct{}{}:
		case <-gateway.ctx.Done():
			<-gateway.workers
			return
		}

		state, attemptCtx := ur.takeGatewayWork(gateway)
		if state == nil {
			<-ur.recovery.workers
			<-gateway.workers
			continue
		}

		go func(registration *registrationState, ctx context.Context) {
			defer func() {
				<-ur.recovery.workers
				<-gateway.workers
				signal(gateway.wake)
			}()
			started := time.Now()
			outcome := ur.performRegister(ctx, registration.reg, ur.cfg.ExpiresSeconds)
			outcome.latency = time.Since(started)
			ur.completeRegistration(gateway, registration, outcome)
		}(state, attemptCtx)
	}
}

func (ur *UpstreamRegistrar) waitForGatewayWork(gateway *gatewayState) bool {
	for {
		ur.mu.RLock()
		ready := !ur.closed && gateway.reachable && !gateway.suspect && gateway.head < len(gateway.pending)
		pause := time.Until(gateway.pauseUntil)
		ur.mu.RUnlock()

		if ready && pause <= 0 {
			return true
		}

		if ready && pause > 0 {
			timer := time.NewTimer(pause)
			select {
			case <-timer.C:
				continue
			case <-gateway.wake:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				continue
			case <-gateway.ctx.Done():
				timer.Stop()
				return false
			}
		}

		select {
		case <-gateway.wake:
		case <-gateway.ctx.Done():
			return false
		}
	}
}

func (ur *UpstreamRegistrar) takeGatewayWork(gateway *gatewayState) (*registrationState, context.Context) {
	ur.mu.Lock()
	defer ur.mu.Unlock()

	if ur.closed || !gateway.reachable || gateway.suspect || time.Now().Before(gateway.pauseUntil) {
		return nil, nil
	}

	for gateway.head < len(gateway.pending) {
		task := gateway.pending[gateway.head]
		gateway.head++
		state, ok := ur.regs[task.deviceID]
		if !ok || state.gateway != gateway || state.generation != task.generation || !state.queued {
			continue
		}
		state.queued = false
		if state.inFlight {
			continue
		}
		state.inFlight = true
		attemptCtx, cancel := context.WithTimeout(gateway.ctx, time.Duration(ur.cfg.AttemptTimeoutSeconds)*time.Second)
		state.cancelAttempt = cancel
		ur.compactGatewayQueueLocked(gateway)
		return state, attemptCtx
	}
	ur.compactGatewayQueueLocked(gateway)
	return nil, nil
}

func (ur *UpstreamRegistrar) compactGatewayQueueLocked(gateway *gatewayState) {
	if gateway.head == len(gateway.pending) {
		gateway.pending = gateway.pending[:0]
		gateway.head = 0
		return
	}
	if gateway.head >= 1024 && gateway.head*2 >= len(gateway.pending) {
		copy(gateway.pending, gateway.pending[gateway.head:])
		gateway.pending = gateway.pending[:len(gateway.pending)-gateway.head]
		gateway.head = 0
	}
}

func (ur *UpstreamRegistrar) completeRegistration(gateway *gatewayState, state *registrationState, outcome registrationAttempt) {
	now := time.Now()
	ur.mu.Lock()
	current, ok := ur.regs[state.reg.DeviceID]
	if !ok || current != state || state.gateway != gateway {
		ur.mu.Unlock()
		return
	}
	if state.cancelAttempt != nil {
		state.cancelAttempt()
		state.cancelAttempt = nil
	}
	state.inFlight = false

	if outcome.gatewayReachable {
		ur.markGatewayReachableLocked(gateway, state.reg.DeviceID, now)
	}

	for _, waiter := range state.waiters {
		select {
		case waiter <- outcome.err:
		default:
		}
	}
	state.waiters = nil

	if outcome.err == nil {
		state.registered = true
		state.expiresAt = now.Add(outcome.expires)
		state.lastSuccess = now
		state.lastError = ""
		state.retryAttempts = 0
		ur.noteRegistrationSuccessLocked(gateway, outcome.latency)
		ur.scheduleRefreshLocked(state, outcome.expires)
		ur.mu.Unlock()
		return
	}

	state.lastError = outcome.err.Error()
	state.retryAttempts++
	if !state.expiresAt.After(now) {
		state.registered = false
	}

	if outcome.overloaded {
		ur.noteGatewayOverloadLocked(gateway, outcome.retryAfter)
	}
	if !outcome.gatewayReachable {
		ur.markGatewaySuspectLocked(gateway)
	}

	delay := transientRetryDelay(state.retryAttempts)
	if outcome.permanent {
		delay = jitter(permanentRetryDelay, 0.10)
	}
	if outcome.retryAfter > delay {
		delay = jitter(outcome.retryAfter, 0.05)
	}
	ur.scheduleRetryLocked(state, delay)
	ur.mu.Unlock()

	log.Warn().
		Err(outcome.err).
		Str("device", state.reg.DeviceID).
		Str("gateway", gateway.key).
		Dur("retry_in", delay).
		Bool("permanent", outcome.permanent).
		Msg("upstream registration attempt failed")
}

func (ur *UpstreamRegistrar) scheduleRefreshLocked(state *registrationState, expiry time.Duration) {
	percent := float64(ur.cfg.RefreshPercent) / 100
	spread := 0.10
	factor := percent - spread/2 + rand.Float64()*spread
	delay := time.Duration(float64(expiry) * factor)
	if delay < 100*time.Millisecond {
		delay = 100 * time.Millisecond
	}
	ur.scheduleRegistrationLocked(state, delay)
}

func (ur *UpstreamRegistrar) scheduleRetryLocked(state *registrationState, delay time.Duration) {
	ur.scheduleRegistrationLocked(state, delay)
}

func (ur *UpstreamRegistrar) scheduleRegistrationLocked(state *registrationState, delay time.Duration) {
	if state.timer != nil {
		state.timer.Stop()
	}
	deviceID := state.reg.DeviceID
	generation := state.generation
	state.timer = time.AfterFunc(delay, func() {
		ur.mu.Lock()
		defer ur.mu.Unlock()
		current, ok := ur.regs[deviceID]
		if !ok || current.generation != generation || current != state || ur.closed {
			return
		}
		state.timer = nil
		ur.enqueueRegistrationLocked(state)
	})
}

func transientRetryDelay(attempt int) time.Duration {
	index := attempt - 1
	if index < 0 {
		index = 0
	}
	if index >= len(transientRetryDelays) {
		index = len(transientRetryDelays) - 1
	}
	return jitter(transientRetryDelays[index], 0.20)
}

func jitter(duration time.Duration, fraction float64) time.Duration {
	if duration <= 0 || fraction <= 0 {
		return duration
	}
	delta := (rand.Float64()*2 - 1) * fraction
	result := time.Duration(float64(duration) * (1 + delta))
	if result < time.Millisecond {
		return time.Millisecond
	}
	return result
}

func (ur *UpstreamRegistrar) noteRegistrationSuccessLocked(gateway *gatewayState, latency time.Duration) {
	gateway.lastSIPAt = time.Now()
	previousRTT := gateway.registerRTT
	if previousRTT == 0 {
		gateway.registerRTT = latency
	} else {
		gateway.registerRTT = time.Duration(float64(previousRTT)*0.8 + float64(latency)*0.2)
	}
	// Sparse normal refreshes do not prove burst capacity. Learn a higher rate
	// only while a recovery wave has multiple in-flight or queued accounts.
	underPressure := gateway.head < len(gateway.pending) || len(gateway.workers) > 1
	if !underPressure {
		return
	}
	if latency > 2*time.Second {
		if previousRTT == 0 || latency > 2*previousRTT {
			ur.reduceGatewayRateLocked(gateway, 0, "registration latency")
		}
		return
	}
	if !gateway.lastRateDecrease.IsZero() && time.Since(gateway.lastRateDecrease) < 2*time.Second {
		return
	}
	if gateway.currentRate >= ur.cfg.RecoveryMaxRate {
		return
	}
	gateway.successesSinceAdjust++
	threshold := int(math.Max(8, gateway.currentRate/2))
	if gateway.successesSinceAdjust < threshold {
		return
	}

	oldRate := gateway.currentRate
	gateway.currentRate = math.Min(ur.cfg.RecoveryMaxRate, gateway.currentRate*1.5)
	gateway.learnedRate = math.Max(gateway.learnedRate, gateway.currentRate)
	gateway.successesSinceAdjust = 0
	gateway.limiter.SetLimitAt(time.Now(), rate.Limit(gateway.currentRate))
	log.Info().
		Str("gateway", gateway.key).
		Float64("old_rate", oldRate).
		Float64("new_rate", gateway.currentRate).
		Msg("increased upstream recovery rate")
}

func (ur *UpstreamRegistrar) noteGatewayOverloadLocked(gateway *gatewayState, retryAfter time.Duration) {
	ur.reduceGatewayRateLocked(gateway, retryAfter, "overload response")
}

func (ur *UpstreamRegistrar) reduceGatewayRateLocked(gateway *gatewayState, retryAfter time.Duration, reason string) {
	now := time.Now()
	if retryAfter > 0 {
		until := now.Add(retryAfter)
		if until.After(gateway.pauseUntil) {
			gateway.pauseUntil = until
		}
	}
	if !gateway.lastRateDecrease.IsZero() && now.Sub(gateway.lastRateDecrease) < time.Second {
		signal(gateway.wake)
		return
	}
	oldRate := gateway.currentRate
	gateway.currentRate = math.Max(5, gateway.currentRate/2)
	gateway.learnedRate = math.Max(ur.cfg.RecoveryInitialRate, math.Min(gateway.learnedRate, gateway.currentRate))
	gateway.successesSinceAdjust = 0
	gateway.lastRateDecrease = now
	gateway.limiter.SetLimitAt(now, rate.Limit(gateway.currentRate))
	signal(gateway.wake)
	log.Warn().
		Str("gateway", gateway.key).
		Float64("old_rate", oldRate).
		Float64("new_rate", gateway.currentRate).
		Dur("retry_after", retryAfter).
		Str("reason", reason).
		Msg("reduced upstream recovery rate")
}

func (ur *UpstreamRegistrar) markGatewaySuspectLocked(gateway *gatewayState) {
	if !ur.cfg.ProbeEnabled || !gateway.reachable {
		return
	}
	if !gateway.suspect {
		gateway.suspect = true
		gateway.probeFailures = 0
		log.Warn().Str("gateway", gateway.key).Msg("upstream gateway became suspect; pausing registration traffic")
	}
	signal(gateway.probe)
	signal(gateway.wake)
}

func (ur *UpstreamRegistrar) markGatewayReachableLocked(gateway *gatewayState, skipDeviceID string, now time.Time) {
	wasImpaired := !gateway.reachable || gateway.suspect
	gateway.reachable = true
	gateway.suspect = false
	gateway.probeFailures = 0
	gateway.lastSIPAt = now
	if !wasImpaired {
		return
	}

	restartRate := math.Max(ur.cfg.RecoveryInitialRate, gateway.learnedRate/2)
	gateway.currentRate = math.Min(ur.cfg.RecoveryMaxRate, restartRate)
	gateway.limiter.SetLimitAt(now, rate.Limit(gateway.currentRate))
	gateway.successesSinceAdjust = 0
	for deviceID := range gateway.members {
		if deviceID == skipDeviceID {
			continue
		}
		if state, ok := ur.regs[deviceID]; ok {
			ur.enqueueRegistrationLocked(state)
		}
	}
	signal(gateway.wake)
	log.Info().
		Str("gateway", gateway.key).
		Float64("recovery_rate", gateway.currentRate).
		Int("registrations", len(gateway.members)).
		Msg("upstream gateway reachable; recovery started")
}

func (ur *UpstreamRegistrar) markGatewayDownLocked(gateway *gatewayState, now time.Time) {
	if !gateway.reachable {
		return
	}
	gateway.reachable = false
	gateway.suspect = false
	gateway.downSince = now
	for deviceID := range gateway.members {
		state, ok := ur.regs[deviceID]
		if !ok {
			continue
		}
		state.registered = false
		if state.timer != nil {
			state.timer.Stop()
			state.timer = nil
		}
		ur.enqueueRegistrationLocked(state)
	}
	signal(gateway.wake)
	log.Error().
		Str("gateway", gateway.key).
		Int("registrations", len(gateway.members)).
		Msg("upstream gateway unavailable; registration traffic circuit opened")
}

func (ur *UpstreamRegistrar) probeGateway(gateway *gatewayState) {
	timer := time.NewTimer(jitter(time.Duration(ur.cfg.ProbeIntervalMilliseconds)*time.Millisecond, 0.10))
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
		case <-gateway.probe:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-gateway.ctx.Done():
			return
		}

		if err := ur.recovery.probeLimiter.Wait(gateway.ctx); err != nil {
			return
		}
		select {
		case ur.recovery.probeWorkers <- struct{}{}:
		case <-gateway.ctx.Done():
			return
		}
		started := time.Now()
		probeCtx, cancel := context.WithTimeout(gateway.ctx, time.Duration(ur.cfg.ProbeTimeoutMilliseconds)*time.Millisecond)
		response, err := ur.client.Do(probeCtx, ur.buildOptionsRequest(gateway))
		cancel()
		<-ur.recovery.probeWorkers
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		ur.recordProbe(gateway, response != nil, status, time.Since(started), err)

		delay, ok := ur.nextProbeDelay(gateway)
		if !ok {
			return
		}
		timer.Reset(delay)
	}
}

func (ur *UpstreamRegistrar) buildOptionsRequest(gateway *gatewayState) *sip.Request {
	target := sip.Uri{Host: gateway.host, Port: gateway.port}
	if gateway.transport != "udp" {
		target.UriParams = sip.NewParams()
		target.UriParams.Add("transport", gateway.transport)
	}
	req := sip.NewRequest(sip.OPTIONS, target)
	req.SetBody(nil)
	return req
}

func (ur *UpstreamRegistrar) recordProbe(gateway *gatewayState, success bool, status int, rtt time.Duration, probeErr error) {
	now := time.Now()
	ur.mu.Lock()
	current, ok := ur.gateways[gateway.key]
	if !ok || current != gateway || ur.closed {
		ur.mu.Unlock()
		return
	}
	gateway.lastProbeAt = now
	gateway.lastProbeRTT = rtt

	if success {
		gateway.probeFailures = 0
		gateway.probeValidated = true
		gateway.probeWarned = false
		ur.markGatewayReachableLocked(gateway, "", now)
		ur.mu.Unlock()
		log.Debug().
			Str("gateway", gateway.key).
			Int("status", status).
			Dur("rtt", rtt).
			Msg("upstream gateway probe succeeded")
		return
	}

	gateway.probeFailures++
	failures := gateway.probeFailures
	if failures < ur.cfg.ProbeFailureThreshold {
		gateway.suspect = true
		signal(gateway.wake)
	} else if !gateway.probeValidated {
		// Some registrars silently discard OPTIONS. Until this endpoint has
		// answered at least one probe, keep bounded REGISTER attempts as the
		// fallback signal instead of opening a circuit that could never recover.
		gateway.probeFailures = 0
		gateway.suspect = false
		warn := !gateway.probeWarned
		gateway.probeWarned = true
		signal(gateway.wake)
		ur.mu.Unlock()
		if warn {
			log.Warn().
				Err(probeErr).
				Str("gateway", gateway.key).
				Msg("upstream gateway has not answered OPTIONS; using bounded registration health fallback")
		}
		return
	} else {
		ur.markGatewayDownLocked(gateway, now)
	}
	ur.mu.Unlock()

	if failures < ur.cfg.ProbeFailureThreshold {
		log.Warn().
			Err(probeErr).
			Str("gateway", gateway.key).
			Int("failures", failures).
			Msg("upstream gateway probe failed; confirming immediately")
	} else {
		log.Debug().
			Err(probeErr).
			Str("gateway", gateway.key).
			Int("failures", failures).
			Msg("upstream gateway remains unavailable")
	}
}

func (ur *UpstreamRegistrar) nextProbeDelay(gateway *gatewayState) (time.Duration, bool) {
	ur.mu.RLock()
	defer ur.mu.RUnlock()
	current, ok := ur.gateways[gateway.key]
	if !ok || current != gateway || ur.closed {
		return 0, false
	}
	if gateway.suspect {
		return probeConfirmationDelay, true
	}
	if !gateway.reachable {
		base := time.Duration(ur.cfg.DownProbeIntervalMillis) * time.Millisecond
		downFor := time.Since(gateway.downSince)
		switch {
		case downFor < 15*time.Second:
			return jitter(base, 0.10), true
		case downFor < time.Minute:
			return jitter(2*base, 0.10), true
		default:
			return jitter(5*base, 0.10), true
		}
	}
	return jitter(time.Duration(ur.cfg.ProbeIntervalMilliseconds)*time.Millisecond, 0.10), true
}
