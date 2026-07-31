package push

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/AlchemillaHQ/Sentry/config"
	"github.com/rs/zerolog/log"
)

type CallPush struct {
	Platform   string
	Token      string
	CallID     string
	DeviceID   string
	CallerURI  string
	CallerName string
}

type DeadTokenHandler func(call CallPush)

type Sender interface {
	Send(ctx context.Context, call CallPush) error
	Start(ctx context.Context)
	CancelPush(callID string)
	OnDeadToken(handler DeadTokenHandler)
}

type platformSender interface {
	SendCallPush(ctx context.Context, call CallPush) error
}

type Dispatcher struct {
	senders map[string]platformSender

	queue   chan CallPush
	workers int
	timeout time.Duration

	cancelMu    sync.Mutex
	cancelFuncs map[string]context.CancelFunc
	queued      map[string]struct{}
	cancelled   map[string]time.Time

	deadTokenHandler DeadTokenHandler
}

var _ Sender = (*Dispatcher)(nil)

const (
	maxAttempts     = 5
	workerQueueSize = 1000
	workerCount     = 50
)

var backoffSchedule = []time.Duration{0, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}

var (
	newFCM  = NewFCMSender
	newAPNs = NewAPNsSender
)

const (
	defaultRetryTimeout    = 35 * time.Second
	cancelledCallRetention = time.Minute
)

func NewDispatcher(cfg config.PushConfig) (*Dispatcher, error) {
	d := &Dispatcher{
		senders:     make(map[string]platformSender),
		queue:       make(chan CallPush, workerQueueSize),
		workers:     workerCount,
		timeout:     defaultRetryTimeout,
		cancelFuncs: make(map[string]context.CancelFunc),
		queued:      make(map[string]struct{}),
		cancelled:   make(map[string]time.Time),
	}

	fcm, err := newFCM(cfg.FCMServiceAccount)
	if err != nil {
		return nil, fmt.Errorf("init fcm: %w", err)
	}
	if fcm != nil {
		d.senders["android"] = fcm
	}

	apns, err := newAPNs(cfg.APNsCert, cfg.APNsBundleID, cfg.APNsProduction)
	if err != nil {
		log.Warn().Err(err).Msg("APNs push disabled")
	} else if apns != nil {
		d.senders["ios"] = apns
	}

	return d, nil
}

func (d *Dispatcher) OnDeadToken(handler DeadTokenHandler) {
	d.deadTokenHandler = handler
}

func (d *Dispatcher) Start(ctx context.Context) {
	for i := 0; i < d.workers; i++ {
		go d.worker(ctx)
	}
	go d.cleanupCancelledCalls(ctx)
	log.Info().Int("workers", d.workers).Msg("push dispatcher started")
}

func (d *Dispatcher) worker(ctx context.Context) {
	for {
		select {
		case req := <-d.queue:
			d.sendWithRetry(req)
		case <-ctx.Done():
			return
		}
	}
}

func (d *Dispatcher) sendWithRetry(req CallPush) {
	timeout := d.timeout
	if timeout <= 0 {
		timeout = defaultRetryTimeout
	}
	retryCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	d.cancelMu.Lock()
	d.ensureStateLocked()
	if _, cancelled := d.cancelled[req.CallID]; cancelled {
		delete(d.queued, req.CallID)
		delete(d.cancelled, req.CallID)
		d.cancelMu.Unlock()
		log.Debug().Str("call_id", req.CallID).Str("device", req.DeviceID).Msg("queued push discarded after cancellation")
		return
	}
	d.cancelFuncs[req.CallID] = cancel
	d.cancelMu.Unlock()

	defer func() {
		d.cancelMu.Lock()
		delete(d.cancelFuncs, req.CallID)
		delete(d.queued, req.CallID)
		delete(d.cancelled, req.CallID)
		d.cancelMu.Unlock()
	}()

	for i, delay := range backoffSchedule {
		if retryCtx.Err() != nil {
			log.Debug().Str("call_id", req.CallID).Str("device", req.DeviceID).Msg("push retry cancelled before send")
			return
		}

		if i > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-retryCtx.Done():
				timer.Stop()
				log.Debug().Str("call_id", req.CallID).Str("device", req.DeviceID).Msg("push retry cancelled during backoff")
				return
			case <-timer.C:
			}
		}

		err := d.sendImmediate(retryCtx, req)
		if err == nil {
			log.Debug().Str("call_id", req.CallID).Str("device", req.DeviceID).Int("attempt", i+1).Msg("push sent successfully")
			return
		}

		if errors.Is(err, ErrTokenInvalid) {
			log.Warn().Str("call_id", req.CallID).Str("device", req.DeviceID).Msg("push token is invalid, stopping retries")
			if d.deadTokenHandler != nil {
				d.deadTokenHandler(req)
			}
			return
		}

		log.Warn().Err(err).Str("call_id", req.CallID).Str("device", req.DeviceID).Int("attempt", i+1).Msg("push attempt failed, will retry")
	}

	log.Error().Str("call_id", req.CallID).Str("device", req.DeviceID).Int("attempts", maxAttempts).Msg("all push retries exhausted")
}

func (d *Dispatcher) Send(ctx context.Context, call CallPush) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if call.CallID == "" {
		return fmt.Errorf("call ID is required")
	}
	if call.DeviceID == "" {
		return fmt.Errorf("device ID is required")
	}

	d.cancelMu.Lock()
	d.ensureStateLocked()
	d.pruneCancelledLocked(time.Now())
	if _, cancelled := d.cancelled[call.CallID]; cancelled {
		delete(d.cancelled, call.CallID)
		d.cancelMu.Unlock()
		return fmt.Errorf("call %s was cancelled", call.CallID)
	}
	if _, exists := d.queued[call.CallID]; exists {
		d.cancelMu.Unlock()
		return fmt.Errorf("call %s is already queued", call.CallID)
	}
	d.queued[call.CallID] = struct{}{}

	select {
	case d.queue <- call:
		d.cancelMu.Unlock()
		log.Debug().Str("call_id", call.CallID).Str("device", call.DeviceID).Str("platform", call.Platform).Msg("push request enqueued")
		return nil
	default:
		delete(d.queued, call.CallID)
		d.cancelMu.Unlock()
		log.Warn().Str("call_id", call.CallID).Str("device", call.DeviceID).Msg("push queue full, dropping notification")
		return fmt.Errorf("push queue full")
	}
}

func (d *Dispatcher) CancelPush(callID string) {
	d.cancelMu.Lock()
	defer d.cancelMu.Unlock()
	d.ensureStateLocked()
	now := time.Now()
	d.pruneCancelledLocked(now)
	d.cancelled[callID] = now
	if cancel, ok := d.cancelFuncs[callID]; ok {
		cancel()
		delete(d.cancelFuncs, callID)
		log.Debug().Str("call_id", callID).Msg("push retries cancelled")
	}
}

func (d *Dispatcher) ensureStateLocked() {
	if d.cancelFuncs == nil {
		d.cancelFuncs = make(map[string]context.CancelFunc)
	}
	if d.queued == nil {
		d.queued = make(map[string]struct{})
	}
	if d.cancelled == nil {
		d.cancelled = make(map[string]time.Time)
	}
}

func (d *Dispatcher) pruneCancelledLocked(now time.Time) {
	for callID, cancelledAt := range d.cancelled {
		if now.Sub(cancelledAt) > cancelledCallRetention {
			delete(d.cancelled, callID)
		}
	}
}

func (d *Dispatcher) cleanupCancelledCalls(ctx context.Context) {
	interval := cancelledCallRetention / 2
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			d.cancelMu.Lock()
			d.ensureStateLocked()
			d.pruneCancelledLocked(now)
			d.cancelMu.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

func (d *Dispatcher) sendImmediate(ctx context.Context, call CallPush) error {
	sender, ok := d.senders[call.Platform]
	if !ok {
		return fmt.Errorf("%s push not configured", call.Platform)
	}
	return sender.SendCallPush(ctx, call)
}
