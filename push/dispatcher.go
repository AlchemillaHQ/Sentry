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

type DeadTokenHandler func(platform, token, callID string)

type Sender interface {
	Send(ctx context.Context, platform, token, callID, callerURI, callerName string) error
	Start(ctx context.Context)
	CancelPush(callID string)
	OnDeadToken(handler DeadTokenHandler)
}

type pushRequest struct {
	platform   string
	token      string
	callID     string
	callerURI  string
	callerName string
}

type platformSender interface {
	SendCallPush(ctx context.Context, token, callID, callerURI, callerName string) error
}

type Dispatcher struct {
	senders map[string]platformSender

	queue   chan pushRequest
	workers int

	cancelMu    sync.Mutex
	cancelFuncs map[string]context.CancelFunc

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

var retryTimeout = 35 * time.Second

func NewDispatcher(cfg config.PushConfig) (*Dispatcher, error) {
	d := &Dispatcher{
		senders:     make(map[string]platformSender),
		queue:       make(chan pushRequest, workerQueueSize),
		workers:     workerCount,
		cancelFuncs: make(map[string]context.CancelFunc),
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

func (d *Dispatcher) sendWithRetry(req pushRequest) {
	retryCtx, cancel := context.WithTimeout(context.Background(), retryTimeout)
	defer cancel()

	d.cancelMu.Lock()
	d.cancelFuncs[req.callID] = cancel
	d.cancelMu.Unlock()

	defer func() {
		d.cancelMu.Lock()
		delete(d.cancelFuncs, req.callID)
		d.cancelMu.Unlock()
	}()

	for i, delay := range backoffSchedule {
		if retryCtx.Err() != nil {
			log.Debug().Str("call_id", req.callID).Msg("push retry cancelled before send")
			return
		}

		if i > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-retryCtx.Done():
				timer.Stop()
				log.Debug().Str("call_id", req.callID).Msg("push retry cancelled during backoff")
				return
			case <-timer.C:
			}
		}

		err := d.sendImmediate(retryCtx, req.platform, req.token, req.callID, req.callerURI, req.callerName)
		if err == nil {
			log.Debug().Str("call_id", req.callID).Int("attempt", i+1).Msg("push sent successfully")
			return
		}

		if errors.Is(err, ErrTokenInvalid) {
			log.Warn().Str("call_id", req.callID).Str("token", req.token).Msg("push token is invalid, stopping retries")
			if d.deadTokenHandler != nil {
				d.deadTokenHandler(req.platform, req.token, req.callID)
			}
			return
		}

		log.Warn().Err(err).Str("call_id", req.callID).Int("attempt", i+1).Msg("push attempt failed, will retry")
	}

	log.Error().Str("call_id", req.callID).Int("attempts", maxAttempts).Msg("all push retries exhausted")
}

func (d *Dispatcher) Send(ctx context.Context, platform, token, callID, callerURI, callerName string) error {
	select {
	case d.queue <- pushRequest{
		platform:   platform,
		token:      token,
		callID:     callID,
		callerURI:  callerURI,
		callerName: callerName,
	}:
		log.Debug().Str("call_id", callID).Str("platform", platform).Msg("push request enqueued")
		return nil
	default:
		log.Warn().Str("call_id", callID).Msg("push queue full, dropping notification")
		return fmt.Errorf("push queue full")
	}
}

func (d *Dispatcher) CancelPush(callID string) {
	d.cancelMu.Lock()
	defer d.cancelMu.Unlock()
	if cancel, ok := d.cancelFuncs[callID]; ok {
		cancel()
		delete(d.cancelFuncs, callID)
		log.Debug().Str("call_id", callID).Msg("push retries cancelled")
	}
}

func (d *Dispatcher) sendImmediate(ctx context.Context, platform, token, callID, callerURI, callerName string) error {
	sender, ok := d.senders[platform]
	if !ok {
		return fmt.Errorf("%s push not configured", platform)
	}
	return sender.SendCallPush(ctx, token, callID, callerURI, callerName)
}
