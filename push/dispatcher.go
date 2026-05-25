package push

import (
	"context"
	"fmt"

	"github.com/AlchemillaHQ/Sentry/config"
	"github.com/rs/zerolog/log"
)

type Sender interface {
	Send(ctx context.Context, platform, token, callID, callerURI, callerName string) error
	Start(ctx context.Context)
}

type pushRequest struct {
	platform   string
	token      string
	callID     string
	callerURI  string
	callerName string
}

type Dispatcher struct {
	fcm     *FCMSender
	apns    *APNsSender
	queue   chan pushRequest
	workers int
}

var _ Sender = (*Dispatcher)(nil)

func NewDispatcher(cfg config.PushConfig) (*Dispatcher, error) {
	d := &Dispatcher{
		queue:   make(chan pushRequest, 1000),
		workers: 50,
	}

	fcm, err := NewFCMSender(cfg.FCMServiceAccount)
	if err != nil {
		return nil, fmt.Errorf("init fcm: %w", err)
	}
	d.fcm = fcm

	apns, err := NewAPNsSender(cfg.APNsCert, cfg.APNsBundleID, cfg.APNsProduction)
	if err != nil {
		log.Warn().Err(err).Msg("APNs push disabled")
	} else {
		d.apns = apns
	}

	return d, nil
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
			// We use Background here as the SIP transaction ctx might be dead
			err := d.sendImmediate(context.Background(), req.platform, req.token, req.callID, req.callerURI, req.callerName)
			if err != nil {
				log.Error().Err(err).Str("call_id", req.callID).Msg("async push failed")
			}
		case <-ctx.Done():
			return
		}
	}
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
		return nil
	default:
		log.Warn().Str("call_id", callID).Msg("push queue full, dropping notification")
		return fmt.Errorf("push queue full")
	}
}

func (d *Dispatcher) sendImmediate(ctx context.Context, platform, token, callID, callerURI, callerName string) error {
	switch platform {
	case "android":
		if d.fcm == nil {
			return fmt.Errorf("FCM not configured")
		}
		return d.fcm.SendCallPush(ctx, token, callID, callerURI, callerName)
	case "ios":
		if d.apns == nil {
			return fmt.Errorf("APNs not configured")
		}
		return d.apns.SendCallPush(ctx, token, callID, callerURI, callerName)
	default:
		log.Warn().Str("platform", platform).Msg("unknown push platform")
		return fmt.Errorf("unknown platform: %s", platform)
	}
}
