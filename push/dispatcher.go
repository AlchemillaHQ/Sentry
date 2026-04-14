package push

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/AlchemillaHQ/Difuse-B2BUA/config"
)

type Sender interface {
	SendCallPush(ctx context.Context, token, callID, callerURI, callerName string) error
}

type Dispatcher struct {
	fcm  *FCMSender
	apns *APNsSender
}

func NewDispatcher(cfg config.PushConfig) (*Dispatcher, error) {
	d := &Dispatcher{}
	var err error
	d.fcm, err = NewFCMSender(cfg.FCMServiceAccount)
	if err != nil {
		return nil, fmt.Errorf("init fcm: %w", err)
	}
	d.apns, err = NewAPNsSender(cfg.APNsCert, cfg.APNsBundleID, cfg.APNsProduction)
	if err != nil {
		return nil, fmt.Errorf("init apns: %w", err)
	}
	return d, nil
}

func (d *Dispatcher) Send(ctx context.Context, platform, token, callID, callerURI, callerName string) error {
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
		slog.Warn("unknown push platform", "platform", platform)
		return fmt.Errorf("unknown platform: %s", platform)
	}
}
