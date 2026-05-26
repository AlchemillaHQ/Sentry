package push

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/sideshow/apns2"
	"github.com/sideshow/apns2/certificate"
	"github.com/sideshow/apns2/payload"
)

type APNsSender struct {
	client   *apns2.Client
	bundleID string
}

func NewAPNsSender(certPath, bundleID string, production bool) (*APNsSender, error) {
	if certPath == "" {
		return nil, nil
	}
	cert, err := certificate.FromP12File(certPath, "")
	if err != nil {
		return nil, fmt.Errorf("load apns cert: %w", err)
	}
	var client *apns2.Client
	if production {
		client = apns2.NewClient(cert).Production()
	} else {
		client = apns2.NewClient(cert).Development()
	}
	return &APNsSender{client: client, bundleID: bundleID}, nil
}

func (a *APNsSender) SendCallPush(ctx context.Context, token, callID, callerURI, callerName string) error {
	p := payload.NewPayload().
		AlertTitle("Incoming Call").
		AlertBody(callerName).
		Sound("default").
		ContentAvailable().
		Custom("call-id", callID).
		Custom("caller-uri", callerURI).
		Custom("caller-name", callerName)

	notification := &apns2.Notification{
		DeviceToken: token,
		Topic:       a.bundleID + ".voip",
		Payload:     p,
		PushType:    apns2.PushTypeVOIP,
		Priority:    apns2.PriorityHigh,
	}
	resp, err := a.client.PushWithContext(ctx, notification)
	if err != nil {
		return fmt.Errorf("apns send: %w", err)
	}
	if !resp.Sent() {
		if isAPNsTokenInvalid(resp) {
			return fmt.Errorf("apns send: %w: %d %s", ErrTokenInvalid, resp.StatusCode, resp.Reason)
		}
		return fmt.Errorf("apns rejected: %d %s", resp.StatusCode, resp.Reason)
	}
	log.Debug().Str("apns_id", resp.ApnsID).Str("call_id", callID).Msg("apns push sent")
	return nil
}

func isAPNsTokenInvalid(resp *apns2.Response) bool {
	switch resp.Reason {
	case apns2.ReasonBadDeviceToken,
		apns2.ReasonDeviceTokenNotForTopic,
		apns2.ReasonUnregistered,
		apns2.ReasonExpiredToken:
		return true
	}
	return false
}
