package push

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/sideshow/apns2"
	"github.com/sideshow/apns2/certificate"
	"github.com/sideshow/apns2/payload"
)

type apnsClient interface {
	PushWithContext(ctx apns2.Context, n *apns2.Notification) (*apns2.Response, error)
}

type APNsSender struct {
	client   apnsClient
	bundleID string
}

var initAPNs = createAPNsClient

func createAPNsClient(certPath, bundleID string, production bool) (apnsClient, error) {
	if certPath == "" {
		return nil, nil
	}
	cert, err := certificate.FromP12File(certPath, "")
	if err != nil {
		return nil, fmt.Errorf("load apns cert: %w", err)
	}
	var client apnsClient
	if production {
		client = apns2.NewClient(cert).Production()
	} else {
		client = apns2.NewClient(cert).Development()
	}
	return client, nil
}

func NewAPNsSender(certPath, bundleID string, production bool) (*APNsSender, error) {
	client, err := initAPNs(certPath, bundleID, production)
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, nil
	}
	return &APNsSender{client: client, bundleID: bundleID}, nil
}

func newAPNsSenderWithClient(client apnsClient, bundleID string) *APNsSender {
	return &APNsSender{client: client, bundleID: bundleID}
}

func (a *APNsSender) SendCallPush(ctx context.Context, call CallPush) error {
	p := payload.NewPayload().
		AlertTitle("Incoming Call").
		AlertBody(call.CallerName).
		Sound("default").
		ContentAvailable().
		Custom("call-id", call.CallID).
		Custom("device-id", call.DeviceID).
		Custom("caller-uri", call.CallerURI).
		Custom("caller-name", call.CallerName).
		Custom("content-type", "application/call-info")

	notification := &apns2.Notification{
		DeviceToken: call.Token,
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
	log.Debug().Str("apns_id", resp.ApnsID).Str("call_id", call.CallID).Str("device", call.DeviceID).Msg("apns push sent")
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
