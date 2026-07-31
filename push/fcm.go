package push

import (
	"context"
	"fmt"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"github.com/rs/zerolog/log"
	"google.golang.org/api/option"
)

type fcmClient interface {
	Send(ctx context.Context, msg *messaging.Message) (string, error)
}

type FCMSender struct {
	client fcmClient
}

var initFCM = createFCMClient

func createFCMClient(serviceAccountPath string) (fcmClient, error) {
	if serviceAccountPath == "" {
		return nil, nil
	}
	ctx := context.Background()
	app, _ := firebase.NewApp(ctx, nil, option.WithCredentialsFile(serviceAccountPath))
	client, err := app.Messaging(ctx)
	if err != nil {
		return nil, fmt.Errorf("firebase messaging: %w", err)
	}
	return client, nil
}

func NewFCMSender(serviceAccountPath string) (*FCMSender, error) {
	client, err := initFCM(serviceAccountPath)
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, nil
	}
	return &FCMSender{client: client}, nil
}

func newFCMSenderWithClient(client fcmClient) *FCMSender {
	return &FCMSender{client: client}
}

var fcmIsTokenInvalid = func(err error) bool {
	return messaging.IsUnregistered(err) || messaging.IsInvalidArgument(err) || messaging.IsSenderIDMismatch(err)
}

func (f *FCMSender) SendCallPush(ctx context.Context, call CallPush) error {
	msg := &messaging.Message{
		Token: call.Token,
		Data: map[string]string{
			"call-id":      call.CallID,
			"device-id":    call.DeviceID,
			"caller-uri":   call.CallerURI,
			"caller-name":  call.CallerName,
			"content-type": "application/call-info",
		},
		Android: &messaging.AndroidConfig{
			Priority: "high",
		},
	}
	resp, err := f.client.Send(ctx, msg)
	if err != nil {
		if fcmIsTokenInvalid(err) {
			return fmt.Errorf("fcm send: %w: %w", ErrTokenInvalid, err)
		}
		return fmt.Errorf("fcm send: %w", err)
	}
	log.Debug().Str("response", resp).Str("call_id", call.CallID).Str("device", call.DeviceID).Msg("fcm push sent")
	return nil
}
