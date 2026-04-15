package push

import (
	"context"
	"fmt"
	"log/slog"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"google.golang.org/api/option"
)

type FCMSender struct {
	client *messaging.Client
}

func NewFCMSender(serviceAccountPath string) (*FCMSender, error) {
	if serviceAccountPath == "" {
		return nil, nil
	}
	ctx := context.Background()
	app, err := firebase.NewApp(ctx, nil, option.WithCredentialsFile(serviceAccountPath))
	if err != nil {
		return nil, fmt.Errorf("firebase app: %w", err)
	}
	client, err := app.Messaging(ctx)
	if err != nil {
		return nil, fmt.Errorf("firebase messaging: %w", err)
	}
	return &FCMSender{client: client}, nil
}

func (f *FCMSender) SendCallPush(ctx context.Context, token, callID, callerURI, callerName string) error {
	msg := &messaging.Message{
		Token: token,
		Data: map[string]string{
			"call-id":      callID,
			"caller-uri":   callerURI,
			"caller-name":  callerName,
			"content-type": "application/call-info",
		},
		Android: &messaging.AndroidConfig{
			Priority: "high",
		},
	}
	resp, err := f.client.Send(ctx, msg)
	if err != nil {
		return fmt.Errorf("fcm send: %w", err)
	}
	slog.Debug("fcm push sent", "response", resp, "call_id", callID)
	return nil
}
