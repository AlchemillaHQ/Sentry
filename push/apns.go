package push

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/AlchemillaHQ/Sentry/config"
	"github.com/rs/zerolog/log"
	"github.com/sideshow/apns2"
	apnstoken "github.com/sideshow/apns2/token"
)

type apnsClient interface {
	PushWithContext(ctx apns2.Context, n *apns2.Notification) (*apns2.Response, error)
}

type APNsSender struct {
	client   apnsClient
	bundleID string
}

var (
	initAPNs            = createAPNsClient
	loadAPNsAuthKey     = apnstoken.AuthKeyFromFile
	loadAPNsCertificate = loadP12Certificate
)

func createAPNsClient(cfg config.PushConfig) (apnsClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if !cfg.APNsEnabled() {
		return nil, nil
	}

	var client *apns2.Client
	if cfg.APNsAuthMode() == "token" {
		authKey, err := loadAPNsAuthKey(strings.TrimSpace(cfg.APNsKey))
		if err != nil {
			return nil, fmt.Errorf("load apns key: %w", err)
		}
		providerToken := &apnstoken.Token{
			AuthKey: authKey,
			KeyID:   strings.TrimSpace(cfg.APNsKeyID),
			TeamID:  strings.TrimSpace(cfg.APNsTeamID),
		}
		if _, err := providerToken.Generate(); err != nil {
			return nil, fmt.Errorf("initialize apns provider token: %w", err)
		}
		client = apns2.NewTokenClient(providerToken)
	} else {
		cert, err := loadAPNsCertificate(strings.TrimSpace(cfg.APNsCert), cfg.APNsCertPassword)
		if err != nil {
			return nil, fmt.Errorf("load apns cert: %w", err)
		}
		if cert.Leaf == nil {
			return nil, fmt.Errorf("load apns cert: certificate metadata is missing")
		}
		now := time.Now()
		if now.Before(cert.Leaf.NotBefore) {
			return nil, fmt.Errorf("load apns cert: certificate is not valid yet")
		}
		if now.After(cert.Leaf.NotAfter) {
			return nil, fmt.Errorf("load apns cert: certificate has expired")
		}
		client = apns2.NewClient(cert)
	}

	if cfg.APNsProduction {
		client = client.Production()
	} else {
		client = client.Development()
	}
	return client, nil
}

func NewAPNsSender(cfg config.PushConfig) (*APNsSender, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	client, err := initAPNs(cfg)
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, nil
	}

	environment := "sandbox"
	if cfg.APNsProduction {
		environment = "production"
	}
	bundleID := strings.TrimSpace(cfg.APNsBundleID)
	log.Info().
		Str("environment", environment).
		Str("topic", bundleID+".voip").
		Str("auth_mode", cfg.APNsAuthMode()).
		Msg("APNs push enabled")
	return &APNsSender{client: client, bundleID: bundleID}, nil
}

func newAPNsSenderWithClient(client apnsClient, bundleID string) *APNsSender {
	return &APNsSender{client: client, bundleID: bundleID}
}

type apnsCallAPS struct {
	ContentAvailable int    `json:"content-available"`
	CallID           string `json:"call-id"`
}

type apnsCallPayload struct {
	APS         apnsCallAPS `json:"aps"`
	CallID      string      `json:"call-id"`
	DeviceID    string      `json:"device-id"`
	CallerURI   string      `json:"caller-uri"`
	CallerName  string      `json:"caller-name"`
	ContentType string      `json:"content-type"`
}

func (a *APNsSender) SendCallPush(ctx context.Context, call CallPush) error {
	deviceToken, err := NormalizeToken("ios", call.Token)
	if err != nil {
		return fmt.Errorf("apns token: %w", err)
	}

	p := apnsCallPayload{
		APS: apnsCallAPS{
			ContentAvailable: 1,
			CallID:           call.CallID,
		},
		CallID:      call.CallID,
		DeviceID:    call.DeviceID,
		CallerURI:   call.CallerURI,
		CallerName:  call.CallerName,
		ContentType: "application/call-info",
	}

	notification := &apns2.Notification{
		DeviceToken: deviceToken,
		Topic:       a.bundleID + ".voip",
		Payload:     p,
		PushType:    apns2.PushTypeVOIP,
		Priority:    apns2.PriorityHigh,
	}
	resp, err := a.client.PushWithContext(ctx, notification)
	if err != nil {
		return fmt.Errorf("apns send: %w", err)
	}
	if resp == nil {
		return fmt.Errorf("apns send: empty response")
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
