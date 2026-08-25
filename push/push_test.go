package push

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"firebase.google.com/go/v4/messaging"
	"github.com/AlchemillaHQ/Sentry/config"
	"github.com/sideshow/apns2"
	"github.com/stretchr/testify/assert"
)

type mockPlatformSender struct {
	sendFunc func(ctx context.Context, token, callID, callerURI, callerName string) error
}

func (m *mockPlatformSender) SendCallPush(ctx context.Context, call CallPush) error {
	if m.sendFunc != nil {
		return m.sendFunc(ctx, call.Token, call.CallID, call.CallerURI, call.CallerName)
	}
	return nil
}

func testCallPush(platform, token, callID, callerURI, callerName string) CallPush {
	return CallPush{
		Platform:   platform,
		Token:      token,
		CallID:     callID,
		DeviceID:   "550e8400-e29b-41d4-a716-446655440000",
		CallerURI:  callerURI,
		CallerName: callerName,
	}
}

func newTestDispatcher() *Dispatcher {
	return &Dispatcher{
		senders:     make(map[string]platformSender),
		queue:       make(chan CallPush, 10),
		cancelFuncs: make(map[string]context.CancelFunc),
	}
}

func TestDispatcher_SendAsync(t *testing.T) {
	d := newTestDispatcher()

	ctx := context.Background()
	err := d.Send(ctx, testCallPush("android", "token", "call-1", "sip:caller", "Name"))
	assert.NoError(t, err)

	assert.Equal(t, 1, len(d.queue))
	req := <-d.queue
	assert.Equal(t, "call-1", req.CallID)
	assert.Equal(t, "android", req.Platform)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", req.DeviceID)
}

func TestDispatcher_SendRejectsMissingIdentity(t *testing.T) {
	d := newTestDispatcher()

	missingDevice := testCallPush("android", "token", "call-1", "sip:caller", "Name")
	missingDevice.DeviceID = ""
	assert.ErrorContains(t, d.Send(context.Background(), missingDevice), "device ID")

	missingCall := testCallPush("android", "token", "call-1", "sip:caller", "Name")
	missingCall.CallID = ""
	assert.ErrorContains(t, d.Send(context.Background(), missingCall), "call ID")

	assert.Empty(t, d.queue)
}

func TestDispatcher_CancelPush(t *testing.T) {
	d := newTestDispatcher()

	called := false
	d.cancelMu.Lock()
	_, cancel := context.WithCancel(context.Background())
	d.cancelFuncs["call-1"] = func() {
		cancel()
		called = true
	}
	d.cancelMu.Unlock()

	d.CancelPush("call-1")

	d.cancelMu.Lock()
	_, exists := d.cancelFuncs["call-1"]
	d.cancelMu.Unlock()

	assert.True(t, called)
	assert.False(t, exists)
}

func TestDispatcher_CancelPush_NotFound(t *testing.T) {
	d := newTestDispatcher()

	d.CancelPush("nonexistent")
}

func TestDispatcher_CancelPush_BeforeQueueRejectsLaterSend(t *testing.T) {
	d := newTestDispatcher()
	call := testCallPush("android", "token", "call-before-queue", "sip:caller", "Name")

	d.CancelPush(call.CallID)
	err := d.Send(context.Background(), call)

	assert.ErrorContains(t, err, "cancelled")
	assert.Empty(t, d.queue)
}

func TestDispatcher_CancelPush_DiscardsQueuedPush(t *testing.T) {
	d := newTestDispatcher()
	sends := 0
	d.senders["android"] = &mockPlatformSender{
		sendFunc: func(context.Context, string, string, string, string) error {
			sends++
			return nil
		},
	}
	call := testCallPush("android", "token", "call-queued", "sip:caller", "Name")
	assert.NoError(t, d.Send(context.Background(), call))

	d.CancelPush(call.CallID)
	d.sendWithRetry(<-d.queue)

	assert.Zero(t, sends)
}

func TestDispatcher_OnDeadToken(t *testing.T) {
	d := newTestDispatcher()

	var mu sync.Mutex
	var calls [][3]string

	d.OnDeadToken(func(call CallPush) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, [3]string{call.Platform, call.Token, call.CallID})
	})

	assert.NotNil(t, d.deadTokenHandler)

	d.deadTokenHandler(testCallPush("android", "dead-token", "call-1", "", ""))
	d.deadTokenHandler(testCallPush("ios", "bad-token", "call-2", "", ""))

	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, calls, 2)
	assert.Equal(t, [3]string{"android", "dead-token", "call-1"}, calls[0])
	assert.Equal(t, [3]string{"ios", "bad-token", "call-2"}, calls[1])
}

func TestDispatcher_SendQueueFull(t *testing.T) {
	d := &Dispatcher{
		senders:     make(map[string]platformSender),
		queue:       make(chan CallPush, 1),
		cancelFuncs: make(map[string]context.CancelFunc),
	}

	ctx := context.Background()

	err := d.Send(ctx, testCallPush("android", "token", "call-1", "sip:caller", "Name"))
	assert.NoError(t, err)

	err = d.Send(ctx, testCallPush("android", "token", "call-2", "sip:caller", "Name"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "queue full")
}

func TestDispatcher_CancelPush_StopsRetryLoop(t *testing.T) {
	d := newTestDispatcher()

	var wg sync.WaitGroup
	wg.Add(1)

	start := time.Now()
	go func() {
		defer wg.Done()
		d.sendWithRetry(CallPush{
			Platform:  "android",
			Token:     "token",
			CallID:    "call-cancel",
			DeviceID:  "device-1",
			CallerURI: "sip:caller",
		})
	}()

	time.Sleep(50 * time.Millisecond)
	d.CancelPush("call-cancel")

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		elapsed := time.Since(start)
		assert.Less(t, elapsed, 1*time.Second,
			"retry loop should stop quickly after CancelPush, but took %v", elapsed)
	case <-time.After(3 * time.Second):
		t.Fatal("retry loop did not stop within 3s after CancelPush")
	}

	d.cancelMu.Lock()
	_, exists := d.cancelFuncs["call-cancel"]
	d.cancelMu.Unlock()

	assert.False(t, exists, "cancel func should be cleaned up after retry loop exits")
}

func TestSendImmediate_Success(t *testing.T) {
	d := newTestDispatcher()
	d.senders["android"] = &mockPlatformSender{
		sendFunc: func(ctx context.Context, token, callID, callerURI, callerName string) error {
			assert.Equal(t, "token123", token)
			assert.Equal(t, "call-1", callID)
			assert.Equal(t, "sip:alice", callerURI)
			assert.Equal(t, "Alice", callerName)
			return nil
		},
	}

	err := d.sendImmediate(context.Background(), testCallPush("android", "token123", "call-1", "sip:alice", "Alice"))
	assert.NoError(t, err)
}

func TestSendImmediate_PlatformNotConfigured(t *testing.T) {
	d := newTestDispatcher()

	err := d.sendImmediate(context.Background(), testCallPush("android", "token", "call-1", "uri", "n"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not configured")
}

func TestSendImmediate_SenderError(t *testing.T) {
	d := newTestDispatcher()
	d.senders["ios"] = &mockPlatformSender{
		sendFunc: func(ctx context.Context, token, callID, callerURI, callerName string) error {
			return errors.New("network error")
		},
	}

	err := d.sendImmediate(context.Background(), testCallPush("ios", "token", "call-1", "uri", "n"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "network error")
}

func TestSendWithRetry_SuccessOnFirstAttempt(t *testing.T) {
	d := newTestDispatcher()
	d.senders["android"] = &mockPlatformSender{}

	d.sendWithRetry(CallPush{
		Platform:  "android",
		Token:     "token",
		CallID:    "call-1",
		DeviceID:  "device-1",
		CallerURI: "sip:caller",
	})

	d.cancelMu.Lock()
	_, exists := d.cancelFuncs["call-1"]
	d.cancelMu.Unlock()
	assert.False(t, exists, "cancel func should be cleaned up after success")
}

func TestSendWithRetry_RetriesOnTransientError(t *testing.T) {
	d := newTestDispatcher()
	attempts := 0
	d.senders["android"] = &mockPlatformSender{
		sendFunc: func(ctx context.Context, token, callID, callerURI, callerName string) error {
			attempts++
			if attempts < 3 {
				return errors.New("transient error")
			}
			return nil
		},
	}

	d.sendWithRetry(CallPush{
		Platform:  "android",
		Token:     "token",
		CallID:    "call-retry",
		DeviceID:  "device-1",
		CallerURI: "sip:caller",
	})

	assert.Equal(t, 3, attempts, "should succeed on 3rd attempt")
}

func TestSendWithRetry_ExhaustsAllAttempts(t *testing.T) {
	d := newTestDispatcher()
	attempts := 0
	d.senders["android"] = &mockPlatformSender{
		sendFunc: func(ctx context.Context, token, callID, callerURI, callerName string) error {
			attempts++
			return errors.New("persistent error")
		},
	}

	d.sendWithRetry(CallPush{
		Platform:  "android",
		Token:     "token",
		CallID:    "call-exhaust",
		DeviceID:  "device-1",
		CallerURI: "sip:caller",
	})

	assert.Equal(t, maxAttempts, attempts, "should exhaust all retry attempts")
}

func TestSendWithRetry_DeadToken(t *testing.T) {
	d := newTestDispatcher()
	d.senders["ios"] = &mockPlatformSender{
		sendFunc: func(ctx context.Context, token, callID, callerURI, callerName string) error {
			return ErrTokenInvalid
		},
	}

	var handlerCalls [][3]string
	d.OnDeadToken(func(call CallPush) {
		handlerCalls = append(handlerCalls, [3]string{call.Platform, call.Token, call.CallID})
	})

	d.sendWithRetry(CallPush{
		Platform:  "ios",
		Token:     "dead-ios-token",
		CallID:    "call-dead",
		DeviceID:  "device-1",
		CallerURI: "sip:caller",
	})

	assert.Len(t, handlerCalls, 1)
	assert.Equal(t, [3]string{"ios", "dead-ios-token", "call-dead"}, handlerCalls[0])

	d.cancelMu.Lock()
	_, exists := d.cancelFuncs["call-dead"]
	d.cancelMu.Unlock()
	assert.False(t, exists, "cancel func should be cleaned up after dead token")
}

func TestSendImmediate_IOS(t *testing.T) {
	d := newTestDispatcher()
	d.senders["ios"] = &mockPlatformSender{
		sendFunc: func(ctx context.Context, token, callID, callerURI, callerName string) error {
			assert.Equal(t, "ios-token", token)
			return nil
		},
	}

	err := d.sendImmediate(context.Background(), testCallPush("ios", "ios-token", "call-1", "sip:bob", "Bob"))
	assert.NoError(t, err)
}

func TestSendWithRetry_NilDeadTokenHandler(t *testing.T) {
	d := newTestDispatcher()
	d.senders["android"] = &mockPlatformSender{
		sendFunc: func(ctx context.Context, token, callID, callerURI, callerName string) error {
			return ErrTokenInvalid
		},
	}

	assert.NotPanics(t, func() {
		d.sendWithRetry(CallPush{
			Platform:  "android",
			Token:     "dead-token",
			CallID:    "call-nil-handler",
			DeviceID:  "device-1",
			CallerURI: "sip:caller",
		})
	})
}

func TestIsAPNsTokenInvalid(t *testing.T) {
	tests := []struct {
		name   string
		reason string
		want   bool
	}{
		{"bad device token", apns2.ReasonBadDeviceToken, true},
		{"token not for topic", apns2.ReasonDeviceTokenNotForTopic, true},
		{"unregistered", apns2.ReasonUnregistered, true},
		{"expired token", apns2.ReasonExpiredToken, true},
		{"missing device token", apns2.ReasonMissingDeviceToken, false},
		{"payload empty", apns2.ReasonPayloadEmpty, false},
		{"too many requests", apns2.ReasonTooManyRequests, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &apns2.Response{Reason: tt.reason}
			assert.Equal(t, tt.want, isAPNsTokenInvalid(resp))
		})
	}
}

func TestNormalizeToken(t *testing.T) {
	tests := []struct {
		name     string
		platform string
		input    string
		want     string
		wantErr  bool
	}{
		{name: "android remains opaque", platform: "android", input: "  opaque:FCM&token  ", want: "  opaque:FCM&token  "},
		{name: "raw iOS token", platform: "ios", input: "AABBCCDD", want: "aabbccdd"},
		{name: "Linphone VoIP suffix", platform: "ios", input: " AABBCCDD:voip ", want: "aabbccdd"},
		{name: "combined VoIP first", platform: "ios", input: "AABBCCDD:voip&11223344:remote", want: "aabbccdd"},
		{name: "combined VoIP second", platform: "ios", input: "11223344:remote&AABBCCDD:voip", want: "aabbccdd"},
		{name: "case insensitive suffix", platform: "IOS", input: "AABBCCDD:VoIP", want: "aabbccdd"},
		{name: "empty", platform: "ios", input: " ", wantErr: true},
		{name: "odd length", platform: "ios", input: "abc", wantErr: true},
		{name: "not hexadecimal", platform: "ios", input: "nothex", wantErr: true},
		{name: "remote only", platform: "ios", input: "11223344:remote", wantErr: true},
		{name: "combined without VoIP", platform: "ios", input: "11223344&55667788", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizeToken(tt.platform, tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Empty(t, got)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNewDispatcher_EmptyConfig(t *testing.T) {
	cfg := config.PushConfig{}
	d, err := NewDispatcher(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, d)
	assert.Empty(t, d.senders)
	assert.NotNil(t, d.queue)
	assert.NotNil(t, d.cancelFuncs)
}

type mockFCMClient struct {
	sendFunc func(ctx context.Context, msg *messaging.Message) (string, error)
}

func (m *mockFCMClient) Send(ctx context.Context, msg *messaging.Message) (string, error) {
	if m.sendFunc != nil {
		return m.sendFunc(ctx, msg)
	}
	return "msg-id-1", nil
}

func TestFCMSender_SendCallPush_Success(t *testing.T) {
	sender := newFCMSenderWithClient(&mockFCMClient{
		sendFunc: func(ctx context.Context, msg *messaging.Message) (string, error) {
			assert.Equal(t, "android-token", msg.Token)
			assert.Equal(t, "call-1", msg.Data["call-id"])
			assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", msg.Data["device-id"])
			assert.Equal(t, "sip:alice", msg.Data["caller-uri"])
			assert.Equal(t, "Alice", msg.Data["caller-name"])
			assert.Equal(t, "application/call-info", msg.Data["content-type"])
			assert.Equal(t, "high", msg.Android.Priority)
			return "projects/test/messages/msg-1", nil
		},
	})

	err := sender.SendCallPush(context.Background(), testCallPush("android", "android-token", "call-1", "sip:alice", "Alice"))
	assert.NoError(t, err)
}

func TestFCMSender_SendCallPush_UnregisteredToken(t *testing.T) {
	oldCheck := fcmIsTokenInvalid
	fcmIsTokenInvalid = func(err error) bool { return true }
	defer func() { fcmIsTokenInvalid = oldCheck }()

	sender := newFCMSenderWithClient(&mockFCMClient{
		sendFunc: func(ctx context.Context, msg *messaging.Message) (string, error) {
			return "", errors.New("token not registered")
		},
	})

	err := sender.SendCallPush(context.Background(), testCallPush("android", "dead-token", "call-1", "sip:alice", "Alice"))
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrTokenInvalid))
}

func TestFCMSender_SendCallPush_OtherError(t *testing.T) {
	sender := newFCMSenderWithClient(&mockFCMClient{
		sendFunc: func(ctx context.Context, msg *messaging.Message) (string, error) {
			return "", errors.New("network error")
		},
	})

	err := sender.SendCallPush(context.Background(), testCallPush("android", "token", "call-1", "sip:alice", "Alice"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fcm send")
}

type mockAPNsClient struct {
	pushFunc func(ctx apns2.Context, n *apns2.Notification) (*apns2.Response, error)
}

func (m *mockAPNsClient) PushWithContext(ctx apns2.Context, n *apns2.Notification) (*apns2.Response, error) {
	if m.pushFunc != nil {
		return m.pushFunc(ctx, n)
	}
	return &apns2.Response{StatusCode: 200, Reason: "OK", ApnsID: "apns-id-1"}, nil
}

func TestAPNsSender_SendCallPush_Success(t *testing.T) {
	sender := newAPNsSenderWithClient(&mockAPNsClient{
		pushFunc: func(ctx apns2.Context, n *apns2.Notification) (*apns2.Response, error) {
			assert.Equal(t, "aabbccdd", n.DeviceToken)
			assert.Equal(t, "com.test.voip", n.Topic)
			assert.Equal(t, apns2.PushTypeVOIP, n.PushType)
			assert.Equal(t, apns2.PriorityHigh, n.Priority)
			payloadJSON, err := json.Marshal(n.Payload)
			assert.NoError(t, err)
			assert.NotContains(t, string(payloadJSON), "aabbccdd")
			var data map[string]interface{}
			assert.NoError(t, json.Unmarshal(payloadJSON, &data))
			assert.Equal(t, "call-1", data["call-id"])
			assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", data["device-id"])
			assert.Equal(t, "sip:bob", data["caller-uri"])
			assert.Equal(t, "Bob", data["caller-name"])
			assert.Equal(t, "application/call-info", data["content-type"])
			aps, ok := data["aps"].(map[string]interface{})
			assert.True(t, ok)
			assert.Equal(t, float64(1), aps["content-available"])
			assert.Equal(t, "call-1", aps["call-id"])
			assert.NotContains(t, aps, "alert")
			assert.NotContains(t, aps, "sound")
			return &apns2.Response{StatusCode: 200, Reason: "OK", ApnsID: "apns-id-1"}, nil
		},
	}, "com.test")

	err := sender.SendCallPush(context.Background(), testCallPush("ios", " AABBCCDD:voip ", "call-1", "sip:bob", "Bob"))
	assert.NoError(t, err)
}

func TestAPNsSender_SendCallPush_TokenInvalid(t *testing.T) {
	sender := newAPNsSenderWithClient(&mockAPNsClient{
		pushFunc: func(ctx apns2.Context, n *apns2.Notification) (*apns2.Response, error) {
			return &apns2.Response{StatusCode: 410, Reason: apns2.ReasonUnregistered}, nil
		},
	}, "com.test")

	err := sender.SendCallPush(context.Background(), testCallPush("ios", "deadbeef", "call-1", "sip:bob", "Bob"))
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrTokenInvalid))
}

func TestAPNsSender_SendCallPush_Rejected(t *testing.T) {
	sender := newAPNsSenderWithClient(&mockAPNsClient{
		pushFunc: func(ctx apns2.Context, n *apns2.Notification) (*apns2.Response, error) {
			return &apns2.Response{StatusCode: 400, Reason: apns2.ReasonPayloadEmpty}, nil
		},
	}, "com.test")

	err := sender.SendCallPush(context.Background(), testCallPush("ios", "cafebabe", "call-1", "sip:bob", "Bob"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "apns rejected")
}

func TestAPNsSender_SendCallPush_NetworkError(t *testing.T) {
	sender := newAPNsSenderWithClient(&mockAPNsClient{
		pushFunc: func(ctx apns2.Context, n *apns2.Notification) (*apns2.Response, error) {
			return nil, errors.New("connection refused")
		},
	}, "com.test")

	err := sender.SendCallPush(context.Background(), testCallPush("ios", "feedface", "call-1", "sip:bob", "Bob"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "apns send")
}

func TestAPNsSender_SendCallPush_RejectsMalformedTokenBeforeSend(t *testing.T) {
	called := false
	sender := newAPNsSenderWithClient(&mockAPNsClient{
		pushFunc: func(ctx apns2.Context, n *apns2.Notification) (*apns2.Response, error) {
			called = true
			return &apns2.Response{StatusCode: 200}, nil
		},
	}, "com.test")

	err := sender.SendCallPush(context.Background(), testCallPush("ios", "not-a-token:remote", "call-1", "sip:bob", "Bob"))
	assert.ErrorContains(t, err, "apns token")
	assert.False(t, called)
}

func TestNewDispatcher_FCMFileNotFound(t *testing.T) {
	cfg := config.PushConfig{
		FCMServiceAccount: "/nonexistent/fcm-credentials.json",
	}
	d, err := NewDispatcher(cfg)
	assert.Nil(t, d)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "init fcm")
}

func TestNewDispatcher_APNsFileNotFound(t *testing.T) {
	cfg := config.PushConfig{
		APNsCert:     "/nonexistent/apns-cert.p12",
		APNsBundleID: "com.test",
	}
	d, err := NewDispatcher(cfg)
	assert.Error(t, err)
	assert.Nil(t, d)
	assert.ErrorContains(t, err, "init apns")
}

func TestStartAndWorker(t *testing.T) {
	d := &Dispatcher{
		senders:     map[string]platformSender{"android": &mockPlatformSender{}},
		queue:       make(chan CallPush, 10),
		workers:     5,
		cancelFuncs: make(map[string]context.CancelFunc),
	}

	ctx, cancel := context.WithCancel(context.Background())
	d.Start(ctx)

	err := d.Send(context.Background(), testCallPush("android", "token", "call-worker", "sip:caller", "Name"))
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, len(d.queue))

	cancel()
}

func TestStart_MultipleWorkers(t *testing.T) {
	d := &Dispatcher{
		senders:     map[string]platformSender{"android": &mockPlatformSender{}},
		queue:       make(chan CallPush, 100),
		workers:     10,
		cancelFuncs: make(map[string]context.CancelFunc),
	}

	ctx, cancel := context.WithCancel(context.Background())
	d.Start(ctx)

	for i := 0; i < 20; i++ {
		d.Send(context.Background(), testCallPush("android", "token", "call-"+string(rune('a'+i)), "sip:caller", "Name"))
	}

	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, 0, len(d.queue))

	cancel()
}

func TestNewDispatcher_WithFCMAndAPNs(t *testing.T) {
	oldFCM := newFCM
	oldAPNs := newAPNs
	defer func() { newFCM = oldFCM; newAPNs = oldAPNs }()

	mockFCM := newFCMSenderWithClient(&mockFCMClient{})
	mockAPNs := newAPNsSenderWithClient(&mockAPNsClient{}, "com.test")
	newFCM = func(string) (*FCMSender, error) { return mockFCM, nil }
	newAPNs = func(config.PushConfig) (*APNsSender, error) { return mockAPNs, nil }

	cfg := config.PushConfig{
		FCMServiceAccount: "/fake/creds.json",
		APNsCert:          "/fake/cert.p12",
		APNsBundleID:      "com.test",
	}

	d, err := NewDispatcher(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, d)
	assert.NotNil(t, d.senders["android"])
	assert.NotNil(t, d.senders["ios"])
}

func TestSendWithRetry_ContextExpiredBeforeSend(t *testing.T) {
	d := newTestDispatcher()
	d.timeout = time.Nanosecond
	d.senders["android"] = &mockPlatformSender{
		sendFunc: func(ctx context.Context, token, callID, callerURI, callerName string) error {
			return errors.New("transient")
		},
	}

	d.sendWithRetry(CallPush{
		Platform:  "android",
		Token:     "token",
		CallID:    "call-expired",
		DeviceID:  "device-1",
		CallerURI: "sip:caller",
	})
}

func TestNewFCMSender_Success(t *testing.T) {
	oldInit := initFCM
	initFCM = func(serviceAccountPath string) (fcmClient, error) {
		return &mockFCMClient{}, nil
	}
	defer func() { initFCM = oldInit }()

	s, err := NewFCMSender("/fake/creds.json")
	assert.NoError(t, err)
	assert.NotNil(t, s)
	assert.NotNil(t, s.client)
}

func TestNewAPNsSender_Success(t *testing.T) {
	oldInit := initAPNs
	initAPNs = func(cfg config.PushConfig) (apnsClient, error) {
		return &mockAPNsClient{}, nil
	}
	defer func() { initAPNs = oldInit }()

	s, err := NewAPNsSender(config.PushConfig{APNsCert: "/fake/cert.p12", APNsBundleID: "com.test"})
	assert.NoError(t, err)
	assert.NotNil(t, s)
	assert.Equal(t, "com.test", s.bundleID)
}

func TestNewAPNsSender_Production(t *testing.T) {
	oldInit := initAPNs
	initAPNs = func(cfg config.PushConfig) (apnsClient, error) {
		assert.True(t, cfg.APNsProduction)
		return &mockAPNsClient{}, nil
	}
	defer func() { initAPNs = oldInit }()

	s, err := NewAPNsSender(config.PushConfig{APNsCert: "/fake/cert.p12", APNsBundleID: "com.test", APNsProduction: true})
	assert.NoError(t, err)
	assert.NotNil(t, s)
}

func TestInitFCM_RealFile(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	assert.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})

	creds := map[string]string{
		"type":           "service_account",
		"project_id":     "test-project",
		"private_key_id": "test-key-id",
		"private_key":    string(keyPEM),
		"client_email":   "test@test-project.iam.gserviceaccount.com",
		"client_id":      "12345",
	}
	data, _ := json.Marshal(creds)
	tmpfile, _ := os.CreateTemp("", "fcm-*.json")
	tmpfile.Write(data)
	tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	result, err := initFCM(tmpfile.Name())
	t.Logf("initFCM with dummy creds: result=%v, err=%v", result, err)
}

func TestInitAPNs_RealFile(t *testing.T) {
	p12Path := "/tmp/test-cert.p12"
	if _, err := os.Stat(p12Path); os.IsNotExist(err) {
		t.Skip("test P12 file not found")
	}

	t.Run("development", func(t *testing.T) {
		client, err := initAPNs(config.PushConfig{APNsCert: p12Path, APNsBundleID: "com.test"})
		assert.NoError(t, err)
		assert.NotNil(t, client)
	})

	t.Run("production", func(t *testing.T) {
		client, err := initAPNs(config.PushConfig{APNsCert: p12Path, APNsBundleID: "com.test", APNsProduction: true})
		assert.NoError(t, err)
		assert.NotNil(t, client)
	})
}

func TestInitAPNs_ErrorPath(t *testing.T) {
	_, err := initAPNs(config.PushConfig{APNsCert: "/nonexistent/cert.p12", APNsBundleID: "com.test"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "load apns cert")
}

func TestInitAPNs_CertificateAuthentication(t *testing.T) {
	oldLoader := loadAPNsCertificate
	defer func() { loadAPNsCertificate = oldLoader }()
	loadAPNsCertificate = func(path, password string) (tls.Certificate, error) {
		assert.Equal(t, "/run/secrets/voip.p12", path)
		assert.Equal(t, "certificate-password", password)
		return tls.Certificate{Leaf: &x509.Certificate{
			NotBefore: time.Now().Add(-time.Hour),
			NotAfter:  time.Now().Add(time.Hour),
		}}, nil
	}

	client, err := initAPNs(config.PushConfig{
		APNsCert:         "/run/secrets/voip.p12",
		APNsCertPassword: "certificate-password",
		APNsBundleID:     "com.test",
	})
	assert.NoError(t, err)
	assert.NotNil(t, client)
	assert.Equal(t, apns2.HostDevelopment, client.(*apns2.Client).Host)
}

func TestInitAPNs_RejectsExpiredCertificate(t *testing.T) {
	oldLoader := loadAPNsCertificate
	defer func() { loadAPNsCertificate = oldLoader }()
	loadAPNsCertificate = func(string, string) (tls.Certificate, error) {
		return tls.Certificate{Leaf: &x509.Certificate{
			NotBefore: time.Now().Add(-2 * time.Hour),
			NotAfter:  time.Now().Add(-time.Hour),
		}}, nil
	}

	client, err := initAPNs(config.PushConfig{APNsCert: "voip.p12", APNsBundleID: "com.test"})
	assert.Nil(t, client)
	assert.ErrorContains(t, err, "expired")
}

func TestInitAPNs_TokenAuthentication(t *testing.T) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	assert.NoError(t, err)
	der, err := x509.MarshalPKCS8PrivateKey(privateKey)
	assert.NoError(t, err)
	keyPath := filepath.Join(t.TempDir(), "AuthKey_TEST.p8")
	assert.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600))

	client, err := initAPNs(config.PushConfig{
		APNsKey:        keyPath,
		APNsKeyID:      "KEYID12345",
		APNsTeamID:     "TEAMID1234",
		APNsBundleID:   "com.test",
		APNsProduction: true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, client)
	tokenClient := client.(*apns2.Client)
	assert.Equal(t, apns2.HostProduction, tokenClient.Host)
	assert.NotEmpty(t, tokenClient.Token.Bearer)
}

func TestInitFCM_ErrorPath(t *testing.T) {
	// garbage content that firebase.NewApp will reject
	tmpfile, _ := os.CreateTemp("", "fcm-bad-*.json")
	tmpfile.Write([]byte("not valid json"))
	tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	_, err := initFCM(tmpfile.Name())
	assert.Error(t, err)
}
