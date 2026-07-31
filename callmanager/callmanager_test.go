package callmanager

import (
	"context"
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"github.com/AlchemillaHQ/Sentry/db"
	"github.com/AlchemillaHQ/Sentry/push"
	"github.com/AlchemillaHQ/Sentry/secrets"
	"github.com/AlchemillaHQ/Sentry/sipstack"
	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockPushSender struct{ mock.Mock }

func (m *MockPushSender) Send(ctx context.Context, call push.CallPush) error {
	return m.Called(ctx, call).Error(0)
}
func (m *MockPushSender) Start(ctx context.Context)                 { m.Called(ctx) }
func (m *MockPushSender) CancelPush(callID string)                  { m.Called(callID) }
func (m *MockPushSender) OnDeadToken(handler push.DeadTokenHandler) { m.Called(handler) }

type MockRegistrar struct{ mock.Mock }

func (m *MockRegistrar) Register(ctx context.Context, reg *sipstack.UpstreamReg) error {
	return m.Called(ctx, reg).Error(0)
}
func (m *MockRegistrar) Unregister(ctx context.Context, deviceID string) error {
	return m.Called(ctx, deviceID).Error(0)
}
func (m *MockRegistrar) IsRegistered(deviceID string) bool { return m.Called(deviceID).Bool(0) }
func (m *MockRegistrar) GetReg(deviceID string) *sipstack.UpstreamReg {
	return m.Called(deviceID).Get(0).(*sipstack.UpstreamReg)
}
func (m *MockRegistrar) StopAll()                          { m.Called() }
func (m *MockRegistrar) UnregisterAll(ctx context.Context) { m.Called(ctx) }

type MockQuerier struct{ mock.Mock }

func (m *MockQuerier) CreatePendingCall(ctx context.Context, arg db.CreatePendingCallParams) error {
	return m.Called(ctx, arg).Error(0)
}
func (m *MockQuerier) DeletePendingCall(ctx context.Context, callID string) error {
	return m.Called(ctx, callID).Error(0)
}
func (m *MockQuerier) DeleteDeviceByID(ctx context.Context, deviceID string) error {
	return m.Called(ctx, deviceID).Error(0)
}
func (m *MockQuerier) GetDeviceByB2BUASIPUser(ctx context.Context, b2buaSipUser string) (db.Device, error) {
	args := m.Called(ctx, b2buaSipUser)
	return args.Get(0).(db.Device), args.Error(1)
}
func (m *MockQuerier) GetDeviceByID(ctx context.Context, deviceID string) (db.Device, error) {
	args := m.Called(ctx, deviceID)
	return args.Get(0).(db.Device), args.Error(1)
}
func (m *MockQuerier) GetDevicesByUpstreamUser(ctx context.Context, upstreamUser string) ([]db.Device, error) {
	args := m.Called(ctx, upstreamUser)
	return args.Get(0).([]db.Device), args.Error(1)
}
func (m *MockQuerier) GetPendingCall(ctx context.Context, callID string) (db.PendingCall, error) {
	args := m.Called(ctx, callID)
	return args.Get(0).(db.PendingCall), args.Error(1)
}
func (m *MockQuerier) GetSetting(ctx context.Context, key string) ([]byte, error) {
	args := m.Called(ctx, key)
	return args.Get(0).([]byte), args.Error(1)
}
func (m *MockQuerier) PruneDevices(ctx context.Context, expiresAt pgtype.Timestamptz) error {
	return m.Called(ctx, expiresAt).Error(0)
}
func (m *MockQuerier) PrunePendingCalls(ctx context.Context, expiresAt pgtype.Timestamptz) error {
	return m.Called(ctx, expiresAt).Error(0)
}
func (m *MockQuerier) RefreshDeviceExpiry(ctx context.Context, arg db.RefreshDeviceExpiryParams) error {
	return m.Called(ctx, arg).Error(0)
}
func (m *MockQuerier) UpdateDeviceContact(ctx context.Context, arg db.UpdateDeviceContactParams) error {
	return m.Called(ctx, arg).Error(0)
}
func (m *MockQuerier) UpdateDeviceLastSeen(ctx context.Context, b2buaSipUser string) error {
	return m.Called(ctx, b2buaSipUser).Error(0)
}
func (m *MockQuerier) UpdatePendingCallState(ctx context.Context, arg db.UpdatePendingCallStateParams) error {
	return m.Called(ctx, arg).Error(0)
}
func (m *MockQuerier) UpsertDevice(ctx context.Context, arg db.UpsertDeviceParams) error {
	return m.Called(ctx, arg).Error(0)
}
func (m *MockQuerier) CreateUser(ctx context.Context, arg db.CreateUserParams) error {
	return m.Called(ctx, arg).Error(0)
}
func (m *MockQuerier) DeleteUser(ctx context.Context, username string) error {
	return m.Called(ctx, username).Error(0)
}
func (m *MockQuerier) GetUser(ctx context.Context, username string) (db.User, error) {
	args := m.Called(ctx, username)
	return args.Get(0).(db.User), args.Error(1)
}
func (m *MockQuerier) ListUsers(ctx context.Context) ([]db.ListUsersRow, error) {
	args := m.Called(ctx)
	return args.Get(0).([]db.ListUsersRow), args.Error(1)
}
func (m *MockQuerier) UpdateUserPassword(ctx context.Context, arg db.UpdateUserPasswordParams) error {
	return m.Called(ctx, arg).Error(0)
}
func (m *MockQuerier) UpsertSetting(ctx context.Context, arg db.UpsertSettingParams) error {
	return m.Called(ctx, arg).Error(0)
}
func (m *MockQuerier) SetDeviceDisabled(ctx context.Context, arg db.SetDeviceDisabledParams) error {
	return m.Called(ctx, arg).Error(0)
}

func newTestBox(t *testing.T) *secrets.Box {
	key := make([]byte, 32)
	rand.Read(key)
	box, _ := secrets.NewBox(key)
	return box
}

type mockServerTx struct{ mock.Mock }

func (m *mockServerTx) Origin() *sip.Request                  { return nil }
func (m *mockServerTx) String() string                        { return "mock" }
func (m *mockServerTx) Errors() <-chan error                  { return nil }
func (m *mockServerTx) Done() <-chan struct{}                 { return nil }
func (m *mockServerTx) Terminate()                            {}
func (m *mockServerTx) OnTerminate(fn sip.FnTxTerminate) bool { return false }
func (m *mockServerTx) OnCancel(fn sip.FnTxCancel) bool       { return false }
func (m *mockServerTx) Err() error                            { return nil }
func (m *mockServerTx) Respond(res *sip.Response) error       { return m.Called(res).Error(0) }
func (m *mockServerTx) Acks() <-chan *sip.Request             { return nil }
func (m *mockServerTx) Cancels() <-chan *sip.Request          { return nil }

type mockDialogSrv struct{ mock.Mock }

func (m *mockDialogSrv) ReadAck(req *sip.Request, tx sip.ServerTransaction) error {
	return m.Called(req, tx).Error(0)
}
func (m *mockDialogSrv) ReadBye(req *sip.Request, tx sip.ServerTransaction) error {
	return m.Called(req, tx).Error(0)
}
func (m *mockDialogSrv) ReadInvite(req *sip.Request, tx sip.ServerTransaction) (serverSession, error) {
	args := m.Called(req, tx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(serverSession), args.Error(1)
}

type mockDialogCli struct{ mock.Mock }

func (m *mockDialogCli) ReadBye(req *sip.Request, tx sip.ServerTransaction) error {
	return m.Called(req, tx).Error(0)
}
func (m *mockDialogCli) Invite(ctx context.Context, recipient sip.Uri, body []byte, from *sip.FromHeader, contentType sip.Header) (clientSession, error) {
	args := m.Called(ctx, recipient, body, from, contentType)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(clientSession), args.Error(1)
}

type mockSrvSess struct {
	respond func(int, string, []byte) error
	bye     func(ctx context.Context) error
	ctx     func() context.Context
}

func (m *mockSrvSess) Respond(statusCode int, reason string, body []byte, headers ...sip.Header) error {
	return m.respond(statusCode, reason, body)
}
func (m *mockSrvSess) Close() error { return nil }
func (m *mockSrvSess) Bye(ctx context.Context) error {
	if m.bye != nil {
		return m.bye(ctx)
	}
	return nil
}
func (m *mockSrvSess) Context() context.Context {
	if m.ctx != nil {
		return m.ctx()
	}
	return context.Background()
}

type mockCliSess struct {
	waitAnswer func(ctx context.Context, opts sipgo.AnswerOptions) error
	ack        func(ctx context.Context) error
	bye        func(ctx context.Context) error
	close      func() error
	ctx        func() context.Context
	inviteResp func() *sip.Response
	inviteReq  func() *sip.Request
	do         func(ctx context.Context, req *sip.Request) (*sip.Response, error)
}

func (m *mockCliSess) WaitAnswer(ctx context.Context, opts sipgo.AnswerOptions) error {
	return m.waitAnswer(ctx, opts)
}
func (m *mockCliSess) Ack(ctx context.Context) error { return m.ack(ctx) }
func (m *mockCliSess) Bye(ctx context.Context) error { return m.bye(ctx) }
func (m *mockCliSess) Close() error                  { return m.close() }
func (m *mockCliSess) Context() context.Context      { return m.ctx() }
func (m *mockCliSess) InviteResponse() *sip.Response { return m.inviteResp() }
func (m *mockCliSess) InviteRequest() *sip.Request   { return m.inviteReq() }
func (m *mockCliSess) Do(ctx context.Context, req *sip.Request) (*sip.Response, error) {
	return m.do(ctx, req)
}

func TestHandleRegister_SuspendedDeviceIsRejected(t *testing.T) {
	mockDB := new(MockQuerier)
	device := db.Device{B2buaSipUser: "7337_abcdefgh", DeviceID: "dev"}
	mockDB.On("GetDeviceByB2BUASIPUser", mock.Anything, device.B2buaSipUser).Return(device, nil)

	cm := &CallManager{
		dbQueries: mockDB,
		suspended: map[string]struct{}{"dev": {}},
	}
	req := sip.NewRequest(sip.REGISTER, sip.Uri{User: device.B2buaSipUser, Host: "sentry.example.com"})
	req.AppendHeader(&sip.ToHeader{Address: sip.Uri{User: device.B2buaSipUser, Host: "sentry.example.com"}})
	tx := new(mockServerTx)
	tx.On("Respond", mock.MatchedBy(func(response *sip.Response) bool {
		return response.StatusCode == sip.StatusForbidden
	})).Return(nil)

	cm.handleRegister(req, tx)

	tx.AssertExpectations(t)
}

func TestHandleInvite_DisabledDevice(t *testing.T) {
	mockDB := new(MockQuerier)
	mockSrv := new(mockDialogSrv)
	ss := &mockSrvSess{respond: func(int, string, []byte) error { return nil }}

	device := db.Device{B2buaSipUser: "7337_abcdefgh", DeviceID: "dev", Platform: "android", Disabled: true}
	mockDB.On("GetDeviceByB2BUASIPUser", mock.Anything, "7337").Return(device, nil)
	mockSrv.On("ReadInvite", mock.Anything, mock.Anything).Return(serverSession(ss), nil)

	cm := &CallManager{dbQueries: mockDB, dialogSrv: mockSrv, pending: make(map[string]*pendingCall)}
	req := sip.NewRequest(sip.INVITE, sip.Uri{User: "7337", Host: "pbx.com"})
	req.AppendHeader(&sip.ToHeader{Address: sip.Uri{User: "7337", Host: "pbx.com"}})
	req.AppendHeader(&sip.FromHeader{DisplayName: "A", Address: sip.Uri{User: "a", Host: "h"}})
	cid := sip.CallIDHeader("inv-1")
	req.AppendHeader(&cid)
	tx := new(mockServerTx)
	tx.On("Respond", mock.Anything).Return(nil)

	cm.handleInvite(req, tx)
	mockSrv.AssertCalled(t, "ReadInvite", mock.Anything, mock.Anything)
}

func TestHandleInvite_SuspendedAfterLookupIsRejected(t *testing.T) {
	mockDB := new(MockQuerier)
	mockSrv := new(mockDialogSrv)
	statuses := make([]int, 0)
	ss := &mockSrvSess{respond: func(status int, _ string, _ []byte) error {
		statuses = append(statuses, status)
		return nil
	}}

	device := db.Device{B2buaSipUser: "7337_abcdefgh", DeviceID: "dev", Platform: "android"}
	mockDB.On("GetDeviceByB2BUASIPUser", mock.Anything, "7337").Return(device, nil)
	mockSrv.On("ReadInvite", mock.Anything, mock.Anything).Return(serverSession(ss), nil)

	cm := &CallManager{
		dbQueries: mockDB,
		dialogSrv: mockSrv,
		pending:   make(map[string]*pendingCall),
		suspended: map[string]struct{}{"dev": {}},
	}
	req := sip.NewRequest(sip.INVITE, sip.Uri{User: "7337", Host: "pbx.com"})
	req.AppendHeader(&sip.ToHeader{Address: sip.Uri{User: "7337", Host: "pbx.com"}})
	req.AppendHeader(&sip.FromHeader{DisplayName: "A", Address: sip.Uri{User: "a", Host: "h"}})
	cid := sip.CallIDHeader("inv-suspended")
	req.AppendHeader(&cid)
	tx := new(mockServerTx)

	cm.handleInvite(req, tx)

	assert.Contains(t, statuses, sip.StatusTemporarilyUnavailable)
	assert.Empty(t, cm.pending)
}

func TestHandleInvite_Timeout(t *testing.T) {
	oldTimeout := callTimeout
	callTimeout = 50 * time.Millisecond
	defer func() { callTimeout = oldTimeout }()

	box := newTestBox(t)
	encToken, _ := box.Encrypt([]byte("device-token"))
	mockDB := new(MockQuerier)
	mockPush := new(MockPushSender)
	mockSrv := new(mockDialogSrv)
	ss := &mockSrvSess{respond: func(int, string, []byte) error { return nil }}

	device := db.Device{B2buaSipUser: "7337_abcdefgh", DeviceID: "dev", Platform: "android", PushToken: encToken}
	mockDB.On("GetDeviceByB2BUASIPUser", mock.Anything, "7337").Return(device, nil)
	mockDB.On("CreatePendingCall", mock.Anything, mock.Anything).Return(nil)
	mockDB.On("UpdatePendingCallState", mock.Anything, mock.Anything).Return(nil)
	mockPush.On("Send", mock.Anything, mock.MatchedBy(func(call push.CallPush) bool {
		return call.Platform == "android" && call.DeviceID == "dev" && call.CallID != ""
	})).Return(nil)
	mockPush.On("CancelPush", mock.Anything).Return()
	mockSrv.On("ReadInvite", mock.Anything, mock.Anything).Return(serverSession(ss), nil)

	cm := &CallManager{dbQueries: mockDB, box: box, pushSender: mockPush, dialogSrv: mockSrv, pending: make(map[string]*pendingCall)}
	req := sip.NewRequest(sip.INVITE, sip.Uri{User: "7337", Host: "pbx.com"})
	req.AppendHeader(&sip.ToHeader{Address: sip.Uri{User: "7337", Host: "pbx.com"}})
	req.AppendHeader(&sip.FromHeader{DisplayName: "Alice", Address: sip.Uri{User: "alice", Host: "pbx.com"}})
	cid := sip.CallIDHeader("inv-t")
	req.AppendHeader(&cid)
	tx := new(mockServerTx)
	tx.On("Respond", mock.Anything).Return(nil)

	cm.handleInvite(req, tx)
	mockSrv.AssertCalled(t, "ReadInvite", mock.Anything, mock.Anything)
}

func TestHandleInvite_UnknownDevice(t *testing.T) {
	mockDB := new(MockQuerier)
	mockSrv := new(mockDialogSrv)
	ss := &mockSrvSess{respond: func(int, string, []byte) error { return nil }}
	mockDB.On("GetDeviceByB2BUASIPUser", mock.Anything, "7337").Return(db.Device{}, pgx.ErrNoRows)
	mockDB.On("GetDevicesByUpstreamUser", mock.Anything, "7337").Return([]db.Device{}, nil)
	mockSrv.On("ReadInvite", mock.Anything, mock.Anything).Return(serverSession(ss), nil)

	cm := &CallManager{dbQueries: mockDB, dialogSrv: mockSrv, pending: make(map[string]*pendingCall)}
	req := sip.NewRequest(sip.INVITE, sip.Uri{User: "7337", Host: "pbx.com"})
	req.AppendHeader(&sip.ToHeader{Address: sip.Uri{User: "7337", Host: "pbx.com"}})
	req.AppendHeader(&sip.FromHeader{DisplayName: "A", Address: sip.Uri{User: "a", Host: "h"}})
	cid := sip.CallIDHeader("inv-u")
	req.AppendHeader(&cid)
	tx := new(mockServerTx)
	tx.On("Respond", mock.Anything).Return(nil)

	cm.handleInvite(req, tx)
	mockSrv.AssertCalled(t, "ReadInvite", mock.Anything, mock.Anything)
}

func TestRelayCall_NoDeviceSource(t *testing.T) {
	srvSess := &mockSrvSess{respond: func(int, string, []byte) error { return nil }}
	ctx, cancel := context.WithCancel(context.Background())
	pc := &pendingCall{
		id: "relay-1", sipUser: "7337_abcdefgh", serverDlg: srvSess,
		sdpOffer: []byte("v=0"), callerUser: "alice", callerHost: "pbx.com",
		callerName: "Alice", ctx: ctx, cancel: func() {},
	}
	mockDB := new(MockQuerier)
	mockDB.On("UpdatePendingCallState", mock.Anything, mock.Anything).Return(nil)
	cm := &CallManager{deviceSource: make(map[string]sip.Uri), dbQueries: mockDB}
	cm.stack = &sipstack.Stack{}

	cm.relayCall(ctx, pc, &db.Device{B2buaSipUser: "7337_abcdefgh", DeviceID: "dev"})
	cancel()
}

func TestHandleAck_Success(t *testing.T) {
	mockSrv := new(mockDialogSrv)
	mockSrv.On("ReadAck", mock.Anything, mock.Anything).Return(nil)
	cm := &CallManager{dialogSrv: mockSrv}
	req := sip.NewRequest(sip.ACK, sip.Uri{Host: "test"})
	cid := sip.CallIDHeader("ack-1")
	req.AppendHeader(&cid)
	tx := new(mockServerTx)
	cm.handleAck(req, tx)
	mockSrv.AssertCalled(t, "ReadAck", mock.Anything, mock.Anything)
}

func TestHandleAck_Error(t *testing.T) {
	mockSrv := new(mockDialogSrv)
	mockSrv.On("ReadAck", mock.Anything, mock.Anything).Return(assert.AnError)
	cm := &CallManager{dialogSrv: mockSrv}
	req := sip.NewRequest(sip.ACK, sip.Uri{Host: "test"})
	cid := sip.CallIDHeader("ack-2")
	req.AppendHeader(&cid)
	tx := new(mockServerTx)
	cm.handleAck(req, tx)
	mockSrv.AssertCalled(t, "ReadAck", mock.Anything, mock.Anything)
}

func TestHandleBye_ServerSide(t *testing.T) {
	mockSrv := new(mockDialogSrv)
	mockSrv.On("ReadBye", mock.Anything, mock.Anything).Return(nil)
	mockCli := new(mockDialogCli)
	mockCli.On("ReadBye", mock.Anything, mock.Anything).Return(nil)
	cm := &CallManager{
		dialogSrv: mockSrv, dialogCli: mockCli,
		pending: map[string]*pendingCall{"c1": {id: "c1", callID: "bye-1", ctx: context.Background(), cancel: func() {}}},
	}
	req := sip.NewRequest(sip.BYE, sip.Uri{Host: "test"})
	cid := sip.CallIDHeader("bye-1")
	req.AppendHeader(&cid)
	tx := new(mockServerTx)
	cm.handleBye(req, tx)
	mockSrv.AssertCalled(t, "ReadBye", mock.Anything, mock.Anything)
}

func TestHandleBye_ClientSide(t *testing.T) {
	mockSrv := new(mockDialogSrv)
	mockSrv.On("ReadBye", mock.Anything, mock.Anything).Return(assert.AnError)
	mockCli := new(mockDialogCli)
	mockCli.On("ReadBye", mock.Anything, mock.Anything).Return(nil)
	cm := &CallManager{dialogSrv: mockSrv, dialogCli: mockCli}
	req := sip.NewRequest(sip.BYE, sip.Uri{Host: "test"})
	cid := sip.CallIDHeader("bye-2")
	req.AppendHeader(&cid)
	tx := new(mockServerTx)
	cm.handleBye(req, tx)
	mockCli.AssertCalled(t, "ReadBye", mock.Anything, mock.Anything)
}

func TestHandleBye_UnknownDialog(t *testing.T) {
	mockSrv := new(mockDialogSrv)
	mockSrv.On("ReadBye", mock.Anything, mock.Anything).Return(assert.AnError)
	mockCli := new(mockDialogCli)
	mockCli.On("ReadBye", mock.Anything, mock.Anything).Return(assert.AnError)
	cm := &CallManager{dialogSrv: mockSrv, dialogCli: mockCli}
	req := sip.NewRequest(sip.BYE, sip.Uri{Host: "test"})
	cid := sip.CallIDHeader("bye-3")
	req.AppendHeader(&cid)
	tx := new(mockServerTx)
	tx.On("Respond", mock.Anything).Return(nil)
	cm.handleBye(req, tx)
	tx.AssertCalled(t, "Respond", mock.Anything)
}

func TestHandleCancel(t *testing.T) {
	ua, _ := sipgo.NewUA(sipgo.WithUserAgent("test"))
	defer ua.Close()
	client, _ := sipgo.NewClient(ua)
	cancelled := false
	pc := &pendingCall{id: "c1", callID: "cancel-1", cancel: func() { cancelled = true }, sipUser: "u"}
	mockPush := new(MockPushSender)
	mockPush.On("CancelPush", "c1").Return()
	mockDB := new(MockQuerier)
	mockDB.On("UpdatePendingCallState", mock.Anything, db.UpdatePendingCallStateParams{
		CallID: "c1",
		State:  "CANCELLED",
	}).Return(nil)
	cm := &CallManager{
		dbQueries:  mockDB,
		pending:    map[string]*pendingCall{"c1": pc},
		pushSender: mockPush,
		dialogSrv:  &dialogSrvAdapter{cache: sipgo.NewDialogServerCache(client, sip.ContactHeader{Address: sip.Uri{Host: "10.0.0.1", Port: 5060}})},
		dialogCli:  &dialogCliAdapter{cache: sipgo.NewDialogClientCache(client, sip.ContactHeader{Address: sip.Uri{Host: "10.0.0.1", Port: 5060}})},
	}
	req := sip.NewRequest(sip.CANCEL, sip.Uri{Host: "test"})
	cid := sip.CallIDHeader("cancel-1")
	req.AppendHeader(&cid)
	tx := new(mockServerTx)
	tx.On("Respond", mock.Anything).Return(nil)
	cm.handleCancel(req, tx)
	mockPush.AssertCalled(t, "CancelPush", "c1")
	assert.True(t, cancelled)
	tx.AssertCalled(t, "Respond", mock.Anything)
}

func TestHandleCancel_UnknownCall(t *testing.T) {
	cm := &CallManager{
		pending:   make(map[string]*pendingCall),
		dialogSrv: &dialogSrvAdapter{cache: sipgo.NewDialogServerCache(nil, sip.ContactHeader{})},
		dialogCli: &dialogCliAdapter{cache: sipgo.NewDialogClientCache(nil, sip.ContactHeader{})},
	}
	req := sip.NewRequest(sip.CANCEL, sip.Uri{Host: "test"})
	cid := sip.CallIDHeader("unknown")
	req.AppendHeader(&cid)
	tx := new(mockServerTx)
	tx.On("Respond", mock.Anything).Return(nil)
	cm.handleCancel(req, tx)
	tx.AssertCalled(t, "Respond", mock.Anything)
}

func TestSendByeToAllBridgedCalls(t *testing.T) {
	called := false
	srv := &mockSrvSess{bye: func(ctx context.Context) error { called = true; return nil }}
	cm := &CallManager{
		pending: map[string]*pendingCall{
			"c1": {id: "c1", serverDlg: srv, clientDlg: nil, clientDlgMu: sync.Mutex{}, ctx: context.Background(), cancel: func() {}},
		},
	}
	cm.SendByeToAllBridgedCalls(context.Background())
	assert.True(t, called)
}

func TestHandleCancel_WithClientDialog(t *testing.T) {
	ua, _ := sipgo.NewUA(sipgo.WithUserAgent("test"))
	defer ua.Close()
	client, _ := sipgo.NewClient(ua)
	cli := &mockCliSess{bye: func(ctx context.Context) error { return nil }}
	pc := &pendingCall{id: "c1", callID: "cancel-cli", clientDlg: cli, clientDlgMu: sync.Mutex{}, cancel: func() {}, sipUser: "u"}
	mockPush := new(MockPushSender)
	mockPush.On("CancelPush", "c1").Return()
	mockDB := new(MockQuerier)
	mockDB.On("UpdatePendingCallState", mock.Anything, mock.Anything).Return(nil)
	cm := &CallManager{
		dbQueries:  mockDB,
		pending:    map[string]*pendingCall{"c1": pc},
		pushSender: mockPush,
		dialogSrv:  &dialogSrvAdapter{cache: sipgo.NewDialogServerCache(client, sip.ContactHeader{Address: sip.Uri{Host: "10.0.0.1", Port: 5060}})},
		dialogCli:  &dialogCliAdapter{cache: sipgo.NewDialogClientCache(client, sip.ContactHeader{Address: sip.Uri{Host: "10.0.0.1", Port: 5060}})},
	}
	req := sip.NewRequest(sip.CANCEL, sip.Uri{Host: "test"})
	cid := sip.CallIDHeader("cancel-cli")
	req.AppendHeader(&cid)
	tx := new(mockServerTx)
	tx.On("Respond", mock.Anything).Return(nil)
	cm.handleCancel(req, tx)
	mockPush.AssertCalled(t, "CancelPush", "c1")
	tx.AssertCalled(t, "Respond", mock.Anything)
}

func TestMatchDevice(t *testing.T) {
	ctx := context.Background()
	mockDB := new(MockQuerier)
	cm := &CallManager{dbQueries: mockDB}
	device := db.Device{B2buaSipUser: "7337_abcdefgh", DeviceID: "device-123"}
	mockDB.On("GetDeviceByB2BUASIPUser", ctx, "7337").Return(device, nil)
	matched, err := cm.matchDevice(ctx, "7337")
	assert.NoError(t, err)
	assert.Equal(t, device.DeviceID, matched.DeviceID)
}

func TestMatchDevice_FallbackToUpstreamUser(t *testing.T) {
	ctx := context.Background()
	mockDB := new(MockQuerier)
	cm := &CallManager{dbQueries: mockDB, pending: make(map[string]*pendingCall)}
	devices := []db.Device{{B2buaSipUser: "john_a", DeviceID: "d1"}}
	mockDB.On("GetDeviceByB2BUASIPUser", ctx, "john").Return(db.Device{}, pgx.ErrNoRows)
	mockDB.On("GetDevicesByUpstreamUser", ctx, "john").Return(devices, nil)
	matched, err := cm.matchDevice(ctx, "john")
	assert.NoError(t, err)
	assert.Equal(t, "d1", matched.DeviceID)
}

func TestMatchDevice_RejectsAmbiguousFallback(t *testing.T) {
	ctx := context.Background()
	mockDB := new(MockQuerier)
	cm := &CallManager{dbQueries: mockDB}
	devices := []db.Device{
		{B2buaSipUser: "john_a", DeviceID: "d1"},
		{B2buaSipUser: "john_b", DeviceID: "d2"},
	}
	mockDB.On("GetDeviceByB2BUASIPUser", ctx, "john").Return(db.Device{}, pgx.ErrNoRows)
	mockDB.On("GetDevicesByUpstreamUser", ctx, "john").Return(devices, nil)

	matched, err := cm.matchDevice(ctx, "john")

	assert.Nil(t, matched)
	assert.ErrorIs(t, err, errAmbiguousDevice)
}

func TestIsPrivateIP(t *testing.T) {
	assert.True(t, isPrivateIP("127.0.0.1"))
	assert.True(t, isPrivateIP("10.0.0.1"))
	assert.True(t, isPrivateIP("192.168.1.1"))
	assert.False(t, isPrivateIP("8.8.8.8"))
	assert.False(t, isPrivateIP("not-an-ip"))
}

func TestNew(t *testing.T) {
	ua, _ := sipgo.NewUA(sipgo.WithUserAgent("test"))
	defer ua.Close()
	stack := &sipstack.Stack{}
	mockPush := new(MockPushSender)
	database := &db.Database{Queries: new(MockQuerier)}
	cm := New(database, stack, nil, mockPush, nil)
	assert.NotNil(t, cm)
	assert.NotNil(t, cm.dialogSrv)
	assert.NotNil(t, cm.dialogCli)
	assert.NotNil(t, cm.pending)
	assert.NotNil(t, cm.suspended)
}

func TestCleanup(t *testing.T) {
	mockPush := new(MockPushSender)
	mockPush.On("CancelPush", "call-1").Return()
	cm := &CallManager{pending: make(map[string]*pendingCall), pushSender: mockPush}
	pc := &pendingCall{id: "call-1"}
	pc.ctx, pc.cancel = context.WithCancel(context.Background())
	cm.pending["call-1"] = pc
	cm.cleanup("call-1")
	cm.mu.RLock()
	_, exists := cm.pending["call-1"]
	cm.mu.RUnlock()
	assert.False(t, exists)
	mockPush.AssertCalled(t, "CancelPush", "call-1")
}

func TestSuspendDeviceCancelsOnlyUnbridgedCallsForDevice(t *testing.T) {
	mockDB := new(MockQuerier)
	mockPush := new(MockPushSender)
	mockPush.On("CancelPush", "call-a").Return()
	mockDB.On("UpdatePendingCallState", mock.Anything, db.UpdatePendingCallStateParams{
		CallID: "call-a",
		State:  "DEVICE_DISABLED",
	}).Return(nil)

	cancelledA := false
	cancelledB := false
	serverA := &mockSrvSess{respond: func(status int, _ string, _ []byte) error {
		assert.Equal(t, sip.StatusTemporarilyUnavailable, status)
		return nil
	}}
	cm := &CallManager{
		dbQueries:  mockDB,
		pushSender: mockPush,
		pending: map[string]*pendingCall{
			"call-a": {
				id:        "call-a",
				deviceID:  "device-a",
				serverDlg: serverA,
				cancel:    func() { cancelledA = true },
			},
			"call-b": {
				id:       "call-b",
				deviceID: "device-b",
				cancel:   func() { cancelledB = true },
			},
		},
		deviceSource: map[string]sip.Uri{
			"shadow-a": {User: "shadow-a", Host: "10.0.0.1"},
			"shadow-b": {User: "shadow-b", Host: "10.0.0.2"},
		},
	}

	cm.SuspendDevice("device-a", "shadow-a")

	assert.True(t, cancelledA)
	assert.False(t, cancelledB)
	_, sourceAExists := cm.deviceSource["shadow-a"]
	_, sourceBExists := cm.deviceSource["shadow-b"]
	_, deviceSuspended := cm.suspended["device-a"]
	assert.False(t, sourceAExists)
	assert.True(t, sourceBExists)
	assert.True(t, deviceSuspended)
	mockPush.AssertNotCalled(t, "CancelPush", "call-b")

	cm.ResumeDevice("device-a")
	_, deviceSuspended = cm.suspended["device-a"]
	assert.False(t, deviceSuspended)
}

func TestIsBanned(t *testing.T) {
	cm := &CallManager{banlist: make(map[string]time.Time)}
	assert.False(t, cm.isBanned("1.2.3.4"))
	cm.banlistMu.Lock()
	cm.banlist["1.2.3.4"] = time.Now().Add(time.Hour)
	cm.banlistMu.Unlock()
	assert.True(t, cm.isBanned("1.2.3.4"))
}

func TestRecordReject(t *testing.T) {
	cm := &CallManager{banlist: make(map[string]time.Time), failCounts: make(map[string]int)}
	cm.recordReject("127.0.0.1")
	cm.failMu.Lock()
	_, exists := cm.failCounts["127.0.0.1"]
	cm.failMu.Unlock()
	assert.False(t, exists)
	cm.recordReject("1.2.3.4")
	cm.failMu.Lock()
	assert.Equal(t, 1, cm.failCounts["1.2.3.4"])
	cm.failMu.Unlock()
}

func TestClearFailures(t *testing.T) {
	cm := &CallManager{failCounts: map[string]int{"1.2.3.4": 7}}
	cm.clearFailures("1.2.3.4")
	cm.failMu.Lock()
	_, exists := cm.failCounts["1.2.3.4"]
	cm.failMu.Unlock()
	assert.False(t, exists)
}

func TestAllowRejectLog(t *testing.T) {
	cm := &CallManager{rejectThrottle: make(map[string]time.Time)}
	assert.True(t, cm.allowRejectLog("user1"))
	assert.False(t, cm.allowRejectLog("user1"))
	assert.True(t, cm.allowRejectLog("user2"))
}

func TestPruneStaleState(t *testing.T) {
	cm := &CallManager{
		rejectThrottle: make(map[string]time.Time), banlist: make(map[string]time.Time), failCounts: make(map[string]int),
	}
	cm.rejectThrottle["fresh"] = time.Now()
	cm.rejectThrottle["stale"] = time.Now().Add(-10 * time.Minute)
	cm.banlistMu.Lock()
	cm.banlist["expired"] = time.Now().Add(-time.Second)
	cm.banlistMu.Unlock()
	cm.failMu.Lock()
	cm.failCounts["at_threshold"] = maxRejectsBeforeBan
	cm.failMu.Unlock()
	cm.pruneStaleState()
	cm.rejectThrottleMu.Lock()
	_, exists := cm.rejectThrottle["stale"]
	cm.rejectThrottleMu.Unlock()
	assert.False(t, exists)
	cm.banlistMu.Lock()
	_, exists = cm.banlist["expired"]
	cm.banlistMu.Unlock()
	assert.False(t, exists)
	cm.failMu.Lock()
	_, exists = cm.failCounts["at_threshold"]
	cm.failMu.Unlock()
	assert.False(t, exists)
}

func TestGetPendingCallsCount(t *testing.T) {
	cm := &CallManager{pending: make(map[string]*pendingCall)}
	assert.Equal(t, 0, cm.GetPendingCallsCount())
}

func TestRemoveDeviceSource(t *testing.T) {
	cm := &CallManager{deviceSource: map[string]sip.Uri{"u1": {Host: "h"}}}
	cm.RemoveDeviceSource("u1")
	cm.mu.RLock()
	_, exists := cm.deviceSource["u1"]
	cm.mu.RUnlock()
	assert.False(t, exists)
}

func TestUpdateDeviceFromContact(t *testing.T) {
	ctx := context.Background()
	box := newTestBox(t)
	mockDB := new(MockQuerier)
	device := db.Device{
		B2buaSipUser: "7337_abcdefgh", DeviceID: "d", Platform: "android",
		UpstreamHost: "h", UpstreamPort: 5060, UpstreamTransport: "u",
		UpstreamUser: "7337", UpstreamPassword: []byte("p"),
		UpstreamRealm: pgtype.Text{String: "r", Valid: true},
		PushProvider:  pgtype.Text{String: "f", Valid: true},
		PushParam:     pgtype.Text{String: "p", Valid: true},
		PushPrid:      pgtype.Text{String: "p", Valid: true},
		PushToken:     []byte("t"),
	}
	mockDB.On("GetDeviceByB2BUASIPUser", ctx, "7337_abcdefgh").Return(device, nil)
	mockDB.On("UpsertDevice", ctx, mock.AnythingOfType("db.UpsertDeviceParams")).Return(nil)
	cp := sip.NewParams()
	cp.Add("pn-provider", "a")
	cp.Add("pn-param", "b")
	cp.Add("pn-prid", "c")
	contact := &sip.ContactHeader{Address: sip.Uri{User: "7337_abcdefgh", Host: "10.0.0.1", Port: 5060, UriParams: cp}}
	cm := &CallManager{dbQueries: mockDB, box: box}
	cm.updateDeviceFromContact(ctx, "7337_abcdefgh", contact, "TestAgent")
	mockDB.AssertCalled(t, "UpsertDevice", ctx, mock.AnythingOfType("db.UpsertDeviceParams"))
}
