package callmanager

import (
	"context"
	"testing"

	"github.com/AlchemillaHQ/Sentry/db"
	"github.com/AlchemillaHQ/Sentry/push"
	"github.com/AlchemillaHQ/Sentry/sipstack"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockPushSender struct {
	mock.Mock
}

func (m *MockPushSender) Send(ctx context.Context, platform, token, callID, callerURI, callerName string) error {
	args := m.Called(ctx, platform, token, callID, callerURI, callerName)
	return args.Error(0)
}

func (m *MockPushSender) Start(ctx context.Context) {
	m.Called(ctx)
}

func (m *MockPushSender) CancelPush(callID string) {
	m.Called(callID)
}

func (m *MockPushSender) OnDeadToken(handler push.DeadTokenHandler) {
	m.Called(handler)
}

type MockRegistrar struct {
	mock.Mock
}

func (m *MockRegistrar) Register(ctx context.Context, reg *sipstack.UpstreamReg) error {
	return m.Called(ctx, reg).Error(0)
}
func (m *MockRegistrar) Unregister(ctx context.Context, deviceID string) error {
	return m.Called(ctx, deviceID).Error(0)
}
func (m *MockRegistrar) IsRegistered(deviceID string) bool {
	return m.Called(deviceID).Bool(0)
}
func (m *MockRegistrar) GetReg(deviceID string) *sipstack.UpstreamReg {
	return m.Called(deviceID).Get(0).(*sipstack.UpstreamReg)
}
func (m *MockRegistrar) StopAll() {
	m.Called()
}
func (m *MockRegistrar) UnregisterAll(ctx context.Context) {
	m.Called(ctx)
}

type MockQuerier struct {
	mock.Mock
}

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

func TestMatchDevice(t *testing.T) {
	ctx := context.Background()
	mockDB := new(MockQuerier)
	cm := &CallManager{
		dbQueries: mockDB,
	}

	sipUser := "7337"
	device := db.Device{
		B2buaSipUser: "7337_abcdefgh",
		DeviceID:     "device-123",
	}

	mockDB.On("GetDeviceByB2BUASIPUser", ctx, sipUser).Return(device, nil)

	matched, err := cm.matchDevice(ctx, sipUser)
	assert.NoError(t, err)
	assert.Equal(t, device.DeviceID, matched.DeviceID)
}
