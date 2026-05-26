package db

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/mock"
)

type MockQuerier struct {
	mock.Mock
}

func (m *MockQuerier) CreatePendingCall(ctx context.Context, arg CreatePendingCallParams) error {
	return m.Called(ctx, arg).Error(0)
}
func (m *MockQuerier) DeletePendingCall(ctx context.Context, callID string) error {
	return m.Called(ctx, callID).Error(0)
}
func (m *MockQuerier) GetDeviceByB2BUASIPUser(ctx context.Context, b2buaSipUser string) (Device, error) {
	args := m.Called(ctx, b2buaSipUser)
	return args.Get(0).(Device), args.Error(1)
}
func (m *MockQuerier) GetDeviceByID(ctx context.Context, deviceID string) (Device, error) {
	args := m.Called(ctx, deviceID)
	return args.Get(0).(Device), args.Error(1)
}
func (m *MockQuerier) GetDevicesByUpstreamUser(ctx context.Context, upstreamUser string) ([]Device, error) {
	args := m.Called(ctx, upstreamUser)
	return args.Get(0).([]Device), args.Error(1)
}
func (m *MockQuerier) GetPendingCall(ctx context.Context, callID string) (PendingCall, error) {
	args := m.Called(ctx, callID)
	return args.Get(0).(PendingCall), args.Error(1)
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
func (m *MockQuerier) UpdateDeviceContact(ctx context.Context, arg UpdateDeviceContactParams) error {
	return m.Called(ctx, arg).Error(0)
}
func (m *MockQuerier) UpdateDeviceLastSeen(ctx context.Context, b2buaSipUser string) error {
	return m.Called(ctx, b2buaSipUser).Error(0)
}
func (m *MockQuerier) UpdatePendingCallState(ctx context.Context, arg UpdatePendingCallStateParams) error {
	return m.Called(ctx, arg).Error(0)
}
func (m *MockQuerier) UpsertDevice(ctx context.Context, arg UpsertDeviceParams) error {
	return m.Called(ctx, arg).Error(0)
}
func (m *MockQuerier) UpsertSetting(ctx context.Context, arg UpsertSettingParams) error {
	return m.Called(ctx, arg).Error(0)
}
func (m *MockQuerier) CreateUser(ctx context.Context, arg CreateUserParams) error {
	return m.Called(ctx, arg).Error(0)
}
func (m *MockQuerier) DeleteUser(ctx context.Context, username string) error {
	return m.Called(ctx, username).Error(0)
}
func (m *MockQuerier) GetUser(ctx context.Context, username string) (User, error) {
	args := m.Called(ctx, username)
	return args.Get(0).(User), args.Error(1)
}
func (m *MockQuerier) ListUsers(ctx context.Context) ([]ListUsersRow, error) {
	args := m.Called(ctx)
	return args.Get(0).([]ListUsersRow), args.Error(1)
}
func (m *MockQuerier) UpdateUserPassword(ctx context.Context, arg UpdateUserPasswordParams) error {
	return m.Called(ctx, arg).Error(0)
}
func (m *MockQuerier) SetDeviceDisabled(ctx context.Context, arg SetDeviceDisabledParams) error {
	return m.Called(ctx, arg).Error(0)
}

func TestDatabase_Cleanup(t *testing.T) {
	mockQ := new(MockQuerier)
	db := &Database{Queries: mockQ}

	ctx := context.Background()
	mockQ.On("PruneDevices", ctx, mock.Anything).Return(nil)
	mockQ.On("PrunePendingCalls", ctx, mock.Anything).Return(nil)

	db.CleanupStaleState(ctx)

	mockQ.AssertExpectations(t)
}
