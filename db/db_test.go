package db

import (
	"context"
	"testing"
	"time"

	"github.com/AlchemillaHQ/Sentry/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockQuerier implements Querier for testing
type MockQuerier struct {
	mock.Mock
}

func (m *MockQuerier) CreatePendingCall(ctx context.Context, arg CreatePendingCallParams) error {
	return m.Called(ctx, arg).Error(0)
}
func (m *MockQuerier) DeletePendingCall(ctx context.Context, callID string) error {
	return m.Called(ctx, callID).Error(0)
}
func (m *MockQuerier) DeleteDeviceByID(ctx context.Context, deviceID string) error {
	return m.Called(ctx, deviceID).Error(0)
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
func (m *MockQuerier) PruneDevices(ctx context.Context, expiresAt pgtype.Timestamptz) ([]PruneDevicesRow, error) {
	args := m.Called(ctx, expiresAt)
	rows, _ := args.Get(0).([]PruneDevicesRow)
	return rows, args.Error(1)
}
func (m *MockQuerier) PrunePendingCalls(ctx context.Context, expiresAt pgtype.Timestamptz) error {
	return m.Called(ctx, expiresAt).Error(0)
}
func (m *MockQuerier) RefreshDeviceExpiry(ctx context.Context, arg RefreshDeviceExpiryParams) (int64, error) {
	args := m.Called(ctx, arg)
	return args.Get(0).(int64), args.Error(1)
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

// MockDBTX implements DBTX for testing pool operations
type MockDBTX struct {
	mock.Mock
}

func (m *MockDBTX) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	callArgs := make([]interface{}, 0, len(args)+2)
	callArgs = append(callArgs, ctx, sql)
	callArgs = append(callArgs, args...)
	result := m.Called(callArgs...)
	return result.Get(0).(pgconn.CommandTag), result.Error(1)
}

func (m *MockDBTX) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	callArgs := make([]interface{}, 0, len(args)+2)
	callArgs = append(callArgs, ctx, sql)
	callArgs = append(callArgs, args...)
	result := m.Called(callArgs...)
	return result.Get(0).(pgx.Rows), result.Error(1)
}

func (m *MockDBTX) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	callArgs := make([]interface{}, 0, len(args)+2)
	callArgs = append(callArgs, ctx, sql)
	callArgs = append(callArgs, args...)
	result := m.Called(callArgs...)
	return result.Get(0).(pgx.Row)
}

// --------------- CleanupStaleState tests ---------------

func TestCleanupStaleState_Success(t *testing.T) {
	mockQ := new(MockQuerier)
	db := &Database{Queries: mockQ}

	ctx := context.Background()
	pruned := []PruneDevicesRow{{DeviceID: "expired-device", B2buaSipUser: "1001_expired"}}
	mockQ.On("PruneDevices", ctx, mock.Anything).Return(pruned, nil)
	mockQ.On("PrunePendingCalls", ctx, mock.Anything).Return(nil)

	result := db.CleanupStaleState(ctx)

	assert.Equal(t, pruned, result)
	mockQ.AssertExpectations(t)
	mockQ.AssertCalled(t, "PruneDevices", ctx, mock.Anything)
	mockQ.AssertCalled(t, "PrunePendingCalls", ctx, mock.Anything)
}

func TestCleanupStaleState_PrunDevicesError(t *testing.T) {
	mockQ := new(MockQuerier)
	db := &Database{Queries: mockQ}

	ctx := context.Background()
	mockQ.On("PruneDevices", ctx, mock.Anything).Return(nil, assert.AnError)
	mockQ.On("PrunePendingCalls", ctx, mock.Anything).Return(nil)

	// Should not panic even on error
	result := db.CleanupStaleState(ctx)

	assert.Empty(t, result)
	mockQ.AssertCalled(t, "PruneDevices", ctx, mock.Anything)
	mockQ.AssertCalled(t, "PrunePendingCalls", ctx, mock.Anything)
}

func TestCleanupStaleState_PrunPendingCallsError(t *testing.T) {
	mockQ := new(MockQuerier)
	db := &Database{Queries: mockQ}

	ctx := context.Background()
	pruned := []PruneDevicesRow{{DeviceID: "expired-device", B2buaSipUser: "1001_expired"}}
	mockQ.On("PruneDevices", ctx, mock.Anything).Return(pruned, nil)
	mockQ.On("PrunePendingCalls", ctx, mock.Anything).Return(assert.AnError)

	// Should not panic even on error
	result := db.CleanupStaleState(ctx)

	assert.Equal(t, pruned, result)
	mockQ.AssertExpectations(t)
}

// --------------- CleanupJunkDevices tests ---------------

func TestCleanupJunkDevices_DeletesRows(t *testing.T) {
	mockPool := new(MockDBTX)
	db := &Database{Pool: mockPool, Queries: new(MockQuerier)}

	ctx := context.Background()
	mockPool.On("Exec", ctx, mock.AnythingOfType("string")).
		Return(pgconn.NewCommandTag("DELETE 3"), nil)

	count := db.CleanupJunkDevices(ctx)

	assert.Equal(t, int64(3), count)
	mockPool.AssertCalled(t, "Exec", ctx, mock.AnythingOfType("string"))
}

func TestCleanupJunkDevices_ZeroRows(t *testing.T) {
	mockPool := new(MockDBTX)
	db := &Database{Pool: mockPool, Queries: new(MockQuerier)}

	ctx := context.Background()
	mockPool.On("Exec", ctx, mock.AnythingOfType("string")).
		Return(pgconn.NewCommandTag("DELETE 0"), nil)

	count := db.CleanupJunkDevices(ctx)

	assert.Equal(t, int64(0), count)
}

func TestCleanupJunkDevices_DBError(t *testing.T) {
	mockPool := new(MockDBTX)
	db := &Database{Pool: mockPool, Queries: new(MockQuerier)}

	ctx := context.Background()
	mockPool.On("Exec", ctx, mock.AnythingOfType("string")).
		Return(pgconn.CommandTag{}, assert.AnError)

	count := db.CleanupJunkDevices(ctx)

	assert.Equal(t, int64(0), count)
}

// --------------- Init tests ---------------

func TestInit_Success(t *testing.T) {
	mockPool := new(MockDBTX)
	db := &Database{Pool: mockPool, Queries: new(MockQuerier)}

	ctx := context.Background()
	mockPool.On("Exec", ctx, mock.AnythingOfType("string")).
		Return(pgconn.NewCommandTag(""), nil)

	err := db.Init(ctx)

	assert.NoError(t, err)
	mockPool.AssertCalled(t, "Exec", ctx, mock.AnythingOfType("string"))
}

func TestInit_Error(t *testing.T) {
	mockPool := new(MockDBTX)
	db := &Database{Pool: mockPool, Queries: new(MockQuerier)}

	ctx := context.Background()
	mockPool.On("Exec", ctx, mock.AnythingOfType("string")).
		Return(pgconn.CommandTag{}, assert.AnError)

	err := db.Init(ctx)

	assert.Error(t, err)
}

func TestGetOrCreateEncryptionKey_Existing(t *testing.T) {
	mockQ := new(MockQuerier)
	db := &Database{Queries: mockQ}

	ctx := context.Background()
	expectedKey := []byte("abcdefghijklmnopqrstuvwxyz123456")
	mockQ.On("GetSetting", ctx, "encryption_key").Return(expectedKey, nil)

	key, err := db.GetOrCreateEncryptionKey(ctx, "")
	assert.NoError(t, err)
	assert.Equal(t, expectedKey, key)
	mockQ.AssertNotCalled(t, "UpsertSetting")
}

func TestGetOrCreateEncryptionKey_NewKey(t *testing.T) {
	mockQ := new(MockQuerier)
	db := &Database{Queries: mockQ}

	ctx := context.Background()
	mockQ.On("GetSetting", ctx, "encryption_key").Return([]byte(nil), pgx.ErrNoRows)
	mockQ.On("UpsertSetting", ctx, mock.MatchedBy(func(p UpsertSettingParams) bool {
		return p.Key == "encryption_key" && len(p.Value) == 32
	})).Return(nil)

	key, err := db.GetOrCreateEncryptionKey(ctx, "")
	assert.NoError(t, err)
	assert.Len(t, key, 32)
	mockQ.AssertCalled(t, "UpsertSetting", ctx, mock.AnythingOfType("db.UpsertSettingParams"))
}

func TestGetOrCreateEncryptionKey_QueryError(t *testing.T) {
	mockQ := new(MockQuerier)
	db := &Database{Queries: mockQ}

	ctx := context.Background()
	mockQ.On("GetSetting", ctx, "encryption_key").Return([]byte(nil), assert.AnError)

	key, err := db.GetOrCreateEncryptionKey(ctx, "")
	assert.Nil(t, key)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "query setting")
}

func TestGetOrCreateEncryptionKey_StoreError(t *testing.T) {
	mockQ := new(MockQuerier)
	db := &Database{Queries: mockQ}

	ctx := context.Background()
	mockQ.On("GetSetting", ctx, "encryption_key").Return([]byte(nil), pgx.ErrNoRows)
	mockQ.On("UpsertSetting", ctx, mock.AnythingOfType("db.UpsertSettingParams")).Return(assert.AnError)

	key, err := db.GetOrCreateEncryptionKey(ctx, "")
	assert.Nil(t, key)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "store key")
}

func TestGetOrCreateEncryptionKey_FromConfig(t *testing.T) {
	mockQ := new(MockQuerier)
	db := &Database{Queries: mockQ}
	ctx := context.Background()

	hexKey := "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	mockQ.On("UpsertSetting", ctx, mock.MatchedBy(func(p UpsertSettingParams) bool {
		return p.Key == "encryption_key" && len(p.Value) == 32
	})).Return(nil)

	key, err := db.GetOrCreateEncryptionKey(ctx, hexKey)
	assert.NoError(t, err)
	assert.Len(t, key, 32)
	mockQ.AssertNotCalled(t, "GetSetting")
}

func TestGetOrCreateEncryptionKey_InvalidHex(t *testing.T) {
	mockQ := new(MockQuerier)
	db := &Database{Queries: mockQ}
	ctx := context.Background()

	_, err := db.GetOrCreateEncryptionKey(ctx, "not-hex")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "64 hex")
}

func TestGetOrCreateEncryptionKey_WrongLength(t *testing.T) {
	mockQ := new(MockQuerier)
	db := &Database{Queries: mockQ}
	ctx := context.Background()

	_, err := db.GetOrCreateEncryptionKey(ctx, "abcd")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "32 bytes")
}

func TestBootstrapUsers(t *testing.T) {
	mockQ := new(MockQuerier)
	db := &Database{Queries: mockQ}

	ctx := context.Background()
	mockQ.On("CreateUser", ctx, mock.MatchedBy(func(p CreateUserParams) bool {
		return p.Username == "admin1" && p.Role == "admin"
	})).Return(nil)

	err := db.BootstrapUsers(ctx, []config.BootstrapUser{
		{Username: "admin1", Password: "short"},
	})
	assert.NoError(t, err)
	mockQ.AssertCalled(t, "CreateUser", ctx, mock.AnythingOfType("db.CreateUserParams"))
}

func TestBootstrapUsers_CreateError(t *testing.T) {
	mockQ := new(MockQuerier)
	db := &Database{Queries: mockQ}

	ctx := context.Background()
	mockQ.On("CreateUser", ctx, mock.AnythingOfType("db.CreateUserParams")).Return(assert.AnError)

	err := db.BootstrapUsers(ctx, []config.BootstrapUser{
		{Username: "admin1", Password: "short"},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "bootstrap user")
}

func TestStartCleanupWorker(t *testing.T) {
	oldInterval := cleanupWorkerInterval
	cleanupWorkerInterval = 10 * time.Millisecond
	defer func() { cleanupWorkerInterval = oldInterval }()

	mockQ := new(MockQuerier)
	pruned := []PruneDevicesRow{{DeviceID: "expired-device", B2buaSipUser: "1001_expired"}}
	mockQ.On("PruneDevices", mock.Anything, mock.Anything).Return(pruned, nil)
	mockQ.On("PrunePendingCalls", mock.Anything, mock.Anything).Return(nil)

	db := &Database{Queries: mockQ}

	ctx, cancel := context.WithCancel(context.Background())
	callback := make(chan []PruneDevicesRow, 1)
	db.StartCleanupWorker(ctx, func(_ context.Context, devices []PruneDevicesRow) {
		callback <- devices
	})
	select {
	case result := <-callback:
		assert.Equal(t, pruned, result)
	case <-time.After(time.Second):
		t.Fatal("cleanup worker did not report pruned devices")
	}
	cancel()
}
