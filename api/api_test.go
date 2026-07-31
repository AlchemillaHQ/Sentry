package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AlchemillaHQ/Sentry/auth"
	"github.com/AlchemillaHQ/Sentry/config"
	"github.com/AlchemillaHQ/Sentry/db"
	"github.com/AlchemillaHQ/Sentry/secrets"
	"github.com/AlchemillaHQ/Sentry/sipstack"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// --------------- Mocks ---------------

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

type mockCallManager struct {
	removeCalls []string
	suspended   [][2]string
	resumed     []string
	forgotten   [][2]string
}

func (m *mockCallManager) RemoveDeviceSource(sipUser string) {
	m.removeCalls = append(m.removeCalls, sipUser)
}

func (m *mockCallManager) SuspendDevice(deviceID, sipUser string) {
	m.suspended = append(m.suspended, [2]string{deviceID, sipUser})
}

func (m *mockCallManager) ResumeDevice(deviceID string) {
	m.resumed = append(m.resumed, deviceID)
}

func (m *mockCallManager) ForgetDevice(deviceID, sipUser string) {
	m.forgotten = append(m.forgotten, [2]string{deviceID, sipUser})
}

func (m *mockCallManager) GetPendingCallsCount() int {
	return len(m.removeCalls)
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

// MockDBTX implements db.DBTX for testing handlers that use dbPool directly
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

// --------------- Helpers ---------------

func newTestBox(t *testing.T) *secrets.Box {
	t.Helper()
	key := make([]byte, 32)
	_, err := rand.Read(key)
	assert.NoError(t, err)
	box, err := secrets.NewBox(key)
	assert.NoError(t, err)
	return box
}

// --------------- isValidHost tests ---------------

func TestIsValidHost_ValidIPv4(t *testing.T) {
	assert.True(t, isValidHost("192.168.1.1"))
	assert.True(t, isValidHost("10.0.0.1"))
	assert.True(t, isValidHost("8.8.8.8"))
	assert.True(t, isValidHost("0.0.0.0"))
	assert.True(t, isValidHost("255.255.255.255"))
}

func TestIsValidHost_ValidDomains(t *testing.T) {
	// Basic FQDNs
	assert.True(t, isValidHost("example.com"))
	assert.True(t, isValidHost("office.difusedns.com"))
	assert.True(t, isValidHost("a.b.c.d.example.org"))

	// Hyphens in labels (RFC-1123 allows)
	assert.True(t, isValidHost("sip-server-1.example.com"))
	assert.True(t, isValidHost("my-host.example.com"))
	assert.True(t, isValidHost("a-b-c.example.com"))

	// Single char labels
	assert.True(t, isValidHost("a.com"))
	assert.True(t, isValidHost("x.y.com"))

	// Label starting with digit (RFC-1123 allows, RFC-952 didn't)
	assert.True(t, isValidHost("123.example.com"))
	assert.True(t, isValidHost("9host.example.com"))
	assert.True(t, isValidHost("1a2b3c.example.com"))

	// Mixed case
	assert.True(t, isValidHost("Example.COM"))
	assert.True(t, isValidHost("MyHost.Example.Org"))
}

func TestIsValidHost_MaxLengths(t *testing.T) {
	// Max label length (63 chars)
	longLabel := strings.Repeat("a", 63)
	assert.True(t, isValidHost(longLabel+".com"))

	// Label too long (64 chars)
	tooLongLabel := strings.Repeat("a", 64)
	assert.False(t, isValidHost(tooLongLabel+".com"))

	// Max total hostname length (253 chars)
	// Build: 63 + "." + 63 + "." + 63 + "." + 61 = 253 chars total
	maxHost := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)
	assert.Equal(t, 253, len(maxHost))
	assert.True(t, isValidHost(maxHost))

	// Total too long (254 chars)
	tooLongHost := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 62)
	assert.Equal(t, 254, len(tooLongHost))
	assert.False(t, isValidHost(tooLongHost))
}

func TestIsValidHost_InvalidInputs(t *testing.T) {
	// Empty
	assert.False(t, isValidHost(""))

	// No dot (single label)
	assert.False(t, isValidHost("localhost"))
	assert.False(t, isValidHost("nodot"))
	assert.False(t, isValidHost("com"))

	// Starts with dot
	assert.False(t, isValidHost(".example.com"))

	// Ends with dot
	assert.False(t, isValidHost("example.com."))

	// Consecutive dots (empty label)
	assert.False(t, isValidHost("example..com"))
	assert.False(t, isValidHost("a..b.com"))

	// Label starts with hyphen
	assert.False(t, isValidHost("-example.com"))

	// Label ends with hyphen
	assert.False(t, isValidHost("example-.com"))

	// Contains space
	assert.False(t, isValidHost("has space.com"))

	// Contains special characters
	assert.False(t, isValidHost("special!char.com"))
	assert.False(t, isValidHost("under_score.com"))
	assert.False(t, isValidHost("at@sign.com"))

	// TLD is only numbers
	assert.False(t, isValidHost("example.123"))
	assert.False(t, isValidHost("example.456"))
}

// --------------- DeviceStatus tests ---------------

func TestDeviceStatus_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{dbQueries: mockDB, registrar: mockReg}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	oldExpiry := time.Now().Add(time.Hour)
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID:  deviceID,
		ExpiresAt: pgtype.Timestamptz{Time: oldExpiry, Valid: true},
		LastSeen:  pgtype.Timestamptz{Time: time.Now(), Valid: true},
	}, nil)
	mockReg.On("IsRegistered", deviceID).Return(true)
	mockDB.On("RefreshDeviceExpiry", mock.Anything, mock.MatchedBy(func(p db.RefreshDeviceExpiryParams) bool {
		return p.DeviceID == deviceID
	})).Return(nil)

	r := gin.Default()
	r.GET("/v1/devices/:device_id/status", handler.DeviceStatus)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/v1/devices/"+deviceID+"/status", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var response map[string]interface{}
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	assert.Equal(t, false, response["db_expired"])
	assert.Equal(t, false, response["was_db_expired"])
	assert.NotEqual(t, oldExpiry.Format(time.RFC3339Nano), response["expires_at"])
}

func TestDeviceStatus_ReportsPreRefreshExpiry(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{dbQueries: mockDB, registrar: mockReg}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID:  deviceID,
		ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(-time.Hour), Valid: true},
		LastSeen:  pgtype.Timestamptz{Time: time.Now().Add(-time.Hour), Valid: true},
	}, nil)
	mockDB.On("RefreshDeviceExpiry", mock.Anything, mock.Anything).Return(nil)
	mockReg.On("IsRegistered", deviceID).Return(false)

	r := gin.Default()
	r.GET("/v1/devices/:device_id/status", handler.DeviceStatus)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/v1/devices/"+deviceID+"/status", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var response map[string]interface{}
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	assert.Equal(t, true, response["db_expired"])
	assert.Equal(t, true, response["was_db_expired"])
}

func TestDeviceStatus_RefreshFailureIsReported(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	handler := &Handler{dbQueries: mockDB}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID:  deviceID,
		ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(time.Hour), Valid: true},
	}, nil)
	mockDB.On("RefreshDeviceExpiry", mock.Anything, mock.Anything).Return(assert.AnError)

	r := gin.Default()
	r.GET("/v1/devices/:device_id/status", handler.DeviceStatus)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/v1/devices/"+deviceID+"/status", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestRegisterDevice_DefaultPortAndTransport(t *testing.T) {
	gin.SetMode(gin.TestMode)
	box := newTestBox(t)
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{dbQueries: mockDB, registrar: mockReg, box: box, stack: &sipstack.Stack{}}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{}, pgx.ErrNoRows)
	mockDB.On("UpsertDevice", mock.Anything, mock.Anything).Return(nil)
	mockReg.On("Register", mock.Anything, mock.Anything).Return(nil)

	r := gin.Default()
	r.POST("/v1/devices/register", handler.RegisterDevice)

	body, _ := json.Marshal(map[string]interface{}{
		"device_id":         deviceID,
		"platform":          "android",
		"upstream_host":     "pbx.example.com",
		"upstream_user":     "user1",
		"upstream_password": "secret",
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/register", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	assert.NotEqual(t, http.StatusBadRequest, w.Code)
}

func TestRegisterDevice_InvalidJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &Handler{}

	r := gin.Default()
	r.POST("/v1/devices/register", handler.RegisterDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/register", bytes.NewBuffer([]byte("bad json")))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestNewHandler(t *testing.T) {
	db := &db.Database{}
	handler := NewHandler(db, nil, nil, nil, config.APIConfig{JWTSecret: "test"})
	assert.NotNil(t, handler)
	assert.Equal(t, "test", handler.jwtSecret)
	assert.Equal(t, db.Queries, handler.dbQueries)
	assert.Equal(t, db.Pool, handler.dbPool)
}

func TestSetupRouter_Routes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockPool := new(MockDBTX)
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	box := newTestBox(t)

	handler := &Handler{
		dbQueries: mockDB,
		dbPool:    mockPool,
		registrar: mockReg,
		box:       box,
		jwtSecret: "test-secret",
	}

	mockPool.On("Exec", mock.Anything, "SELECT 1").Return(pgconn.NewCommandTag("SELECT 1"), nil)

	r := SetupRouter(handler, nil)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/health", nil)
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/v1/admin/stats", nil)
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestSetupRouter_WithRateLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockPool := new(MockDBTX)
	mockDB := new(MockQuerier)
	box := newTestBox(t)

	handler := &Handler{
		dbQueries: mockDB,
		dbPool:    mockPool,
		box:       box,
		jwtSecret: "test",
	}

	mockPool.On("Exec", mock.Anything, "SELECT 1").Return(pgconn.NewCommandTag("SELECT 1"), nil)

	cfg := &config.Config{
		RateLimit: config.RateLimitConfig{
			RegisterRate:  1.0,
			RegisterBurst: 10,
		},
	}

	r := SetupRouter(handler, cfg)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/health", nil)
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestSetupRouter_HealthDBDown(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockPool := new(MockDBTX)

	handler := &Handler{
		dbPool: mockPool,
	}

	mockPool.On("Exec", mock.Anything, "SELECT 1").Return(pgconn.CommandTag{}, assert.AnError)

	r := SetupRouter(handler, nil)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/health", nil)
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestDeviceStatus_InvalidUUID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &Handler{}

	r := gin.Default()
	r.GET("/v1/devices/:device_id/status", handler.DeviceStatus)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/v1/devices/not-a-uuid/status", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDeviceStatus_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	handler := &Handler{dbQueries: mockDB}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{}, assert.AnError)

	r := gin.Default()
	r.GET("/v1/devices/:device_id/status", handler.DeviceStatus)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/v1/devices/"+deviceID+"/status", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

// --------------- RegisterDevice validation tests ---------------

func TestRegisterDevice_InvalidUUID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &Handler{}

	r := gin.Default()
	r.POST("/v1/devices/register", handler.RegisterDevice)

	body, _ := json.Marshal(map[string]interface{}{
		"device_id":         "not-a-uuid",
		"platform":          "android",
		"upstream_host":     "pbx.example.com",
		"upstream_user":     "user1",
		"upstream_password": "pass",
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/register", bytes.NewBuffer(body))
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Contains(t, resp["message"], "device_id")
}

func TestRegisterDevice_InvalidHost(t *testing.T) {
	gin.SetMode(gin.TestMode)
	box := newTestBox(t)
	handler := &Handler{box: box}

	r := gin.Default()
	r.POST("/v1/devices/register", handler.RegisterDevice)

	body, _ := json.Marshal(map[string]interface{}{
		"device_id":         "550e8400-e29b-41d4-a716-446655440000",
		"platform":          "android",
		"upstream_host":     "no-dot-host",
		"upstream_user":     "user1",
		"upstream_password": "pass",
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/register", bytes.NewBuffer(body))
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Contains(t, resp["message"], "upstream_host")
}

func TestRegisterDevice_ValidIP(t *testing.T) {
	gin.SetMode(gin.TestMode)
	box := newTestBox(t)
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	stack := &sipstack.Stack{}
	handler := &Handler{dbQueries: mockDB, registrar: mockReg, box: box, stack: stack}

	mockDB.On("GetDeviceByID", mock.Anything, "550e8400-e29b-41d4-a716-446655440000").Return(db.Device{}, pgx.ErrNoRows)
	mockDB.On("UpsertDevice", mock.Anything, mock.Anything).Return(nil)
	mockReg.On("Register", mock.Anything, mock.Anything).Return(nil)

	r := gin.Default()
	r.POST("/v1/devices/register", handler.RegisterDevice)

	body, _ := json.Marshal(map[string]interface{}{
		"device_id":          "550e8400-e29b-41d4-a716-446655440000",
		"platform":           "android",
		"push_token":         "fcm-token-123",
		"upstream_host":      "192.168.1.100",
		"upstream_port":      5060,
		"upstream_transport": "udp",
		"upstream_user":      "2025",
		"upstream_password":  "secret",
		"upstream_realm":     "example.com",
		"display_name":       "Test Device",
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/register", bytes.NewBuffer(body))
	r.ServeHTTP(w, req)

	// Will fail at stack.ExternalIP() because stack is nil, but validates input passes
	// The key test is that it doesn't return 400 for invalid host/uuid
	assert.NotEqual(t, http.StatusBadRequest, w.Code)
}

func TestRegisterDevice_ExistingDisabledStaysDisabled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	box := newTestBox(t)
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{
		dbQueries: mockDB,
		registrar: mockReg,
		box:       box,
		stack:     &sipstack.Stack{},
	}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	contact := pgtype.Text{String: "sip:shadow@10.0.0.2:5060", Valid: true}
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID:      deviceID,
		Disabled:      true,
		DeviceContact: contact,
	}, nil)
	mockDB.On("UpsertDevice", mock.Anything, mock.MatchedBy(func(params db.UpsertDeviceParams) bool {
		return params.DeviceID == deviceID && params.DeviceContact == contact
	})).Return(nil)

	r := gin.Default()
	r.POST("/v1/devices/register", handler.RegisterDevice)

	body, _ := json.Marshal(map[string]interface{}{
		"device_id":          deviceID,
		"platform":           "android",
		"push_token":         "new-token",
		"upstream_host":      "pbx.example.com",
		"upstream_port":      5060,
		"upstream_transport": "udp",
		"upstream_user":      "2025",
		"upstream_password":  "secret",
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/register", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var response map[string]interface{}
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	assert.Equal(t, "disabled", response["status"])
	assert.Equal(t, true, response["disabled"])
	mockReg.AssertNotCalled(t, "Register", mock.Anything, mock.Anything)
}

func TestRegisterDevice_InvalidPort(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &Handler{}

	r := gin.Default()
	r.POST("/v1/devices/register", handler.RegisterDevice)

	body, _ := json.Marshal(map[string]interface{}{
		"device_id":         "550e8400-e29b-41d4-a716-446655440000",
		"platform":          "android",
		"upstream_host":     "pbx.example.com",
		"upstream_port":     99999,
		"upstream_user":     "user1",
		"upstream_password": "pass",
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/register", bytes.NewBuffer(body))
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Contains(t, resp["message"], "port")
}

// --------------- UnregisterDevice tests ---------------

func TestUnregisterDevice_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{dbQueries: mockDB, registrar: mockReg}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID:     deviceID,
		B2buaSipUser: "2025_abcdefgh",
	}, nil)
	mockDB.On("SetDeviceDisabled", mock.Anything, db.SetDeviceDisabledParams{
		DeviceID: deviceID,
		Disabled: true,
	}).Return(nil)
	mockReg.On("Unregister", mock.Anything, deviceID).Return(nil)
	mockDB.On("DeleteDeviceByID", mock.Anything, deviceID).Return(nil)

	r := gin.Default()
	r.DELETE("/v1/devices/:device_id", handler.UnregisterDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("DELETE", "/v1/devices/"+deviceID, nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Equal(t, "unregistered", resp["status"])
	mockDB.AssertCalled(t, "DeleteDeviceByID", mock.Anything, deviceID)
}

func TestUnregisterDevice_InvalidUUID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &Handler{}

	r := gin.Default()
	r.DELETE("/v1/devices/:device_id", handler.UnregisterDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("DELETE", "/v1/devices/not-a-uuid", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestUnregisterDevice_DeleteFails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{dbQueries: mockDB, registrar: mockReg}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID:     deviceID,
		B2buaSipUser: "2025_abcdefgh",
	}, nil)
	mockDB.On("SetDeviceDisabled", mock.Anything, db.SetDeviceDisabledParams{
		DeviceID: deviceID,
		Disabled: true,
	}).Return(nil)
	mockReg.On("Unregister", mock.Anything, deviceID).Return(nil)
	mockDB.On("DeleteDeviceByID", mock.Anything, deviceID).Return(assert.AnError)

	r := gin.Default()
	r.DELETE("/v1/devices/:device_id", handler.UnregisterDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("DELETE", "/v1/devices/"+deviceID, nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// --------------- DisableDevice tests ---------------

func TestDisableDevice_InvalidUUID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &Handler{}

	r := gin.Default()
	r.POST("/v1/devices/:device_id/disable", handler.DisableDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/not-a-uuid/disable", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDisableDevice_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	handler := &Handler{dbQueries: mockDB}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{}, pgx.ErrNoRows)

	r := gin.Default()
	r.POST("/v1/devices/:device_id/disable", handler.DisableDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/"+deviceID+"/disable", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestDisableDevice_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{dbQueries: mockDB, registrar: mockReg}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID: deviceID,
	}, nil)
	disabledPersisted := false
	mockDB.On("SetDeviceDisabled", mock.Anything, db.SetDeviceDisabledParams{
		DeviceID: deviceID,
		Disabled: true,
	}).Run(func(mock.Arguments) {
		disabledPersisted = true
	}).Return(nil)
	mockReg.On("Unregister", mock.Anything, deviceID).Run(func(mock.Arguments) {
		assert.True(t, disabledPersisted, "disabled state must be persisted before upstream cleanup")
	}).Return(nil)

	r := gin.Default()
	r.POST("/v1/devices/:device_id/disable", handler.DisableDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/"+deviceID+"/disable", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Equal(t, "disabled", resp["status"])
}

func TestDisableDevice_DBError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{dbQueries: mockDB, registrar: mockReg}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID: deviceID,
	}, nil)
	mockReg.On("Unregister", mock.Anything, deviceID).Return(nil)
	mockDB.On("SetDeviceDisabled", mock.Anything, mock.Anything).Return(assert.AnError)

	r := gin.Default()
	r.POST("/v1/devices/:device_id/disable", handler.DisableDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/"+deviceID+"/disable", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// --------------- EnableDevice tests ---------------

func TestEnableDevice_InvalidUUID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &Handler{}

	r := gin.Default()
	r.POST("/v1/devices/:device_id/enable", handler.EnableDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/not-a-uuid/enable", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestEnableDevice_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	handler := &Handler{dbQueries: mockDB}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{}, assert.AnError)

	r := gin.Default()
	r.POST("/v1/devices/:device_id/enable", handler.EnableDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/"+deviceID+"/enable", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestEnableDevice_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	box := newTestBox(t)
	encPassword, err := box.Encrypt([]byte("secret"))
	assert.NoError(t, err)

	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{dbQueries: mockDB, registrar: mockReg, box: box}
	callMgr := &mockCallManager{}
	handler.SetCallManager(callMgr)

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID:          deviceID,
		Disabled:          true,
		UpstreamUser:      "user1",
		UpstreamHost:      "pbx.example.com",
		UpstreamPort:      5060,
		UpstreamTransport: "udp",
		UpstreamPassword:  encPassword,
		UpstreamRealm:     pgtype.Text{String: "realm", Valid: true},
	}, nil)
	mockReg.On("Register", mock.Anything, mock.Anything).Return(nil)
	mockDB.On("SetDeviceDisabled", mock.Anything, db.SetDeviceDisabledParams{
		DeviceID: deviceID,
		Disabled: false,
	}).Return(nil)

	r := gin.Default()
	r.POST("/v1/devices/:device_id/enable", handler.EnableDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/"+deviceID+"/enable", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Equal(t, "enabled", resp["status"])
	assert.Equal(t, []string{deviceID}, callMgr.resumed)
}

func TestEnableDevice_AlreadyEnabledAndRegisteredIsIdempotent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{dbQueries: mockDB, registrar: mockReg}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID: deviceID,
		Disabled: false,
	}, nil)
	mockReg.On("IsRegistered", deviceID).Return(true)

	r := gin.Default()
	r.POST("/v1/devices/:device_id/enable", handler.EnableDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/"+deviceID+"/enable", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockReg.AssertNotCalled(t, "Register", mock.Anything, mock.Anything)
	mockDB.AssertNotCalled(t, "SetDeviceDisabled", mock.Anything, mock.Anything)
}

func TestEnsureEnabledRegistration_SuppressesDisabledDevice(t *testing.T) {
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{dbQueries: mockDB, registrar: mockReg}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID: deviceID,
		Disabled: true,
	}, nil)

	registered, err := handler.EnsureEnabledRegistration(context.Background(), deviceID, "startup")

	assert.NoError(t, err)
	assert.False(t, registered)
	mockReg.AssertNotCalled(t, "Register", mock.Anything, mock.Anything)
}

// --------------- ForceReregister tests ---------------

func TestForceReregister_InvalidUUID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &Handler{}

	r := gin.Default()
	r.POST("/v1/devices/:device_id/reregister", handler.ForceReregister)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/not-a-uuid/reregister", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestForceReregister_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	handler := &Handler{dbQueries: mockDB}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{}, assert.AnError)

	r := gin.Default()
	r.POST("/v1/devices/:device_id/reregister", handler.ForceReregister)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/"+deviceID+"/reregister", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestForceReregister_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	box := newTestBox(t)
	encPassword, err := box.Encrypt([]byte("secret"))
	assert.NoError(t, err)

	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{dbQueries: mockDB, registrar: mockReg, box: box}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID:          deviceID,
		B2buaSipUser:      "user1_device-1",
		UpstreamUser:      "user1",
		UpstreamHost:      "pbx.example.com",
		UpstreamPort:      5060,
		UpstreamTransport: "udp",
		UpstreamPassword:  encPassword,
		UpstreamRealm:     pgtype.Text{String: "realm", Valid: true},
	}, nil)
	mockReg.On("Register", mock.Anything, mock.Anything).Return(nil)
	mockDB.On("RefreshDeviceExpiry", mock.Anything, mock.MatchedBy(func(p db.RefreshDeviceExpiryParams) bool {
		return p.DeviceID == deviceID
	})).Return(nil)

	r := gin.Default()
	r.POST("/v1/devices/:device_id/reregister", handler.ForceReregister)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/"+deviceID+"/reregister", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestForceReregister_DisabledDeviceIsRejected(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{dbQueries: mockDB, registrar: mockReg}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID: deviceID,
		Disabled: true,
	}, nil)

	r := gin.Default()
	r.POST("/v1/devices/:device_id/reregister", handler.ForceReregister)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/"+deviceID+"/reregister", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)
	var response map[string]interface{}
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	assert.Equal(t, "device_disabled", response["code"])
	mockReg.AssertNotCalled(t, "Register", mock.Anything, mock.Anything)
}

// --------------- RefreshDevice tests ---------------

func TestRefreshDevice_InvalidUUID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &Handler{}

	r := gin.Default()
	r.PUT("/v1/devices/:device_id/refresh", handler.RefreshDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", "/v1/devices/not-a-uuid/refresh", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestRefreshDevice_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	handler := &Handler{dbQueries: mockDB}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{}, assert.AnError)

	r := gin.Default()
	r.PUT("/v1/devices/:device_id/refresh", handler.RefreshDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", "/v1/devices/"+deviceID+"/refresh", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestRefreshDevice_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	box := newTestBox(t)
	mockDB := new(MockQuerier)
	handler := &Handler{dbQueries: mockDB, box: box}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID:          deviceID,
		UpstreamUser:      "user1",
		UpstreamHost:      "pbx.example.com",
		UpstreamPort:      5060,
		UpstreamTransport: "udp",
		UpstreamPassword:  []byte("encrypted"),
		UpstreamRealm:     pgtype.Text{String: "realm", Valid: true},
	}, nil)
	mockDB.On("UpsertDevice", mock.Anything, mock.Anything).Return(nil)

	r := gin.Default()
	r.PUT("/v1/devices/:device_id/refresh", handler.RefreshDevice)

	body, _ := json.Marshal(map[string]string{"push_token": "new-fcm-token"})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", "/v1/devices/"+deviceID+"/refresh", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Equal(t, "ok", resp["status"])
}

func TestRefreshDevice_Success_NoTokenUpdate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	handler := &Handler{dbQueries: mockDB}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID:          deviceID,
		UpstreamUser:      "user1",
		UpstreamHost:      "pbx.example.com",
		UpstreamPort:      5060,
		UpstreamTransport: "udp",
		UpstreamPassword:  []byte("encrypted"),
		UpstreamRealm:     pgtype.Text{String: "realm", Valid: true},
	}, nil)
	mockDB.On("UpsertDevice", mock.Anything, mock.Anything).Return(nil)

	r := gin.Default()
	r.PUT("/v1/devices/:device_id/refresh", handler.RefreshDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", "/v1/devices/"+deviceID+"/refresh", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Equal(t, "ok", resp["status"])
}

// --------------- AuthMiddleware tests ---------------

func TestAuthMiddleware_NoToken(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &Handler{jwtSecret: "secret"}

	r := gin.New()
	r.Use(handler.AuthMiddleware())
	r.GET("/protected", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/protected", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuthMiddleware_InvalidToken(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &Handler{jwtSecret: "secret"}

	r := gin.New()
	r.Use(handler.AuthMiddleware())
	r.GET("/protected", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/protected", nil)
	req.Header.Set("Authorization", "Bearer invalid-token")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuthMiddleware_ValidToken(t *testing.T) {
	gin.SetMode(gin.TestMode)
	secret := "test-secret"
	handler := &Handler{jwtSecret: secret}

	token, err := auth.GenerateToken("admin", "admin", secret)
	assert.NoError(t, err)

	r := gin.New()
	r.Use(handler.AuthMiddleware())
	r.GET("/protected", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/protected", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestAuthMiddleware_EmptySecret(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &Handler{jwtSecret: ""}

	r := gin.New()
	r.Use(handler.AuthMiddleware())
	r.GET("/protected", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/protected", nil)
	req.Header.Set("Authorization", "Bearer some-token")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// --------------- Login tests ---------------

func TestLogin_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	secret := "jwt-secret"
	handler := &Handler{dbQueries: mockDB, jwtSecret: secret}

	hash, _ := auth.HashPassword("password123")
	mockDB.On("GetUser", mock.Anything, "admin").Return(db.User{
		Username:     "admin",
		PasswordHash: hash,
		Role:         "admin",
	}, nil)

	r := gin.Default()
	r.POST("/v1/auth/login", handler.Login)

	body, _ := json.Marshal(LoginRequest{Username: "admin", Password: "password123"})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/auth/login", bytes.NewBuffer(body))
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Equal(t, "ok", resp["status"])
	assert.NotEmpty(t, resp["token"])
}

func TestLogin_WrongPassword(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	handler := &Handler{dbQueries: mockDB, jwtSecret: "secret"}

	hash, _ := auth.HashPassword("correct")
	mockDB.On("GetUser", mock.Anything, "admin").Return(db.User{
		Username:     "admin",
		PasswordHash: hash,
		Role:         "admin",
	}, nil)

	r := gin.Default()
	r.POST("/v1/auth/login", handler.Login)

	body, _ := json.Marshal(LoginRequest{Username: "admin", Password: "wrong"})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/auth/login", bytes.NewBuffer(body))
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestLogin_UserNotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	handler := &Handler{dbQueries: mockDB, jwtSecret: "secret"}

	mockDB.On("GetUser", mock.Anything, "nobody").Return(db.User{}, assert.AnError)

	r := gin.Default()
	r.POST("/v1/auth/login", handler.Login)

	body, _ := json.Marshal(LoginRequest{Username: "nobody", Password: "pass"})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/auth/login", bytes.NewBuffer(body))
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestLogin_MissingFields(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &Handler{jwtSecret: "secret"}

	r := gin.Default()
	r.POST("/v1/auth/login", handler.Login)

	body, _ := json.Marshal(map[string]string{"username": "admin"})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/auth/login", bytes.NewBuffer(body))
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// --------------- ListUsers tests ---------------

func TestListUsers_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	handler := &Handler{dbQueries: mockDB}

	mockDB.On("ListUsers", mock.Anything).Return([]db.ListUsersRow{
		{Username: "admin", Role: "admin"},
		{Username: "viewer", Role: "viewer"},
	}, nil)

	r := gin.Default()
	r.GET("/v1/admin/users", handler.ListUsers)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/v1/admin/users", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp []map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Len(t, resp, 2)
}

func TestListUsers_Error(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	handler := &Handler{dbQueries: mockDB}

	mockDB.On("ListUsers", mock.Anything).Return([]db.ListUsersRow{}, assert.AnError)

	r := gin.Default()
	r.GET("/v1/admin/users", handler.ListUsers)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/v1/admin/users", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// MockRow implements pgx.Row for testing
type MockRow struct {
	mock.Mock
}

func (m *MockRow) Scan(dest ...interface{}) error {
	return m.Called(dest...).Error(0)
}

// MockRows implements pgx.Rows for testing
type MockRows struct {
	mock.Mock
	rows    [][]interface{}
	idx     int
	scanErr error
}

func NewMockRows(columns []string, data [][]interface{}) *MockRows {
	return &MockRows{rows: data}
}

func NewMockRowsWithScanError(data [][]interface{}, scanErr error) *MockRows {
	return &MockRows{rows: data, scanErr: scanErr}
}

func (m *MockRows) Close()                                       {}
func (m *MockRows) Err() error                                   { return nil }
func (m *MockRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (m *MockRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (m *MockRows) Next() bool {
	if m.idx < len(m.rows) {
		m.idx++
		return true
	}
	return false
}
func (m *MockRows) Scan(dest ...interface{}) error {
	if m.scanErr != nil {
		return m.scanErr
	}
	row := m.rows[m.idx-1]
	for i, d := range dest {
		switch v := d.(type) {
		case *string:
			*v = row[i].(string)
		case *bool:
			*v = row[i].(bool)
		case *pgtype.Text:
			*v = row[i].(pgtype.Text)
		case *pgtype.Timestamptz:
			*v = row[i].(pgtype.Timestamptz)
		}
	}
	return nil
}
func (m *MockRows) Values() ([]interface{}, error) { return nil, nil }
func (m *MockRows) RawValues() [][]byte            { return nil }
func (m *MockRows) Conn() *pgx.Conn                { return nil }

// --------------- DashboardStats tests ---------------

type fakeCallMgr struct{ count int }

func (f *fakeCallMgr) RemoveDeviceSource(string)    {}
func (f *fakeCallMgr) SuspendDevice(string, string) {}
func (f *fakeCallMgr) ResumeDevice(string)          {}
func (f *fakeCallMgr) ForgetDevice(string, string)  {}
func (f *fakeCallMgr) GetPendingCallsCount() int    { return f.count }

func TestDashboardStats_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockPool := new(MockDBTX)
	handler := &Handler{dbPool: mockPool}
	handler.callMgr = &fakeCallMgr{count: 5}

	mockRow := new(MockRow)
	mockRow.On("Scan", mock.AnythingOfType("*int64")).Return(nil).Run(func(args mock.Arguments) {
		ptr := args.Get(0).(*int64)
		*ptr = 42
	})
	mockPool.On("QueryRow", mock.Anything, "SELECT COUNT(*) FROM devices").Return(mockRow)

	r := gin.Default()
	r.GET("/v1/admin/stats", handler.DashboardStats)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/v1/admin/stats", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Equal(t, float64(42), resp["registered_devices"])
	assert.Equal(t, float64(5), resp["active_calls"])
	assert.Equal(t, "healthy", resp["db_status"])
}

// --------------- ListDevices tests ---------------

func TestListDevices_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockPool := new(MockDBTX)
	handler := &Handler{dbPool: mockPool}

	mockRows := NewMockRows(nil, [][]interface{}{
		{
			"device-1", "android", "office.example.com", "2025",
			pgtype.Text{String: "Hayzam", Valid: true},
			pgtype.Timestamptz{Time: time.Now(), Valid: true},
			false,
		},
	})
	mockPool.On("Query", mock.Anything, mock.AnythingOfType("string")).Return(mockRows, nil)

	r := gin.Default()
	r.GET("/v1/admin/devices", handler.ListDevices)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/v1/admin/devices", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp []map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Len(t, resp, 1)
	assert.Equal(t, "device-1", resp[0]["device_id"])
	assert.Equal(t, "2025", resp[0]["upstream_user"])
}

func TestListDevices_QueryError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockPool := new(MockDBTX)
	handler := &Handler{dbPool: mockPool}

	mockPool.On("Query", mock.Anything, mock.AnythingOfType("string")).Return((*MockRows)(nil), assert.AnError)

	r := gin.Default()
	r.GET("/v1/admin/devices", handler.ListDevices)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/v1/admin/devices", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestListDevices_ScanError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockPool := new(MockDBTX)
	handler := &Handler{dbPool: mockPool}

	mockRows := NewMockRowsWithScanError([][]interface{}{
		{"device-1", "android", "host", "user", pgtype.Text{}, pgtype.Timestamptz{Time: time.Now(), Valid: true}, false},
	}, assert.AnError)
	mockPool.On("Query", mock.Anything, mock.AnythingOfType("string")).Return(mockRows, nil)

	r := gin.Default()
	r.GET("/v1/admin/devices", handler.ListDevices)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/v1/admin/devices", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp []map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Empty(t, resp)
}

// --------------- CleanupJunkDevices tests ---------------

func TestCleanupJunkDevices_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockPool := new(MockDBTX)
	handler := &Handler{dbPool: mockPool}

	mockPool.On("Exec", mock.Anything, mock.AnythingOfType("string")).
		Return(pgconn.NewCommandTag("DELETE 5"), nil)

	r := gin.Default()
	r.POST("/v1/admin/devices/cleanup-junk", handler.CleanupJunkDevices)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/admin/devices/cleanup-junk", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Equal(t, "ok", resp["status"])
	assert.Equal(t, float64(5), resp["deleted"])
}

func TestCleanupJunkDevices_DBError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockPool := new(MockDBTX)
	handler := &Handler{dbPool: mockPool}

	mockPool.On("Exec", mock.Anything, mock.AnythingOfType("string")).
		Return(pgconn.CommandTag{}, assert.AnError)

	r := gin.Default()
	r.POST("/v1/admin/devices/cleanup-junk", handler.CleanupJunkDevices)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/admin/devices/cleanup-junk", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// --------------- Health check tests ---------------

func TestHealthCheck_OK(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockPool := new(MockDBTX)
	handler := &Handler{dbPool: mockPool}

	mockPool.On("Exec", mock.Anything, "SELECT 1").Return(pgconn.NewCommandTag("SELECT 1"), nil)

	r := gin.Default()
	r.GET("/health", func(c *gin.Context) {
		if _, err := handler.dbPool.Exec(c.Request.Context(), "SELECT 1"); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "degraded", "db": "unreachable"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/health", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Equal(t, "ok", resp["status"])
}

func TestHealthCheck_DBDown(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockPool := new(MockDBTX)
	handler := &Handler{dbPool: mockPool}

	mockPool.On("Exec", mock.Anything, "SELECT 1").Return(pgconn.CommandTag{}, assert.AnError)

	r := gin.Default()
	r.GET("/health", func(c *gin.Context) {
		if _, err := handler.dbPool.Exec(c.Request.Context(), "SELECT 1"); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "degraded", "db": "unreachable"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/health", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Equal(t, "degraded", resp["status"])
}

func TestCreateUser_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockPool := new(MockDBTX)
	handler := &Handler{dbPool: mockPool}

	mockPool.On("Exec", mock.Anything, mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything).
		Return(pgconn.NewCommandTag("INSERT 0 1"), nil)

	r := gin.Default()
	r.POST("/v1/admin/users", handler.CreateUser)

	body, _ := json.Marshal(map[string]string{
		"username": "newuser",
		"password": "password123",
		"role":     "admin",
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/admin/users", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)
	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Equal(t, "ok", resp["status"])
}

func TestCreateUser_Conflict(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockPool := new(MockDBTX)
	handler := &Handler{dbPool: mockPool}

	mockPool.On("Exec", mock.Anything, mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything).
		Return(pgconn.NewCommandTag("INSERT 0 0"), nil)

	r := gin.Default()
	r.POST("/v1/admin/users", handler.CreateUser)

	body, _ := json.Marshal(map[string]string{
		"username": "existing",
		"password": "password123",
		"role":     "viewer",
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/admin/users", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)
	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Contains(t, resp["message"], "already exists")
}

func TestCreateUser_InvalidRole(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &Handler{}

	r := gin.Default()
	r.POST("/v1/admin/users", handler.CreateUser)

	body, _ := json.Marshal(map[string]string{
		"username": "newuser",
		"password": "password123",
		"role":     "superadmin",
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/admin/users", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestCreateUser_DBError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockPool := new(MockDBTX)
	handler := &Handler{dbPool: mockPool}

	mockPool.On("Exec", mock.Anything, mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything).
		Return(pgconn.CommandTag{}, assert.AnError)

	r := gin.Default()
	r.POST("/v1/admin/users", handler.CreateUser)

	body, _ := json.Marshal(map[string]string{
		"username": "newuser",
		"password": "password123",
		"role":     "admin",
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/admin/users", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestDeleteUser_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockPool := new(MockDBTX)
	handler := &Handler{dbPool: mockPool}

	username := "todelete"
	mockPool.On("Exec", mock.Anything, mock.AnythingOfType("string"), username).
		Return(pgconn.NewCommandTag("DELETE 1"), nil)

	r := gin.Default()
	r.DELETE("/v1/admin/users/:username", handler.DeleteUser)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("DELETE", "/v1/admin/users/"+username, nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Equal(t, "ok", resp["status"])
}

func TestDeleteUser_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockPool := new(MockDBTX)
	handler := &Handler{dbPool: mockPool}

	username := "nonexistent"
	mockPool.On("Exec", mock.Anything, mock.AnythingOfType("string"), username).
		Return(pgconn.NewCommandTag("DELETE 0"), nil)

	r := gin.Default()
	r.DELETE("/v1/admin/users/:username", handler.DeleteUser)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("DELETE", "/v1/admin/users/"+username, nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestUnregisterDevice_WithCallManager(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)

	callMgr := &mockCallManager{}

	handler := &Handler{dbQueries: mockDB, registrar: mockReg}
	handler.SetCallManager(callMgr)

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID:     deviceID,
		B2buaSipUser: "user_abcdefgh",
	}, nil)
	mockDB.On("SetDeviceDisabled", mock.Anything, db.SetDeviceDisabledParams{
		DeviceID: deviceID,
		Disabled: true,
	}).Return(nil)
	mockReg.On("Unregister", mock.Anything, deviceID).Return(nil)
	mockDB.On("DeleteDeviceByID", mock.Anything, deviceID).Return(nil)

	r := gin.Default()
	r.DELETE("/v1/devices/:device_id", handler.UnregisterDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("DELETE", "/v1/devices/"+deviceID, nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Equal(t, "unregistered", resp["status"])
	assert.Equal(t, [][2]string{{deviceID, "user_abcdefgh"}}, callMgr.forgotten)
}

func TestEnableDevice_DecryptError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	box := newTestBox(t)
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{dbQueries: mockDB, registrar: mockReg, box: box}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID:          deviceID,
		Disabled:          true,
		UpstreamUser:      "user1",
		UpstreamHost:      "pbx.example.com",
		UpstreamPort:      5060,
		UpstreamTransport: "udp",
		UpstreamPassword:  []byte("not-encrypted-data"),
		UpstreamRealm:     pgtype.Text{String: "realm", Valid: true},
	}, nil)

	r := gin.Default()
	r.POST("/v1/devices/:device_id/enable", handler.EnableDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/"+deviceID+"/enable", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestEnableDevice_RegisterError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	box := newTestBox(t)
	encPassword, err := box.Encrypt([]byte("secret"))
	assert.NoError(t, err)

	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{dbQueries: mockDB, registrar: mockReg, box: box}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID:          deviceID,
		UpstreamUser:      "user1",
		UpstreamHost:      "pbx.example.com",
		UpstreamPort:      5060,
		UpstreamTransport: "udp",
		UpstreamPassword:  encPassword,
		UpstreamRealm:     pgtype.Text{String: "realm", Valid: true},
	}, nil)
	mockReg.On("IsRegistered", deviceID).Return(false)
	mockReg.On("Register", mock.Anything, mock.Anything).Return(assert.AnError)

	r := gin.Default()
	r.POST("/v1/devices/:device_id/enable", handler.EnableDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/"+deviceID+"/enable", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadGateway, w.Code)
}

func TestEnableDevice_SetDisabledError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	box := newTestBox(t)
	encPassword, err := box.Encrypt([]byte("secret"))
	assert.NoError(t, err)

	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{dbQueries: mockDB, registrar: mockReg, box: box}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID:          deviceID,
		Disabled:          true,
		UpstreamUser:      "user1",
		UpstreamHost:      "pbx.example.com",
		UpstreamPort:      5060,
		UpstreamTransport: "udp",
		UpstreamPassword:  encPassword,
		UpstreamRealm:     pgtype.Text{String: "realm", Valid: true},
	}, nil)
	mockReg.On("Register", mock.Anything, mock.Anything).Return(nil)
	mockDB.On("SetDeviceDisabled", mock.Anything, mock.Anything).Return(assert.AnError)
	mockReg.On("Unregister", mock.Anything, deviceID).Return(nil)

	r := gin.Default()
	r.POST("/v1/devices/:device_id/enable", handler.EnableDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/"+deviceID+"/enable", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	mockReg.AssertCalled(t, "Unregister", mock.Anything, deviceID)
}

func TestForceReregister_DecryptError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	box := newTestBox(t)
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{dbQueries: mockDB, registrar: mockReg, box: box}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID:          deviceID,
		B2buaSipUser:      "user1_device-1",
		UpstreamUser:      "user1",
		UpstreamHost:      "pbx.example.com",
		UpstreamPort:      5060,
		UpstreamTransport: "udp",
		UpstreamPassword:  []byte("bad-encrypted-data"),
		UpstreamRealm:     pgtype.Text{String: "realm", Valid: true},
	}, nil)

	r := gin.Default()
	r.POST("/v1/devices/:device_id/reregister", handler.ForceReregister)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/"+deviceID+"/reregister", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestForceReregister_RegisterError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	box := newTestBox(t)
	encPassword, err := box.Encrypt([]byte("secret"))
	assert.NoError(t, err)

	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{dbQueries: mockDB, registrar: mockReg, box: box}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID:          deviceID,
		B2buaSipUser:      "user1_device-1",
		UpstreamUser:      "user1",
		UpstreamHost:      "pbx.example.com",
		UpstreamPort:      5060,
		UpstreamTransport: "udp",
		UpstreamPassword:  encPassword,
		UpstreamRealm:     pgtype.Text{String: "realm", Valid: true},
	}, nil)
	mockReg.On("Register", mock.Anything, mock.Anything).Return(assert.AnError)

	r := gin.Default()
	r.POST("/v1/devices/:device_id/reregister", handler.ForceReregister)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/"+deviceID+"/reregister", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadGateway, w.Code)
}

func TestDeleteUser_DBError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockPool := new(MockDBTX)
	handler := &Handler{dbPool: mockPool}

	username := "todelete"
	mockPool.On("Exec", mock.Anything, mock.AnythingOfType("string"), username).
		Return(pgconn.CommandTag{}, assert.AnError)

	r := gin.Default()
	r.DELETE("/v1/admin/users/:username", handler.DeleteUser)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("DELETE", "/v1/admin/users/"+username, nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestRegisterDevice_UpsertDeviceError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	box := newTestBox(t)
	mockDB := new(MockQuerier)
	handler := &Handler{dbQueries: mockDB, box: box}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{}, pgx.ErrNoRows)
	mockDB.On("UpsertDevice", mock.Anything, mock.Anything).Return(assert.AnError)

	r := gin.Default()
	r.POST("/v1/devices/register", handler.RegisterDevice)

	body, _ := json.Marshal(map[string]interface{}{
		"device_id":          deviceID,
		"platform":           "android",
		"upstream_host":      "pbx.example.com",
		"upstream_port":      5060,
		"upstream_transport": "udp",
		"upstream_user":      "user1",
		"upstream_password":  "secret",
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/register", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestRegisterDevice_RegisterError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	box := newTestBox(t)
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{dbQueries: mockDB, registrar: mockReg, box: box, stack: &sipstack.Stack{}}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{}, pgx.ErrNoRows)
	mockDB.On("UpsertDevice", mock.Anything, mock.Anything).Return(nil)
	mockReg.On("Register", mock.Anything, mock.Anything).Return(assert.AnError)

	r := gin.Default()
	r.POST("/v1/devices/register", handler.RegisterDevice)

	body, _ := json.Marshal(map[string]interface{}{
		"device_id":          deviceID,
		"platform":           "android",
		"upstream_host":      "pbx.example.com",
		"upstream_port":      5060,
		"upstream_transport": "udp",
		"upstream_user":      "user1",
		"upstream_password":  "secret",
	})
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/register", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadGateway, w.Code)
}

func TestRefreshDevice_DBError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	handler := &Handler{dbQueries: mockDB}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID:          deviceID,
		UpstreamUser:      "user1",
		UpstreamHost:      "pbx.example.com",
		UpstreamPort:      5060,
		UpstreamTransport: "udp",
		UpstreamPassword:  []byte("encrypted"),
		UpstreamRealm:     pgtype.Text{String: "realm", Valid: true},
	}, nil)
	mockDB.On("UpsertDevice", mock.Anything, mock.Anything).Return(assert.AnError)

	r := gin.Default()
	r.PUT("/v1/devices/:device_id/refresh", handler.RefreshDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PUT", "/v1/devices/"+deviceID+"/refresh", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestLogin_InvalidJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &Handler{}

	r := gin.Default()
	r.POST("/v1/auth/login", handler.Login)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/auth/login", bytes.NewBuffer([]byte("not json")))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDisableDevice_UnregisterError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{dbQueries: mockDB, registrar: mockReg}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID: deviceID,
	}, nil)
	mockReg.On("Unregister", mock.Anything, deviceID).Return(assert.AnError)
	mockDB.On("SetDeviceDisabled", mock.Anything, mock.Anything).Return(nil)

	r := gin.Default()
	r.POST("/v1/devices/:device_id/disable", handler.DisableDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/"+deviceID+"/disable", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestUnregisterDevice_UnregisterError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{dbQueries: mockDB, registrar: mockReg}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockReg.On("Unregister", mock.Anything, deviceID).Return(assert.AnError)
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID:     deviceID,
		B2buaSipUser: "user_abcdefgh",
	}, nil)
	mockDB.On("SetDeviceDisabled", mock.Anything, db.SetDeviceDisabledParams{
		DeviceID: deviceID,
		Disabled: true,
	}).Return(nil)

	r := gin.Default()
	r.DELETE("/v1/devices/:device_id", handler.UnregisterDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("DELETE", "/v1/devices/"+deviceID, nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadGateway, w.Code)
}

func TestForceReregister_RefreshExpiryError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	box := newTestBox(t)
	encPassword, err := box.Encrypt([]byte("secret"))
	assert.NoError(t, err)

	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{dbQueries: mockDB, registrar: mockReg, box: box}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID:          deviceID,
		B2buaSipUser:      "user1_device-1",
		UpstreamUser:      "user1",
		UpstreamHost:      "pbx.example.com",
		UpstreamPort:      5060,
		UpstreamTransport: "udp",
		UpstreamPassword:  encPassword,
		UpstreamRealm:     pgtype.Text{String: "realm", Valid: true},
	}, nil)
	mockReg.On("Register", mock.Anything, mock.Anything).Return(nil)
	mockDB.On("RefreshDeviceExpiry", mock.Anything, mock.MatchedBy(func(p db.RefreshDeviceExpiryParams) bool {
		return p.DeviceID == deviceID
	})).Return(assert.AnError)

	r := gin.Default()
	r.POST("/v1/devices/:device_id/reregister", handler.ForceReregister)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/"+deviceID+"/reregister", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}
