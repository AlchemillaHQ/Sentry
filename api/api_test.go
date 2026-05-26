package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AlchemillaHQ/Sentry/db"
	"github.com/AlchemillaHQ/Sentry/secrets"
	"github.com/AlchemillaHQ/Sentry/sipstack"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

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

func TestDeviceStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{
		dbQueries: mockDB,
		registrar: mockReg,
	}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID:  deviceID,
		ExpiresAt: pgtype.Timestamptz{Valid: true},
	}, nil)
	mockReg.On("IsRegistered", deviceID).Return(true)

	r := gin.Default()
	r.GET("/v1/devices/:device_id/status", handler.DeviceStatus)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/v1/devices/"+deviceID+"/status", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Equal(t, "ok", resp["status"])
	assert.Equal(t, deviceID, resp["device_id"])
}

func TestRegisterDevice_Validation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &Handler{}

	r := gin.Default()
	r.POST("/v1/devices/register", handler.RegisterDevice)

	w := httptest.NewRecorder()
	reqBody, _ := json.Marshal(map[string]interface{}{
		"device_id": "invalid",
	})
	req, _ := http.NewRequest("POST", "/v1/devices/register", bytes.NewBuffer(reqBody))
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDisableDevice(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)
	handler := &Handler{
		dbQueries: mockDB,
		registrar: mockReg,
	}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID: deviceID,
	}, nil)
	mockReg.On("Unregister", mock.Anything, deviceID).Return(nil)
	mockDB.On("SetDeviceDisabled", mock.Anything, mock.MatchedBy(func(p db.SetDeviceDisabledParams) bool {
		return p.DeviceID == deviceID && p.Disabled == true
	})).Return(nil)

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

func TestEnableDevice(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockDB := new(MockQuerier)
	mockReg := new(MockRegistrar)

	testKey := make([]byte, 32)
	rand.Read(testKey)
	box, err := secrets.NewBox(testKey)
	assert.NoError(t, err)

	encPassword, err := box.Encrypt([]byte("testpassword"))
	assert.NoError(t, err)

	handler := &Handler{
		dbQueries: mockDB,
		registrar: mockReg,
		box:       box,
	}

	deviceID := "550e8400-e29b-41d4-a716-446655440000"
	mockDB.On("GetDeviceByID", mock.Anything, deviceID).Return(db.Device{
		DeviceID:         deviceID,
		UpstreamHost:     "pbx.example.com",
		UpstreamPort:     5061,
		UpstreamPassword: encPassword,
	}, nil)
	mockReg.On("Register", mock.Anything, mock.Anything).Return(nil)
	mockDB.On("SetDeviceDisabled", mock.Anything, mock.MatchedBy(func(p db.SetDeviceDisabledParams) bool {
		return p.DeviceID == deviceID && p.Disabled == false
	})).Return(nil)

	r := gin.Default()
	r.POST("/v1/devices/:device_id/enable", handler.EnableDevice)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/devices/"+deviceID+"/enable", nil)
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	assert.Equal(t, "enabled", resp["status"])
}
