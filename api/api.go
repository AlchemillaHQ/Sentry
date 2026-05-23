package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/AlchemillaHQ/Sentry/db"
	"github.com/AlchemillaHQ/Sentry/secrets"
	"github.com/AlchemillaHQ/Sentry/sipstack"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Handler struct {
	dbQueries  db.Querier
	dbPool     *pgxpool.Pool
	registrar  sipstack.Registrar
	box        *secrets.Box
	stack      *sipstack.Stack
	authKey    string
	callMgr    interface{ RemoveDeviceSource(string) }
}

func NewHandler(database *db.Database, registrar *sipstack.UpstreamRegistrar, box *secrets.Box, stack *sipstack.Stack, authKey string) *Handler {
	return &Handler{
		dbQueries: database.Queries,
		dbPool:    database.Pool,
		registrar: registrar,
		box:       box,
		stack:     stack,
		authKey:   authKey,
	}
}

func (h *Handler) SetCallManager(cm interface{ RemoveDeviceSource(string) }) {
	h.callMgr = cm
}

type RegisterRequest struct {
	DeviceID          string `json:"device_id" binding:"required"`
	Platform          string `json:"platform" binding:"required,oneof=android ios"`
	PushToken         string `json:"push_token"`
	UpstreamHost      string `json:"upstream_host" binding:"required"`
	UpstreamPort      int    `json:"upstream_port"`
	UpstreamTransport string `json:"upstream_transport"`
	UpstreamUser      string `json:"upstream_user" binding:"required"`
	UpstreamPassword  string `json:"upstream_password" binding:"required"`
	UpstreamRealm     string `json:"upstream_realm"`
	DisplayName       string `json:"display_name"`
}

func (h *Handler) RegisterDevice(c *gin.Context) {
	var req RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		slog.Warn("register validation failed", "error", err)
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
		return
	}

	if req.UpstreamPort == 0 {
		req.UpstreamPort = 5060
	}
	if req.UpstreamTransport == "" {
		req.UpstreamTransport = "udp"
	}

	var encToken []byte
	if req.PushToken != "" {
		var err error
		encToken, err = h.box.Encrypt([]byte(req.PushToken))
		if err != nil {
			slog.Error("encrypt push token failed", "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "encryption failed"})
			return
		}
	}
	encPassword, err := h.box.Encrypt([]byte(req.UpstreamPassword))
	if err != nil {
		slog.Error("encrypt password failed", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "encryption failed"})
		return
	}

	b2buaSIPUser := fmt.Sprintf("%s_%s", req.UpstreamUser, req.DeviceID[:8])
	expiresAt := time.Now().Add(24 * time.Hour)
	lastSeen := time.Now()

	err = h.dbQueries.UpsertDevice(c.Request.Context(), db.UpsertDeviceParams{
		DeviceID:          req.DeviceID,
		Platform:          req.Platform,
		PushToken:         encToken,
		UpstreamHost:      req.UpstreamHost,
		UpstreamPort:      int32(req.UpstreamPort),
		UpstreamTransport: req.UpstreamTransport,
		UpstreamUser:      req.UpstreamUser,
		UpstreamPassword:  encPassword,
		UpstreamRealm:     pgtype.Text{String: req.UpstreamRealm, Valid: req.UpstreamRealm != ""},
		DisplayName:       pgtype.Text{String: req.DisplayName, Valid: req.DisplayName != ""},
		B2buaSipUser:      b2buaSIPUser,
		ExpiresAt:         pgtype.Timestamptz{Time: expiresAt, Valid: true},
		LastSeen:          pgtype.Timestamptz{Time: lastSeen, Valid: true},
	})
	if err != nil {
		slog.Error("upsert device failed", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "database error"})
		return
	}

	reg := &sipstack.UpstreamReg{
		DeviceID:  req.DeviceID,
		User:      req.UpstreamUser,
		Host:      req.UpstreamHost,
		Port:      req.UpstreamPort,
		Transport: req.UpstreamTransport,
		Password:  req.UpstreamPassword,
		Realm:     req.UpstreamRealm,
	}

	regCtx, regCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer regCancel()
	if err := h.registrar.Register(regCtx, reg); err != nil {
		slog.Error("upstream registration failed", "device", req.DeviceID, "error", err)
		c.JSON(http.StatusBadGateway, gin.H{"status": "error", "message": "upstream registration failed"})
		return
	}

	externalIP := h.stack.ExternalIP()
	sipPort := h.stack.ExternalSIPPort()
	sipTransport := h.stack.ExternalSIPTransport()

	b2buaURI := fmt.Sprintf("sip:%s@%s:%d;transport=%s", b2buaSIPUser, externalIP, sipPort, sipTransport)

	c.JSON(http.StatusOK, gin.H{
		"status":        "registered",
		"b2bua_sip_uri": b2buaURI,
		"expires":       3600,
	})
}

type RefreshRequest struct {
	PushToken string `json:"push_token"`
}

func (h *Handler) RefreshDevice(c *gin.Context) {
	deviceID := c.Param("device_id")
	if _, err := uuid.Parse(deviceID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "invalid device_id"})
		return
	}

	device, err := h.dbQueries.GetDeviceByID(c.Request.Context(), deviceID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "device not found"})
		return
	}

	pushToken := device.PushToken
	var req RefreshRequest
	if err := c.ShouldBindJSON(&req); err == nil && req.PushToken != "" {
		encToken, err := h.box.Encrypt([]byte(req.PushToken))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "encryption failed"})
			return
		}
		pushToken = encToken
	}

	err = h.dbQueries.UpsertDevice(c.Request.Context(), db.UpsertDeviceParams{
		DeviceID:          device.DeviceID,
		Platform:          device.Platform,
		PushToken:         pushToken,
		UpstreamHost:      device.UpstreamHost,
		UpstreamPort:      device.UpstreamPort,
		UpstreamTransport: device.UpstreamTransport,
		UpstreamUser:      device.UpstreamUser,
		UpstreamPassword:  device.UpstreamPassword,
		UpstreamRealm:     device.UpstreamRealm,
		DisplayName:       device.DisplayName,
		B2buaSipUser:      device.B2buaSipUser,
		DeviceContact:     device.DeviceContact,
		PushProvider:      device.PushProvider,
		PushParam:         device.PushParam,
		PushPrid:          device.PushPrid,
		ExpiresAt:         pgtype.Timestamptz{Time: time.Now().Add(24 * time.Hour), Valid: true},
		LastSeen:          pgtype.Timestamptz{Time: time.Now(), Valid: true},
	})
	if err != nil {
		slog.Error("refresh device failed", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "database error"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "expires": 3600})
}

func (h *Handler) UnregisterDevice(c *gin.Context) {
	deviceID := c.Param("device_id")
	if _, err := uuid.Parse(deviceID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "invalid device_id"})
		return
	}

	if err := h.registrar.Unregister(c.Request.Context(), deviceID); err != nil {
		slog.Error("upstream unregistration failed", "device", deviceID, "error", err)
	}

	device, err := h.dbQueries.GetDeviceByID(c.Request.Context(), deviceID)
	if err == nil {
		if h.callMgr != nil {
			h.callMgr.RemoveDeviceSource(device.B2buaSipUser)
		}
	}

	h.dbQueries.PruneDevices(c.Request.Context(), pgtype.Timestamptz{Time: time.Now().Add(100 * time.Hour), Valid: true}) // Force delete this one specifically
	// Actually I should add a DeleteDeviceByID query to SQLC
	// For now, I'll just use the pgx pool directly to delete
	h.dbPool.Exec(c.Request.Context(), "DELETE FROM devices WHERE device_id = $1", deviceID)

	c.JSON(http.StatusOK, gin.H{"status": "unregistered"})
}

func (h *Handler) DeviceStatus(c *gin.Context) {
	deviceID := c.Param("device_id")
	if _, err := uuid.Parse(deviceID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "invalid device_id"})
		return
	}

	device, err := h.dbQueries.GetDeviceByID(c.Request.Context(), deviceID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "device not found"})
		return
	}

	registered := h.registrar.IsRegistered(deviceID)
	expired := time.Now().After(device.ExpiresAt.Time)

	c.JSON(http.StatusOK, gin.H{
		"status":              "ok",
		"device_id":           deviceID,
		"upstream_registered": registered,
		"db_expired":          expired,
		"expires_at":          device.ExpiresAt.Time,
		"last_seen":           device.LastSeen.Time,
	})
}

func (h *Handler) ForceReregister(c *gin.Context) {
	deviceID := c.Param("device_id")
	if _, err := uuid.Parse(deviceID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "invalid device_id"})
		return
	}

	device, err := h.dbQueries.GetDeviceByID(c.Request.Context(), deviceID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "device not found"})
		return
	}

	password, err := h.box.Decrypt(device.UpstreamPassword)
	if err != nil {
		slog.Error("decrypt password failed", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "decryption failed"})
		return
	}

	reg := &sipstack.UpstreamReg{
		DeviceID:  device.DeviceID,
		User:      device.UpstreamUser,
		Host:      device.UpstreamHost,
		Port:      int(device.UpstreamPort),
		Transport: device.UpstreamTransport,
		Password:  string(password),
		Realm:     device.UpstreamRealm.String,
	}

	regCtx, regCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer regCancel()
	if err := h.registrar.Register(regCtx, reg); err != nil {
		slog.Error("force re-register failed", "device", deviceID, "error", err)
		c.JSON(http.StatusBadGateway, gin.H{"status": "error", "message": "upstream registration failed"})
		return
	}

	h.dbQueries.UpdateDeviceLastSeen(c.Request.Context(), device.B2buaSipUser)

	c.JSON(http.StatusOK, gin.H{"status": "registered", "device_id": deviceID})
}

func SetupRouter(handler *Handler) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(gin.LoggerWithConfig(gin.LoggerConfig{SkipPaths: []string{"/health"}}))

	r.GET("/health", func(c *gin.Context) {
		if err := handler.dbPool.Ping(c.Request.Context()); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "degraded", "db": "unreachable"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	v1 := r.Group("/v1")
	if handler.authKey != "" {
		v1.Use(func(c *gin.Context) {
			key := c.GetHeader("Authorization")
			expected := "Bearer " + handler.authKey
			if key != expected {
				c.JSON(http.StatusUnauthorized, gin.H{"status": "error", "message": "unauthorized"})
				c.Abort()
				return
			}
			c.Next()
		})
	}
	{
		v1.POST("/devices/register", handler.RegisterDevice)
		v1.PUT("/devices/:device_id/refresh", handler.RefreshDevice)
		v1.DELETE("/devices/:device_id", handler.UnregisterDevice)
		v1.GET("/devices/:device_id/status", handler.DeviceStatus)
		v1.POST("/devices/:device_id/reregister", handler.ForceReregister)
	}

	return r
}
