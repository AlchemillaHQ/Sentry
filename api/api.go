package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/AlchemillaHQ/Difuse-B2BUA/db"
	"github.com/AlchemillaHQ/Difuse-B2BUA/secrets"
	"github.com/AlchemillaHQ/Difuse-B2BUA/sipstack"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Handler struct {
	database  *gorm.DB
	registrar *sipstack.UpstreamRegistrar
	box       *secrets.Box
	stack     *sipstack.Stack
}

func NewHandler(database *gorm.DB, registrar *sipstack.UpstreamRegistrar, box *secrets.Box, stack *sipstack.Stack) *Handler {
	return &Handler{
		database:  database,
		registrar: registrar,
		box:       box,
		stack:     stack,
	}
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

	device := db.Device{
		DeviceID:          req.DeviceID,
		Platform:          req.Platform,
		PushToken:         encToken,
		UpstreamHost:      req.UpstreamHost,
		UpstreamPort:      req.UpstreamPort,
		UpstreamTransport: req.UpstreamTransport,
		UpstreamUser:      req.UpstreamUser,
		UpstreamPassword:  encPassword,
		UpstreamRealm:     req.UpstreamRealm,
		DisplayName:       req.DisplayName,
		B2BUASIPUser:      b2buaSIPUser,
		ExpiresAt:         time.Now().Add(1 * time.Hour),
		LastSeen:          time.Now(),
	}

	result := h.database.Where("device_id = ?", req.DeviceID).First(&db.Device{})
	if result.Error == gorm.ErrRecordNotFound {
		if err := h.database.Create(&device).Error; err != nil {
			slog.Error("create device failed", "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "database error"})
			return
		}
	} else {
		if err := h.database.Model(&db.Device{}).Where("device_id = ?", req.DeviceID).Updates(map[string]interface{}{
			"platform":           device.Platform,
			"push_token":         device.PushToken,
			"upstream_host":      device.UpstreamHost,
			"upstream_port":      device.UpstreamPort,
			"upstream_transport": device.UpstreamTransport,
			"upstream_user":      device.UpstreamUser,
			"upstream_password":  device.UpstreamPassword,
			"upstream_realm":     device.UpstreamRealm,
			"display_name":       device.DisplayName,
			"b2bua_sip_user":     device.B2BUASIPUser,
			"expires_at":         device.ExpiresAt,
			"last_seen":          device.LastSeen,
		}).Error; err != nil {
			slog.Error("update device failed", "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "database error"})
			return
		}
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
	if externalIP == "" {
		externalIP = "localhost"
	}

	c.JSON(http.StatusOK, gin.H{
		"status":        "registered",
		"b2bua_sip_uri": fmt.Sprintf("sip:%s@%s", b2buaSIPUser, externalIP),
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

	var device db.Device
	if err := h.database.Where("device_id = ?", deviceID).First(&device).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "device not found"})
		return
	}

	updates := map[string]interface{}{
		"expires_at": time.Now().Add(1 * time.Hour),
		"last_seen":  time.Now(),
	}

	var req RefreshRequest
	if err := c.ShouldBindJSON(&req); err == nil && req.PushToken != "" {
		encToken, err := h.box.Encrypt([]byte(req.PushToken))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "encryption failed"})
			return
		}
		updates["push_token"] = encToken
	}

	h.database.Model(&db.Device{}).Where("device_id = ?", deviceID).Updates(updates)
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

	h.database.Where("device_id = ?", deviceID).Delete(&db.Device{})
	h.database.Where("device_id = ?", deviceID).Delete(&db.PendingCall{})
	c.JSON(http.StatusOK, gin.H{"status": "unregistered"})
}

func (h *Handler) DeviceStatus(c *gin.Context) {
	deviceID := c.Param("device_id")
	if _, err := uuid.Parse(deviceID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "invalid device_id"})
		return
	}

	var device db.Device
	if err := h.database.Where("device_id = ?", deviceID).First(&device).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "device not found"})
		return
	}

	registered := h.registrar.IsRegistered(deviceID)
	expired := device.ExpiresAt.Before(time.Now())

	c.JSON(http.StatusOK, gin.H{
		"status":              "ok",
		"device_id":           deviceID,
		"upstream_registered": registered,
		"db_expired":          expired,
		"expires_at":          device.ExpiresAt,
		"last_seen":           device.LastSeen,
	})
}

func (h *Handler) ForceReregister(c *gin.Context) {
	deviceID := c.Param("device_id")
	if _, err := uuid.Parse(deviceID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "invalid device_id"})
		return
	}

	var device db.Device
	if err := h.database.Where("device_id = ?", deviceID).First(&device).Error; err != nil {
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
		Port:      device.UpstreamPort,
		Transport: device.UpstreamTransport,
		Password:  string(password),
		Realm:     device.UpstreamRealm,
	}

	regCtx, regCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer regCancel()
	if err := h.registrar.Register(regCtx, reg); err != nil {
		slog.Error("force re-register failed", "device", deviceID, "error", err)
		c.JSON(http.StatusBadGateway, gin.H{"status": "error", "message": "upstream registration failed"})
		return
	}

	h.database.Model(&db.Device{}).Where("device_id = ?", deviceID).Updates(map[string]interface{}{
		"expires_at": time.Now().Add(1 * time.Hour),
		"last_seen":  time.Now(),
	})

	c.JSON(http.StatusOK, gin.H{"status": "registered", "device_id": deviceID})
}

func SetupRouter(handler *Handler) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(gin.LoggerWithConfig(gin.LoggerConfig{SkipPaths: []string{"/health"}}))

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	v1 := r.Group("/v1")
	{
		v1.POST("/devices/register", handler.RegisterDevice)
		v1.PUT("/devices/:device_id/refresh", handler.RefreshDevice)
		v1.DELETE("/devices/:device_id", handler.UnregisterDevice)
		v1.GET("/devices/:device_id/status", handler.DeviceStatus)
		v1.POST("/devices/:device_id/reregister", handler.ForceReregister)
	}

	return r
}
