package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/AlchemillaHQ/Sentry/auth"
	"github.com/AlchemillaHQ/Sentry/config"
	"github.com/AlchemillaHQ/Sentry/db"
	"github.com/AlchemillaHQ/Sentry/secrets"
	"github.com/AlchemillaHQ/Sentry/sipstack"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog/log"
)

type Handler struct {
	dbQueries  db.Querier
	dbPool     db.DBTX
	registrar  sipstack.Registrar
	box        *secrets.Box
	stack      *sipstack.Stack
	jwtSecret  string
	callMgr    interface {
		RemoveDeviceSource(string)
		GetPendingCallsCount() int
	}
}

func NewHandler(database *db.Database, registrar sipstack.Registrar, box *secrets.Box, stack *sipstack.Stack, apiCfg config.APIConfig) *Handler {
	return &Handler{
		dbQueries: database.Queries,
		dbPool:    database.Pool,
		registrar: registrar,
		box:       box,
		stack:     stack,
		jwtSecret: apiCfg.JWTSecret,
	}
}

func (h *Handler) SetCallManager(cm interface {
	RemoveDeviceSource(string)
	GetPendingCallsCount() int
}) {
	h.callMgr = cm
}

func (h *Handler) AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		token := c.GetHeader("Authorization")
		if len(token) > 7 && token[:7] == "Bearer " {
			token = token[7:]
		}

		if h.jwtSecret == "" {
			c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "JWT secret not configured"})
			c.Abort()
			return
		}

		claims, err := auth.ValidateToken(token, h.jwtSecret)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"status": "error", "message": "unauthorized"})
			c.Abort()
			return
		}

		c.Set("username", claims.Username)
		c.Set("role", claims.Role)
		c.Next()
	}
}

type LoginRequest struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

func (h *Handler) Login(c *gin.Context) {
	var req LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
		return
	}

	user, err := h.dbQueries.GetUser(c.Request.Context(), req.Username)
	if err != nil {
		log.Warn().Str("username", req.Username).Msg("login failed: user not found")
		c.JSON(http.StatusUnauthorized, gin.H{"status": "error", "message": "invalid credentials"})
		return
	}

	if !auth.CheckPasswordHash(req.Password, user.PasswordHash) {
		log.Warn().Str("username", req.Username).Msg("login failed: invalid password")
		c.JSON(http.StatusUnauthorized, gin.H{"status": "error", "message": "invalid credentials"})
		return
	}

	token, _ := auth.GenerateToken(user.Username, user.Role, h.jwtSecret)

	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
		"token":  token,
		"user": gin.H{
			"username": user.Username,
			"role":     user.Role,
		},
	})
}

func (h *Handler) DashboardStats(c *gin.Context) {
	ctx := c.Request.Context()

	var deviceCount int64
	h.dbPool.QueryRow(ctx, "SELECT COUNT(*) FROM devices").Scan(&deviceCount)

	activeCalls := 0
	if h.callMgr != nil {
		activeCalls = h.callMgr.GetPendingCallsCount()
	}

	c.JSON(http.StatusOK, gin.H{
		"registered_devices": deviceCount,
		"active_calls":       activeCalls,
		"db_status":          "healthy",
	})
}

func (h *Handler) ListDevices(c *gin.Context) {
	// We could use SQLC for this, but for simple lists a raw query is fine too.
	// For consistency with SQLC models, let's use a query that matches.
	rows, err := h.dbPool.Query(c.Request.Context(), "SELECT device_id, platform, upstream_host, upstream_user, display_name, last_seen, disabled FROM devices ORDER BY last_seen DESC")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "failed to list devices"})
		return
	}
	defer rows.Close()

	devices := make([]map[string]interface{}, 0)
	for rows.Next() {
		var dID, platform, host, user string
		var displayName pgtype.Text
		var lastSeen pgtype.Timestamptz
		var disabled bool
		if err := rows.Scan(&dID, &platform, &host, &user, &displayName, &lastSeen, &disabled); err != nil {
			log.Error().Err(err).Msg("failed to scan device row")
			continue
		}
		devices = append(devices, map[string]interface{}{
			"device_id":     dID,
			"platform":      platform,
			"upstream_host": host,
			"upstream_user": user,
			"display_name":  displayName.String,
			"last_seen":     lastSeen.Time,
			"disabled":      disabled,
		})
	}
	c.JSON(http.StatusOK, devices)
}

func (h *Handler) ListUsers(c *gin.Context) {
	users, err := h.dbQueries.ListUsers(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "failed to list users"})
		return
	}
	c.JSON(http.StatusOK, users)
}

type CreateUserRequest struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
	Role     string `json:"role" binding:"required,oneof=admin viewer"`
}

func (h *Handler) CreateUser(c *gin.Context) {
	var req CreateUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
		return
	}

	hash, _ := auth.HashPassword(req.Password)

	tag, err := h.dbPool.Exec(c.Request.Context(),
		"INSERT INTO users (username, password_hash, role) VALUES ($1, $2, $3) ON CONFLICT (username) DO NOTHING",
		req.Username, hash, req.Role)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "failed to create user"})
		return
	}

	if tag.RowsAffected() == 0 {
		c.JSON(http.StatusConflict, gin.H{"status": "error", "message": "username already exists"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"status": "ok"})
}

func (h *Handler) CleanupJunkDevices(c *gin.Context) {
	tag, err := h.dbPool.Exec(c.Request.Context(), `
		DELETE FROM devices
		WHERE upstream_host !~ '^([0-9]{1,3}\.){3}[0-9]{1,3}$'
		  AND upstream_host !~ '^([a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$'
	`)
	if err != nil {
		log.Error().Err(err).Msg("failed to cleanup junk devices")
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "cleanup failed"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok", "deleted": tag.RowsAffected()})
}

func (h *Handler) DeleteUser(c *gin.Context) {
	username := c.Param("username")

	tag, err := h.dbPool.Exec(c.Request.Context(), "DELETE FROM users WHERE username = $1", username)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "failed to delete user"})
		return
	}

	if tag.RowsAffected() == 0 {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "user not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
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

func isValidHost(host string) bool {
	if ip := net.ParseIP(host); ip != nil {
		return true
	}

	// RFC-1123 hostname validation:
	// - Total length 1-253 characters
	// - Labels separated by dots, each label 1-63 characters
	// - Characters: a-z, A-Z, 0-9, hyphen
	// - Labels cannot start or end with hyphen
	// - At least two labels for FQDN
	// - TLD must contain at least one letter
	if len(host) == 0 || len(host) > 253 {
		return false
	}

	labels := strings.Split(host, ".")
	if len(labels) < 2 {
		return false
	}

	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 {
			return false
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-') {
				return false
			}
		}
	}

	// TLD must contain at least one letter (rejectes "123", "456", etc.)
	tld := labels[len(labels)-1]
	for i := 0; i < len(tld); i++ {
		if (tld[i] >= 'a' && tld[i] <= 'z') || (tld[i] >= 'A' && tld[i] <= 'Z') {
			return true
		}
	}
	return false
}

func (h *Handler) RegisterDevice(c *gin.Context) {
	var req RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.Warn().Err(err).Msg("register validation failed")
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
		return
	}

	if _, err := uuid.Parse(req.DeviceID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "invalid device_id: must be a valid UUID"})
		return
	}

	if !isValidHost(req.UpstreamHost) {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "invalid upstream_host: must be a valid IP address or FQDN"})
		return
	}

	if req.UpstreamPort == 0 {
		req.UpstreamPort = 5060
	}
	if req.UpstreamPort < 1 || req.UpstreamPort > 65535 {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "invalid upstream_port: must be 1-65535"})
		return
	}
	if req.UpstreamTransport == "" {
		req.UpstreamTransport = "udp"
	}

	var encToken []byte
	if req.PushToken != "" {
		encToken, _ = h.box.Encrypt([]byte(req.PushToken))
	}
	encPassword, _ := h.box.Encrypt([]byte(req.UpstreamPassword))

	b2buaSIPUser := fmt.Sprintf("%s_%s", req.UpstreamUser, req.DeviceID[:8])
	expiresAt := time.Now().Add(24 * time.Hour)
	lastSeen := time.Now()

	err := h.dbQueries.UpsertDevice(c.Request.Context(), db.UpsertDeviceParams{
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
		DeviceContact:     pgtype.Text{Valid: false},
		UserAgent:         pgtype.Text{Valid: false},
		PushProvider:      pgtype.Text{Valid: false},
		PushParam:         pgtype.Text{Valid: false},
		PushPrid:          pgtype.Text{Valid: false},
		ExpiresAt:         pgtype.Timestamptz{Time: expiresAt, Valid: true},
		LastSeen:          pgtype.Timestamptz{Time: lastSeen, Valid: true},
	})
	if err != nil {
		log.Error().Err(err).Msg("upsert device failed")
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
		log.Error().Err(err).Str("device", req.DeviceID).Msg("upstream registration failed")
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
		encToken, _ := h.box.Encrypt([]byte(req.PushToken))
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
		UserAgent:         device.UserAgent,
		PushProvider:      device.PushProvider,
		PushParam:         device.PushParam,
		PushPrid:          device.PushPrid,
		ExpiresAt:         pgtype.Timestamptz{Time: time.Now().Add(24 * time.Hour), Valid: true},
		LastSeen:          pgtype.Timestamptz{Time: time.Now(), Valid: true},
	})
	if err != nil {
		log.Error().Err(err).Msg("refresh device failed")
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
		log.Error().Err(err).Str("device", deviceID).Msg("upstream unregistration failed")
	}

	device, err := h.dbQueries.GetDeviceByID(c.Request.Context(), deviceID)
	if err == nil {
		if h.callMgr != nil {
			h.callMgr.RemoveDeviceSource(device.B2buaSipUser)
		}
	}

	if err := h.dbQueries.DeleteDeviceByID(c.Request.Context(), deviceID); err != nil {
		log.Error().Err(err).Str("device", deviceID).Msg("failed to delete device")
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "database error"})
		return
	}

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
		"disabled":            device.Disabled,
	})
}

func (h *Handler) DisableDevice(c *gin.Context) {
	deviceID := c.Param("device_id")
	if _, err := uuid.Parse(deviceID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "invalid device_id"})
		return
	}

	_, err := h.dbQueries.GetDeviceByID(c.Request.Context(), deviceID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "device not found"})
		return
	}

	if err := h.registrar.Unregister(c.Request.Context(), deviceID); err != nil {
		log.Error().Err(err).Str("device", deviceID).Msg("upstream unregistration on disable failed")
	}

	err = h.dbQueries.SetDeviceDisabled(c.Request.Context(), db.SetDeviceDisabledParams{
		DeviceID: deviceID,
		Disabled: true,
	})
	if err != nil {
		log.Error().Err(err).Str("device", deviceID).Msg("set disabled flag failed")
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "database error"})
		return
	}

	log.Info().Str("device", deviceID).Msg("device disabled")
	c.JSON(http.StatusOK, gin.H{"status": "disabled"})
}

func (h *Handler) EnableDevice(c *gin.Context) {
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
		log.Error().Err(err).Msg("decrypt password failed")
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
		log.Error().Err(err).Str("device", deviceID).Msg("upstream re-registration on enable failed")
		c.JSON(http.StatusBadGateway, gin.H{"status": "error", "message": "upstream registration failed"})
		return
	}

	err = h.dbQueries.SetDeviceDisabled(c.Request.Context(), db.SetDeviceDisabledParams{
		DeviceID: deviceID,
		Disabled: false,
	})
	if err != nil {
		log.Error().Err(err).Str("device", deviceID).Msg("clear disabled flag failed")
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "database error"})
		return
	}

	log.Info().Str("device", deviceID).Msg("device enabled")
	c.JSON(http.StatusOK, gin.H{"status": "enabled"})
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
		log.Error().Err(err).Msg("decrypt password failed")
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
		log.Error().Err(err).Str("device", deviceID).Msg("force re-register failed")
		c.JSON(http.StatusBadGateway, gin.H{"status": "error", "message": "upstream registration failed"})
		return
	}

	if err := h.dbQueries.UpdateDeviceLastSeen(c.Request.Context(), device.B2buaSipUser); err != nil {
		log.Error().Err(err).Str("device", deviceID).Msg("failed to update last_seen after re-register")
	}

	c.JSON(http.StatusOK, gin.H{"status": "registered", "device_id": deviceID})
}

func SetupRouter(handler *Handler, cfg *config.Config) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"http://localhost:5173", "http://127.0.0.1:5173"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))
	r.Use(gin.LoggerWithConfig(gin.LoggerConfig{SkipPaths: []string{"/health"}}))

	r.GET("/health", func(c *gin.Context) {
		if _, err := handler.dbPool.Exec(c.Request.Context(), "SELECT 1"); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "degraded", "db": "unreachable"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	v1 := r.Group("/v1")

	// Public routes
	v1.POST("/auth/login", handler.Login)

	// Admin routes (protected by JWT)
	admin := v1.Group("/admin")
	admin.Use(handler.AuthMiddleware())
	{
		admin.GET("/stats", handler.DashboardStats)
		admin.GET("/devices", handler.ListDevices)
		admin.POST("/devices/cleanup-junk", handler.CleanupJunkDevices)
		admin.GET("/users", handler.ListUsers)
		admin.POST("/users", handler.CreateUser)
		admin.DELETE("/users/:username", handler.DeleteUser)
	}

	// Mobile API routes
	mobile := v1.Group("/devices")
	{
		if cfg != nil && cfg.RateLimit.RegisterRate > 0 {
			rl := NewIPRateLimiter(cfg.RateLimit.RegisterRate, cfg.RateLimit.RegisterBurst)
			mobile.POST("/register", RateLimitMiddleware(rl), handler.RegisterDevice)
		} else {
			mobile.POST("/register", handler.RegisterDevice)
		}
		mobile.PUT("/:device_id/refresh", handler.RefreshDevice)
		mobile.DELETE("/:device_id", handler.UnregisterDevice)
		mobile.GET("/:device_id/status", handler.DeviceStatus)
		mobile.POST("/:device_id/reregister", handler.ForceReregister)
		mobile.POST("/:device_id/disable", handler.DisableDevice)
		mobile.POST("/:device_id/enable", handler.EnableDevice)
	}

	return r
}
