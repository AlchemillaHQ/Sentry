package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type SIPConfig struct {
	UDPAddr               string `yaml:"udp_addr"`
	TCPAddr               string `yaml:"tcp_addr"`
	TLSAddr               string `yaml:"tls_addr"`
	TLSCert               string `yaml:"tls_cert"`
	TLSKey                string `yaml:"tls_key"`
	ExternalIP            string `yaml:"external_ip"`
	ExternalSIPPort       int    `yaml:"external_sip_port"`
	ExternalSIPTransport  string `yaml:"external_sip_transport"`
	UserAgent             string `yaml:"user_agent"`
	LogSIP                bool   `yaml:"log_sip"`
	TLSInsecureSkipVerify bool   `yaml:"tls_insecure_skip_verify"`
}

type APIConfig struct {
	Addr      string `yaml:"addr"`
	TLSCert   string `yaml:"tls_cert"`
	TLSKey    string `yaml:"tls_key"`
	JWTSecret string `yaml:"jwt_secret"`
}

type BootstrapUser struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type AdminConfig struct {
	BootstrapUsers []BootstrapUser `yaml:"bootstrap_users"`
}

type DatabaseConfig struct {
	Driver string `yaml:"driver"`
	DSN    string `yaml:"dsn"`
}

type PushConfig struct {
	FCMServiceAccount string `yaml:"fcm_service_account"`
	APNsCert          string `yaml:"apns_cert"`
	APNsKeyID         string `yaml:"apns_key_id"`
	APNsTeamID        string `yaml:"apns_team_id"`
	APNsBundleID      string `yaml:"apns_bundle_id"`
	APNsProduction    bool   `yaml:"apns_production"`
}

type PprofConfig struct {
	Addr string `yaml:"addr"`
}

type LogConfig struct {
	Level string `yaml:"level"`
}

type RateLimitConfig struct {
	RegisterRate  float64 `yaml:"register_rate"`
	RegisterBurst int     `yaml:"register_burst"`
}

// RegistrarConfig controls upstream registration health and recovery. The
// defaults intentionally favor fast failure detection while bounding the
// amount of traffic sent to any one gateway and by the process as a whole.
type RegistrarConfig struct {
	ExpiresSeconds               int     `yaml:"expires_seconds"`
	RefreshPercent               int     `yaml:"refresh_percent"`
	AttemptTimeoutSeconds        int     `yaml:"attempt_timeout_seconds"`
	ProbeEnabled                 bool    `yaml:"probe_enabled"`
	ProbeIntervalMilliseconds    int     `yaml:"probe_interval_milliseconds"`
	ProbeTimeoutMilliseconds     int     `yaml:"probe_timeout_milliseconds"`
	ProbeFailureThreshold        int     `yaml:"probe_failure_threshold"`
	DownProbeIntervalMillis      int     `yaml:"down_probe_interval_milliseconds"`
	RegisterCanaryIntervalMillis int     `yaml:"register_canary_interval_milliseconds"`
	ProbeGlobalWorkers           int     `yaml:"probe_global_workers"`
	ProbeGlobalMaxRate           float64 `yaml:"probe_global_max_rate"`
	RecoveryWorkersPerGateway    int     `yaml:"recovery_workers_per_gateway"`
	RecoveryInitialRate          float64 `yaml:"recovery_initial_rate"`
	RecoveryMaxRate              float64 `yaml:"recovery_max_rate"`
	RecoveryGlobalWorkers        int     `yaml:"recovery_global_workers"`
	RecoveryGlobalMaxRate        float64 `yaml:"recovery_global_max_rate"`
}

func DefaultRegistrarConfig() RegistrarConfig {
	return RegistrarConfig{
		ExpiresSeconds:               600,
		RefreshPercent:               70,
		AttemptTimeoutSeconds:        8,
		ProbeEnabled:                 true,
		ProbeIntervalMilliseconds:    3000,
		ProbeTimeoutMilliseconds:     1200,
		ProbeFailureThreshold:        2,
		DownProbeIntervalMillis:      1000,
		RegisterCanaryIntervalMillis: 10000,
		ProbeGlobalWorkers:           128,
		ProbeGlobalMaxRate:           1000,
		RecoveryWorkersPerGateway:    64,
		RecoveryInitialRate:          25,
		RecoveryMaxRate:              500,
		RecoveryGlobalWorkers:        512,
		RecoveryGlobalMaxRate:        1000,
	}
}

// WithDefaults makes programmatically-created partial configurations safe.
// ProbeEnabled is deliberately not changed so callers can explicitly disable
// active probing.
func (c RegistrarConfig) WithDefaults() RegistrarConfig {
	d := DefaultRegistrarConfig()
	if c.ExpiresSeconds <= 0 {
		c.ExpiresSeconds = d.ExpiresSeconds
	}
	if c.RefreshPercent <= 0 {
		c.RefreshPercent = d.RefreshPercent
	}
	if c.AttemptTimeoutSeconds <= 0 {
		c.AttemptTimeoutSeconds = d.AttemptTimeoutSeconds
	}
	if c.ProbeIntervalMilliseconds <= 0 {
		c.ProbeIntervalMilliseconds = d.ProbeIntervalMilliseconds
	}
	if c.ProbeTimeoutMilliseconds <= 0 {
		c.ProbeTimeoutMilliseconds = d.ProbeTimeoutMilliseconds
	}
	if c.ProbeFailureThreshold <= 0 {
		c.ProbeFailureThreshold = d.ProbeFailureThreshold
	}
	if c.DownProbeIntervalMillis <= 0 {
		c.DownProbeIntervalMillis = d.DownProbeIntervalMillis
	}
	if c.RegisterCanaryIntervalMillis <= 0 {
		c.RegisterCanaryIntervalMillis = d.RegisterCanaryIntervalMillis
	}
	if c.ProbeGlobalWorkers <= 0 {
		c.ProbeGlobalWorkers = d.ProbeGlobalWorkers
	}
	if c.ProbeGlobalMaxRate <= 0 {
		c.ProbeGlobalMaxRate = d.ProbeGlobalMaxRate
	}
	if c.RecoveryWorkersPerGateway <= 0 {
		c.RecoveryWorkersPerGateway = d.RecoveryWorkersPerGateway
	}
	if c.RecoveryInitialRate <= 0 {
		c.RecoveryInitialRate = d.RecoveryInitialRate
	}
	if c.RecoveryMaxRate <= 0 {
		c.RecoveryMaxRate = d.RecoveryMaxRate
	}
	if c.RecoveryGlobalWorkers <= 0 {
		c.RecoveryGlobalWorkers = d.RecoveryGlobalWorkers
	}
	if c.RecoveryGlobalMaxRate <= 0 {
		c.RecoveryGlobalMaxRate = d.RecoveryGlobalMaxRate
	}
	return c
}

type Config struct {
	SIP           SIPConfig       `yaml:"sip"`
	API           APIConfig       `yaml:"api"`
	Admin         AdminConfig     `yaml:"admin"`
	Database      DatabaseConfig  `yaml:"database"`
	Push          PushConfig      `yaml:"push"`
	Pprof         PprofConfig     `yaml:"pprof"`
	Log           LogConfig       `yaml:"log"`
	RateLimit     RateLimitConfig `yaml:"ratelimit"`
	Registrar     RegistrarConfig `yaml:"registrar"`
	EncryptionKey string          `yaml:"encryption_key"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	cfg := &Config{
		SIP: SIPConfig{
			UDPAddr:   "0.0.0.0:5060",
			TCPAddr:   "0.0.0.0:5060",
			UserAgent: "Sentry/1.0",
			LogSIP:    false,
		},
		API: APIConfig{
			Addr: "0.0.0.0:8080",
		},
		Database: DatabaseConfig{
			Driver: "sqlite",
			DSN:    "sentry.db",
		},
		Log: LogConfig{
			Level: "info",
		},
		RateLimit: RateLimitConfig{
			RegisterRate:  0.5,
			RegisterBurst: 5,
		},
		Registrar: DefaultRegistrarConfig(),
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if cfg.SIP.ExternalIP == "" {
		return nil, fmt.Errorf("sip.external_ip is required (public IP or hostname reachable by SIP clients)")
	}
	if cfg.SIP.ExternalSIPPort == 0 {
		cfg.SIP.ExternalSIPPort = 5060
	}
	if cfg.SIP.ExternalSIPTransport == "" {
		cfg.SIP.ExternalSIPTransport = "udp"
	}
	if cfg.Registrar.RefreshPercent < 50 || cfg.Registrar.RefreshPercent > 90 {
		return nil, fmt.Errorf("registrar.refresh_percent must be between 50 and 90")
	}
	if cfg.Registrar.ExpiresSeconds < 60 {
		return nil, fmt.Errorf("registrar.expires_seconds must be at least 60")
	}
	if cfg.Registrar.AttemptTimeoutSeconds < 1 {
		return nil, fmt.Errorf("registrar.attempt_timeout_seconds must be at least 1")
	}
	if cfg.Registrar.ProbeFailureThreshold < 2 {
		return nil, fmt.Errorf("registrar.probe_failure_threshold must be at least 2")
	}
	if cfg.Registrar.ProbeEnabled && cfg.Registrar.ProbeIntervalMilliseconds < 500 {
		return nil, fmt.Errorf("registrar.probe_interval_milliseconds must be at least 500")
	}
	if cfg.Registrar.ProbeEnabled && cfg.Registrar.ProbeTimeoutMilliseconds < 100 {
		return nil, fmt.Errorf("registrar.probe_timeout_milliseconds must be at least 100")
	}
	if cfg.Registrar.ProbeEnabled && cfg.Registrar.DownProbeIntervalMillis < 250 {
		return nil, fmt.Errorf("registrar.down_probe_interval_milliseconds must be at least 250")
	}
	if cfg.Registrar.ProbeEnabled && cfg.Registrar.RegisterCanaryIntervalMillis < 1000 {
		return nil, fmt.Errorf("registrar.register_canary_interval_milliseconds must be at least 1000")
	}
	if cfg.Registrar.ProbeEnabled && (cfg.Registrar.ProbeGlobalWorkers < 1 || cfg.Registrar.ProbeGlobalMaxRate <= 0) {
		return nil, fmt.Errorf("registrar global probe limits must be positive")
	}
	if cfg.Registrar.RecoveryWorkersPerGateway < 1 || cfg.Registrar.RecoveryGlobalWorkers < 1 {
		return nil, fmt.Errorf("registrar recovery worker counts must be positive")
	}
	if cfg.Registrar.RecoveryInitialRate <= 0 || cfg.Registrar.RecoveryMaxRate <= 0 || cfg.Registrar.RecoveryGlobalMaxRate <= 0 {
		return nil, fmt.Errorf("registrar recovery rates must be positive")
	}
	if cfg.Registrar.RecoveryMaxRate < cfg.Registrar.RecoveryInitialRate {
		return nil, fmt.Errorf("registrar.recovery_max_rate must be at least recovery_initial_rate")
	}
	if cfg.Registrar.RecoveryGlobalWorkers < cfg.Registrar.RecoveryWorkersPerGateway {
		return nil, fmt.Errorf("registrar.recovery_global_workers must be at least recovery_workers_per_gateway")
	}

	return cfg, nil
}
