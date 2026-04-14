package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type SIPConfig struct {
	UDPAddr    string `yaml:"udp_addr"`
	TCPAddr    string `yaml:"tcp_addr"`
	TLSAddr    string `yaml:"tls_addr"`
	TLSCert    string `yaml:"tls_cert"`
	TLSKey     string `yaml:"tls_key"`
	ExternalIP string `yaml:"external_ip"`
	UserAgent  string `yaml:"user_agent"`
}

type APIConfig struct {
	Addr    string `yaml:"addr"`
	TLSCert string `yaml:"tls_cert"`
	TLSKey  string `yaml:"tls_key"`
}

type DatabaseConfig struct {
	Driver string `yaml:"driver"`
	DSN    string `yaml:"dsn"`
}

type SecretsConfig struct {
	EncryptionKey string `yaml:"encryption_key"`
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

type Config struct {
	SIP      SIPConfig      `yaml:"sip"`
	API      APIConfig      `yaml:"api"`
	Database DatabaseConfig `yaml:"database"`
	Secrets  SecretsConfig  `yaml:"secrets"`
	Push     PushConfig     `yaml:"push"`
	Pprof    PprofConfig    `yaml:"pprof"`
	Log      LogConfig      `yaml:"log"`
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
			UserAgent: "Difuse-B2BUA/1.0",
		},
		API: APIConfig{
			Addr: "0.0.0.0:8080",
		},
		Database: DatabaseConfig{
			Driver: "sqlite",
			DSN:    "difuse.db",
		},
		Log: LogConfig{
			Level: "info",
		},
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	return cfg, nil
}
