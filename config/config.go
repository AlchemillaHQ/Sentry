package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

// SIPConfig holds all SIP transport settings.
// Leave TLSAddr or WSSAddr empty to disable that transport.
type SIPConfig struct {
	UDPAddr     string `yaml:"udp_addr"`
	TCPAddr     string `yaml:"tcp_addr"`
	TLSAddr     string `yaml:"tls_addr"`
	WSSAddr     string `yaml:"wss_addr"`
	TLSCert     string `yaml:"tls_cert"`
	TLSKey      string `yaml:"tls_key"`
	DNS         string `yaml:"dns"`
	DisableAuth bool   `yaml:"disable_auth"`
}

// PprofConfig controls the pprof debug HTTP server.
// Bind to 127.0.0.1 in production; never expose to 0.0.0.0.
type PprofConfig struct {
	Addr string `yaml:"addr"`
}

// PushConfig holds file paths for push notification credentials.
type PushConfig struct {
	APNSCert          string `yaml:"apns_cert"`
	FCMServiceAccount string `yaml:"fcm_service_account"`
}

// Config is the top-level configuration struct.
type Config struct {
	SIP   SIPConfig   `yaml:"sip"`
	Pprof PprofConfig `yaml:"pprof"`
	Push  PushConfig  `yaml:"push"`
}

// Defaults returns a Config pre-populated with safe production defaults.
func Defaults() *Config {
	return &Config{
		SIP: SIPConfig{
			UDPAddr: "0.0.0.0:5060",
			TCPAddr: "0.0.0.0:5060",
			DNS:     "8.8.8.8",
			TLSCert: "certs/cert.pem",
			TLSKey:  "certs/key.pem",
		},
		Pprof: PprofConfig{
			Addr: "127.0.0.1:6658",
		},
		Push: PushConfig{
			APNSCert:          "./voip-callkeep.p12",
			FCMServiceAccount: "service-account.json",
		},
	}
}

// Load reads a YAML config file from path, merging over Defaults().
// Missing keys retain their default values.
func Load(path string) (*Config, error) {
	cfg := Defaults()
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
