package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoad(t *testing.T) {
	content := `
sip:
  udp_addr: ":5060"
  external_ip: "1.2.3.4"
database:
  driver: "postgres"
  dsn: "postgres://localhost:5432"
`
	tmpfile, err := os.CreateTemp("", "config*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(tmpfile.Name())
	assert.NoError(t, err)
	if cfg == nil {
		t.Fatal("config is nil")
	}
	assert.Equal(t, ":5060", cfg.SIP.UDPAddr)
	assert.Equal(t, "1.2.3.4", cfg.SIP.ExternalIP)
	assert.Equal(t, "postgres", cfg.Database.Driver)
	assert.Equal(t, "postgres://localhost:5432", cfg.Database.DSN)
}

func TestLoad_MissingFile(t *testing.T) {
	cfg, err := Load("/nonexistent/config.yaml")
	assert.Nil(t, cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read config")
}

func TestLoad_InvalidYAML(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "config*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte("{{{bad yaml!!!")); err != nil {
		t.Fatal(err)
	}
	tmpfile.Close()

	cfg, err := Load(tmpfile.Name())
	assert.Nil(t, cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parse config")
}

func TestLoad_MissingExternalIP(t *testing.T) {
	content := `
sip:
  udp_addr: ":5060"
database:
  dsn: "postgres://localhost:5432"
`
	tmpfile, err := os.CreateTemp("", "config*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	tmpfile.Close()

	cfg, err := Load(tmpfile.Name())
	assert.Nil(t, cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "external_ip")
}

func TestLoad_Defaults(t *testing.T) {
	content := `
sip:
  external_ip: "10.0.0.1"
`
	tmpfile, err := os.CreateTemp("", "config*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	tmpfile.Close()

	cfg, err := Load(tmpfile.Name())
	assert.NoError(t, err)

	assert.Equal(t, "10.0.0.1", cfg.SIP.ExternalIP)
	assert.Equal(t, 5060, cfg.SIP.ExternalSIPPort)
	assert.Equal(t, "udp", cfg.SIP.ExternalSIPTransport)
	assert.Equal(t, "0.0.0.0:5060", cfg.SIP.UDPAddr)
	assert.Equal(t, "0.0.0.0:5060", cfg.SIP.TCPAddr)
	assert.Equal(t, "Sentry/1.0", cfg.SIP.UserAgent)
	assert.False(t, cfg.SIP.LogSIP)
	assert.Equal(t, "0.0.0.0:8080", cfg.API.Addr)
	assert.Equal(t, "sqlite", cfg.Database.Driver)
	assert.Equal(t, "sentry.db", cfg.Database.DSN)
	assert.Equal(t, "info", cfg.Log.Level)
	assert.Equal(t, 0.5, cfg.RateLimit.RegisterRate)
	assert.Equal(t, 5, cfg.RateLimit.RegisterBurst)
	assert.Equal(t, DefaultRegistrarConfig(), cfg.Registrar)
}

func TestLoad_TransportDefaults(t *testing.T) {
	tests := []struct {
		name      string
		transport string
		expected  string
	}{
		{"empty uses default", "", "udp"},
		{"explicit udp", "udp", "udp"},
		{"tls", "tls", "tls"},
		{"tcp", "tcp", "tcp"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content := "sip:\n  external_ip: \"10.0.0.1\"\n"
			if tt.transport != "" {
				content += "  external_sip_transport: \"" + tt.transport + "\"\n"
			}

			tmpfile, err := os.CreateTemp("", "config*.yaml")
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(tmpfile.Name())

			if _, err := tmpfile.Write([]byte(content)); err != nil {
				t.Fatal(err)
			}
			tmpfile.Close()

			cfg, err := Load(tmpfile.Name())
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, cfg.SIP.ExternalSIPTransport)
		})
	}
}

func TestLoad_ExternalSIPPortDefault(t *testing.T) {
	content := `
sip:
  external_ip: "10.0.0.1"
`
	tmpfile, err := os.CreateTemp("", "config*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	tmpfile.Close()

	cfg, err := Load(tmpfile.Name())
	assert.NoError(t, err)
	assert.Equal(t, 5060, cfg.SIP.ExternalSIPPort)
}

func TestLoad_AllDefaultsPreserved(t *testing.T) {
	content := `
sip:
  external_ip: "1.2.3.4"
  external_sip_port: 5080
  external_sip_transport: "tls"
  log_sip: true
api:
  addr: ":9000"
  jwt_secret: "test-secret"
database:
  driver: "postgres"
  dsn: "user=test dbname=sentry"
log:
  level: "debug"
ratelimit:
  register_rate: 0.2
  register_burst: 10
registrar:
  probe_interval_milliseconds: 5000
  register_canary_interval_milliseconds: 12000
  recovery_initial_rate: 40
  recovery_max_rate: 300
`
	tmpfile, err := os.CreateTemp("", "config*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	tmpfile.Close()

	cfg, err := Load(tmpfile.Name())
	assert.NoError(t, err)

	assert.Equal(t, "1.2.3.4", cfg.SIP.ExternalIP)
	assert.Equal(t, 5080, cfg.SIP.ExternalSIPPort)
	assert.Equal(t, "tls", cfg.SIP.ExternalSIPTransport)
	assert.True(t, cfg.SIP.LogSIP)
	assert.Equal(t, ":9000", cfg.API.Addr)
	assert.Equal(t, "test-secret", cfg.API.JWTSecret)
	assert.Equal(t, "postgres", cfg.Database.Driver)
	assert.Equal(t, "user=test dbname=sentry", cfg.Database.DSN)
	assert.Equal(t, "debug", cfg.Log.Level)
	assert.Equal(t, 0.2, cfg.RateLimit.RegisterRate)
	assert.Equal(t, 10, cfg.RateLimit.RegisterBurst)
	assert.Equal(t, 5000, cfg.Registrar.ProbeIntervalMilliseconds)
	assert.Equal(t, 12000, cfg.Registrar.RegisterCanaryIntervalMillis)
	assert.Equal(t, float64(40), cfg.Registrar.RecoveryInitialRate)
	assert.Equal(t, float64(300), cfg.Registrar.RecoveryMaxRate)
	assert.True(t, cfg.Registrar.ProbeEnabled)
}

func TestLoad_RegistrarValidation(t *testing.T) {
	tests := []struct {
		name      string
		registrar string
		errorText string
	}{
		{
			name:      "refresh percentage",
			registrar: "refresh_percent: 95",
			errorText: "refresh_percent",
		},
		{
			name:      "failure threshold",
			registrar: "probe_failure_threshold: 1",
			errorText: "probe_failure_threshold",
		},
		{
			name:      "probe traffic floor",
			registrar: "probe_interval_milliseconds: 100",
			errorText: "probe_interval_milliseconds",
		},
		{
			name:      "registration canary traffic floor",
			registrar: "register_canary_interval_milliseconds: 500",
			errorText: "register_canary_interval_milliseconds",
		},
		{
			name:      "rate ordering",
			registrar: "recovery_initial_rate: 300\n  recovery_max_rate: 200",
			errorText: "recovery_max_rate",
		},
		{
			name:      "worker ordering",
			registrar: "recovery_workers_per_gateway: 64\n  recovery_global_workers: 32",
			errorText: "recovery_global_workers",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content := "sip:\n  external_ip: 10.0.0.1\nregistrar:\n  " + tt.registrar + "\n"
			tmpfile, err := os.CreateTemp("", "config*.yaml")
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(tmpfile.Name())
			if _, err := tmpfile.Write([]byte(content)); err != nil {
				t.Fatal(err)
			}
			tmpfile.Close()

			cfg, err := Load(tmpfile.Name())
			assert.Nil(t, cfg)
			assert.ErrorContains(t, err, tt.errorText)
		})
	}
}

func TestPushConfigValidate(t *testing.T) {
	tests := []struct {
		name      string
		cfg       PushConfig
		wantMode  string
		errorText string
	}{
		{name: "disabled", cfg: PushConfig{}, wantMode: "disabled"},
		{
			name: "token authentication",
			cfg: PushConfig{
				APNsKey:      "/run/secrets/AuthKey_TEST.p8",
				APNsKeyID:    "KEYID",
				APNsTeamID:   "TEAMID",
				APNsBundleID: "com.difuse.phone",
			},
			wantMode: "token",
		},
		{
			name: "certificate authentication",
			cfg: PushConfig{
				APNsCert:         "/run/secrets/voip.p12",
				APNsCertPassword: "secret",
				APNsBundleID:     "com.difuse.phone",
			},
			wantMode: "certificate",
		},
		{
			name:      "missing token key",
			cfg:       PushConfig{APNsKeyID: "KEYID", APNsTeamID: "TEAMID", APNsBundleID: "com.difuse.phone"},
			errorText: "apns_key",
		},
		{
			name:      "missing key id",
			cfg:       PushConfig{APNsKey: "key.p8", APNsTeamID: "TEAMID", APNsBundleID: "com.difuse.phone"},
			errorText: "apns_key_id",
		},
		{
			name:      "missing team id",
			cfg:       PushConfig{APNsKey: "key.p8", APNsKeyID: "KEYID", APNsBundleID: "com.difuse.phone"},
			errorText: "apns_team_id",
		},
		{
			name:      "missing bundle id",
			cfg:       PushConfig{APNsCert: "voip.p12"},
			errorText: "apns_bundle_id",
		},
		{
			name: "conflicting modes",
			cfg: PushConfig{
				APNsKey:      "key.p8",
				APNsKeyID:    "KEYID",
				APNsTeamID:   "TEAMID",
				APNsCert:     "voip.p12",
				APNsBundleID: "com.difuse.phone",
			},
			errorText: "mutually exclusive",
		},
		{
			name:      "certificate password without certificate",
			cfg:       PushConfig{APNsCertPassword: "secret", APNsBundleID: "com.difuse.phone"},
			errorText: "apns_cert",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.errorText != "" {
				assert.ErrorContains(t, err, tt.errorText)
				return
			}
			assert.NoError(t, err)
			assert.True(t, tt.cfg.APNsEnabled() || tt.wantMode == "disabled")
			assert.Equal(t, tt.wantMode, tt.cfg.APNsAuthMode())
		})
	}
}

func TestLoad_RejectsPartialAPNsConfiguration(t *testing.T) {
	content := `
sip:
  external_ip: "10.0.0.1"
push:
  apns_key: "/run/secrets/AuthKey_TEST.p8"
  apns_bundle_id: "com.difuse.phone"
`
	tmpfile, err := os.CreateTemp("", "config*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	if _, err := tmpfile.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(tmpfile.Name())
	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "apns_key_id")
}
