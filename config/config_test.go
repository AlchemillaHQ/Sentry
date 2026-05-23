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
