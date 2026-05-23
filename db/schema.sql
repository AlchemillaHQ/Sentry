CREATE TABLE IF NOT EXISTS settings (
    key VARCHAR(255) PRIMARY KEY,
    value BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS devices (
    device_id VARCHAR(36) PRIMARY KEY,
    platform VARCHAR(10) NOT NULL,
    push_token BYTEA NOT NULL,
    upstream_host VARCHAR(255) NOT NULL,
    upstream_port INTEGER NOT NULL DEFAULT 5060,
    upstream_transport VARCHAR(10) NOT NULL DEFAULT 'udp',
    upstream_user VARCHAR(255) NOT NULL,
    upstream_password BYTEA NOT NULL,
    upstream_realm VARCHAR(255),
    display_name VARCHAR(255),
    b2bua_sip_user VARCHAR(255) UNIQUE NOT NULL,
    device_contact VARCHAR(255),
    push_provider VARCHAR(10),
    push_param VARCHAR(255),
    push_prid VARCHAR(512),
    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_devices_b2bua_sip_user ON devices(b2bua_sip_user);
CREATE INDEX IF NOT EXISTS idx_devices_expires_at ON devices(expires_at);

CREATE TABLE IF NOT EXISTS pending_calls (
    call_id VARCHAR(36) PRIMARY KEY,
    device_id VARCHAR(36) NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
    sip_call_id VARCHAR(255) NOT NULL,
    sip_from TEXT NOT NULL,
    sip_to TEXT NOT NULL,
    sdp_offer TEXT,
    caller_uri TEXT NOT NULL,
    caller_name VARCHAR(255),
    state VARCHAR(30) NOT NULL DEFAULT 'PENDING_PUSH',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pending_calls_device_id ON pending_calls(device_id);
CREATE INDEX IF NOT EXISTS idx_pending_calls_expires_at ON pending_calls(expires_at);
