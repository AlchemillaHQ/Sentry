-- name: GetSetting :one
SELECT value FROM settings WHERE key = $1;

-- name: UpsertSetting :exec
INSERT INTO settings (key, value)
VALUES ($1, $2)
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

-- name: GetDeviceByB2BUASIPUser :one
SELECT * FROM devices WHERE b2bua_sip_user = $1 AND disabled = false LIMIT 1;

-- name: GetDeviceByID :one
SELECT * FROM devices WHERE device_id = $1 LIMIT 1;

-- name: GetDevicesByUpstreamUser :many
SELECT * FROM devices WHERE upstream_user = $1 AND disabled = false;

-- name: UpsertDevice :exec
INSERT INTO devices (
    device_id, platform, push_token, upstream_host, upstream_port,
    upstream_transport, upstream_user, upstream_password, upstream_realm,
    display_name, b2bua_sip_user, device_contact, user_agent, push_provider,
    push_param, push_prid, expires_at, last_seen
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
)
ON CONFLICT (device_id) DO UPDATE SET
    platform = EXCLUDED.platform,
    push_token = EXCLUDED.push_token,
    upstream_host = EXCLUDED.upstream_host,
    upstream_port = EXCLUDED.upstream_port,
    upstream_transport = EXCLUDED.upstream_transport,
    upstream_user = EXCLUDED.upstream_user,
    upstream_password = EXCLUDED.upstream_password,
    upstream_realm = EXCLUDED.upstream_realm,
    display_name = EXCLUDED.display_name,
    b2bua_sip_user = EXCLUDED.b2bua_sip_user,
    device_contact = COALESCE(EXCLUDED.device_contact, devices.device_contact),
    user_agent = COALESCE(EXCLUDED.user_agent, devices.user_agent),
    push_provider = COALESCE(EXCLUDED.push_provider, devices.push_provider),
    push_param = COALESCE(EXCLUDED.push_param, devices.push_param),
    push_prid = COALESCE(EXCLUDED.push_prid, devices.push_prid),
    expires_at = EXCLUDED.expires_at,
    last_seen = EXCLUDED.last_seen;

-- name: UpdateDeviceContact :exec
UPDATE devices SET device_contact = $2, user_agent = $3, last_seen = NOW() WHERE b2bua_sip_user = $1;

-- name: RefreshDeviceExpiry :exec
UPDATE devices SET expires_at = $2, last_seen = NOW() WHERE device_id = $1;

-- name: UpdateDeviceLastSeen :exec
UPDATE devices SET last_seen = NOW() WHERE b2bua_sip_user = $1;

-- name: CreatePendingCall :exec
INSERT INTO pending_calls (
    call_id, device_id, sip_call_id, sip_from, sip_to,
    sdp_offer, caller_uri, caller_name, state, expires_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
);

-- name: GetPendingCall :one
SELECT * FROM pending_calls WHERE call_id = $1 LIMIT 1;

-- name: UpdatePendingCallState :exec
UPDATE pending_calls SET state = $2 WHERE call_id = $1;

-- name: DeletePendingCall :exec
DELETE FROM pending_calls WHERE call_id = $1;

-- name: SetDeviceDisabled :exec
UPDATE devices SET disabled = $2 WHERE device_id = $1;

-- name: PruneDevices :exec
DELETE FROM devices WHERE expires_at < $1;

-- name: DeleteDeviceByID :exec
DELETE FROM devices WHERE device_id = $1;

-- name: PrunePendingCalls :exec
DELETE FROM pending_calls WHERE expires_at < $1;

-- name: GetUser :one
SELECT * FROM users WHERE username = $1 LIMIT 1;

-- name: CreateUser :exec
INSERT INTO users (username, password_hash, role)
VALUES ($1, $2, $3)
ON CONFLICT (username) DO NOTHING;

-- name: UpdateUserPassword :exec
UPDATE users SET password_hash = $2 WHERE username = $1;

-- name: ListUsers :many
SELECT username, role, created_at FROM users ORDER BY created_at DESC;

-- name: DeleteUser :exec
DELETE FROM users WHERE username = $1;
