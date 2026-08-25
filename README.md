# Sentry

Sentry (Back-to-Back User Agent) is a production-grade SIP signaling gateway designed to bridge standard PBX systems (like Asterisk, FreePBX, or FreeSWITCH) with mobile applications using push notifications.

It provides a push-wakeup mechanism for mobile clients while maintaining transparent signaling with the upstream PBX.

## Key Features

- **Scalable Architecture**: Built with Go, PostgreSQL, and SQLC, supporting up to 100,000+ users.
- **Push Notification Support**: Integrated support for Firebase (FCM) and Apple (APNs).
- **High Concurrency**: Uses `pgx/v5` for optimized database connection pooling and binary protocols.
- **Type-Safe Database Layer**: Leverages `sqlc` for zero-reflection, high-performance database operations.
- **B2BUA Signaling**: Manages call legs independently to ensure compatibility with strict PBX requirements.
- **Composite Device IDs**: Supports multiple accounts per device without database collisions.
- **Self-Healing Registration**: Monitors shared upstream gateways and automatically repairs account registration state.

## Getting Started

### Prerequisites

- **Go**: 1.25 or higher.
- **PostgreSQL**: 15 or higher.
- **sqlc**: For generating database code (optional, pre-generated code is included).

### Installation

1. Clone the repository.
2. Install dependencies:
   ```bash
   go mod download
   ```
3. Copy the example configuration and edit it:
   ```bash
   cp config.example.yaml config.yaml
   ```

### Configuration

Update `config.yaml` with your environment details. See `config.example.yaml` for all options.

```yaml
sip:
  udp_addr: "0.0.0.0:5060"
  tcp_addr: "0.0.0.0:5060"
  tls_addr: "0.0.0.0:5061"
  tls_cert: "/path/to/cert.pem"
  tls_key: "/path/to/key.pem"
  external_ip: "your-server-public-ip"
  external_sip_port: 5060
  external_sip_transport: "tls"
  user_agent: "Sentry/1.0"

api:
  addr: "0.0.0.0:8080"
  jwt_secret: "change-me-to-a-random-string"

admin:
  bootstrap_users:
    - username: "admin"
      password: "admin"

database:
  driver: "postgres"
  dsn: "postgres://user:pass@localhost:5432/sentry?sslmode=require"

push:
  fcm_service_account: "service-account.json"
  apns_key: "/run/secrets/AuthKey_XXXXXXXXXX.p8"
  apns_key_id: "XXXXXXXXXX"
  apns_team_id: "YYYYYYYYYY"
  apns_bundle_id: "com.example.difuse" # replace with the final Difuse bundle ID
  apns_production: true

  # Certificate fallback; configure this instead of the three token-auth fields.
  apns_cert: ""
  apns_cert_password: ""

log:
  level: "info"
```

#### Upstream registration health

Enabled accounts targeting the same normalized host, port, and transport share
one gateway supervisor. The default policy sends one SIP OPTIONS probe per
gateway every three seconds, confirms a failure before opening the circuit,
and probes an unavailable gateway aggressively for recovery. It never sends a
probe per extension, and process-wide probe limits prevent large gateway sets
from creating an outbound traffic spike. A gateway must answer at least one
probe before OPTIONS alone can open its circuit. Gateways that silently drop
OPTIONS switch to one shared REGISTER canary every ten seconds; a confirmed
outage uses the same single canary at the faster down-probe cadence. Only
recovery from a confirmed outage fans registration back out to every account.

When a gateway becomes reachable, registrations enter a bounded parallel
queue. The queue starts at 25 registrations per second, increases toward the
configured 500-per-second ceiling only while successful recovery work is
backlogged, and immediately reduces its learned rate on overload responses or
registration-latency spikes.
`Retry-After`, per-gateway worker limits, and process-wide rate and concurrency
limits are always honored. Normal registrations request a 600-second lifetime
and refresh around 70 percent of the lifetime negotiated by the upstream
server.

The `/health` response includes aggregate registrar counts for managed,
healthy, and pending registrations plus canary-mode, suspect, or unavailable
gateways.
Every value is configurable under `registrar`; see `config.example.yaml`.

#### Apple Push Notification service

APNs token authentication with a `.p8` key is preferred. Certificate
authentication with a VoIP `.p12` remains available as a fallback. The two
credential modes are mutually exclusive, and the bundle ID is required when
either mode is enabled. Sentry fails at startup if APNs credentials are
partial, conflicting, unreadable, or invalid. Leaving every APNs credential
field empty deliberately disables iOS push without affecting FCM.

`apns_production: false` selects the sandbox endpoint; `true` selects the
production endpoint. Run staging and production Sentry deployments with
separate databases so sandbox and production PushKit tokens are never mixed.
At startup Sentry logs the selected environment, `<bundle-id>.voip` topic, and
authentication mode without logging credentials or device tokens.

### Running

Build and run Sentry:

```bash
make build
./bin/sentry -config config.yaml
```

### Mobile Device Contract

`devices.disabled` is the authoritative Sentry-side account intent:

- Registering or refreshing an existing disabled device updates its metadata
  but does not enable or register it upstream.
- `POST /v1/devices/{device_id}/enable` is the only operation that changes a
  disabled device to enabled.
- `POST /v1/devices/{device_id}/reregister` repairs only enabled devices and
  returns `409` with code `device_disabled` for a disabled device.
- Disabling a device blocks new routing before Sentry unregisters it from the
  upstream PBX and cancels pending wake-up work.

Android incoming-call pushes use these data keys:

```json
{
  "call-id": "sentry-call-uuid",
  "device-id": "sentry-device-uuid",
  "caller-uri": "sip:caller@example.com",
  "caller-name": "Caller",
  "content-type": "application/call-info"
}
```

iOS receives the same top-level keys plus the Linphone-native call identifier
inside `aps`:

```json
{
  "aps": {
    "content-available": 1,
    "call-id": "sentry-call-uuid"
  },
  "call-id": "sentry-call-uuid",
  "device-id": "sentry-device-uuid",
  "caller-uri": "sip:caller@example.com",
  "caller-name": "Caller",
  "content-type": "application/call-info"
}
```

The pending-call UUID, both APNs `call-id` values, and the downstream SIP
INVITE Call-ID are identical. The upstream PBX Call-ID remains independent, as
required for the two B2BUA dialog legs.

For `platform: "ios"`, `push_token` may be raw hexadecimal, `TOKEN:voip`, or a
combined Linphone value such as `TOKEN:voip&REMOTE:remote`. Sentry selects the
VoIP segment, validates it, lowercases the hexadecimal token, and stores it
encrypted. A new iOS registration requires a usable token. Re-registering or
refreshing an existing iOS device without a new token preserves the current
encrypted token. Android FCM tokens remain opaque and unchanged.

Enabled iOS devices are retained when their seven-day REST heartbeat lease
expires because iOS cannot guarantee periodic background execution. Expired
Android devices and expired disabled iOS devices are pruned normally. An APNs
invalid-token response disables the iOS device, allowing subsequent cleanup;
explicit account deletion remains immediate.

The mobile client uses `device-id` to select one account pair and temporarily
registers the hidden account returned as `b2bua_sip_uri`. Sentry relays the
pending INVITE to that hidden B2BUA account; the visible direct-PBX account is
not the relay destination.

#### APNs rejection runbook

- `BadDeviceToken` usually means the token is malformed or was sent to the
  wrong sandbox/production endpoint.
- `DeviceTokenNotForTopic` means the configured bundle ID or `.voip` topic does
  not match the application entitlement or credential.
- `Unregistered` or `ExpiredProviderToken` means APNs no longer accepts the
  device/provider token. Device-token rejections disable that Sentry device;
  provider-token errors require checking the `.p8` key, Key ID, Team ID, and
  server clock.
- Authentication or certificate failures at startup require checking file
  permissions, certificate password/expiry, and whether the credential is
  authorized for the configured VoIP topic.

#### CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-config` | `config.yaml` | Path to configuration file |
| `-data-dir` | `./data` | Directory for logs and persistent data |
| `-reset-db` | `false` | Drop and recreate all database tables, then exit |
| `-version` | | Print version information and exit |

## Development

A `Makefile` is provided for common development tasks:

- `make build`: Build the binary.
- `make test`: Run all tests.
- `make generate`: Re-generate SQLC code.
- `make lint`: Run static analysis.

## License

This project is dual-licensed under the GNU AGPL v3 for open source use. A commercial license is available for proprietary use. See [LICENSE](LICENSE) for details.
