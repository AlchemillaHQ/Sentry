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
  apns_cert: "/path/to/apns-cert.pem"
  apns_key_id: ""
  apns_team_id: ""
  apns_bundle_id: "com.example.app"
  apns_production: false

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

Incoming-call pushes use these data keys:

```json
{
  "call-id": "sentry-call-uuid",
  "device-id": "sentry-device-uuid",
  "caller-uri": "sip:caller@example.com",
  "caller-name": "Caller",
  "content-type": "application/call-info"
}
```

The mobile client uses `device-id` to select one account pair and temporarily
registers the hidden account returned as `b2bua_sip_uri`. Sentry relays the
pending INVITE to that hidden B2BUA account; the visible direct-PBX account is
not the relay destination.

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
