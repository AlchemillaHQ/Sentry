# Sentry

Sentry (Back-to-Back User Agent) is a production-grade SIP signaling gateway designed to bridge standard PBX systems (like Asterisk, FreePBX, or FreeSWITCH) with mobile applications using push notifications.

It works similarly to SIPIS or Flexisip, providing a "Push Wakeup" mechanism for mobile clients while maintaining transparent signaling with the upstream PBX.

## Key Features

- **Scalable Architecture**: Built with Go, PostgreSQL, and SQLC, supporting up to 100,000+ users.
- **Push Notification Support**: Integrated support for Firebase (FCM) and Apple (APNs).
- **High Concurrency**: Uses `pgx/v5` for optimized database connection pooling and binary protocols.
- **Type-Safe Database Layer**: Leverages `sqlc` for zero-reflection, high-performance database operations.
- **B2BUA Signaling**: Manages call legs independently to ensure compatibility with strict PBX requirements.
- **Composite Device IDs**: Supports multiple accounts per device without database collisions.
- **Self-Healing Heartbeat**: Monitors device status and automatically repairs registration state.

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

Update `config.yaml` with your environment details:

```yaml
database:
  driver: "postgres"
  dsn: "postgres://user:pass@localhost:5432/difuse?sslmode=disable"

sip:
  listen_addr: ":5060"
  external_ip: "your-server-public-ip"
  external_port: 5060
  transport: "tls"

push:
  fcm_service_account: "service-account.json"
  apns_cert: ""
  apns_key_id: ""
  apns_team_id: ""
  apns_bundle_id: ""
  apns_production: false
```

### Running

Build and run the B2BUA:

```bash
make build
./sentry -config config.yaml
```

## Development

A `Makefile` is provided for common development tasks:

- `make build`: Build the binary.
- `make test`: Run all tests.
- `make generate`: Re-generate SQLC code.
- `make lint`: Run static analysis.

## License

This project is licensed under the GPL v3.0 - see the [LICENSE](LICENSE) file for details.
