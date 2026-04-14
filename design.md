# Difuse B2BUA — Design Document

## 1. Overview

The Difuse B2BUA is a SIP push notification relay server. It keeps a persistent SIP registration at the PBX on behalf of each mobile device. When an incoming call arrives, it sends a push notification to wake the app, waits for the app to signal readiness via SIP REGISTER, then relays the call. Media flows directly between caller and device — the B2BUA only bridges SIP signaling.

The app has **one visible SIP account** registered directly with the PBX. The B2BUA is opaque to the end user.

---

## 2. REST API

### 2.1 Register Device

```
POST /v1/devices/register
Content-Type: application/json

{
  "device_id":          "550e8400-e29b-41d4-a716-446655440000",
  "platform":           "android" | "ios",
  "push_token":         "fcm-or-apns-device-token",
  "upstream_host":      "pbx.example.com",
  "upstream_port":      5060,
  "upstream_transport": "udp",
  "upstream_user":      "1001",
  "upstream_password":  "secret",
  "upstream_realm":     "pbx.example.com",
  "display_name":       "Alice"
}
```

**Response (200):**

```json
{
  "status": "registered",
  "b2bua_sip_uri": "sip:1001_550e8400@203.0.113.50",
  "expires": 3600
}
```

Called on login and on every push token refresh. Idempotent (upserts).

**What happens server-side:**
1. Push token and upstream password are AES-256-GCM encrypted at rest
2. Device record is created/updated in the database
3. B2BUA sends a SIP REGISTER to the upstream PBX using the provided credentials, with **its own IP** as the contact address
4. The PBX now routes incoming calls for this user to the B2BUA

### 2.2 Refresh Registration

```
PUT /v1/devices/{device_id}/refresh
Content-Type: application/json

{
  "push_token": "new-token-if-changed"
}
```

**Response (200):** `{ "status": "ok", "expires": 3600 }`

Called periodically (every 30–60 min) or when the push token rotates. The `push_token` field is optional — omit it if unchanged.

### 2.3 Unregister Device

```
DELETE /v1/devices/{device_id}
```

**Response (200):** `{ "status": "unregistered" }`

Called on logout. B2BUA sends REGISTER with Expires: 0 to upstream PBX and deletes all state.

### 2.4 Health Check

```
GET /health
```

**Response (200):** `{ "status": "ok" }`

### 2.5 No `/ready` Endpoint

The readiness signal is the device's SIP REGISTER to the B2BUA (see §3). No REST readiness endpoint exists.

---

## 3. Call Flow

**States:** `PENDING_PUSH` → `PUSH_SENT` → `DEVICE_READY` → `BRIDGED` → `TERMINATED` (or `EXPIRED` / `CANCELLED`)

```
Device          B2BUA           PBX          Push Service
  │                │              │                │
  │  POST /register│              │                │
  │───────────────►│              │                │
  │                │──REGISTER───►│                │
  │                │◄──401────────│                │
  │                │──REGISTER+auth──►│            │
  │                │◄──200 OK─────│                │
  │◄──200 registered│             │                │
  │                │              │                │
  │  (app goes to sleep)          │                │
  │                │              │                │
  │                │◄──INVITE─────│  (incoming call)
  │                │──100 Trying─►│                │
  │                │──180 Ringing►│                │
  │                │  [store pending_call]          │
  │                │──push(call_id, caller)────────►│
  │                │              │                │──► device wakes
  │◄── push notification ────────────────────────│
  │                │              │                │
  │  (init SIP stack)             │                │
  │──REGISTER─────►│              │                │
  │◄──200 OK───────│  (readiness confirmed)        │
  │                │              │                │
  │◄──INVITE (original SDP offer)──│              │
  │──200 OK (SDP answer)──────────►│              │
  │                │──200 OK (SDP answer)─►│       │
  │                │◄──ACK────────│                │
  │◄──ACK──────────│              │                │
  │                │              │                │
  │  [RTP media flows directly: Device ◄════════► Caller]
  │                │              │                │
  │──BYE──────────►│              │                │
  │                │──BYE────────►│                │
  │                │◄──200 OK─────│                │
  │◄──200 OK───────│              │                │
```

---

## 4. Client Integration — Design Decisions

### 4.1 Single Account or Dual Account?

**Dual account.** The B2BUA's sole job is catching incoming calls while the app sleeps.

| Scenario | SIP target | Identity | Auth |
|---|---|---|---|
| Foreground / outgoing calls | PBX directly | `sip:1001@pbx.example.com` | PBX digest auth |
| After push wake (re-REGISTER) | B2BUA | `sip:1001_550e8400@b2bua-host` | None |
| Receiving relayed INVITE | From B2BUA | Standard INVITE handling | None |

- **App in foreground:** Register directly to PBX (Account 1). Make and receive calls normally. The B2BUA's registration is also alive — the PBX forks incoming calls to both. The app gets the INVITE directly when awake.
- **App sleeping:** B2BUA catches the INVITE, pushes, app wakes, re-REGISTERs to B2BUA (Account 2), call is relayed.
- **Outgoing calls:** Always direct to PBX via Account 1. The B2BUA has no outgoing call path.

### 4.2 SIP Auth for B2BUA REGISTER

**None.** The B2BUA responds `200 OK` immediately with no `401` challenge. The `b2bua_sip_user` value (e.g. `1001_550e8400`) is matched by the call manager against pending calls. The `b2bua_sip_user` is effectively a bearer token — unique per device, contains a UUID suffix, only revealed via the authenticated REST registration response.

### 4.3 X-Upstream Headers?

**Not needed.** The REST `POST /v1/devices/register` provides all upstream credentials (host, port, user, password, realm), which are encrypted and stored in the DB. The SIP REGISTER to the B2BUA carries zero upstream info — it's purely a readiness signal. Remove all `X-Upstream-*` header code from the client.

### 4.4 How to Use `b2bua_sip_uri`

The `b2bua_sip_user` (from the `b2bua_sip_uri` in the REST response) is used as the SIP identity **only when talking to the B2BUA**:

```
REGISTER sip:<b2bua-host> SIP/2.0
To: <sip:1001_550e8400@<b2bua-host>>
From: <sip:1001_550e8400@<b2bua-host>>
Contact: <sip:1001_550e8400@<device-ip>:<port>>
Expires: 120
```

When talking to the PBX directly (foreground, outgoing calls), the app uses its real identity `sip:1001@pbx.example.com`. The `b2bua_sip_user` is never used with the PBX.

### 4.5 Duplicate Call Handling (Foreground Edge Case)

When the app is in foreground with Account 1 registered at PBX, the PBX forks an incoming INVITE to **both** the app and the B2BUA:

1. App gets INVITE directly on Account 1 → shows call UI
2. B2BUA gets INVITE → sends push → app receives push while already ringing

The PBX handles this via standard SIP forking: once the app answers on Account 1, the PBX CANCELs the B2BUA leg. The B2BUA's timeout expires or gets cancelled, no harm done.

**Client responsibility:** Suppress/ignore the duplicate push if already ringing a call from the same caller. Match on `caller_uri` from the push payload against active incoming calls on Account 1.

---

## 5. Client Implementation Summary

### On Login

```
1. Generate a stable device_id (UUID), store in SharedPreferences / UserDefaults
2. POST /v1/devices/register with push token + PBX credentials → get b2bua_sip_uri
3. Create two SIP accounts:
   - Account 1: sip:1001@pbx.example.com → PBX direct, visible, push disabled
   - Account 2: sip:1001_550e8400@vega.alchemilla.io → B2BUA, hidden, no auth, push-only
4. Account 2 does NOT maintain a persistent REGISTER — it only registers on push wake
```

### On Push Token Rotation

```
PUT /v1/devices/{device_id}/refresh  { "push_token": "new-token" }
```

### On FCM/APNs Push Received (Incoming Call While Sleeping)

```
1. Extract call_id, caller_uri, caller_name from push payload
2. If already ringing a call from same caller_uri → ignore (foreground duplicate)
3. Initialize SIP stack
4. Send SIP REGISTER to B2BUA using Account 2 identity (b2bua_sip_user)
5. B2BUA responds 200 OK, then sends INVITE with original SDP
6. Handle the INVITE normally (CallKit / custom UI → answer or reject)
```

**Time budget: 28 seconds** from push send to REGISTER completion. After that, the B2BUA gives up and responds `480` to the PBX.

### On Logout

```
1. DELETE /v1/devices/{device_id}
2. Remove both SIP accounts
3. Clear stored device_id and b2bua_sip_uri
```

---

## 6. Security

| Concern | Implementation |
|---|---|
| REST API transport | TLS strongly recommended in production |
| SIP transport (device ↔ B2BUA) | Configurable (UDP/TCP/TLS) |
| SIP transport (B2BUA ↔ PBX) | Configurable per-device via `upstream_transport` |
| Upstream passwords at rest | AES-256-GCM encrypted in DB |
| Push tokens at rest | AES-256-GCM encrypted in DB |
| Encryption key | Auto-generated (32 random bytes), stored in `settings` table |
| B2BUA REGISTER auth | `b2bua_sip_user` acts as bearer token (UUID-suffixed, REST-only issue) |
| Call-ID guessing | UUID-v4, cryptographically random |
