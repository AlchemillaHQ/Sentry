# Difuse B2BUA — Backend Design Specification

## 1. Overview

The Difuse B2BUA (Back-to-Back User Agent) is a SIP push notification relay server.
Its sole responsibility is to:

1. Accept device registrations (push token + upstream PBX credentials)
2. Maintain a persistent SIP registration with the upstream PBX on behalf of each device
3. When an incoming call arrives at the PBX for a device, send a push notification to wake it
4. Once the device signals readiness, relay the held incoming call to the device
5. Bridge SIP signaling between the two legs — media flows **directly** between the caller and the device (B2BUA is NOT in the media path)

The app has **one visible SIP account** registered directly with the upstream PBX.
The B2BUA is completely opaque to the end user.

---

## 2. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DIFUSE B2BUA                                  │
│                                                                          │
│  ┌─────────────┐    ┌──────────────────┐    ┌───────────────────────┐  │
│  │  REST API   │    │   SIP Stack      │    │  Push Sender          │  │
│  │  (HTTPS)    │    │  (Kamailio /     │    │  (FCM + APNs)         │  │
│  │             │    │   FreeSWITCH)    │    │                       │  │
│  └──────┬──────┘    └────────┬─────────┘    └───────────┬───────────┘  │
│         │                   │                            │              │
│  ┌──────▼───────────────────▼────────────────────────────▼───────────┐ │
│  │                      Core State Store (Redis + PostgreSQL)        │ │
│  │   devices { push_token, upstream_host, upstream_user, ... }       │ │
│  │   pending_calls { call_id, sip_dialog, caller_info, expires_at }  │ │
│  └───────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
         │                        │                        │
         ▼                        ▼                        ▼
   Mobile App              Upstream PBX              Apple/Google
   (REST calls)          (SIP registration         Push Servers
                          + INVITE relay)
```

---

## 3. REST API

All endpoints are HTTPS only. Authentication via per-device HMAC-signed tokens or mTLS.

### 3.1 Register Device

```
POST /v1/devices/register
Content-Type: application/json

{
  "device_id":          "uuid-v4-generated-by-app",
  "platform":           "android" | "ios",
  "push_token":         "fcm-or-apns-token-string",
  "upstream_host":      "pbx.example.com",
  "upstream_port":      5061,
  "upstream_transport": "tls" | "tcp" | "udp",
  "upstream_user":      "1001",
  "upstream_password":  "secret",
  "upstream_realm":     "pbx.example.com",   // optional, defaults to upstream_host
  "display_name":       "John Smith"          // optional
}

Response 200:
{
  "status": "registered",
  "b2bua_sip_uri": "sip:1001_uuid@vega.alchemilla.io",   // device's B2BUA address for readiness signal
  "expires": 3600
}
```

Called once on login and on every push token refresh. Idempotent.

### 3.2 Refresh Registration

```
PUT /v1/devices/{device_id}/refresh
Content-Type: application/json

{
  "push_token": "new-token-if-changed"   // omit if unchanged
}

Response 200: { "status": "ok", "expires": 3600 }
```

Called periodically (every 30–60 min) while app is in foreground. Also called immediately on push token rotation.

### 3.3 Unregister Device

```
DELETE /v1/devices/{device_id}

Response 200: { "status": "unregistered" }
```

Called on logout. B2BUA sends SIP REGISTER with Expires: 0 to upstream PBX.

### 3.4 Readiness Signal (device woke from push)

```
POST /v1/devices/{device_id}/ready
Content-Type: application/json

{
  "call_id": "uuid-from-push-payload"
}

Response 200:
{
  "status": "relaying",
  "sip_uri": "sip:1001_uuid@vega.alchemilla.io",   // B2BUA will INVITE this URI
  "caller":  "sip:caller@pbx.example.com",
  "display_name": "Jane Doe"
}

Response 404: { "status": "expired" }   // call timed out, tell user "missed call"
Response 409: { "status": "answered" }  // call already answered elsewhere
```

This is the "readiness signal" from SIPIS. Triggers the B2BUA to initiate the relay INVITE to the device.

---

## 4. SIP Stack Behaviour

### 4.1 Outbound Registration (B2BUA → Upstream PBX)

On `POST /register`, the B2BUA:

1. Creates a SIP registration toward `upstream_host:upstream_port;transport=upstream_transport`
2. Registers with identity `sip:upstream_user@upstream_host`
3. Uses upstream credentials for DIGEST authentication
4. Expires: 120–300 seconds (re-registers automatically at 75% of expiry)
5. Stores the resulting registration contact and dialog

```
REGISTER sip:pbx.example.com SIP/2.0
From: <sip:1001@pbx.example.com>
To:   <sip:1001@pbx.example.com>
Contact: <sip:1001_uuid@vega.alchemilla.io;transport=tls>
Expires: 120
```

### 4.2 Incoming INVITE Handling (PBX → B2BUA)

When a call arrives for a registered device:

1. Immediately respond `100 Trying`
2. Look up device by the Request-URI or To header
3. Extract and store the full incoming SIP dialog state:
   - Call-ID, From tag, To tag
   - SDP offer (caller's media description with ICE candidates)
   - All relevant headers: From, To, Subject, Alert-Info, P-Asserted-Identity
4. Respond `180 Ringing` to keep the PBX leg alive (buy up to 30 seconds)
5. Store as `pending_call`:
   ```json
   {
     "call_id":      "b2bua-generated-uuid",
     "sip_call_id":  "original-sip-call-id",
     "device_id":    "uuid",
     "caller_uri":   "sip:caller@pbx.example.com",
     "caller_name":  "Jane Doe",
     "sdp_offer":    "v=0\r\no=...",
     "expires_at":   "now + 28 seconds",
     "state":        "PENDING_PUSH"
   }
   ```
6. Send push notification (see §5)
7. Start 28-second expiry timer → if no `POST /ready`, send `480 Temporarily Unavailable`

### 4.3 Relay INVITE (B2BUA → Device)

Triggered by `POST /ready`:

1. Construct a NEW INVITE to the device's direct SIP address (which the device registered directly with the PBX — obtained from the readiness signal's contact)
2. Preserve caller information:
   ```
   INVITE sip:1001@<device-contact> SIP/2.0
   From: <sip:caller@pbx.example.com>;tag=b2bua-generated
   To:   <sip:1001@pbx.example.com>
   P-Asserted-Identity: "Jane Doe" <sip:caller@pbx.example.com>
   Subject: Incoming call from Jane Doe
   ```
3. **SDP handling (ICE passthrough — media goes direct):**
   - Take the original SDP offer from the PBX leg
   - Forward it to the device AS-IS (do not rewrite RTP addresses)
   - When device answers with SDP answer, forward that answer back to the PBX
   - Result: ICE negotiation happens between caller and device directly, B2BUA carries only SIP
4. Once device sends `200 OK` → send `200 OK` to PBX with device's SDP answer
5. Bridge SIP re-INVITEs, BYEs, and in-dialog requests between the two legs
6. When either side sends BYE → forward to the other side → tear down both legs

### 4.4 SIP BYE / Call Termination

- If PBX sends BYE on the incoming leg → forward BYE to device, clean up state
- If device sends BYE on the relay leg → forward BYE to PBX, clean up state
- If push times out → send `480` to PBX → remove pending_call record

---

## 5. Push Notification

### 5.1 FCM (Android) Payload

```json
{
  "to": "<fcm-device-token>",
  "priority": "high",
  "data": {
    "call_id":      "b2bua-generated-uuid",
    "caller_uri":   "sip:caller@pbx.example.com",
    "caller_name":  "Jane Doe",
    "display_name": "Jane Doe",
    "content_type": "application/call-info"
  }
}
```

Use FCM **data messages** (not notification messages) so the app can handle it in the background handler.

### 5.2 APNs (iOS) Payload

```json
{
  "aps": {
    "alert": { "title": "Incoming Call", "body": "Jane Doe" },
    "sound": "default",
    "content-available": 1
  },
  "call_id":    "b2bua-generated-uuid",
  "caller_uri": "sip:caller@pbx.example.com",
  "caller_name": "Jane Doe"
}
```

Use APNs **VoIP push** (`com.apple.push.type: voip`) for CallKit integration on iOS.

---

## 6. Database Schema

### PostgreSQL

```sql
-- Registered devices
CREATE TABLE devices (
    device_id           UUID PRIMARY KEY,
    platform            VARCHAR(10) NOT NULL,        -- 'android' | 'ios'
    push_token          TEXT NOT NULL,
    upstream_host       VARCHAR(255) NOT NULL,
    upstream_port       INTEGER NOT NULL DEFAULT 5061,
    upstream_transport  VARCHAR(10) NOT NULL DEFAULT 'tls',
    upstream_user       VARCHAR(255) NOT NULL,
    upstream_password   TEXT NOT NULL,               -- encrypted at rest (AES-256)
    upstream_realm      VARCHAR(255),
    display_name        VARCHAR(255),
    b2bua_sip_user      VARCHAR(255) NOT NULL UNIQUE, -- e.g. "1001_<uuid>"
    registered_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at          TIMESTAMPTZ NOT NULL,
    last_seen           TIMESTAMPTZ
);

-- Active upstream SIP registrations
CREATE TABLE upstream_registrations (
    device_id       UUID REFERENCES devices(device_id) ON DELETE CASCADE,
    contact_uri     TEXT NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL,
    call_id         VARCHAR(255),    -- SIP Call-ID of the REGISTER dialog
    cseq            INTEGER DEFAULT 1,
    PRIMARY KEY (device_id)
);

-- Pending incoming calls (waiting for device to wake)
CREATE TABLE pending_calls (
    call_id         UUID PRIMARY KEY,               -- B2BUA-generated, sent in push
    device_id       UUID REFERENCES devices(device_id),
    sip_call_id     VARCHAR(255) NOT NULL,           -- original SIP Call-ID
    sip_from        TEXT NOT NULL,
    sip_to          TEXT NOT NULL,
    caller_uri      TEXT NOT NULL,
    caller_name     VARCHAR(255),
    sdp_offer       TEXT,
    sip_dialog      JSONB NOT NULL,                 -- full dialog state for bridging
    state           VARCHAR(30) NOT NULL DEFAULT 'PENDING_PUSH',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ NOT NULL             -- now + 28 seconds
);
-- state values: PENDING_PUSH | PUSH_SENT | DEVICE_READY | BRIDGED | TERMINATED
```

### Redis (ephemeral, fast lookups)

```
device:{device_id}          → JSON snapshot of device record (TTL = expires_at)
pending_call:{call_id}      → JSON snapshot of pending_call (TTL = 28s)
sip_dialog:{sip_call_id}    → device_id mapping (TTL = 60s)
```

---

## 7. Call State Machine

```
                     INVITE from PBX
                           │
                           ▼
                    ┌─────────────┐
                    │ PENDING_PUSH│──── Send 180 Ringing to PBX
                    └──────┬──────┘
                           │ Push sent
                           ▼
                    ┌─────────────┐
                    │  PUSH_SENT  │──── 28s timer running
                    └──────┬──────┘
          ┌────────────────┼────────────────────┐
          │ Timer expires  │ POST /ready         │ PBX cancels
          ▼                ▼                     ▼
   ┌──────────┐    ┌─────────────┐        ┌──────────┐
   │ EXPIRED  │    │DEVICE_READY │        │CANCELLED │
   │ 480 →PBX │    └──────┬──────┘        │ BYE both │
   └──────────┘           │ Send INVITE → device
                          ▼
                   ┌─────────────┐
                   │   BRIDGED   │──── Relay all in-dialog SIP
                   └──────┬──────┘
                          │ BYE from either side
                          ▼
                   ┌─────────────┐
                   │ TERMINATED  │──── Clean up state
                   └─────────────┘
```

---

## 8. Sequence Diagram — Full Incoming Call Flow

```
Device          B2BUA           PBX          Push Service
  │                │              │                │
  │  POST /register│              │                │
  │───────────────▶│              │                │
  │                │──REGISTER───▶│                │
  │                │◀──200 OK─────│                │
  │◀──200 registered│             │                │
  │                │              │                │
  │  (app goes to background)     │                │
  │                │              │                │
  │                │◀──INVITE─────│  (incoming call)
  │                │──100 Trying─▶│                │
  │                │──180 Ringing▶│                │
  │                │  [store pending_call]          │
  │                │──────────────────────────────▶│  FCM/APNs push
  │                │              │                │──▶ device wakes
  │◀── push notification ─────────────────────────│
  │                │              │                │
  │  POST /ready   │              │                │
  │───────────────▶│              │                │
  │◀── 200 relaying│              │                │
  │                │              │                │
  │◀──INVITE (relay, original SDP offer)           │
  │──200 OK (SDP answer)─────────▶│               │
  │                │──200 OK (SDP answer)──────────│
  │                │              │◀──────────────ACK
  │                │──────────ACK▶│               │
  │                │              │                │
  │  [RTP FLOWS DIRECTLY: Device ◀══════════▶ Caller — B2BUA not in media path]
  │                │              │                │
  │──BYE──────────▶│              │                │
  │                │──────────BYE▶│                │
  │                │◀──────────200│                │
  │◀──200──────────│              │                │
```

---

## 9. Security Requirements

| Concern | Requirement |
|---|---|
| REST API transport | TLS 1.2+ only, valid certificate |
| Device authentication | HMAC-SHA256 signed requests using per-device secret issued at registration |
| SIP transport (device↔B2BUA) | TLS only (`sip:...;transport=tls`) |
| SIP transport (B2BUA↔PBX) | Configurable per-device (tls/tcp/udp), TLS strongly preferred |
| Upstream passwords at rest | AES-256-GCM encrypted in PostgreSQL |
| Push tokens at rest | Encrypted at rest |
| Rate limiting | Max 10 registrations/min per IP, 1 readiness signal per call_id |
| Device ID binding | REST calls validated against device_id + HMAC to prevent spoofing |
| Call ID guessing | call_id is UUID-v4, unguessable — no sequential IDs |

---

## 10. Recommended Technology Stack

```
Language:   Go or Node.js (TypeScript)
SIP stack:  pjsip (via CGo/FFI) OR Kamailio as the SIP layer
            with a sidecar REST service controlling it via RPC/HTTP
REST API:   Gin (Go) / Fastify (Node.js)
Database:   PostgreSQL 15+ (persistent state)
            Redis 7+ (fast ephemeral state, pub/sub for readiness events)
Push:       Official FCM HTTP v1 SDK + node-apn / apns2
Deploy:     Docker + Kubernetes, or a single VM for early stage
TLS certs:  Let's Encrypt via certbot or cert-manager
```

### Simplest viable stack

If you want to move fast, use **Kamailio** for all SIP work:
- Kamailio handles REGISTER mirroring, INVITE bridging, and B2BUA logic natively
- Write a small **sidecar service** (Node.js/Go) that:
  - Exposes the REST API
  - Talks to Kamailio via its **JSONRPC** or **MI** (Management Interface) to control calls
  - Sends push notifications
  - Manages the PostgreSQL/Redis state

```
App ──REST──▶ Sidecar (Node.js) ──JSONRPC──▶ Kamailio ──SIP──▶ PBX
                     │                                           │
                     └────────── PostgreSQL + Redis ─────────────┘
                     │
                     └────────── FCM / APNs
```

---

## 11. Key Kamailio Modules Needed

```
- registrar        — manage device-side REGISTER
- uac              — initiate outbound REGISTER to upstream PBX
- b2bua            — bridge two SIP call legs for the relay
- pv               — pseudo-variables for scripting
- htable           — in-memory table for pending calls (fast lookup)
- http_async_client — notify sidecar service on events (incoming INVITE, re-REGISTER)
- push_notification — FCM/APNs push (or delegate to sidecar)
- tls              — TLS transport
- dialog           — track SIP dialogs for BYE relay
- nathelper + rtpengine — ICE passthrough (media direct)
```

---

## 12. What the App Needs to Do (Summary)

On **login**:
```
1. Create ONE SIP account: sip:user@upstream_host (direct PBX, push disabled)
2. POST /v1/devices/register with push token + PBX credentials → get b2bua_sip_uri back
3. Store device_id and b2bua_sip_uri in SharedPreferences
```

On **push token refresh** (FirebaseMessagingService.onNewToken):
```
PUT /v1/devices/{device_id}/refresh with new push token
```

On **FCM push received** (incoming call):
```
1. Extract call_id from push payload
2. Wake the SIP core (re-register direct PBX account)
3. POST /v1/devices/{device_id}/ready with call_id
4. Wait for B2BUA to INVITE the device on the direct SIP account
5. Linphone handles the incoming INVITE normally — user sees incoming call UI
```

On **logout**:
```
1. DELETE /v1/devices/{device_id}
2. Remove SIP account
3. Clear stored device_id
```
