# P2P Share — direct browser-to-browser file transfer

Upload on one device → get a **QR code + 6-digit code** → up to **5 devices**
scan / enter it → files go **directly between browsers**. No accounts, no file storage.

## Stack (and why)

| Layer | Choice | Reason |
|---|---|---|
| Server | **Node.js + Express + `ws`** | WebRTC only needs a tiny signaling channel (exchange SDP/ICE). No DB, no upload storage, ~130 lines. |
| Frontend | **Vanilla HTML/CSS/JS, no build step** | Keeps it runnable via `npm start` with zero toolchain; WebRTC/DataChannel APIs need no framework. |
| P2P | **WebRTC `RTCDataChannel`** | Encrypted, NAT-traversing, chunked binary transfer with backpressure. File bytes bypass the server. |
| Signaling | **WebSocket JSON** (`create` / `join` / `signal` / `relay`) | Minimal room protocol keyed by 6-digit code. |
| QR | **qrcodejs CDN** (client-side) | Code encodes `https://host/?code=123456`; scanning auto-joins as receiver. |
| NAT traversal | **STUN + TURN** (via `GET /api/ice`) + **WS relay fallback** | STUN covers most home/office NATs; TURN (UDP-first) covers symmetric NATs; WS relay is the final fallback. TURN credentials live in server env, never in the frontend. |

## Quick start

```bash
npm install
npm start
# open http://localhost:3000 on device A (Send)
# open http://localhost:3000 on device B (Receive) — same Wi-Fi, or expose via tunnel
```

Both devices must reach the **same** server URL (e.g. `http://192.168.1.5:3000`
on a LAN, or a public URL via ngrok / Tailscale / deployment).

## Usage

1. **Sender (device A):** open app → *Send* → drop a file → **Get QR & code** →
   show the QR / read out the 6 digits. Keep the tab open.
2. **Receivers:** up to 5 devices can use the same code — scan the QR
   (opens `?code=XXXXXX` and auto-connects) or open the app → *Receive* →
   type the code → **Connect**.
3. Transfer starts automatically; both sides see progress, speed, and status.
   The receiver gets a **Download** button (auto-click attempted).

## Extra features

- **Text tab:** send links / passwords / notes instantly to the connected
  device (same P2P channel, relay fallback if P2P isn't up yet).
- **Receive → Scan QR with camera** (+ Upload QR image fallback for desktops).
- **ETA** on both progress bars; **code expiry countdown** (10:00 → 0:00).
- **Screen wake-lock** during transfers so phones don't sleep mid-file;
  **beep + title flash** when a transfer or message completes.
- **Auto-relay:** if direct P2P fails or takes >12s (typical for PC-WiFi ↔
  phone-data), the sender switches to server relay by itself — no button press.
- **Cancel auto-clears** both sides (progress reset, partial chunks dropped);
  sender can press Start again for a fresh send. Receiver has its own
  **Cancel** button — pressing it stops only that receiver; others continue.
- **Nicknames:** editable name on top, saved in browser, shown to peers
  (receiver list, statuses, history). Changes propagate live.
- **History:** sent + received files of this session with Download-again links.
- **1 sender → 5 receivers:** same code for all; sender shows each receiver
  with live %. 6th device gets "Room full". Each receiver has its own
  connection with independent speed control, so a slow phone never slows a
  fast PC — everyone gets their link's full speed.
- **Live dashboard:** per-receiver current/avg/peak MB/s (decimal), ACK-verified
  delivered bytes, ETA states (Calculating/Estimating/Almost done/Stalled),
  throughput sparkline, per-receiver cards, and a collapsible
  **Connection details** panel (direct vs TURN relay, UDP/TCP transport from
  live stats, RTT, retries, tune results) Everything displayed was measured, never simulated.
- **Speed tune:** one-click disposable probe (1/2/4 connections, ~2.5 s each)
  over the real path; winner saved per mode with the ≥10% rule. Manual
  override anytime.
- **Themes:** System / Light / Dark / Custom (accent + background + dark base),
  persisted, RWD down to 360 px, `prefers-reduced-motion` honored, live
  regions for status changes, visible focus states.

## How it works

```
 A (sender)                signaling (Node/ws)              B (receiver)
 ─────────                ───────────────────              ────────────
 create →                     code=482913
                              ← join 482913 ─────────────────┤
 offer (SDP/ICE) ────────── relay ──────────→ ondatachannel
 ⇆ ICE candidates (STUN-assisted hole-punching)
 ═══════════ WebRTC DataChannel: meta → 16KB chunks → done ═══════════
 (legacy peers: ordered 16 KiB chunks on 1 channel — unchanged)
 (fallback, only if P2P fails: TURN relay, then relay chunks via WS, base64)
```

- Chunking: v2 sends `[u32 index][u32 chunk-CRC] + 64 KiB` frames striped over
 1–4 parallel PeerConnections (independent SCTP windows); v1 keeps raw 16 KiB.
- Backpressure: v2 pauses per channel at 2 MiB (event-driven, no polling) with a 16 MiB aggregate cap; v1 keeps the 4 MiB pause.
- Files are sliced on demand — never fully loaded into RAM on send;
  the receiver reassembles chunks into a `Blob` for download.
- Resume/repair: v1 resumes from the receiver's offset; v2 NACK-sweeps gaps
  and only completes on a whole-file CRC match (mismatch ⇒ not complete).

## Edge cases handled

- **Large files:** chunked streaming + progress/speed/ETA; >2 GB warns about receiver RAM (Blob assembly is the browser limit, not the protocol).
- **Interrupted transfers:** `onclose`/`connectionstate` detection, status message,
  **Retry** resumes from the receiver's acked offset instead of restarting.
  Striped v2 transfers additionally self-repair via NACK and verify integrity.
- **Connection mode** (top of the page): `Auto` (recommended) → same WiFi-la
  direct, mudiyalana relay. `Same WiFi direct` → relay-e use pannaathu
  (local only, fastest). `TURN relay` → media unga TURN server vazhiya
  pogum — thoorama iruntha rendu device-layum itha select pannunga.
- **Browser compatibility:** startup check for `RTCPeerConnection`/`WebSocket`
  with a plain-English warning; works on recent Chrome/Edge/Firefox/Safari,
  desktop and mobile; layout is responsive down to 360 px.
- **Code expiry:** rooms auto-expire after 10 min; sender-leaves destroys the
  room, receiver-leaves keeps it so the code can be reused.

## Project layout

```
server.js          signaling + static hosting + relay fallback + /api/ice
public/
  index.html       Send / Receive UI, progress, diagnostics, themes
  styles.css       theme system (light/dark/system/custom), responsive
  app.js           WS signaling, WebRTC engine (v1 + striped v2), QR, stats
test/              `npm test`: framing units, config matrix, signaling, relay bench
scripts/turn-probe.cjs  manual TURN allocate probe (creds via argv, never logged)
docs/TURN-SPEED.md measurements, tuning notes, firewall ports, bench procedure
.env.example       TURN_* / PORT placeholders (copy to .env, never commit secrets)
```

## TURN setup (required for far networks)

Copy `.env.example` to `.env` and set **one** of:

- Self-hosted coturn (preferred): `TURN_URLS` (UDP-first; see file for format),
  `TURN_SECRET` (your coturn `static-auth-secret`), `TURN_REALM`, `TURN_TTL`.
  Browsers get time-limited `<expiry>:<id>` credentials via `/api/ice`.
- Managed TURN without REST: `TURN_URLS` + `TURN_USER` + `TURN_PASS`.
- Nothing: STUN-only (fine on LAN / easy NATs; far networks fall back to WS relay).

Check status anytime: `GET /api/turn-check` (hostnames only, never secrets).
Throughput notes, firewall ports, and benchmark evidence: `docs/TURN-SPEED.md`.

## Transfer engine (v1 + v2)

- **v2 (new receivers):** receiver advertises `{kind:'hello', frame:'v2'}` after
  joining; sender opens 1/2/4 parallel PeerConnections (Auto-tuned or manual)
  and stripes `[u32 index][u32 chunk-CRC] + 64 KiB` frames across all open
  channels with per-channel backpressure. Out-of-order arrival is normal;
  duplicates/invalid frames are dropped; gaps are NACK-repaired; completion
  requires the receiver's whole-file CRC match (`fin{ok:true}`).
- **v1 (legacy):** single ordered channel, raw 16 KiB chunks — unchanged wire
  behavior, so old clients keep working both directions.
- Extra parallel connections travel inside signal payloads as `{conn: N}`;
  top-level WebSocket message types are unchanged.
- Speeds are decimal MB/s (1 MB = 10⁶ B); "delivered" bytes are
  receiver-acknowledged, never queue-drain estimates.

## Scripts

| Command | What |
|---|---|
| `npm start` | serve on `PORT` (default 3000) |
| `npm test` | syntax + 23 framing units + 17 config + 12 signaling + relay bench |
| `GET /health` | `{"ok":true}` liveness check |
| `GET /api/ice` | ICE servers (`no-store`; TURN creds only, secret never included) |
| `GET /api/turn-check` | TURN configured/valid/hostnames (no credentials) |

## Security notes

- DataChannel media is DTLS-encrypted; signaling should run behind **HTTPS**
  in production (WebRTC and camera-style permissions increasingly expect secure
  context; LAN `http://` still works for testing).
- TURN credentials live only in server env. Browsers receive them via
  `/api/ice` (required for WebRTC to use TURN at all); they are never
  hardcoded in the frontend, never logged, and rotate per fetch in REST mode.
- No file bytes are logged or persisted; relay chunks live only in socket buffers.
- Inbound WS messages capped at 1 MiB; relay frames validated (name/size/
  index/chunk bounds); per-destination queue cap with pressure notices;
  per-socket rate cap (5000 msg/s).
- Codes are 6-digit single-room with TTL — discoverability, **not
  authentication**. Don't use this for secrets without adding real access control.
- Integrity uses CRC32 (corruption detection, not tamper-proofing).
- Max file index space: u32 chunks × 64 KiB (≈256 TiB theoretical);
  enforced server cap 8 GiB advertised size (browser RAM is the real limit).
