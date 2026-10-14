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
| NAT traversal | **Public STUN** (`stun.l.google.com`, Twilio STUN) + **WS relay fallback** | STUN covers most home/office NATs with zero setup; relay guarantees delivery behind symmetric NATs. Add TURN (below) for 100% direct-media coverage. |

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

## How it works

```
 A (sender)                signaling (Node/ws)              B (receiver)
 ─────────                ───────────────────              ────────────
 create →                     code=482913
                              ← join 482913 ─────────────────┤
 offer (SDP/ICE) ────────── relay ──────────→ ondatachannel
 ⇆ ICE candidates (STUN-assisted hole-punching)
 ═══════════ WebRTC DataChannel: meta → 16KB chunks → done ═══════════
 (fallback, only if P2P fails: relay-meta/chunks via WS, base64)
```

- Chunk size **16 KB** (safe SCTP message size on all browsers).
- Backpressure: sender pauses while `bufferedAmount > 4 MB`.
- Files are sliced with `File.slice()` — never fully loaded into RAM on send;
  the receiver reassembles chunks into a `Blob` for download.
- Resume: receiver reports `{kind:"resume", received}` after `meta`; sender
  continues from that offset after an interruption.

## Edge cases handled

- **Large files:** chunked streaming + progress/speed/ETA; >2 GB warns about receiver RAM (Blob assembly is the browser limit, not the protocol).
- **Interrupted transfers:** `onclose`/`connectionstate` detection, status message,
  **Retry** resumes from the receiver's acked offset instead of restarting.
- **Different networks:** STUN for most NATs; `P2P_TIMEOUT_MS` (12 s) surfaces a
  **Use relay instead** button that forwards chunks through the server. For
  production add TURN (one-line change in `ICE_SERVERS` in `public/app.js`):
  ```js
  { urls: "turn:your-host:3478", username: "user", credential: "pass" }
  // self-host with coturn, or managed (Twilio/Cloudflare/Metered)
  ```
- **Browser compatibility:** startup check for `RTCPeerConnection`/`WebSocket`
  with a plain-English warning; works on recent Chrome/Edge/Firefox/Safari,
  desktop and mobile; layout is responsive down to 360 px.
- **Code expiry:** rooms auto-expire after 10 min; sender-leaves destroys the
  room, receiver-leaves keeps it so the code can be reused.

## Project layout

```
server.js          signaling + static hosting + relay fallback
public/
  index.html       Send / Receive UI, progress, log
  styles.css       clean responsive styling, no framework
  app.js           WS signaling, WebRTC setup, chunk pump, relay, resume, QR
package.json
```

## Scripts

| Command | What |
|---|---|
| `npm start` | serve on `PORT` (default 3000) |
| `GET /health` | `{"ok":true}` liveness check |

## Security notes

- DataChannel media is DTLS-encrypted; signaling should run behind **HTTPS**
  in production (WebRTC and camera-style permissions increasingly expect secure
  context; LAN `http://` still works for testing).
- No file bytes are logged or persisted; relay chunks live only in socket buffers.
- Codes are 6-digit single-room with TTL — not authentication. Don't use this
  for secrets without adding your own access control.
