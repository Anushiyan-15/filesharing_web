/**
 * P2P Share — signaling server (1 sender → up to 5 receivers)
 *
 * Stack choice: Node.js + Express (static hosting) + ws (WebSocket signaling).
 * WebRTC needs only a tiny signaling channel (SDP/ICE exchange). No DB, no
 * accounts, no file storage — file bytes go peer-to-peer via RTCDataChannel
 * whenever possible. The server only relays signaling JSON and, as a last
 * resort, chunked file bytes when direct P2P is impossible.
 *
 * TURN credentials are NEVER hardcoded here for the frontend: browsers fetch
 * short-lived (or env-configured) credentials from GET /api/ice. The shared
 * secret itself is never sent to any client and never logged.
 */

const path = require("path");
const http = require("http");
const express = require("express");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
// Room lives this long without activity (ms). Keeps 6-digit codes fresh.
const CODE_TTL_MS = 10 * 60 * 1000;
// One sender can serve this many receivers at once on the same code.
const MAX_RECEIVERS = 5;

// ---- Hardening limits (see docs/TURN-SPEED.md §8) ----
const MAX_WS_MSG = 1 * 1024 * 1024;      // max inbound WebSocket message (bytes)
const MAX_RELAY_B64 = 700 * 1024;        // max relay-chunk base64 payload (~512 KB)
const MAX_META_NAME = 255;               // max file name length
const MAX_FILE_SIZE = 8 * 1024 * 1024 * 1024; // max advertised file size (8 GiB)
const MAX_TOTAL_CHUNKS = 1000000;        // max chunks per transfer
const MAX_MIME_LEN = 128;
const DEST_QUEUE_CAP = 16 * 1024 * 1024; // per-destination queued bytes before pressure/drop
const MAX_MSG_PER_SEC = 5000;            // per-socket inbound rate cap

// ---- TURN configuration (all from environment; never logged) ----
const TURN_URLS_RAW = (process.env.TURN_URLS || "").split(",").map((s) => s.trim()).filter(Boolean);
const TURN_USER = process.env.TURN_USER || "";
const TURN_PASS = process.env.TURN_PASS || "";
const TURN_SECRET = process.env.TURN_SECRET || ""; // coturn static-auth-secret (REST)
const TURN_REALM = process.env.TURN_REALM || "p2p-share";
const TURN_TTL = Math.min(86400, Math.max(60, parseInt(process.env.TURN_TTL || "3600", 10) || 3600));

const STUN_SERVERS = ["stun:stun.l.google.com:19302", "stun:stun1.l.google.com:3478"];

function validTurnUrl(u) {
  return /^(turn|turns):[^\s/]+$/i.test(u);
}

// Deterministic UDP-first ordering: ?transport=udp, then bare turn:, then
// ?transport=tcp, then turns:. Lets ICE try UDP relay before TCP/TLS.
function orderTurnUrls(urls) {
  const rank = (u) => {
    const l = u.toLowerCase();
    if (l.startsWith("turns:")) return 3;
    if (l.includes("transport=tcp")) return 2;
    if (l.includes("transport=udp")) return 0;
    return 1;
  };
  return [...urls].sort((a, b) => rank(a) - rank(b));
}

// Build the TURN entry for /api/ice, or null when unconfigured.
// REST mode (TURN_SECRET, coturn use-auth-secret): time-limited credentials
//   username "<expiry>:<random>", password base64(HMAC-SHA1(secret, username)).
// Static mode (TURN_USER + TURN_PASS): managed providers without REST support.
function buildTurnEntry() {
  const urls = orderTurnUrls(TURN_URLS_RAW.filter(validTurnUrl));
  if (!urls.length) return { entry: null, error: TURN_URLS_RAW.length ? "no valid TURN URLs" : "TURN_URLS not set" };
  if (TURN_SECRET) {
    const expiry = Math.floor(Date.now() / 1000) + TURN_TTL;
    const username = `${expiry}:${crypto.randomBytes(8).toString("hex")}`;
    const password = crypto.createHmac("sha1", TURN_SECRET).update(username).digest("base64");
    return { entry: { urls, username, credential: password }, ttl: TURN_TTL };
  }
  if (TURN_USER && TURN_PASS) {
    return { entry: { urls, username: TURN_USER, credential: TURN_PASS }, ttl: 0 };
  }
  return { entry: null, error: "TURN URLs set but no TURN_SECRET or TURN_USER+TURN_PASS" };
}

function turnHosts() {
  const out = [];
  for (const u of orderTurnUrls(TURN_URLS_RAW.filter(validTurnUrl))) {
    const noQuery = u.split("?")[0];
    const afterScheme = noQuery.split(":").slice(1).join(":");
    const host = afterScheme.startsWith("[")
      ? afterScheme.slice(0, afterScheme.indexOf("]") + 1) // IPv6 literal
      : afterScheme.split(":")[0].split("/")[0];
    if (host && !out.includes(host)) out.push(host);
  }
  return out;
}

const app = express();
app.use(express.static(path.join(__dirname, "public")));
app.get("/health", (_req, res) => res.json({ ok: true, maxReceivers: MAX_RECEIVERS }));

// Short-lived ICE configuration. Cache explicitly disabled: credentials expire.
app.get("/api/ice", (_req, res) => {
  res.set("Cache-Control", "no-store");
  const iceServers = [{ urls: [...STUN_SERVERS] }];
  const { entry, error } = buildTurnEntry();
  if (entry) iceServers.push(entry);
  res.json({ iceServers, turn: !!entry, ttl: entry ? (TURN_SECRET ? TURN_TTL : 0) : 0, error: entry ? undefined : error });
});

// Configuration presence check. Reports hostnames only — never credentials.
// NOTE: "configured" means the env is present and valid, NOT that the TURN
// host is reachable. Test reachability separately (see docs/TURN-SPEED.md).
app.get("/api/turn-check", (_req, res) => {
  res.set("Cache-Control", "no-store");
  const { entry, error } = buildTurnEntry();
  res.json({ configured: !!entry, valid: !!entry, hosts: turnHosts(), error: entry ? undefined : error });
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws", maxPayload: MAX_WS_MSG });

// code -> { sender: {ws, nick}, receivers: Map(peerId -> {ws, nick}), createdAt, timer }
const rooms = new Map();

function generateCode() {
  for (let i = 0; i < 50; i++) {
    // 6 digits, no leading zero so it reads well over the phone.
    const code = String(Math.floor(100000 + Math.random() * 900000));
    if (!rooms.has(code)) return code;
  }
  throw new Error("code space exhausted");
}

function makePeerId(room) {
  let id;
  do {
    id = crypto.randomBytes(3).toString("hex");
  } while (room.receivers.has(id));
  return id;
}

function cleanNick(n, fallback) {
  const s = String(n || "").trim().slice(0, 24);
  return s || fallback;
}

function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function destroyRoom(code) {
  const room = rooms.get(code);
  if (!room) return;
  clearTimeout(room.timer);
  rooms.delete(code);
}

// ---- Payload validation (invalid frames are dropped, never crash us) ----
const SIGNAL_KINDS = new Set(["offer", "answer", "ice", "hello"]);
const RELAY_KINDS = new Set(["relay-meta", "relay-chunk", "relay-ack", "relay-text", "cancel", "cancel-request", "probe-report"]);

function validSignalPayload(p) {
  if (!p || typeof p !== "object") return false;
  if (!SIGNAL_KINDS.has(p.kind)) return false;
  if (p.conn !== undefined && (!Number.isInteger(p.conn) || p.conn < 0 || p.conn > 15)) return false;
  return true;
}

function validRelayPayload(p) {
  if (!p || typeof p !== "object") return false;
  if (!RELAY_KINDS.has(p.kind)) return false;
  if (p.kind === "relay-meta") {
    if (typeof p.name !== "string" || !p.name.length || p.name.length > MAX_META_NAME) return false;
    if (!Number.isFinite(p.size) || p.size < 0 || p.size > MAX_FILE_SIZE) return false;
    if (!Number.isInteger(p.totalChunks) || p.totalChunks < 1 || p.totalChunks > MAX_TOTAL_CHUNKS) return false;
    if (p.mime !== undefined && (typeof p.mime !== "string" || p.mime.length > MAX_MIME_LEN)) return false;
    if (p.fileId !== undefined && typeof p.fileId !== "string") return false;
  }
  if (p.kind === "relay-chunk") {
    if (typeof p.fileId !== "string" || !p.fileId.length || p.fileId.length > 256) return false;
    if (!Number.isInteger(p.index) || p.index < 0 || p.index >= MAX_TOTAL_CHUNKS) return false;
    if (typeof p.base64 !== "string" || !p.base64.length || p.base64.length > MAX_RELAY_B64) return false;
  }
  if (p.kind === "relay-text" && (typeof p.text !== "string" || p.text.length > 20000)) return false;
  return true;
}

// Forward one relay/signal frame with a bounded destination queue. If the
// destination is more than DEST_QUEUE_CAP behind, the chunk is dropped and
// the sender gets a {type:'pressure'} notice so v2 senders can requeue the
// index (v1 senders ignore it; documented in README).
function forwardChecked(destWs, senderWs, kind, code, from, target, payload) {
  if (destWs.readyState !== 1) return false;
  const isChunk = kind === "relay" && payload && payload.kind === "relay-chunk";
  if (isChunk && destWs.bufferedAmount > DEST_QUEUE_CAP) {
    send(senderWs, { type: "pressure", code, fileId: payload.fileId, index: payload.index });
    return false;
  }
  send(destWs, { type: kind, code, from, target: target || null, payload });
  return true;
}

wss.on("connection", (ws) => {
  ws.roomCode = null;
  ws.role = null;
  ws.peerId = null;
  ws._msgTimes = [];

  function rateOk() {
    const now = Date.now();
    ws._msgTimes = ws._msgTimes.filter((t) => now - t < 1000);
    ws._msgTimes.push(now);
    return ws._msgTimes.length <= MAX_MSG_PER_SEC;
  }

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return send(ws, { type: "error", message: "Invalid JSON" });
    }
    if (!rateOk()) {
      send(ws, { type: "error", message: "Rate limit exceeded" });
      try { ws.close(1013, "rate limit"); } catch {}
      return;
    }

    // ---- CREATE: sender asks for a fresh code ----
    if (msg.type === "create") {
      try {
        if (ws.roomCode) leaveRoom(ws);
        const code = generateCode();
        const nick = cleanNick(msg.nick, "Sender");
        const timer = setTimeout(() => {
          const r = rooms.get(code);
          if (r) {
            send(r.sender.ws, { type: "expired", code });
            for (const p of r.receivers.values()) send(p.ws, { type: "expired", code });
            destroyRoom(code);
          }
        }, CODE_TTL_MS);
        rooms.set(code, { sender: { ws, nick }, receivers: new Map(), createdAt: Date.now(), timer });
        ws.roomCode = code;
        ws.role = "sender";
        send(ws, { type: "created", code });
      } catch (err) {
        send(ws, { type: "error", message: "Could not create room" });
      }
      return;
    }

    // ---- JOIN: receiver enters the 6-digit code (max 5 per room) ----
    if (msg.type === "join") {
      const code = String(msg.code || "").trim();
      const room = rooms.get(code);
      if (!room) return send(ws, { type: "error", message: "Code not found or expired. Check the 6 digits." });
      const already = ws.roomCode === code && ws.role === "receiver" && ws.peerId && room.receivers.has(ws.peerId);
      if (!already && room.receivers.size >= MAX_RECEIVERS) {
        return send(ws, { type: "error", message: `Room full — max ${MAX_RECEIVERS} receivers per sender.` });
      }
      if (ws.roomCode && !already) leaveRoom(ws);
      const nick = cleanNick(msg.nick, "Receiver");
      const peerId = already ? ws.peerId : makePeerId(room);
      room.receivers.set(peerId, { ws, nick });
      ws.roomCode = code;
      ws.role = "receiver";
      ws.peerId = peerId;
      send(ws, { type: "joined", code, peerId, senderNick: room.sender.nick, count: room.receivers.size, max: MAX_RECEIVERS });
      send(room.sender.ws, { type: "peer-joined", code, peerId, nick, count: room.receivers.size, max: MAX_RECEIVERS });
      return;
    }

    // ---- NICK: editable display name, forwarded to room peers ----
    if (msg.type === "nick") {
      const code = String(msg.code || ws.roomCode || "");
      const room = rooms.get(code);
      if (!room) return;
      if (ws.role === "sender" && room.sender.ws === ws) {
        room.sender.nick = cleanNick(msg.nick, "Sender");
        for (const p of room.receivers.values()) send(p.ws, { type: "nick", from: "sender", nick: room.sender.nick });
      } else if (ws.role === "receiver" && ws.peerId && room.receivers.has(ws.peerId)) {
        const nick = cleanNick(msg.nick, "Receiver");
        room.receivers.get(ws.peerId).nick = nick;
        send(room.sender.ws, { type: "nick", from: ws.peerId, nick });
      }
      return;
    }

    // ---- SIGNAL / RELAY: route between sender and addressed receiver ----
    // Top-level message types are unchanged (backward compatible). Parallel
    // connections travel inside the payload as {conn: N} (default 0).
    if (msg.type === "signal" || msg.type === "relay") {
      const kind = msg.type;
      const code = String(msg.code || ws.roomCode || "");
      const room = rooms.get(code);
      if (!room) return send(ws, { type: "error", message: "Room not found. Recreate the code." });
      const payload = msg.payload;
      const okPayload = kind === "signal" ? validSignalPayload(payload) : validRelayPayload(payload);
      if (!okPayload) return send(ws, { type: "error", message: "Invalid payload" });

      if (ws.role === "sender" && room.sender.ws === ws) {
        if (msg.target) {
          const rec = room.receivers.get(String(msg.target));
          if (!rec) return send(ws, { type: "error", message: "Receiver left or unknown." });
          forwardChecked(rec.ws, ws, kind, code, "sender", msg.target, payload);
          return;
        }
        if (room.receivers.size === 1) {
          forwardChecked([...room.receivers.values()][0].ws, ws, kind, code, "sender", null, payload);
          return;
        }
        if (kind === "relay" && room.receivers.size > 1) {
          for (const p of room.receivers.values()) forwardChecked(p.ws, ws, kind, code, "sender", null, payload);
          return;
        }
        return send(ws, { type: "waiting", message: "Waiting for a receiver to join…" });
      }
      if (ws.role === "receiver" && ws.peerId) {
        forwardChecked(room.sender.ws, ws, kind, code, ws.peerId, null, payload);
        return;
      }
      return send(ws, { type: "error", message: "Not in this room. Join first." });
    }

    // ---- LEAVE ----
    if (msg.type === "leave") {
      leaveRoom(ws);
      send(ws, { type: "left" });
      return;
    }

    send(ws, { type: "error", message: "Unknown message type" });
  });

  ws.on("close", () => {
    const { roomCode, role, peerId } = ws;
    if (!roomCode) return;
    const room = rooms.get(roomCode);
    if (!room) return;
    // If sender leaves, the code is useless — destroy it.
    // If a receiver leaves, the rest keep going.
    if (role === "sender" && room.sender.ws === ws) {
      for (const p of room.receivers.values()) send(p.ws, { type: "peer-left", role: "sender" });
      destroyRoom(roomCode);
    } else if (role === "receiver" && peerId && room.receivers.has(peerId)) {
      room.receivers.delete(peerId);
      send(room.sender.ws, { type: "peer-left", role: "receiver", peerId });
    }
  });

  ws.on("error", () => {});
});

function leaveRoom(ws) {
  const code = ws.roomCode;
  if (!code) return;
  const room = rooms.get(code);
  if (room) {
    if (ws.role === "sender" && room.sender.ws === ws) {
      for (const p of room.receivers.values()) send(p.ws, { type: "peer-left", role: "sender" });
      destroyRoom(code);
    } else if (ws.role === "receiver" && ws.peerId && room.receivers.has(ws.peerId)) {
      room.receivers.delete(ws.peerId);
      send(room.sender.ws, { type: "peer-left", role: "receiver", peerId: ws.peerId });
    }
  }
  ws.roomCode = null;
  ws.role = null;
  ws.peerId = null;
}

// Startup config report (values redacted — never log secrets).
(function reportConfig() {
  const { entry, error } = buildTurnEntry();
  console.log(`[config] TURN via /api/ice: ${entry ? `enabled (${turnHosts().join(", ") || "hosts?"}, ${TURN_SECRET ? `REST ttl=${TURN_TTL}s` : "static credentials"})` : `disabled (${error})`}`);
})();

server.listen(PORT, () => {
  console.log(`P2P Share running at http://localhost:${PORT}`);
  console.log(`Signaling endpoint: ws://localhost:${PORT}/ws`);
});
