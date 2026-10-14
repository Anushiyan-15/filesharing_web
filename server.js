/**
 * P2P Share — signaling server (1 sender → up to 5 receivers)
 *
 * Stack choice: Node.js + Express (static hosting) + ws (WebSocket signaling).
 * Why: WebRTC needs only a tiny signaling channel (SDP/ICE exchange).
 * No DB, no accounts, no file storage — file bytes go peer-to-peer via
 * RTCDataChannel whenever possible (sender opens one PeerConnection per
 * receiver, each throttled independently so a slow receiver never blocks
 * a fast one). The server only relays signaling JSON and, as a last resort,
 * chunked file bytes when direct P2P is impossible. The frontend is
 * dependency-free vanilla HTML/CSS/JS so there is no build step.
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

const app = express();
app.use(express.static(path.join(__dirname, "public")));
app.get("/health", (_req, res) => res.json({ ok: true, maxReceivers: MAX_RECEIVERS }));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

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

wss.on("connection", (ws) => {
  ws.roomCode = null;
  ws.role = null;
  ws.peerId = null;

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return send(ws, { type: "error", message: "Invalid JSON" });
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
    // Sender -> receiver needs `target: peerId` (or single-receiver rooms
    // work without it). Sender broadcast (no target, 2+ receivers) is
    // allowed for relay only (cancel / text). Receiver -> sender is routed
    // automatically with `from: peerId` attached.
    if (msg.type === "signal" || msg.type === "relay") {
      const kind = msg.type;
      const code = String(msg.code || ws.roomCode || "");
      const room = rooms.get(code);
      if (!room) return send(ws, { type: "error", message: "Room not found. Recreate the code." });
      const fwd = (dest, from) => send(dest, { type: kind, code, from, target: msg.target || null, payload: msg.payload });

      if (ws.role === "sender" && room.sender.ws === ws) {
        if (msg.target) {
          const rec = room.receivers.get(String(msg.target));
          if (!rec) return send(ws, { type: "error", message: "Receiver left or unknown." });
          fwd(rec.ws, "sender");
          return;
        }
        if (room.receivers.size === 1) {
          fwd([...room.receivers.values()][0].ws, "sender");
          return;
        }
        if (kind === "relay" && room.receivers.size > 1) {
          for (const p of room.receivers.values()) fwd(p.ws, "sender"); // broadcast: cancel / text
          return;
        }
        return send(ws, { type: "waiting", message: "Waiting for a receiver to join…" });
      }
      if (ws.role === "receiver" && ws.peerId) {
        fwd(room.sender.ws, ws.peerId);
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

server.listen(PORT, () => {
  console.log(`P2P Share running at http://localhost:${PORT}`);
  console.log(`Signaling endpoint: ws://localhost:${PORT}/ws`);
});
