/**
 * P2P Share — signaling server
 *
 * Stack choice: Node.js + Express (static hosting) + ws (WebSocket signaling).
 * Why: WebRTC needs only a tiny signaling channel (SDP/ICE exchange).
 * No DB, no accounts, no file storage — file bytes go peer-to-peer via
 * RTCDataChannel whenever possible. The server only relays signaling JSON
 * and, as a last resort, chunked file bytes when direct P2P is impossible
 * (symmetric NAT / firewall). The frontend is dependency-free vanilla
 * HTML/CSS/JS so there is no build step.
 */

const path = require("path");
const http = require("http");
const express = require("express");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
// Room lives this long without activity (ms). Keeps 6-digit codes fresh.
const CODE_TTL_MS = 10 * 60 * 1000;

const app = express();
app.use(express.static(path.join(__dirname, "public")));
app.get("/health", (_req, res) => res.json({ ok: true }));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

// code -> { sender: ws|null, receiver: ws|null, createdAt: number, timer: Timeout }
const rooms = new Map();

function generateCode() {
  for (let i = 0; i < 50; i++) {
    // 6 digits, no leading zero so it reads well over the phone.
    const code = String(Math.floor(100000 + Math.random() * 900000));
    if (!rooms.has(code)) return code;
  }
  throw new Error("code space exhausted");
}

function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function peerOf(room, ws) {
  if (room.sender === ws) return room.receiver;
  if (room.receiver === ws) return room.sender;
  return null;
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
        // Leave previous room if any.
        if (ws.roomCode) leaveRoom(ws);
        const code = generateCode();
        const timer = setTimeout(() => {
          const r = rooms.get(code);
          if (r) {
            send(r.sender, { type: "expired", code });
            send(r.receiver, { type: "expired", code });
            destroyRoom(code);
          }
        }, CODE_TTL_MS);
        rooms.set(code, { sender: ws, receiver: null, createdAt: Date.now(), timer });
        ws.roomCode = code;
        ws.role = "sender";
        send(ws, { type: "created", code });
      } catch (err) {
        send(ws, { type: "error", message: "Could not create room" });
      }
      return;
    }

    // ---- JOIN: receiver enters the 6-digit code ----
    if (msg.type === "join") {
      const code = String(msg.code || "").trim();
      const room = rooms.get(code);
      if (!room) return send(ws, { type: "error", message: "Code not found or expired. Check the 6 digits." });
      if (room.receiver && room.receiver !== ws) {
        return send(ws, { type: "error", message: "This code is already in use by another receiver." });
      }
      if (ws.roomCode && ws.roomCode !== code) leaveRoom(ws);
      room.receiver = ws;
      ws.roomCode = code;
      ws.role = "receiver";
      send(ws, { type: "joined", code });
      send(room.sender, { type: "peer-joined", code });
      return;
    }

    // ---- SIGNAL: relay SDP / ICE between the two peers ----
    if (msg.type === "signal") {
      const code = String(msg.code || ws.roomCode || "");
      const room = rooms.get(code);
      if (!room) return send(ws, { type: "error", message: "Room not found. Recreate the code." });
      const peer = peerOf(room, ws);
      if (!peer) return send(ws, { type: "waiting", message: "Waiting for the other device to join…" });
      // Forward payload untouched: { kind: 'offer'|'answer'|'ice', ... }
      send(peer, { type: "signal", code, from: ws.role, payload: msg.payload });
      return;
    }

    // ---- RELAY FALLBACK: forward file bytes when P2P failed ----
    // Payload shapes (forwarded untouched):
    //   { kind:'relay-meta', fileId, name, size, mime, totalChunks }
    //   { kind:'relay-chunk', fileId, index, base64, isLast }
    //   { kind:'relay-ack', fileId, received }
    if (msg.type === "relay") {
      const code = String(msg.code || ws.roomCode || "");
      const room = rooms.get(code);
      if (!room) return send(ws, { type: "error", message: "Room not found." });
      const peer = peerOf(room, ws);
      if (!peer) return send(ws, { type: "waiting", message: "Waiting for peer…" });
      send(peer, { type: "relay", code, from: ws.role, payload: msg.payload });
      return;
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
    const { roomCode, role } = ws;
    if (!roomCode) return;
    const room = rooms.get(roomCode);
    if (!room) return;
    const peer = peerOf(room, ws);
    if (peer) send(peer, { type: "peer-left", role });
    // If sender leaves, the code is useless — destroy it.
    // If receiver leaves, keep room so sender can share the code again.
    if (role === "sender") {
      destroyRoom(roomCode);
    } else {
      room.receiver = null;
    }
  });

  ws.on("error", () => {});
});

function leaveRoom(ws) {
  const code = ws.roomCode;
  if (!code) return;
  const room = rooms.get(code);
  if (room) {
    const peer = peerOf(room, ws);
    if (peer) send(peer, { type: "peer-left", role: ws.role });
    if (ws.role === "sender") destroyRoom(code);
    else if (room.receiver === ws) room.receiver = null;
  }
  ws.roomCode = null;
  ws.role = null;
}

server.listen(PORT, () => {
  console.log(`P2P Share running at http://localhost:${PORT}`);
  console.log(`Signaling endpoint: ws://localhost:${PORT}/ws`);
});
