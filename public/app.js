/* P2P Share frontend — vanilla JS, no build step.
 * Flow:
 *  Send: pick file -> WS "create" -> show QR+code -> receiver joins ->
 *        WebRTC offer/answer/ICE via WS "signal" -> DataChannel chunks P2P.
 *  Fallback: if P2P can't connect (strict NAT), chunks relay via WS "relay".
 */
"use strict";

const CHUNK_SIZE = 64 * 1024;          // v2 striped chunk payload (see pickChunkSize guard)
const CHUNK_SIZE_LEGACY = 16 * 1024;   // v1 fallback chunk (safe for every browser)
const RELAY_CHUNK_SIZE = 32 * 1024;    // WS relay chunk before base64
const MAX_BUFFERED = 4 * 1024 * 1024;  // legacy backpressure threshold (v1 path)
const CH_HIGH_WATER = 2 * 1024 * 1024; // v2 per-channel pause threshold
const CH_LOW_WATER = 512 * 1024;       // v2 per-channel resume threshold (bufferedamountlow)
const AGG_QUEUE_CAP = 16 * 1024 * 1024;// v2 aggregate queued bytes across all channels
const P2P_TIMEOUT_MS = 12000;          // wait this long before suggesting relay
const HELLO_WAIT_MS = 800;             // wait this long for receiver v2 capability

const FALLBACK_STUN = ["stun:stun.l.google.com:19302", "stun:stun1.l.google.com:3478"];

// ICE servers come from GET /api/ice (server env, short-lived when REST is
// configured). No TURN secrets live in this file. STUN-only until it loads.
let ICE_CACHE = [{ urls: [...FALLBACK_STUN] }];
let TURN_STATE = { turn: false, ttl: 0 };
fetch("api/ice", { cache: "no-store" })
  .then((r) => { if (!r.ok) throw new Error("http " + r.status); return r.json(); })
  .then((j) => {
    if (j && Array.isArray(j.iceServers) && j.iceServers.length) ICE_CACHE = j.iceServers;
    TURN_STATE = { turn: !!j.turn, ttl: j.ttl || 0 };
    log(`ICE config loaded (TURN ${TURN_STATE.turn ? "enabled" : "not configured — STUN only"}).`);
  })
  .catch(() => log("ICE config fetch failed — using STUN fallback."));

// ---------- connection mode: auto / direct (same WiFi) / turn (far away) ----------
// Auto: P2P first (STUN + built-in TURN), WS relay fallback. Direct: P2P only.
// TURN: media forced through TURN (iceTransportPolicy relay) — for far networks.
function connMode() { return localStorage.getItem("p2p-mode") || "auto"; }
function allowRelay() { return connMode() !== "direct"; }
function buildIceServers() {
  return ICE_CACHE.map((s) => ({ ...s }));
}
// ---------- v2 protocol pure helpers (unit-tested, no DOM) ----------
// CRC32 (IEEE) with chaining: crc32(B, crc32(A)) === crc32(A||B).
const CRC32_TABLE = (() => {
  const t = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
    t[n] = c >>> 0;
  }
  return t;
})();
function crc32(buf, prev) {
  const u = buf instanceof Uint8Array ? buf : new Uint8Array(buf);
  let c = ((prev === undefined ? 0 : prev) ^ (-1)) >>> 0;
  for (let i = 0; i < u.length; i++) c = (CRC32_TABLE[(c ^ u[i]) & 0xFF] ^ (c >>> 8)) >>> 0;
  return (c ^ (-1)) >>> 0;
}
// zlib crc32_combine port: combine crc1=crc(A), crc2=crc(B) with len(B) bytes.
function crc32Combine(crc1, crc2, len2) {
  function mult(mat, vec) {
    let sum = 0, v = vec >>> 0, i = 0;
    while (v) { if (v & 1) sum ^= mat[i]; v >>>= 1; i++; }
    return sum >>> 0;
  }
  function square(out, mat) { for (let n = 0; n < 32; n++) out[n] = mult(mat, mat[n]); }
  if (len2 <= 0) return crc1 >>> 0;
  const even = new Array(32).fill(0), odd = new Array(32).fill(0);
  odd[0] = 0xEDB88320; let row = 1;
  for (let n = 1; n < 32; n++) { odd[n] = row >>> 0; row = (row << 1) >>> 0; }
  square(even, odd); square(odd, even);
  let c = crc1 >>> 0, len = len2;
  do {
    square(even, odd);
    if (len & 1) c = mult(even, c);
    len >>>= 1;
    if (!len) break;
    square(odd, even);
    if (len & 1) c = mult(odd, c);
    len >>>= 1;
  } while (len);
  return (c ^ (crc2 >>> 0)) >>> 0;
}
// Binary frame: [u32 BE index][u32 BE chunk-crc][payload]. Max index 2^32-1.
const FRAME_HEADER = 8;
function encodeFrame(index, payload) {
  const p = payload instanceof Uint8Array ? payload : new Uint8Array(payload);
  const out = new Uint8Array(FRAME_HEADER + p.length);
  const dv = new DataView(out.buffer);
  dv.setUint32(0, index >>> 0);
  dv.setUint32(4, crc32(p));
  out.set(p, FRAME_HEADER);
  return out.buffer;
}
function decodeFrame(buf) {
  const u = buf instanceof Uint8Array ? buf : new Uint8Array(buf || 0);
  if (u.length < FRAME_HEADER) return { error: "short" };
  const dv = new DataView(u.buffer, u.byteOffset, u.byteLength);
  const index = dv.getUint32(0) >>> 0;
  const crc = dv.getUint32(4) >>> 0;
  const payload = u.subarray(FRAME_HEADER);
  if (crc32(payload) !== crc) return { error: "crc", index };
  return { index, payload: payload.slice().buffer };
}
// Chunk size from negotiated SCTP cap; unknown caps stay at 64 KiB on modern
// browsers (documented Safari-legacy caveat; send errors trigger downgrade).
function pickChunkSize(maxMessageSize) {
  if (maxMessageSize && maxMessageSize < 66000) return CHUNK_SIZE_LEGACY;
  return CHUNK_SIZE;
}
// Decimal throughput units (MB = 1,000,000 bytes — labeled as such in UI).
function fmtRate(bytesPerSec) {
  if (!Number.isFinite(bytesPerSec) || bytesPerSec < 0) return "—";
  if (bytesPerSec < 1000) return `${Math.round(bytesPerSec)} B/s`;
  if (bytesPerSec < 1e6) return `${(bytesPerSec / 1e3).toFixed(1)} KB/s`;
  if (bytesPerSec < 1e9) return `${(bytesPerSec / 1e6).toFixed(1)} MB/s`;
  return `${(bytesPerSec / 1e9).toFixed(2)} GB/s`;
}
// Rolling throughput over timestamped {t, bytes} events (bounded by caller).
function windowedRate(events, nowMs, windowMs) {
  const cut = nowMs - windowMs;
  let sum = 0, first = -1, last = -1, n = 0;
  for (const e of events) {
    if (e.t < cut) continue;
    sum += e.bytes; n++;
    if (first < 0) first = e.t;
    last = e.t;
  }
  if (n < 2) return 0;
  return sum / Math.max(0.5, (last - first) / 1000);
}
function etaState(frac, elapsedSec, stalledSec) {
  if (frac >= 1) return "done";
  if (elapsedSec < 2) return "calculating";
  if (stalledSec > 5) return "stalled";
  if (frac < 0.05) return "estimating";
  if (frac > 0.98) return "almost";
  return "time";
}// After connect, log which path media actually uses (direct vs TURN relay).
async function logPath(pc, who) {
  try {
    const stats = await pc.getStats();
    let remote = null;
    stats.forEach((s) => {
      if (s.type === "candidate-pair" && (s.nominated || s.state === "succeeded") && s.remoteCandidateId) {
        const rc = stats.get(s.remoteCandidateId);
        if (rc) remote = rc.candidateType || rc.type;
      }
    });
    if (remote) log(`${who}: media via ${remote === "relay" ? "TURN relay" : "direct (" + remote + ")"}.`);
  } catch {}
}

// ---------- tiny dom helpers ----------
const $ = (id) => document.getElementById(id);
const logEl = $("log");
function log(msg) {
  const li = document.createElement("li");
  li.textContent = `${new Date().toLocaleTimeString()} — ${msg}`;
  logEl.prepend(li);
}
function setStatus(el, msg, cls = "") {
  el.textContent = msg;
  el.className = "status " + cls;
}
function fmtBytes(n) {
  if (!Number.isFinite(n)) return "—";
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MB`;
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GB`;
}
function fmtSpeed(bps) {
  return `${fmtBytes(bps)}/s`;
}
function fmtETA(elapsedSec, done, total) {
  if (done >= total) return "0s";
  if (!done || !Number.isFinite(elapsedSec) || elapsedSec <= 0) return "—";
  const s = Math.round((elapsedSec / done) * (total - done));
  if (!Number.isFinite(s)) return "—";
  return s < 60 ? `${s}s left` : `${Math.floor(s / 60)}m ${s % 60}s left`;
}
// Screen wake-lock: stops phones from sleeping mid-transfer. Best-effort.
let wakeLock = null;
async function lockScreen() {
  try {
    if ("wakeLock" in navigator && !wakeLock) {
      wakeLock = await navigator.wakeLock.request("screen");
      wakeLock.addEventListener("release", () => { wakeLock = null; });
      log("Screen sleep locked during transfer.");
    }
  } catch {}
}
async function releaseScreen() {
  try { wakeLock && await wakeLock.release(); } catch {}
  wakeLock = null;
}
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible" && (S.started || (R.meta && !R.done))) lockScreen();
});
// Completion feedback: subtle beep + title flash (tab visible from far).
function notifyDone(name) {
  try {
    const Ctx = window.AudioContext || window.webkitAudioContext;
    const ctx = new Ctx();
    const o = ctx.createOscillator(), g = ctx.createGain();
    o.connect(g); g.connect(ctx.destination);
    o.frequency.value = 880; g.gain.value = 0.08;
    o.start(); o.stop(ctx.currentTime + 0.25);
  } catch {}
  const orig = document.title;
  document.title = "✓ Done — " + name;
  setTimeout(() => { document.title = orig; }, 4000);
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------- compatibility ----------
(function compatCheck() {
  const problems = [];
  if (!window.RTCPeerConnection) problems.push("WebRTC is not supported in this browser. Use a recent Chrome, Edge, Firefox or Safari.");
  if (!window.WebSocket) problems.push("WebSockets are not supported in this browser.");
  if (problems.length) {
    const w = $("compatWarn");
    w.hidden = false;
    w.textContent = problems.join(" ");
  }
})();

// ---------- tabs ----------
const tabSend = $("tabSend"), tabReceive = $("tabReceive"), tabText = $("tabText");
const paneSend = $("paneSend"), paneReceive = $("paneReceive"), paneText = $("paneText");
function showTab(which) {
  tabSend.classList.toggle("active", which === "send");
  tabReceive.classList.toggle("active", which === "receive");
  tabText.classList.toggle("active", which === "text");
  paneSend.hidden = which !== "send";
  paneReceive.hidden = which !== "receive";
  paneText.hidden = which !== "text";
}
tabSend.onclick = () => showTab("send");
tabReceive.onclick = () => showTab("receive");
tabText.onclick = () => showTab("text");

// ---------- nickname (editable, shown to the peer, saved locally) ----------
function myNick() { return ($("nickInput").value || "").trim().slice(0, 24) || "Peer"; }
let nickTimer = null;
function sendNickUpdate() {
  const nick = myNick();
  if (S.code) sendSignal({ type: "nick", code: S.code, nick });
  else if (R.code) sendSignal({ type: "nick", code: R.code, nick });
}
$("nickInput").value = localStorage.getItem("p2p-nick") || ("Device-" + Math.floor(1000 + Math.random() * 9000));
$("nickInput").addEventListener("input", () => {
  localStorage.setItem("p2p-nick", $("nickInput").value);
  $("nickSaved").textContent = "saved";
  clearTimeout(nickTimer);
  nickTimer = setTimeout(sendNickUpdate, 600);
});

// ---------- history (this device, this session) ----------
const historyItems = [];
function addHistory(dir, name, size, url, peer) {
  historyItems.unshift({ dir, name, size, url, peer: peer || "", time: new Date().toLocaleTimeString() });
  renderHistory();
}
function renderHistory() {
  const ul = $("historyList");
  ul.innerHTML = "";
  if (!historyItems.length) { ul.innerHTML = "<li>No transfers yet.</li>"; return; }
  for (const h of historyItems) {
    const li = document.createElement("li");
    li.textContent = `${h.time} — ${h.dir === "sent" ? "↑ sent" : "↓ received"} ${h.name} (${fmtBytes(h.size)})${h.peer ? " ↔ " + h.peer : ""}`;
    if (h.url) {
      const a = document.createElement("a");
      a.href = h.url;
      a.download = h.name;
      a.textContent = " Download again";
      a.style.marginLeft = "6px";
      li.appendChild(a);
    }
    ul.appendChild(li);
  }
}
renderHistory();

// ---------- signaling socket (shared) ----------
let ws = null;
let wsReady = null;

function wsUrl() {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${location.host}/ws`;
}

function ensureSocket() {
  if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return wsReady;
  ws = new WebSocket(wsUrl());
  wsReady = new Promise((resolve, reject) => {
    const to = setTimeout(() => reject(new Error("Signaling server unreachable")), 8000);
    ws.addEventListener("open", () => { clearTimeout(to); log("Connected to signaling server."); resolve(ws); });
    ws.addEventListener("error", () => { clearTimeout(to); reject(new Error("Could not reach signaling server. Is `npm start` running?")); });
  });
  ws.addEventListener("message", onSignalMessage);
  ws.addEventListener("close", () => log("Signaling connection closed."));
  return wsReady;
}

async function sendSignal(obj) {
  await ensureSocket();
  ws.send(JSON.stringify(obj));
}

// ---------- sender state (1 sender → up to 5 receivers) ----------
const MAX_PEERS = 5;
const S = {
  file: null,
  fileId: null,
  code: null,
  peers: new Map(), // peerId -> {id, nick, pc, dc, state, sentChunks, totalChunks, startTime, p2pTimer, pendingIce, useRelay, done, left, pumping}
  started: false,
  cancelled: false,
  autoArmed: false, // pickFile arms it; Start/Cancel consume it; auto-start needs it
  startTime: 0,
};

// ---------- receiver state ----------
const R = {
  code: null,
  peerId: null,
  senderNick: "",
  pc: null,
  dc: null,
  paths: [],        // [{pc, dc, idx, pendingIce}] — one per sender path
  probePaths: {},   // conn>=100 -> {pc, pendingIce} (speed tune, disposable)
  pendingIce: [],
  fileId: null,
  meta: null,
  chunks: [],       // Array<ArrayBuffer> indexed by chunk number
  received: 0,
  totalChunks: 0,
  startTime: 0,
  relayMode: false,
  done: false,
  v2: null,         // striped transfer state (see onFileMetaV2)
  events: [],       // {t, bytes} for rolling receive rate (bounded)
  ackT: 0,
  nackT: 0,
  unverT: null,
  probeCount: {},   // session -> bytes (disposable tune traffic, never files)
};

// ================= SENDER UI =================
const dropZone = $("dropZone"), fileInput = $("fileInput");
dropZone.addEventListener("click", () => fileInput.click());
dropZone.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") fileInput.click(); });
["dragover", "dragenter"].forEach((ev) => dropZone.addEventListener(ev, (e) => { e.preventDefault(); dropZone.style.borderColor = "#2563eb"; }));
["dragleave", "drop"].forEach((ev) => dropZone.addEventListener(ev, (e) => { e.preventDefault(); dropZone.style.borderColor = ""; }));
dropZone.addEventListener("drop", (e) => {
  const f = e.dataTransfer.files && e.dataTransfer.files[0];
  if (f) pickFile(f);
});
fileInput.addEventListener("change", () => { if (fileInput.files[0]) pickFile(fileInput.files[0]); });

function pickFile(f) {
  S.file = f;
  S.fileId = `${f.name}-${f.size}-${f.lastModified}`;
  S.started = false; S.cancelled = true; // stop any running pump
  S.autoArmed = true; // arm auto-start for the next receiver that connects
  $("fileRow").hidden = false;
  $("fileName").textContent = f.name;
  $("fileSize").textContent = `${fmtBytes(f.size)} · ${f.type || "unknown type"}`;
  $("btnCreate").disabled = false;
  $("sendProgressWrap").hidden = true;
  $("btnRetrySend").hidden = true;
  $("btnRelay").hidden = true;
  closeAllPeers(false);
  for (const peer of S.peers.values()) { peer.sentChunks = 0; peer.done = false; peer.useRelay = false; peer.totalChunks = 0; peer.left = false; peer.state = "joined"; peer.frame = "v1"; peer.helloV2 = false; peer.cursor = 0; peer.nackQueue = []; peer.crcParts = []; peer.delivered = 0; peer.stats = { sent: 0, events: [], peak: 0 }; }
  renderPeerList();
  if (f.size > 2 * 1024 * 1024 * 1024) {
    log("Warning: file exceeds 2 GB — transfer works but the receiver needs enough RAM to reassemble it.");
  }
  log(`Selected: ${f.name} (${fmtBytes(f.size)})`);
}

$("changeFile").onclick = () => fileInput.click();

$("btnCreate").onclick = async () => {
  if (!S.file) return;
  $("btnCreate").disabled = true;
  try {
    await ensureSocket();
    // If we already hold a code for this file, reuse it; else create fresh.
    if (S.code) {
      showShare(S.code);
      $("btnCreate").disabled = false;
      return;
    }
    sendSignal({ type: "create", nick: myNick() });
    setStatus($("senderStatus"), "Generating code…");
    log("Requesting connection code…");
  } catch (err) {
    setStatus($("senderStatus"), err.message, "err");
    $("btnCreate").disabled = false;
  }
};

function showShare(code) {
  S.code = code;
  clearInterval(S.expiryTimer);
  const expiresAt = Date.now() + 10 * 60 * 1000; // matches server CODE_TTL_MS
  const tick = () => {
    const left = Math.max(0, expiresAt - Date.now());
    const mm = Math.floor(left / 60000), ss = Math.floor((left % 60000) / 1000);
    $("codeExpiry").textContent = left > 0
      ? `Code expires in ${mm}:${String(ss).padStart(2, "0")} — receiver should join soon`
      : "Code expired. Click “Get QR & code” again.";
    if (!left) { clearInterval(S.expiryTimer); S.code = null; $("btnCreate").disabled = false; }
  };
  tick();
  S.expiryTimer = setInterval(tick, 1000);
  S.peers.clear();
  renderPeerList();
  updatePeerCount();
  $("shareBox").hidden = false;
  $("shareCode").textContent = code;
  setStatus($("senderStatus"), "Waiting for receiver to enter the code…");
  $("btnSendStart").disabled = true; // enabled when peer joins
  const link = `${location.origin}${location.pathname}?code=${code}`;
  const qrEl = $("qr");
  qrEl.innerHTML = "";
  try {
    if (typeof QRCode !== "undefined") {
      new QRCode(qrEl, { text: link, width: 128, height: 128, correctLevel: QRCode.CorrectLevel.M });
    } else {
      qrEl.innerHTML = `<a href="${link}" target="_blank" rel="noopener">Open link</a>`;
      log("QR library failed to load (offline?) — use the Copy link button instead.");
    }
  } catch {
    qrEl.textContent = link;
  }
  log(`Share code ${code}. Link: ${link}`);
}

$("btnCopyCode").onclick = async () => {
  if (!S.code) return;
  try { await navigator.clipboard.writeText(S.code); log("Code copied."); }
  catch { prompt("Copy this code:", S.code); }
};
$("btnCopyLink").onclick = async () => {
  const link = `${location.origin}${location.pathname}?code=${S.code || ""}`;
  try { await navigator.clipboard.writeText(link); log("Link copied."); }
  catch { prompt("Copy this link:", link); }
};

$("btnSendStart").onclick = () => startTransfer(false, false);
$("btnRelay").onclick = () => {
  log("Switching all receivers to server relay (slower, works on any network).");
  startTransfer(true, false);
};
function senderCancelledByPeer() {
  // Local Cancel pressed: stop everything and clear.
  S.cancelled = true;
  S.started = false;
  S.autoArmed = false;
  closeAllPeers(false);
  releaseScreen();
  for (const peer of S.peers.values()) { peer.sentChunks = 0; peer.totalChunks = 0; peer.done = false; peer.useRelay = false; peer.state = "joined"; peer.cursor = 0; peer.nackQueue = []; peer.crcParts = []; peer.delivered = 0; peer.stats = { sent: 0, events: [], peak: 0 }; clearTimeout(peer.finTimer); }
  $("sendBar").style.width = "0%";
  $("sendPct").textContent = "0%";
  $("sendStats").textContent = "";
  $("sendProgressWrap").hidden = true;
  $("btnRetrySend").hidden = true;
  $("btnRelay").hidden = true;
  $("btnSendStart").disabled = ![...S.peers.values()].some((p) => !p.left);
  renderPeerList();
  log("Transfer cleared.");
}
$("btnCancelSend").onclick = () => {
  for (const peer of S.peers.values()) {
    try { peer.dc && peer.dc.readyState === "open" && peer.dc.send(JSON.stringify({ kind: "cancel" })); } catch {}
  }
  if (S.code) sendSignal({ type: "relay", code: S.code, payload: { kind: "cancel", fileId: S.fileId } });
  senderCancelledByPeer();
  setStatus($("senderStatus"), "Cancelled — cleared. Press Start transfer for a fresh send.");
};
$("btnRetrySend").onclick = () => {
  $("btnRetrySend").hidden = true;
  startTransfer(false, false);
};

// ---------- sender: parallel striped paths per receiver ----------
// Each receiver gets K independent PeerConnections (K=1 for legacy or
// single-path mode). Striped 64 KiB indexed frames go out over all open
// channels; each channel has independent backpressure so a slow path never
// blocks a healthy one. v1 (ordered raw chunks, 1 channel) is preserved for
// receivers that never advertise v2 capability.
function tunedConns() {
  const ov = localStorage.getItem("p2p-conns") || "auto";
  if (ov === "1" || ov === "2" || ov === "4") return parseInt(ov, 10);
  try {
    const t = JSON.parse(localStorage.getItem("p2p-tune") || "null");
    if (t && (t.k === 1 || t.k === 2 || t.k === 4) && t.mode === connMode()) return t.k;
  } catch {}
  return 2;
}
function syncConnSel() {
  const sel = $("connSel");
  if (sel) sel.value = localStorage.getItem("p2p-conns") || "auto";
  const t = $("tuneResult");
  if (t) {
    try {
      const r = JSON.parse(localStorage.getItem("p2p-tune") || "null");
      t.textContent = r ? `Tuned: ${r.k} conns @ ${fmtRate(r.mbs)} (${r.mode}, ${new Date(r.when).toLocaleString()})` : "Not tuned yet for this mode.";
    } catch { t.textContent = "Not tuned yet for this mode."; }
  }
}
function addPeer(id, nick) {
  let peer = S.peers.get(id);
  if (!peer) {
    peer = { id, nick: nick || "Receiver", pc: null, dc: null, state: "joined",
      sentChunks: 0, totalChunks: 0, startTime: 0, p2pTimer: null, pendingIce: [],
      useRelay: false, done: false, left: false, pumping: false,
      frame: "v1", helloV2: false, helloTimer: null, pathsBuilt: false, paths: [],
      cursor: 0, nackQueue: [], crcParts: [], delivered: 0, chunkSize: CHUNK_SIZE,
      finTries: 0, lastProgressT: 0,
      stats: { sent: 0, events: [], peak: 0 },
      diag: { path: "unknown", transport: "unknown", rttMs: null, retries: 0 } };
    S.peers.set(id, peer);
  } else {
    peer.nick = nick || peer.nick;
    peer.left = false;
    peer.sentChunks = 0; peer.done = false; peer.state = "joined";
    peer.cursor = 0; peer.nackQueue = []; peer.crcParts = []; peer.delivered = 0;
  }
  renderPeerList();
  updatePeerCount();
  return peer;
}

function updatePeerCount() {
  const n = [...S.peers.values()].filter((p) => !p.left).length;
  $("peerCount").textContent = n ? `${n} receiver(s) connected (max ${MAX_PEERS}).` : `No receivers yet (max ${MAX_PEERS}).`;
}

function peerLabel(peer) {
  if (peer.left) return "left";
  if (peer.done) return peer.verified ? "done ✓ verified" : peer.useRelay ? "done (relay)" : "done (P2P)";
  if (peer.useRelay) return "relaying…";
  if (peer.state === "sending") return peer.paths.length > 1 ? `sending… (${peer.paths.length} paths)` : "sending…";
  if (peer.state === "connected") return "connected";
  return "connecting…";
}

function renderPeerList() {
  const box = $("peerList");
  if (!box) return;
  box.innerHTML = "";
  for (const peer of S.peers.values()) {
    const div = document.createElement("div");
    div.className = "peer" + (peer.done ? " peer-done" : "");
    const top = document.createElement("div");
    top.className = "peer-top";
    const nm = document.createElement("span");
    nm.textContent = peer.nick;
    const st = document.createElement("span");
    st.id = "peer-st-" + peer.id;
    st.textContent = peerLabel(peer);
    top.append(nm, st);
    const bar = document.createElement("div");
    bar.className = "progress";
    const fill = document.createElement("div");
    fill.className = "bar";
    fill.id = "peer-bar-" + peer.id;
    fill.style.width = Math.round(peerFrac(peer) * 100) + "%";
    bar.appendChild(fill);
    div.append(top, bar);
    box.appendChild(div);
  }
}

function peerFrac(p) {
  // Bar shows queued progress (like v1); the stats line shows ACK-verified
  // delivered bytes separately — the bar must move even before ACKs arrive.
  const t = p.totalChunks || 1;
  return Math.min(p.sentChunks, t) / t;
}

function peerDoneBytes(p) {
  if (!S.file) return 0;
  if (p.frame === "v2") return Math.min(p.delivered, S.file.size);
  const cs = p.useRelay ? RELAY_CHUNK_SIZE : CHUNK_SIZE_LEGACY;
  return Math.min(p.sentChunks * cs, S.file.size);
}

function updatePeerRow(peer) {
  const st = $("peer-st-" + peer.id), bar = $("peer-bar-" + peer.id);
  const pct = Math.round(peerFrac(peer) * 100) + "%";
  if (st) st.textContent = peerLabel(peer) + (peer.done || peer.left ? "" : ` ${pct}`);
  if (bar) bar.style.width = pct;
}

// ---------- transfer statistics (ACK-verified delivery) ----------
// sentBytes = queued into SCTP; deliveredBytes = receiver-confirmed contiguous.
// Only deliveredBytes is presented as "received" — queue drain is not delivery.
function recordSend(peer, queuedBytes) {
  const st = peer.stats;
  const now = Date.now();
  st.sent += queuedBytes;
  st.events.push({ t: now, bytes: queuedBytes });
  if (st.events.length > 120) st.events.splice(0, st.events.length - 120);
  const rate = windowedRate(st.events, now, 3000);
  if (rate > st.peak) st.peak = rate;
  peer.lastProgressT = now;
}
function recordDelivered(peer, bytes) {
  if (bytes > peer.delivered) {
    peer.delivered = bytes;
    peer.lastProgressT = Date.now();
  }
}
// Candidate-pair diagnostics: path + transport from live getStats.
// transport comes from the remote candidate's `protocol` field (udp/tcp);
// anything else is reported as unknown — never guessed.
async function updateDiagFromStats(peer, path) {
  try {
    const stats = await path.pc.getStats();
    let pair = null;
    stats.forEach((s) => {
      if (s.type === "candidate-pair" && (s.nominated || s.state === "succeeded")) pair = s;
    });
    if (!pair) return;
    const rc = pair.remoteCandidateId ? stats.get(pair.remoteCandidateId) : null;
    const ctype = rc ? (rc.candidateType || "") : "";
    const proto = rc && rc.protocol ? String(rc.protocol).toLowerCase() : "unknown";
    peer.diag.path = ctype === "relay" ? "TURN relay" : ctype === "srflx" || ctype === "host" ? "direct" : "unknown";
    peer.diag.transport = proto === "udp" || proto === "tcp" ? proto : "unknown";
    peer.diag.rttMs = Number.isFinite(pair.currentRoundTripTime) ? Math.round(pair.currentRoundTripTime * 1000) : null;
    peer.diag.turnHost = rc && rc.url ? String(rc.url).split("?")[0].replace(/^(stun|turn|turns):/i, "") : null;
    updatePeerRow(peer);
  } catch {}
}

function makePathPC(peer, idx) {
  const pc = new RTCPeerConnection({ iceServers: buildIceServers(), iceTransportPolicy: connMode() === "turn" ? "relay" : "all", iceCandidatePoolSize: 2, bundlePolicy: "max-bundle" });
  const path = { pc, dc: null, idx, pumping: false, p2pTimer: null, pendingIce: [] };
  peer.paths[idx] = path;
  if (idx === 0) { peer.pc = pc; peer.pendingIce = path.pendingIce; }

  pc.onicecandidate = (e) => {
    if (e.candidate && S.code) sendSignal({ type: "signal", code: S.code, target: peer.id, payload: { kind: "ice", conn: idx, candidate: e.candidate } });
  };
  pc.onconnectionstatechange = () => {
    const st = pc.connectionState;
    if (idx === 0) log(`Sender→${peer.nick}: ${st}`);
    if (st === "connected") {
      clearTimeout(path.p2pTimer);
      peer.state = "connected";
      if (idx === 0) { logPath(pc, peer.nick); updateDiagFromStats(peer, path); }
      updatePeerRow(peer);
      if (S.peers.size === 1 && idx === 0) setStatus($("senderStatus"), `Connected to ${peer.nick} directly (P2P). Sending…`, "ok");
      if (S.started && !S.cancelled && !peer.done && !peer.left && !peer.useRelay) pumpPeer(peer);
    } else if (st === "failed") {
      pathFailed(peer, path);
    } else if (st === "disconnected") {
      if (S.started && !peer.useRelay && !peer.done && !peer.left) {
        updatePeerRow(peer);
        $("btnRelay").hidden = false;
      }
    }
  };

  const dc = pc.createDataChannel("file", { ordered: true });
  path.dc = dc;
  if (idx === 0) peer.dc = dc;
  dc.binaryType = "arraybuffer";
  dc.bufferedAmountLowThreshold = CH_LOW_WATER;
  dc.onopen = () => {
    if (idx === 0) log(`Data channel open to ${peer.nick} — sending.`);
    if (S.started && !S.cancelled && !peer.done && !peer.left && !peer.useRelay) pumpPeer(peer);
  };
  dc.onclose = () => {
    if (S.started && !S.cancelled && !peer.useRelay && !peer.done && !peer.left && peer.sentChunks < peer.totalChunks) {
      peer.state = "connecting";
      updatePeerRow(peer);
      setStatus($("senderStatus"), `${peer.nick}: interrupted. Press Retry to resume.`, "err");
      $("btnRetrySend").hidden = false;
      log(`Interrupted to ${peer.nick} at ${peer.sentChunks}/${peer.totalChunks}.`);
    }
  };
  dc.onbufferedamountlow = () => { if (S.started && !peer.done && !peer.left && !peer.useRelay) pumpPeer(peer); };
  dc.onmessage = (e) => {
    if (typeof e.data !== "string") return;
    try {
      const m = JSON.parse(e.data);
      if (m.kind === "cancel-request") { stopPeer(peer, "cancelled"); setStatus($("senderStatus"), `${peer.nick} cancelled. Others continue.`, "err"); return; }
      if (m.kind === "resume") {
        if (m.frame === "v2" && peer.frame === "v2") {
          log(`Resuming ${peer.nick} (v2, receiver has ${m.received || 0}/${peer.totalChunks}; NACK sweep follows).`);
        } else {
          const have = Math.min(m.received || 0, peer.totalChunks);
          if (have > peer.sentChunks) { peer.sentChunks = have; log(`Resuming ${peer.nick} from chunk ${have}/${peer.totalChunks}.`); }
        }
        return;
      }
      if (m.kind === "ack2" && peer.frame === "v2") {
        const bytes = Math.min(peer.totalChunks * peer.chunkSize, (m.contig || 0) * peer.chunkSize);
        if (bytes > peer.delivered) {
          recordDelivered(peer, bytes);
          updatePeerRow(peer); updateSendProgress();
        }
        return;
      }
      if (m.kind === "nack" && peer.frame === "v2" && Array.isArray(m.missing)) {
        let n = 0;
        for (const ix of m.missing.slice(0, 1000)) {
          if (Number.isInteger(ix) && ix >= 0 && ix < peer.totalChunks && !peer.nackQueue.includes(ix)) { peer.nackQueue.push(ix); n++; }
        }
        if (n) { log(`Repairing ${n} chunks to ${peer.nick}.`); pumpPeer(peer); }
        return;
      }
      if (m.kind === "fin" && peer.frame === "v2") {
        clearTimeout(peer.finTimer);
        if (m.ok) {
          peer.done = true;
          peer.verified = true;
          peer.state = "done";
          updatePeerRow(peer); updateSendProgress(); renderPeerList();
          addHistory("sent", S.file.name, S.file.size, null, peer.nick);
          log(`Sent ${S.file.name} to ${peer.nick} (v2, integrity verified by receiver).`);
          notifyDone(`${S.file.name} → ${peer.nick}`);
          finishSenderIfAllDone();
        } else {
          setStatus($("senderStatus"), `${peer.nick}: integrity check FAILED at receiver.`, "err");
          log(`Integrity failure reported by ${peer.nick} — transfer NOT marked complete.`);
        }
        return;
      }
    } catch {}
  };

  clearTimeout(path.p2pTimer);
  path.p2pTimer = setTimeout(() => {
    if (S.started && !S.cancelled && !peer.useRelay && !peer.done && !peer.left && peer.sentChunks === 0 && pc.connectionState !== "connected") {
      pathFailed(peer, path);
    }
  }, P2P_TIMEOUT_MS);
  renderPeerList();
  return path;
}

// One path failed. Abandon P2P for this receiver only when no path is usable
// and nothing was delivered yet (healthy paths keep going otherwise).
function pathFailed(peer, path) {
  peer.diag.retries++;
  const usable = peer.paths.some((p) => p && p.pc && (p.pc.connectionState === "connected" || p.pc.connectionState === "connecting"));
  if (S.started && !S.cancelled && !peer.useRelay && !peer.done && !peer.left && !usable && peer.sentChunks === 0) {
    if (allowRelay()) {
      log(`P2P to ${peer.nick} unusable — auto-switching only that receiver to relay.`);
      startRelayPeer(peer);
    } else {
      setStatus($("senderStatus"), `${peer.nick}: direct failed (Direct-only mode — join same WiFi).`, "err");
    }
  } else {
    updatePeerRow(peer);
  }
}

function buildPaths(peer, v2) {
  if (peer.pathsBuilt || peer.left) return;
  peer.pathsBuilt = true;
  peer.frame = v2 ? "v2" : "v1";
  const K = v2 ? tunedConns() : 1;
  for (let i = 0; i < K; i++) {
    makePathPC(peer, i);
    peer.paths[i].pc.createOffer()
      .then((o) => peer.paths[i].pc.setLocalDescription(o))
      .then(() => sendSignal({ type: "signal", code: S.code, target: peer.id, payload: { kind: "offer", conn: i, sdp: peer.paths[i].pc.localDescription } }))
      .catch((err) => setStatus($("senderStatus"), "Could not start WebRTC: " + err.message, "err"));
  }
  log(`Offer${K > 1 ? `s (${K} paths)` : ""} sent to ${peer.nick}${v2 ? " [v2 striped]" : " [v1]"}.`);
}

function maybeAutoStart() {
  if (S.file && !S.started && S.autoArmed) { S.autoArmed = false; startTransfer(false, true); } // only newcomers
}

function ensurePaths(peer) {
  if (!peer.pathsBuilt) { buildPaths(peer, peer.helloV2); return; }
  const anyOpen = peer.paths.some((p) => p && p.dc && p.dc.readyState === "open");
  if (anyOpen && S.started && !S.cancelled && !peer.done && !peer.left && !peer.useRelay) pumpPeer(peer);
}

function closePath(peer, path) {
  if (!path) return;
  clearTimeout(path.p2pTimer);
  try { path.dc && path.dc.close(); } catch {}
  try { path.pc && path.pc.close(); } catch {}
  path.dc = null; path.pc = null; path.pumping = false;
}

function closePeer(peer, logIt) {
  if (!peer) return;
  clearTimeout(peer.helloTimer);
  clearTimeout(peer.finTimer);
  for (const p of peer.paths) closePath(peer, p);
  peer.paths = [];
  peer.pathsBuilt = false;
  peer.pc = null; peer.dc = null; peer.pumping = false;
  if (logIt) log(`Connection to ${peer.nick} closed.`);
}

function closeAllPeers(logIt) {
  for (const peer of S.peers.values()) closePeer(peer, false);
  if (S.probeCtx) {
    for (const ent of S.probeCtx.pcs.values()) {
      try { ent.dc && ent.dc.close(); } catch {}
      try { ent.pc && ent.pc.close(); } catch {}
    }
    S.probeCtx = null;
  }
  if (logIt) log("All sender connections closed.");
}

// Stop one receiver without touching the others. The peer is parked
// (left=true) so later Start calls skip it until it rejoins.
function stopPeer(peer, reason) {
  if (!peer || peer.left) return;
  peer.left = true;
  peer.sentChunks = 0; peer.done = false; peer.state = "joined";
  peer.cursor = 0; peer.nackQueue = []; peer.crcParts = []; peer.delivered = 0;
  peer.stats = { sent: 0, events: [], peak: 0 };
  clearTimeout(peer.finTimer);
  closePeer(peer, false);
  updatePeerRow(peer);
  renderPeerList();
  updatePeerCount();
  log(`${peer.nick} ${reason === "cancelled" ? "cancelled — stopped (others continue)" : "left"}.`);
  if (![...S.peers.values()].some((p) => !p.done && !p.left)) {
    S.started = false;
    releaseScreen();
  }
}

async function startTransfer(forceRelay, onlyNew) {
  if (!S.file) { setStatus($("senderStatus"), "Pick a file first.", "err"); return; }
  if (!S.code) { setStatus($("senderStatus"), "Click “Get QR & code” first.", "err"); return; }
  if (forceRelay && !allowRelay()) { setStatus($("senderStatus"), "Direct-only mode: relay disabled. Switch mode to Auto.", "err"); return; }
  let targets = [...S.peers.values()].filter((p) => !p.done && !p.left);
  if (onlyNew) targets = targets.filter((p) => !p.pumping && p.sentChunks === 0 && (p.cursor || 0) === 0);
  if (!targets.length) {
    if ([...S.peers.values()].some((p) => !p.left)) { setStatus($("senderStatus"), "Everyone already has the file.", "ok"); return; }
    setStatus($("senderStatus"), "No receiver connected yet — share the code first.", "err");
    return;
  }
  S.cancelled = false;
  S.autoArmed = false;
  S.started = true;
  S.startTime = Date.now();
  lockScreen();
  $("sendProgressWrap").hidden = false;
  $("btnRetrySend").hidden = true;
  $("sendLabel").textContent = forceRelay ? "Relaying via server…" : "Sending…";
  setStatus($("senderStatus"), `${forceRelay ? "Relaying" : "Sending"} to ${targets.length} receiver(s)…`);
  for (const peer of targets) {
    if (peer.pumping) continue;
    if (peer.frame === "v2" && !peer.useRelay && !forceRelay && peer.cursor >= peer.totalChunks && peer.totalChunks > 0) {
      // All bytes went out but confirmation never arrived: just re-ask.
      peer.finTries = 0;
      sendV2Done(peer);
      continue;
    }
    peer.sentChunks = 0;
    peer.done = false;
    peer.cursor = 0; peer.nackQueue = []; peer.crcParts = [];
    peer.delivered = 0; peer.finTries = 0; peer._lastDelivered = 0;
    peer.stats = { sent: 0, events: [], peak: 0 };
    peer.lastProgressT = Date.now();
    peer.startTime = Date.now();
    clearTimeout(peer.finTimer);
    if (forceRelay || peer.useRelay) startRelayPeer(peer);
    else ensurePaths(peer);
  }
}

function pumpPeer(peer) {
  if (!S.file || peer.done || peer.left || peer.useRelay || S.cancelled) return;
  if (peer.frame === "v2") pumpV2(peer);
  else pumpV1(peer);
}

// v1: legacy ordered raw chunks on path 0 (identical wire behavior as before).
async function pumpV1(peer) {
  const path = peer.paths[0];
  const file = S.file;
  if (!file || !path || !path.dc || path.dc.readyState !== "open") return;
  if (peer.pumping) return;
  const dc = path.dc;
  peer.pumping = true;
  peer.state = "sending";
  peer.totalChunks = Math.max(1, Math.ceil(file.size / CHUNK_SIZE_LEGACY));
  const fileId = S.fileId;
  try {
    dc.send(JSON.stringify({
      kind: "meta", fileId, name: file.name, size: file.size,
      mime: file.type || "application/octet-stream",
      totalChunks: peer.totalChunks, chunkSize: CHUNK_SIZE_LEGACY, nick: myNick(),
    }));
  } catch {
    peer.pumping = false;
    setStatus($("senderStatus"), `${peer.nick}: send failed at handshake. Press Retry.`, "err");
    $("btnRetrySend").hidden = false;
    return;
  }
  await sleep(250); // let receiver reply with resume offset before we blast chunks
  if (S.cancelled || peer.left || peer.done) { peer.pumping = false; return; }

  for (let i = peer.sentChunks; i < peer.totalChunks; i++) {
    if (S.cancelled || peer.left) { peer.pumping = false; return; }
    if (dc.readyState !== "open") { peer.sentChunks = i; peer.pumping = false; return; } // onclose handler offers retry
    while (dc.bufferedAmount > MAX_BUFFERED) await sleep(100);
    if (S.cancelled || peer.left) { peer.pumping = false; return; }
    const slice = file.slice(i * CHUNK_SIZE_LEGACY, (i + 1) * CHUNK_SIZE_LEGACY);
    const buf = await slice.arrayBuffer();
    try { dc.send(buf); }
    catch { peer.sentChunks = i; peer.pumping = false; return; }
    peer.sentChunks = i + 1;
    recordSend(peer, buf.byteLength);
    if (i % 10 === 0 || i === peer.totalChunks - 1) { updatePeerRow(peer); updateSendProgress(); }
  }
  try { dc.send(JSON.stringify({ kind: "done", fileId })); } catch {}
  peer.pumping = false;
  peer.done = true;
  peer.state = "done";
  updatePeerRow(peer); updateSendProgress(); renderPeerList();
  addHistory("sent", file.name, file.size, null, peer.nick);
  log(`Sent ${file.name} to ${peer.nick} directly (${peer.totalChunks} chunks).`);
  notifyDone(`${file.name} → ${peer.nick}`);
  finishSenderIfAllDone();
}

// v2: striped indexed frames across all open channels. Each channel pulls the
// next index atomically; NACK repairs go first. Per-channel backpressure via
// bufferedamountlow (no polling); aggregate cap across all channels.
async function pumpV2(peer) {
  const file = S.file;
  if (!file || peer.pumping) return;
  const open = () => peer.paths.filter((p) => p && p.dc && p.dc.readyState === "open");
  if (!open().length) return;
  peer.pumping = true;
  peer.state = "sending";
  // Chunk size from negotiated SCTP cap; downgrade only if a send fails.
  try {
    const cap = peer.paths[0] && peer.paths[0].pc && peer.paths[0].pc.sctp
      ? peer.paths[0].pc.sctp.maxMessageSize : 0;
    peer.chunkSize = pickChunkSize(cap);
  } catch { peer.chunkSize = CHUNK_SIZE; }
  const CS = peer.chunkSize;
  peer.totalChunks = Math.max(1, Math.ceil(file.size / CS));
  const fileId = S.fileId;
  const metaDc = open()[0].dc;
  try {
    metaDc.send(JSON.stringify({
      kind: "meta", frame: "v2", fileId, name: file.name, size: file.size,
      mime: file.type || "application/octet-stream",
      totalChunks: peer.totalChunks, chunkSize: CS, nick: myNick(),
    }));
  } catch {
    peer.pumping = false;
    setStatus($("senderStatus"), `${peer.nick}: send failed at handshake. Press Retry.`, "err");
    $("btnRetrySend").hidden = false;
    return;
  }
  await sleep(400); // resume/NACK sweep from receiver may fast-forward us
  if (S.cancelled || peer.left || peer.done) { peer.pumping = false; return; }

  const aggQueued = () => peer.paths.reduce((s, p) => s + (p && p.dc ? p.dc.bufferedAmount : 0), 0);
  const sendOne = async (dc, index) => {
    const slice = file.slice(index * CS, (index + 1) * CS);
    const buf = await slice.arrayBuffer();
    const u = new Uint8Array(buf);
    peer.crcParts[index] = crc32(u);
    try { dc.send(encodeFrame(index, u)); }
    catch (e) {
      // Message-too-large (old SCTP cap): downgrade chunk size once and restart.
      if (CS !== CHUNK_SIZE_LEGACY && /too large|size/i.test(e.message || "")) {
        peer.chunkSize = CHUNK_SIZE_LEGACY;
        peer.totalChunks = Math.max(1, Math.ceil(file.size / CHUNK_SIZE_LEGACY));
        peer.cursor = 0; peer.nackQueue = []; peer.crcParts = [];
        log(`Chunk too large for ${peer.nick} — downgrading to 16 KiB and restarting.`);
        return "downgrade";
      }
      return "closed";
    }
    recordSend(peer, u.length + FRAME_HEADER);
    peer.sentChunks = Math.max(peer.sentChunks, index + 1);
    return "ok";
  };

  let uiTick = 0;
  for (;;) {
    if (S.cancelled || peer.left || peer.done || peer.useRelay) { peer.pumping = false; return; }
    // Priority 1: NACK repairs. Priority 2: fresh cursor.
    let idx = -1, fromNack = false;
    if (peer.nackQueue.length) { idx = peer.nackQueue.shift(); fromNack = true; }
    else if (peer.cursor < peer.totalChunks) { idx = peer.cursor++; }
    else break;
    // Pick the least-buffered open channel (keeps paths balanced).
    let best = null;
    for (const p of open()) {
      if (p.dc.bufferedAmount > CH_HIGH_WATER) continue;
      if (!best || p.dc.bufferedAmount < best.dc.bufferedAmount) best = p;
    }
    if (!best || aggQueued() > AGG_QUEUE_CAP) {
      // All channels full: give the index back and wait for bufferedamountlow.
      if (fromNack) peer.nackQueue.unshift(idx);
      else peer.cursor--;
      const anyAlive = peer.paths.some((p) => p && p.pc && (p.pc.connectionState === "connected" || p.pc.connectionState === "connecting"));
      if (!anyAlive) { peer.pumping = false; peer.state = "connecting"; updatePeerRow(peer); return; }
      await new Promise((res) => setTimeout(res, 120));
      continue;
    }
    const r = await sendOne(best.dc, idx);
    if (r === "downgrade") {
      const mdc = open()[0] && open()[0].dc;
      if (!mdc) { peer.pumping = false; return; }
      try {
        mdc.send(JSON.stringify({
          kind: "meta", frame: "v2", fileId, name: file.name, size: file.size,
          mime: file.type || "application/octet-stream",
          totalChunks: peer.totalChunks, chunkSize: peer.chunkSize, nick: myNick(),
        }));
      } catch { peer.pumping = false; return; }
      await sleep(400); continue;
    }
    if (r === "closed") { peer.nackQueue.unshift(idx); peer.pumping = false; return; }
    if (++uiTick % 8 === 0) { updatePeerRow(peer); updateSendProgress(); }
  }
  updatePeerRow(peer); updateSendProgress();
  // Cursor exhausted: ask receiver to verify. Receiver NACKs gaps or FINs.
  sendV2Done(peer);
  peer.pumping = false;
}

function sendV2Done(peer) {
  if (peer.done || peer.left || S.cancelled) return;
  let crc = 0;
  for (let i = 0; i < peer.totalChunks; i++) crc = crc32Combine(crc, peer.crcParts[i] || 0, i === peer.totalChunks - 1 ? (S.file.size - i * peer.chunkSize) : peer.chunkSize);
  const dc = peer.paths.map((p) => p && p.dc).find((d) => d && d.readyState === "open");
  clearTimeout(peer.finTimer);
  if (!dc) {
    // Nothing open to carry the checksum: retry; receiver NACK-sweeps meanwhile.
    if (peer.finTries < 4) peer.finTimer = setTimeout(() => sendV2Done(peer), 8000);
    return;
  }
  peer.finTries++;
  try { dc.send(JSON.stringify({ kind: "v2done", fileId: S.fileId, total: peer.totalChunks, crc })); } catch {}
  clearTimeout(peer.finTimer);
  if (peer.finTries < 4) peer.finTimer = setTimeout(() => sendV2Done(peer), 8000);
}

function finishSenderIfAllDone() {
  if ([...S.peers.values()].some((p) => !p.done && !p.left)) return;
  S.started = false;
  releaseScreen();
  setStatus($("senderStatus"), "All receivers got the file.", "ok");
}

function updateSendProgress() {
  const peers = [...S.peers.values()].filter((p) => !p.left);
  const avg = peers.length ? peers.reduce((s, p) => s + peerFrac(p), 0) / peers.length : 0;
  const pct = Math.round(avg * 100);
  $("sendBar").style.width = pct + "%";
  $("sendPct").textContent = pct + "%";
  const now = Date.now();
  const elapsed = Math.max(0.1, (now - S.startTime) / 1000);
  const doneBytes = peers.reduce((s, p) => s + peerDoneBytes(p), 0);
  const totalBytes = (S.file ? S.file.size : 0) * Math.max(1, peers.length);
  const live = peers.reduce((s, p) => s + windowedRate(p.stats.events, now, 3000), 0);
  const peak = peers.reduce((s, p) => Math.max(s, p.stats.peak || 0), 0);
  const avgRate = doneBytes / elapsed;
  const active = peers.filter((p) => !p.done);
  const stalledSec = active.length ? Math.max(0, ...active.map((p) => (now - (p.lastProgressT || now)) / 1000)) : 0;
  const state = peers.length && peers.every((p) => p.done) ? "done" : etaState(avg, elapsed, stalledSec);
  const etaTxt = state === "done" ? "done"
    : state === "calculating" ? "Calculating…"
    : state === "stalled" ? `Stalled ${Math.round(stalledSec)}s — check connection`
    : state === "estimating" ? "Estimating…"
    : state === "almost" ? "Almost done"
    : `ETA ${fmtETA(elapsed, avg * 100, 100)}`;
  const who = peers.length > 1 ? `${peers.length} receivers` : (peers[0] ? peers[0].nick : "");
  pushChartSample(live);
  $("sendStats").textContent = `${fmtBytes(doneBytes)} of ${fmtBytes(totalBytes)} · live ${fmtRate(live)} · avg ${fmtRate(avgRate)} · peak ${fmtRate(peak)} (MB = 10⁶ B) · ${etaTxt} · ${who}`;
  for (const p of peers) updatePeerRow(p);
  ensureStatTicker();
  renderDiag();
}

// ---------- sender: relay fallback (server-forwarded chunks) ----------
function bufToBase64(buf) {
  const bytes = new Uint8Array(buf);
  let s = "";
  for (let i = 0; i < bytes.length; i += 8192) s += String.fromCharCode(...bytes.subarray(i, i + 8192));
  return btoa(s);
}

function startRelayPeer(peer) {
  peer.useRelay = true;
  peer.state = "relaying";
  closePeer(peer, false);
  peer.pumping = false; // a running v2 loop observes useRelay and exits
  updatePeerRow(peer); renderPeerList();
  relaySendPeer(peer);
}

async function relaySendPeer(peer) {
  const file = S.file, code = S.code;
  if (!file || !code || peer.left) return;
  if (peer.pumping) return;
  peer.pumping = true;
  const rid = S.fileId + "#relay"; // separate ID so P2P-partial chunks never mix with relay chunks
  const total = Math.max(1, Math.ceil(file.size / RELAY_CHUNK_SIZE));
  peer.totalChunks = total;
  peer.sentChunks = 0; // relay-ack below fast-forwards to receiver's offset if it has any
  peer.startTime = Date.now();
  sendSignal({ type: "relay", code, target: peer.id, payload: {
    kind: "relay-meta", fileId: rid, name: file.name, size: file.size,
    mime: file.type || "application/octet-stream", totalChunks: total, nick: myNick(),
  }});
  // If receiver already has part of this file it will answer relay-ack; give it a beat.
  await sleep(400);
  const gen = peer.relayGen || 0;
  for (let i = peer.sentChunks; i < total; i++) {
    if (gen !== (peer.relayGen || 0)) { peer.pumping = false; return; } // superseded restart
    if (S.cancelled || peer.left) { peer.pumping = false; return; }
    const slice = file.slice(i * RELAY_CHUNK_SIZE, (i + 1) * RELAY_CHUNK_SIZE);
    const buf = await slice.arrayBuffer();
    sendSignal({ type: "relay", code, target: peer.id, payload: {
      kind: "relay-chunk", fileId: rid, index: i, base64: bufToBase64(buf), isLast: i === total - 1,
    }});
    peer.sentChunks = i + 1;
    if (i % 5 === 0 || i === total - 1) { updatePeerRow(peer); updateSendProgress(); }
    await sleep(10); // avoid flooding the socket
  }
  peer.pumping = false;
  peer.done = true;
  peer.state = "done";
  updatePeerRow(peer); updateSendProgress(); renderPeerList();
  addHistory("sent", file.name, file.size, null, peer.nick);
  log(`Relayed ${file.name} to ${peer.nick}.`);
  notifyDone(`${file.name} → ${peer.nick}`);
  finishSenderIfAllDone();
}

// Restart a relay pump after a server pressure-drop. The old loop observes
// the generation bump and exits; the new pump resumes from the receiver's
// relay-ack offset.
async function restartRelayPeer(peer) {
  peer.relayGen = (peer.relayGen || 0) + 1;
  peer.pumping = false;
  await sleep(60);
  if (S.cancelled || peer.left || peer.done || !S.started) return;
  relaySendPeer(peer);
}

// ---------- live throughput chart (measured samples only, bounded) ----------
const chartSamples = []; // B/s, newest last
function pushChartSample(rate) {
  if (!Number.isFinite(rate) || rate < 0) return;
  chartSamples.push(rate);
  if (chartSamples.length > 120) chartSamples.shift();
  drawChart();
}
function drawChart() {
  for (const id of ["speedChart", "recvChart"]) {
    const cv = $(id);
    if (!cv || cv.offsetParent === null) continue;
    const dpr = Math.min(2, window.devicePixelRatio || 1);
    const w = cv.clientWidth || 260, h = cv.clientHeight || 48;
    if (cv.width !== Math.round(w * dpr)) { cv.width = Math.round(w * dpr); cv.height = Math.round(h * dpr); }
    const ctx = cv.getContext("2d");
    ctx.clearRect(0, 0, cv.width, cv.height);
    if (chartSamples.length < 2) continue;
    const max = Math.max(...chartSamples, 1);
    const css = getComputedStyle(document.documentElement);
    ctx.beginPath();
    chartSamples.forEach((v, i) => {
      const x = (i / (chartSamples.length - 1)) * cv.width;
      const y = cv.height - (v / max) * (cv.height - 4 * dpr) - 2 * dpr;
      if (i) ctx.lineTo(x, y);
      else ctx.moveTo(x, y);
    });
    ctx.strokeStyle = (css.getPropertyValue("--brand") || "#2563eb").trim() || "#2563eb";
    ctx.lineWidth = 2 * dpr;
    ctx.stroke();
  }
}
// 1 Hz UI ticker: refreshes stall labels and the chart even when no chunk
// events arrive. Self-stops when nothing is active.
let statTicker = null;
function ensureStatTicker() {
  if (statTicker) return;
  statTicker = setInterval(() => {
    const sActive = S.started && [...S.peers.values()].some((p) => !p.done && !p.left);
    const rActive = R.meta && !R.done;
    if (!sActive && !rActive) { clearInterval(statTicker); statTicker = null; return; }
    if (sActive) {
      // Watchdog: connected + open channel + zero progress for >8 s means the
      // pump died silently — re-kick it instead of sitting at 0% forever.
      for (const p of S.peers.values()) {
        if (p.done || p.left || p.useRelay || p.pumping) continue;
        const open = p.paths.some((x) => x && x.dc && x.dc.readyState === "open");
        const idleMs = Date.now() - (p.lastProgressT || p.startTime || Date.now());
        if (open && idleMs > 8000) {
          log(`Watchdog: ${p.nick} connected but idle — re-kicking pump.`);
          p.lastProgressT = Date.now();
          pumpPeer(p);
        }
      }
      updateSendProgress();
    }
    if (rActive) updateRecvProgress();
    drawChart();
  }, 1000);
}

// ---------- speed tune probe (real disposable transfer) ----------
// Measures end-to-end throughput at 1/2/4 parallel connections using temporary
// probe PeerConnections and disposable bytes the receiver counts and drops.
// Never touches real file state. Saves the winner (≥10% rule) per mode.
let probeRunning = false;
function routeProbeSignal(peerId, p) {
  const ctx = S.probeCtx;
  if (!ctx || ctx.peerId !== peerId) return;
  const ent = ctx.pcs.get(p.conn);
  if (!ent) return;
  (async () => {
    try {
      if (p.kind === "answer") {
        await ent.pc.setRemoteDescription(new RTCSessionDescription(p.sdp));
        for (const c of ent.pendingIce) await ent.pc.addIceCandidate(new RTCIceCandidate(c)).catch(() => {});
        ent.pendingIce = [];
      } else if (p.kind === "ice") {
        if (ent.pc.remoteDescription) await ent.pc.addIceCandidate(new RTCIceCandidate(p.candidate)).catch(() => {});
        else ent.pendingIce.push(p.candidate);
      }
    } catch (e) { log("Probe signal error: " + e.message); }
  })();
}
async function runSpeedTune() {
  if (probeRunning) return;
  const peer = [...S.peers.values()].find((p) => !p.left);
  if (!peer || !S.code) { setStatus($("senderStatus"), "Connect a receiver first, then tune.", "err"); return; }
  if (S.started) { setStatus($("senderStatus"), "Tune when idle (no transfer running).", "err"); return; }
  probeRunning = true;
  const results = [];
  setStatus($("senderStatus"), "Speed tuning… sending disposable probes (1/2/4 connections).");
  log("Speed tune started (disposable bytes, real path).");
  try {
    for (const K of [1, 2, 4]) {
      const r = await probeConns(peer, K).catch((e) => ({ k: K, ok: false, error: e.message }));
      results.push(r);
      const line = r.ok ? `${K} conn: ${fmtRate(r.mbs)} (handshake ${(r.hsMs / 1000).toFixed(1)}s)` : `${K} conn: failed (${r.error || "timeout"})`;
      log("Probe " + line);
      setStatus($("senderStatus"), "Speed tuning… " + line);
      if (!r.ok) break; // larger K won't fix a broken path
    }
    const good = results.filter((r) => r.ok);
    if (!good.length) {
      setStatus($("senderStatus"), "Speed tune failed — no probe completed. Keeping current setting.", "err");
    } else {
      let best = good[0];
      for (const r of good) {
        if (r.mbs > best.mbs * 1.1) best = r; // ≥10% rule
        else break; // evaluated in K order 1,2,4: stop at first non-improvement
      }
      try { localStorage.setItem("p2p-tune", JSON.stringify({ k: best.k, mbs: Math.round(best.mbs), hsMs: Math.round(best.hsMs), when: Date.now(), mode: connMode() })); } catch {}
      syncConnSel();
      setStatus($("senderStatus"), `Tune complete: ${best.k} connection(s) @ ${fmtRate(best.mbs)}. Saved for ${connMode()} mode.`, "ok");
      log(`Speed tune winner: ${best.k} conns @ ${fmtRate(best.mbs)} (results: ${good.map((r) => `${r.k}:${fmtRate(r.mbs)}`).join(", ")}).`);
    }
  } finally {
    probeRunning = false;
    S.probeCtx = null;
  }
}
async function probeConns(peer, K) {
  const session = `pb${Date.now()}k${K}`;
  const pcs = new Map();
  S.probeCtx = { peerId: peer.id, pcs, reports: new Map() };
  const t0 = Date.now();
  const buf = new Uint8Array(65536);
  try { crypto.getRandomValues(buf); } catch { for (let i = 0; i < buf.length; i++) buf[i] = i & 255; }
  const opened = [];
  try {
    for (let i = 0; i < K; i++) {
      const conn = 100 + i;
      const pc = new RTCPeerConnection({ iceServers: buildIceServers(), iceTransportPolicy: connMode() === "turn" ? "relay" : "all" });
      const ent = { pc, dc: null, pendingIce: [], openResolve: null, openPromise: null, reportResolve: null };
      ent.openPromise = new Promise((res) => { ent.openResolve = res; });
      pcs.set(conn, ent);
      pc.onicecandidate = (e) => {
        if (e.candidate) sendSignal({ type: "signal", code: S.code, target: peer.id, payload: { kind: "ice", conn, candidate: e.candidate } });
      };
      const dc = pc.createDataChannel("probe", { ordered: true });
      ent.dc = dc;
      dc.binaryType = "arraybuffer";
      dc.onopen = () => ent.openResolve(true);
      dc.onmessage = (e) => {
        if (typeof e.data !== "string") return;
        try {
          const m = JSON.parse(e.data);
          if (m.kind === "probe-report" && m.session === session && ent.reportResolve) ent.reportResolve(m.bytes || 0);
        } catch {}
      };
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      sendSignal({ type: "signal", code: S.code, target: peer.id, payload: { kind: "offer", conn, sdp: pc.localDescription } });
    }
    // Establishment (12 s budget for all K).
    const hsDeadline = Date.now() + 12000;
    for (const ent of pcs.values()) {
      const left = hsDeadline - Date.now();
      if (left <= 0) throw new Error("handshake timeout");
      const ok = await Promise.race([ent.openPromise, sleep(left).then(() => false)]);
      if (!ok || ent.dc.readyState !== "open") throw new Error("probe channel failed to open");
      opened.push(ent);
    }
    const hsMs = Date.now() - t0;
    // Steady-state window: blast disposable bytes ~2.5 s with backpressure.
    const t1 = Date.now();
    for (const ent of opened) {
      try { ent.dc.send(JSON.stringify({ kind: "probe-start", session })); } catch {}
    }
    const end = t1 + 2500;
    let guard = 0;
    while (Date.now() < end && guard++ < 100000) {
      let sent = false;
      for (const ent of opened) {
        if (ent.dc.readyState !== "open" || ent.dc.bufferedAmount > CH_HIGH_WATER) continue;
        try { ent.dc.send(buf); sent = true; } catch {}
      }
      if (!sent) await new Promise((r) => setTimeout(r, 30));
    }
    for (const ent of opened) {
      try { ent.dc.send(JSON.stringify({ kind: "probe-end", session })); } catch {}
    }
    // Collect reports (5 s).
    let bytes = 0;
    for (const ent of opened) {
      const rep = await Promise.race([
        new Promise((res) => { ent.reportResolve = res; }),
        sleep(5000).then(() => 0),
      ]);
      bytes += rep || 0;
    }
    const secs = Math.max(0.1, (Date.now() - t1) / 1000);
    return { k: K, ok: true, mbs: bytes / secs, hsMs };
  } finally {
    for (const ent of pcs.values()) {
      try { ent.dc && ent.dc.close(); } catch {}
      try { ent.pc && ent.pc.close(); } catch {}
    }
  }
}

// ================= RECEIVER UI =================
$("btnJoin").onclick = async () => {
  const code = $("codeInput").value.trim();
  if (!/^\d{6}$/.test(code)) { setStatus($("receiverStatus"), "Enter the 6-digit code.", "err"); return; }
  R.code = code;
  resetReceiverFile();
  try {
    await ensureSocket();
    sendSignal({ type: "join", code, nick: myNick() });
    setStatus($("receiverStatus"), `Joining ${code}…`);
  } catch (err) {
    setStatus($("receiverStatus"), err.message, "err");
  }
};

$("btnLeave").onclick = async () => {
  try { await sendSignal({ type: "leave" }); } catch {}
  closeReceiverPeer();
  setStatus($("receiverStatus"), "Disconnected.");
  $("btnLeave").hidden = true;
};

// Receiver Cancel: stop the incoming transfer and tell the sender to stop too.
$("btnCancelRecv").onclick = async () => {
  try { const dc = openRecvDc(); dc && dc.send(JSON.stringify({ kind: "cancel-request" })); } catch {}
  if (R.code) { try { await sendSignal({ type: "relay", code: R.code, payload: { kind: "cancel-request", fileId: R.fileId } }); } catch {} }
  closeReceiverPeer(false);
  resetReceiverFile();
  setStatus($("receiverStatus"), "Cancelled — cleared.", "err");
  log("Receive cancelled and cleared.");
};

function resetReceiverFile() {
  if (R.v2) clearTimeout(R.unverT);
  R.v2 = null; R.unverT = null; R.integrity = null; R.events = [];
  R.meta = null; R.chunks = []; R.received = 0; R.totalChunks = 0;
  R.done = false; R.relayMode = false; R.fileId = null;
  R.startTime = Date.now();
  $("recvProgressWrap").hidden = true;
  $("downloadLink").hidden = true;
  $("recvBar").style.width = "0%";
  $("recvStats").textContent = "";
  $("recvLabel").textContent = "Receiving…";
  releaseScreen();
}

async function answerProbePath(ci, p) {
  let rp = R.probePaths[ci];
  if (!rp) {
    const pc = new RTCPeerConnection({ iceServers: buildIceServers(), iceTransportPolicy: connMode() === "turn" ? "relay" : "all" });
    rp = { pc, pendingIce: [] };
    R.probePaths[ci] = rp;
    pc.onicecandidate = (e) => {
      if (e.candidate && R.code) sendSignal({ type: "signal", code: R.code, payload: { kind: "ice", conn: ci, candidate: e.candidate } });
    };
    pc.ondatachannel = (e) => { e.channel.binaryType = "arraybuffer"; e.channel.onmessage = onProbeMessage; };
  }
  if (p.kind === "offer") {
    await rp.pc.setRemoteDescription(new RTCSessionDescription(p.sdp));
    for (const c of rp.pendingIce) await rp.pc.addIceCandidate(new RTCIceCandidate(c)).catch(() => {});
    rp.pendingIce = [];
    const answer = await rp.pc.createAnswer();
    await rp.pc.setLocalDescription(answer);
    sendSignal({ type: "signal", code: R.code, payload: { kind: "answer", conn: ci, sdp: rp.pc.localDescription } });
  } else if (p.kind === "ice") {
    if (rp.pc.remoteDescription) await rp.pc.addIceCandidate(new RTCIceCandidate(p.candidate)).catch(() => {});
    else rp.pendingIce.push(p.candidate);
  }
}

function makeRecvPath(idx) {
  let rp = R.paths[idx];
  if (rp && rp.pc) return rp;
  const pc = new RTCPeerConnection({ iceServers: buildIceServers(), iceTransportPolicy: connMode() === "turn" ? "relay" : "all", iceCandidatePoolSize: 2, bundlePolicy: "max-bundle" });
  rp = { pc, dc: null, idx, pendingIce: [] };
  R.paths[idx] = rp;
  if (idx === 0) { R.pc = pc; R.pendingIce = rp.pendingIce; }
  pc.onicecandidate = (e) => {
    if (e.candidate && R.code) sendSignal({ type: "signal", code: R.code, payload: { kind: "ice", conn: idx, candidate: e.candidate } });
  };
  pc.onconnectionstatechange = () => {
    if (idx > 0) return;
    log(`Receiver connection: ${pc.connectionState}`);
    if (pc.connectionState === "connected") { setStatus($("receiverStatus"), "Connected. Receiving…", "ok"); logPath(pc, "Receiver"); }
    if (pc.connectionState === "failed") setStatus($("receiverStatus"), "Direct path failed — Auto mode-la sender relay-ku maridum; Direct mode-la same WiFi must.", "err");
  };
  pc.ondatachannel = (e) => {
    const dc = e.channel;
    dc.binaryType = "arraybuffer";
    if (dc.label === "probe") {
      log("Probe channel received (speed tune).");
      dc.onmessage = onProbeMessage;
      return;
    }
    if (idx === 0 && !R.dc) R.dc = dc;
    log(idx === 0 ? "Data channel received." : `Data channel ${idx + 1} received.`);
    dc.onmessage = onFileMessage(dc);
    dc.onclose = () => {
      if (!R.done && R.meta && R.received < R.totalChunks) {
        setStatus($("receiverStatus"), `Interrupted at ${Math.round(R.received / R.totalChunks * 100)}%. Keep this tab open — pressing Retry on the sender resumes.`, "err");
      }
    };
  };
  return rp;
}

function makeReceiverPC() {
  closeReceiverPeer(false);
  return makeRecvPath(0).pc;
}

// Disposable speed-tune traffic: counted per session, never touches file state.
// Ordered channel: probe-start names the session, binary follows, probe-end
// reports the counted bytes back on the same channel.
function onProbeMessage(e) {
  const dc = e.target;
  if (typeof e.data === "string") {
    try {
      const m = JSON.parse(e.data);
      if (m.kind === "probe-start" && m.session) {
        dc._probeSession = m.session;
        R.probeCount[m.session] = 0;
      } else if (m.kind === "probe-end" && m.session) {
        const bytes = R.probeCount[m.session] || 0;
        delete R.probeCount[m.session];
        if (dc._probeSession === m.session) dc._probeSession = null;
        try { dc.send(JSON.stringify({ kind: "probe-report", session: m.session, bytes })); } catch {}
      }
    } catch {}
    return;
  }
  const key = dc && dc._probeSession;
  if (key) R.probeCount[key] = (R.probeCount[key] || 0) + (e.data.byteLength || 0);
}

function openRecvDc() {
  for (const rp of R.paths) {
    if (rp && rp.dc && rp.dc.readyState === "open") return rp.dc;
  }
  return R.dc && R.dc.readyState === "open" ? R.dc : null;
}

function onFileMetaV2(m, dc) {
  if (R.v2) clearTimeout(R.unverT);
  const sameFile = R.v2 && R.fileId === m.fileId && R.v2.chunkSize === m.chunkSize;
  R.fileId = m.fileId;
  R.relayMode = false;
  if (!sameFile) {
    R.v2 = { total: m.totalChunks, size: m.size, chunkSize: m.chunkSize,
      chunks: new Array(m.totalChunks).fill(null), crcParts: new Array(m.totalChunks).fill(0),
      have: 0, startTime: Date.now(), expectCrc: undefined, bad: 0 };
    R.meta = m; R.totalChunks = m.totalChunks; R.chunks = R.v2.chunks;
    R.received = 0; R.startTime = Date.now(); R.events = [];
  }
  R.done = false;
  R.integrity = null;
  R.lastRecvT = Date.now();
  $("recvProgressWrap").hidden = false;
  $("btnCancelRecv").hidden = false;
  if (m.nick) R.senderNick = m.nick;
  $("recvLabel").textContent = `Receiving ${m.name}…`;
  setStatus($("receiverStatus"), `Receiving ${m.name} (${fmtBytes(m.size)}) from ${R.senderNick || "sender"}…`, "ok");
  lockScreen();
  log(`Receiving ${m.name} — ${m.totalChunks} striped chunks${R.v2.have ? ` (have ${R.v2.have})` : ""}.`);
  try { dc.send(JSON.stringify({ kind: "resume", fileId: m.fileId, received: R.v2.have, frame: "v2" })); } catch {}
  sendNackSweep(true);
  R.ackT = Date.now();
  updateRecvProgress();
}

function sendAck2() {
  const v = R.v2;
  if (!v || !R.fileId) return;
  let contig = 0;
  while (contig < v.total && v.chunks[contig]) contig++;
  const dc = openRecvDc();
  if (dc) { try { dc.send(JSON.stringify({ kind: "ack2", fileId: R.fileId, received: v.have, contig })); } catch {} }
}

function sendNackSweep(force) {
  const v = R.v2;
  if (!v || !R.fileId || v.have >= v.total) return;
  const now = Date.now();
  if (!force && now - R.nackT < 2000) return;
  R.nackT = now;
  const missing = [];
  for (let i = 0; i < v.total && missing.length < 1000; i++) {
    if (!v.chunks[i]) missing.push(i);
  }
  if (!missing.length) return;
  const dc = openRecvDc();
  if (dc) { try { dc.send(JSON.stringify({ kind: "nack", fileId: R.fileId, missing })); } catch {} }
}

function onFileBinaryV2(data) {
  const v = R.v2;
  if (!v) return;
  const d = decodeFrame(data);
  if (d.error || d.index >= v.total) { v.bad++; return; }
  if (v.chunks[d.index]) return; // duplicate retransmit
  const expectLen = Math.min(v.chunkSize, v.size - d.index * v.chunkSize);
  if (d.payload.byteLength !== expectLen) { v.bad++; return; } // will be NACK-repaired
  v.chunks[d.index] = d.payload;
  v.crcParts[d.index] = crc32(new Uint8Array(d.payload));
  v.have++;
  R.received = v.have;
  R.lastRecvT = Date.now();
  R.events.push({ t: Date.now(), bytes: d.payload.byteLength });
  if (R.events.length > 120) R.events.splice(0, R.events.length - 120);
  const now = Date.now();
  if (v.have % 32 === 0 || now - R.ackT > 500) { R.ackT = now; sendAck2(); updateRecvProgress(); }
  if (v.have >= v.total) checkV2Complete(false);
}

function checkV2Complete(fromDone) {
  const v = R.v2;
  if (!v || R.done) return;
  if (v.have < v.total) { sendNackSweep(false); return; }
  if (v.expectCrc === undefined) {
    // All bytes present and every chunk passed its CRC, but the sender's
    // whole-file checksum hasn't arrived yet. Wait for it (v2done retries);
    // after 20 s finish labeled UNCONFIRMED rather than hanging forever.
    if (!R.unverT) R.unverT = setTimeout(() => finishReceiveVerdict("unconfirmed"), 20000);
    return;
  }
  let acc = 0;
  for (let i = 0; i < v.total; i++) {
    acc = crc32Combine(acc, v.crcParts[i] || 0, Math.min(v.chunkSize, v.size - i * v.chunkSize));
  }
  clearTimeout(R.unverT); R.unverT = null;
  const ok = acc === v.expectCrc;
  try { openRecvDc().send(JSON.stringify({ kind: "fin", fileId: R.fileId, ok })); } catch {}
  if (ok) finishReceiveVerdict("verified");
  else {
    R.integrity = "failed";
    setStatus($("receiverStatus"), `Integrity check FAILED for ${R.meta.name} — file NOT saved as complete. Ask sender to retry.`, "err");
    log(`Integrity FAILURE on ${R.meta.name} (combined CRC mismatch) — not completing.`);
  }
  void fromDone;
}

function finishReceiveVerdict(kind) {
  if (R.done) return;
  R.integrity = kind; // 'verified' | 'unconfirmed'
  clearTimeout(R.unverT); R.unverT = null;
  finishReceive();
}

function onFileMessage(dc) {
  return (e) => {
    if (typeof e.data === "string") {
      let m;
      try { m = JSON.parse(e.data); } catch { return; }
      if (m.kind === "meta") {
        if (m.frame === "v2") { onFileMetaV2(m, dc); return; }
        if (R.v2) { clearTimeout(R.unverT); R.v2 = null; }
        const sameFile = R.fileId === m.fileId && R.meta;
        R.fileId = m.fileId;
        if (!sameFile) { R.meta = m; R.totalChunks = m.totalChunks; R.chunks = new Array(m.totalChunks); R.received = 0; R.startTime = Date.now(); R.events = []; }
        R.done = false;
        R.integrity = null;
        R.lastRecvT = Date.now();
        $("recvProgressWrap").hidden = false;
        $("btnCancelRecv").hidden = false;
        if (m.nick) R.senderNick = m.nick;
        $("recvLabel").textContent = `Receiving ${m.name}…`;
        setStatus($("receiverStatus"), `Receiving ${m.name} (${fmtBytes(m.size)}) from ${R.senderNick || "sender"}…`, "ok");
        lockScreen();
        log(`Receiving ${m.name} — ${m.totalChunks} chunks${R.received ? ` (resuming at ${R.received})` : ""}.`);
        // Tell sender where to start (resume support).
        dc.send(JSON.stringify({ kind: "resume", fileId: m.fileId, received: R.received }));
        updateRecvProgress();
      } else if (m.kind === "text") {
        addMessage(m.text || "", false);
      } else if (m.kind === "v2done") {
        const v = R.v2;
        if (v && m.fileId === R.fileId && Number.isInteger(m.total) && m.total === v.total && Number.isInteger(m.crc)) {
          v.expectCrc = m.crc >>> 0;
          clearTimeout(R.unverT); R.unverT = null;
          checkV2Complete(true);
        }
      } else if (m.kind === "done") {
        finishReceive();
      } else if (m.kind === "cancel") {
        resetReceiverFile();
        setStatus($("receiverStatus"), "Sender cancelled — cleared.", "err");
      }
      return;
    }
    if (R.v2 && R.fileId) { onFileBinaryV2(e.data); return; }
    // v1 legacy: chunks arrive in order on an ordered channel.
    if (!R.meta) return;
    R.chunks[R.received] = e.data;
    R.received++;
    R.lastRecvT = Date.now();
    R.events.push({ t: Date.now(), bytes: e.data.byteLength || 0 });
    if (R.events.length > 120) R.events.splice(0, R.events.length - 120);
    if (R.received % 10 === 0 || R.received === R.totalChunks) {
      updateRecvProgress();
      if (R.received % 20 === 0) { try { dc.send(JSON.stringify({ kind: "ack", received: R.received })); } catch {} }
    }
    if (R.received >= R.totalChunks) finishReceive();
  };
}

function updateRecvProgress() {
  const total = R.totalChunks || 1;
  const pct = Math.round((R.received / total) * 100);
  $("recvBar").style.width = pct + "%";
  $("recvPct").textContent = pct + "%";
  const now = Date.now();
  R.events = R.events.filter((e) => now - e.t < 10000);
  const live = windowedRate(R.events, now, 3000);
  const elapsed = Math.max(0.1, (now - R.startTime) / 1000);
  const bytes = R.meta ? Math.min(R.meta.size, R.received * (R.meta.chunkSize || CHUNK_SIZE_LEGACY)) : 0;
  const totalB = R.meta ? R.meta.size : 0;
  let extra = "";
  if (R.meta && !R.done && R.received > 0 && now - (R.lastRecvT || now) > 5000) {
    extra = ` · stalled ${Math.round((now - R.lastRecvT) / 1000)}s — keeping connection open`;
  }
  $("recvStats").textContent = `${fmtBytes(bytes)} of ${fmtBytes(totalB)} · live ${fmtRate(live)} (MB = 10⁶ B) · ETA ${fmtETA(elapsed, R.received, total)} · chunk ${R.received}/${total}${extra}`;
  pushChartSample(live);
  ensureStatTicker();
  renderDiag();
}

function finishReceive() {
  if (R.done) return;
  if (R.received < R.totalChunks) return; // wait for remaining chunks
  R.done = true;
  const blob = new Blob(R.chunks, { type: (R.meta && R.meta.mime) || "application/octet-stream" });
  const url = URL.createObjectURL(blob);
  const a = $("downloadLink");
  a.href = url;
  a.download = (R.meta && R.meta.name) || "download";
  a.hidden = false;
  a.textContent = `Download ${(R.meta && R.meta.name) || "file"} (${fmtBytes(blob.size)})`;
  $("btnCancelRecv").hidden = true;
  updateRecvProgress();
  addHistory("received", R.meta.name, blob.size, url, R.senderNick);
  const integ = R.integrity === "verified" ? " Integrity verified (CRC32)."
    : R.integrity === "unconfirmed" ? " Checksum unconfirmed by sender (per-chunk CRCs all valid)."
    : "";
  setStatus($("receiverStatus"), `Received ${R.meta.name} (${fmtBytes(blob.size)}) from ${R.senderNick || "sender"}.${integ} Tap Download to save it.`, "ok");
  log(`Transfer complete: ${R.meta.name}.${integ}`);
  notifyDone(R.meta.name);
  releaseScreen();
  try { a.click(); } catch {} // best-effort auto-download; button remains regardless
}

function closeReceiverPeer(logIt = true) {
  clearTimeout(R.unverT);
  for (const rp of R.paths) {
    try { rp.dc && rp.dc.close(); } catch {}
    try { rp.pc && rp.pc.close(); } catch {}
  }
  R.paths = [];
  for (const k of Object.keys(R.probePaths || {})) {
    try { R.probePaths[k].pc && R.probePaths[k].pc.close(); } catch {}
    delete R.probePaths[k];
  }
  try { R.pc && R.pc.close(); } catch {}
  R.pc = null;
  R.dc = null;
  releaseScreen();
  if (logIt) log("Receiver peer connection closed.");
}

// ---------- relay receive path ----------
function base64ToBuf(b64) {
  const s = atob(b64);
  const b = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) b[i] = s.charCodeAt(i);
  return b.buffer;
}

async function onRelayPayload(p, from) {
  if (p.kind === "relay-meta") {
    const sameFile = R.fileId === p.fileId && R.meta;
    R.fileId = p.fileId;
    R.relayMode = true;
    if (!sameFile) { R.meta = p; R.totalChunks = p.totalChunks; R.chunks = new Array(p.totalChunks); R.received = 0; R.startTime = Date.now(); }
    p.chunkSize = RELAY_CHUNK_SIZE; // correct progress math for 32 KB relay chunks
    R.done = false;
    $("recvProgressWrap").hidden = false;
    $("btnCancelRecv").hidden = false;
    if (p.nick) R.senderNick = p.nick;
    $("recvLabel").textContent = `Receiving ${p.name} (relay)…`;
    setStatus($("receiverStatus"), `Receiving ${p.name} from ${R.senderNick || "sender"} via relay…`, "ok");
    log(`Relay receive: ${p.name}, ${p.totalChunks} chunks.`);
    lockScreen();
    // Tell sender our offset so relay can resume.
    if (R.code) sendSignal({ type: "relay", code: R.code, payload: { kind: "relay-ack", fileId: p.fileId, received: R.received } });
    updateRecvProgress();
  } else if (p.kind === "relay-chunk") {
    if (p.fileId !== R.fileId || !R.meta) return;
    if (!R.chunks[p.index]) { R.chunks[p.index] = base64ToBuf(p.base64); }
    // received = highest contiguous? simpler: count filled slots
    let n = 0;
    for (let i = 0; i < R.totalChunks; i++) { if (R.chunks[i]) n++; else break; }
    // For out-of-order tolerance count all filled:
    if (n !== R.received) { /* contiguous */ }
    R.received = R.chunks.filter(Boolean).length;
    updateRecvProgress();
    if (p.isLast || R.received >= R.totalChunks) finishReceive();
  } else if (p.kind === "cancel-request") {
    const peer = S.peers.get(from);
    if (peer) { stopPeer(peer, "cancelled"); setStatus($("senderStatus"), `${peer.nick} cancelled. Others continue.`, "err"); }
  } else if (p.kind === "relay-ack" && S.started) {
    // Sender side: that receiver reports its offset.
    const peer = S.peers.get(from);
    if (peer && peer.useRelay && S.fileId && p.fileId === S.fileId + "#relay" && (p.received || 0) > peer.sentChunks) {
      peer.sentChunks = p.received;
      log(`Relay to ${peer.nick} resuming from chunk ${p.received}.`);
    }
  } else if (p.kind === "relay-text") {
    addMessage(p.text || "", false);
  } else if (p.kind === "cancel") {
    resetReceiverFile();
    setStatus($("receiverStatus"), "Sender cancelled — cleared.", "err");
  }
}

// ---------- inbound signaling dispatch ----------
async function onSignalMessage(ev) {
  let m;
  try { m = JSON.parse(ev.data); } catch { return; }

  if (m.type === "created") {
    showShare(m.code);
    setStatus($("senderStatus"), "Code ready. Waiting for receiver…");
    $("btnCreate").disabled = false;
    return;
  }
  if (m.type === "joined") {
    R.code = m.code;
    R.peerId = m.peerId || null;
    R.senderNick = m.senderNick || "Sender";
    setStatus($("receiverStatus"), `Joined ${m.code}. Sender: ${R.senderNick}. Waiting for file…`, "ok");
    $("btnLeave").hidden = false;
    makeReceiverPC(); // ready before offer arrives; queued ICE handled below
    // Advertise striped-v2 capability; legacy senders ignore unknown kinds.
    sendSignal({ type: "signal", code: R.code, payload: { kind: "hello", frame: "v2" } });
    log(`Joined room ${m.code} as ${myNick()} (${m.count}/${m.max} receivers). Sender: ${R.senderNick}.`);
    setStatus($("textStatus"), `Connected to ${R.senderNick} — you can send text here.`, "ok");
    return;
  }
  if (m.type === "peer-joined") {
    const peer = addPeer(m.peerId, m.nick);
    log(`${peer.nick} joined (${m.count}/${m.max}).`);
    setStatus($("senderStatus"), `${m.count} receiver(s) connected. Negotiating…`, "ok");
    setStatus($("textStatus"), `${peer.nick} connected — you can send text here.`, "ok");
    $("btnSendStart").disabled = false;
    $("btnRelay").hidden = true;
    // v2-capable receivers announce themselves with {kind:'hello', frame:'v2'}.
    // Wait briefly; legacy receivers stay silent and get the v1 single path.
    clearTimeout(peer.helloTimer);
    peer.helloTimer = setTimeout(() => {
      if (!peer.pathsBuilt && !peer.left) { buildPaths(peer, peer.helloV2); maybeAutoStart(); }
    }, HELLO_WAIT_MS);
    return;
  }
  if (m.type === "signal") {
    const p = m.payload || {};
    if (m.from === "sender" || S.code == null) {
      // Receiver side: handle offer/ice from sender (conn index for parallel paths).
      const ci = p.conn || 0;
      if (ci >= 100) {
        // Probe connection (speed tune): isolated path, disposable bytes.
        try { await answerProbePath(ci, p); }
        catch (err) { log("Probe answer error: " + err.message); }
        return;
      }
      const rp = makeRecvPath(ci);
      try {
        if (p.kind === "offer") {
          await rp.pc.setRemoteDescription(new RTCSessionDescription(p.sdp));
          for (const c of rp.pendingIce) await rp.pc.addIceCandidate(new RTCIceCandidate(c)).catch(() => {});
          rp.pendingIce = [];
          const answer = await rp.pc.createAnswer();
          await rp.pc.setLocalDescription(answer);
          sendSignal({ type: "signal", code: R.code, payload: { kind: "answer", conn: ci, sdp: rp.pc.localDescription } });
          log(ci === 0 ? "Answer sent." : `Answer sent (path ${ci + 1}).`);
        } else if (p.kind === "ice") {
          if (rp.pc.remoteDescription) await rp.pc.addIceCandidate(new RTCIceCandidate(p.candidate)).catch(() => {});
          else rp.pendingIce.push(p.candidate);
        }
      } catch (err) { log("Receiver WebRTC error: " + err.message); }
    } else {
      if ((p.conn || 0) >= 100) { routeProbeSignal(m.from, p); return; }
      // Sender side: route answer/ice/hello to the right receiver (and path).
      const peer = S.peers.get(m.from);
      if (!peer) return;
      if (p.kind === "hello") {
        if (peer.pathsBuilt || peer.left) return; // late hello: stay on current paths
        peer.helloV2 = true;
        clearTimeout(peer.helloTimer);
        log(`${peer.nick} supports striped v2 transfer.`);
        buildPaths(peer, true);
        maybeAutoStart();
        // If a transfer is already running, fresh channels pump via onopen.
        return;
      }
      const path = peer.paths[p.conn || 0];
      if (!path || !path.pc) return;
      try {
        if (p.kind === "answer") {
          await path.pc.setRemoteDescription(new RTCSessionDescription(p.sdp));
          for (const c of path.pendingIce) await path.pc.addIceCandidate(new RTCIceCandidate(c)).catch(() => {});
          path.pendingIce = [];
          if ((p.conn || 0) === 0) log(`P2P handshake with ${peer.nick} complete.`);
        } else if (p.kind === "ice") {
          if (path.pc.remoteDescription) await path.pc.addIceCandidate(new RTCIceCandidate(p.candidate)).catch(() => {});
          else path.pendingIce.push(p.candidate);
        }
      } catch (err) { log(`Sender WebRTC error (${peer.nick}): ` + err.message); }
    }
    return;
  }
  if (m.type === "relay") {
    if (m.target && m.target !== R.peerId) return; // addressed to another receiver
    onRelayPayload(m.payload || {}, m.from);
    return;
  }
  if (m.type === "nick") {
    if (m.from === "sender") {
      R.senderNick = m.nick || "Sender";
      log(`Sender is now known as ${R.senderNick}.`);
      if (R.code) setStatus($("receiverStatus"), `Connected to ${R.senderNick}.`, "ok");
    } else if (m.from) {
      const peer = S.peers.get(m.from);
      if (peer) { peer.nick = m.nick || peer.nick; renderPeerList(); log(`${peer.nick} updated their name.`); }
    }
    return;
  }
  if (m.type === "pressure") {
    // Server dropped a relay-chunk for a congested destination. Restart the
    // affected relay pumps; receivers dedupe by index and relay-ack resumes
    // from their offset, so no bytes are lost.
    if (S.fileId && m.fileId === S.fileId + "#relay") {
      for (const peer of S.peers.values()) {
        if (peer.useRelay && !peer.done && !peer.left && S.started && !S.cancelled) {
          log(`Congestion on relay to ${peer.nick} — restarting that pump (resumes).`);
          restartRelayPeer(peer);
        }
      }
    } else {
      log(`Server pressure notice (chunk ${m.index}).`);
    }
    return;
  }
  if (m.type === "peer-left") {
    if (m.role === "sender") {
      log("Sender left — transfer over.");
      closeReceiverPeer();
      setStatus($("receiverStatus"), "Sender left. Ask for a new code to try again.", "err");
      setStatus($("textStatus"), "Sender left.", "err");
    } else {
      const peer = S.peers.get(m.peerId);
      if (peer) stopPeer(peer, "left");
      updatePeerCount();
      const left = [...S.peers.values()].filter((p) => !p.left).length;
      log(`A receiver left (${left} remaining).`);
      if (S.code) setStatus($("senderStatus"), `${left} receiver(s) connected.`, left ? "" : "err");
    }
    return;
  }
  if (m.type === "expired") {
    log("Code expired — generate a new one.");
    if (S.code === m.code) {
      S.code = null;
      setStatus($("senderStatus"), "Code expired. Click “Get QR & code” again.", "err");
      $("btnCreate").disabled = false;
    }
    return;
  }
  if (m.type === "waiting") {
    log(m.message || "Waiting for peer…");
    return;
  }
  if (m.type === "error") {
    log("Server: " + m.message);
    if (!paneSend.hidden) setStatus($("senderStatus"), m.message, "err");
    else setStatus($("receiverStatus"), m.message, "err");
    $("btnCreate").disabled = S.file ? false : true;
    return;
  }
  if (m.type === "left") { log("Left room."); return; }
}

// ---------- QR camera scanner (receiver) ----------
let qrScanner = null;
let scanning = false;

function extractCode(text) {
  if (!text) return null;
  try {
    // Full QR link like https://host/?code=482913
    const u = new URL(text, location.href);
    const c = (u.searchParams.get("code") || "").trim();
    if (/^\d{6}$/.test(c)) return c;
  } catch {}
  const m = String(text).match(/(\d{6})/);
  return m ? m[1] : null;
}

function handleScannedText(text) {
  const code = extractCode(text);
  if (!code) {
    log("QR scanned but no 6-digit code found in it.");
    return;
  }
  log(`QR scanned: code ${code}.`);
  stopScan().finally(() => {
    showTab("receive");
    $("codeInput").value = code;
    $("btnJoin").click();
  });
}

async function startScan() {
  if (scanning) return;
  if (typeof Html5Qrcode === "undefined") {
    setStatus($("receiverStatus"), "Scanner library not loaded (offline?). Type the code manually or use Upload QR image.", "err");
    log("Html5Qrcode CDN missing — manual code entry fallback.");
    return;
  }
  scanning = true;
  $("scanWrap").hidden = false;
  $("btnScan").disabled = true;
  setStatus($("receiverStatus"), "Camera starting… point at the sender's QR.");
  try {
    qrScanner = new Html5Qrcode("qrReader");
    await qrScanner.start(
      { facingMode: "environment" },
      { fps: 10, qrbox: { width: 250, height: 250 } },
      (decoded) => handleScannedText(decoded),
      () => {}
    );
    log("Camera scanner started.");
  } catch (err) {
    scanning = false;
    $("btnScan").disabled = false;
    const msg = /secure|https|permission|denied/i.test(err.message || "")
      ? "Camera blocked: use HTTPS (or localhost) and Allow camera permission."
      : "Camera start failed: " + (err.message || err);
    setStatus($("receiverStatus"), msg + " You can type the code manually.", "err");
    log(msg);
  }
}

async function stopScan() {
  try {
    if (qrScanner) {
      try { await qrScanner.stop(); } catch {}
      try { qrScanner.clear(); } catch {}
    }
  } finally {
    qrScanner = null;
    scanning = false;
    $("scanWrap").hidden = true;
    $("btnScan").disabled = false;
  }
}

$("btnScan").onclick = startScan;
$("btnStopScan").onclick = async () => {
  await stopScan();
  setStatus($("receiverStatus"), "Camera stopped. Type the code or scan again.");
};

// Fallback: scan a QR image file (desktop without camera).
$("qrFile").addEventListener("change", async () => {
  const f = $("qrFile").files && $("qrFile").files[0];
  if (!f) return;
  if (typeof Html5Qrcode === "undefined") {
    setStatus($("receiverStatus"), "Scanner library not loaded. Type the code manually.", "err");
    return;
  }
  try {
    const tmp = new Html5Qrcode("qrReader");
    const result = await tmp.scanFile(f, true);
    handleScannedText(result);
    try { tmp.clear(); } catch {}
  } catch {
    setStatus($("receiverStatus"), "Could not read QR from that image. Try a clearer photo or type the code.", "err");
  }
  $("qrFile").value = "";
});

// ---------- instant text share (same connection, either direction) ----------
function activeCode() { return S.code || R.code; }
function openDCs() {
  const out = [];
  for (const peer of S.peers.values()) {
    if (!peer.left && !peer.done && peer.dc && peer.dc.readyState === "open") out.push(peer.dc);
  }
  for (const rp of R.paths) {
    if (rp && rp.dc && rp.dc.readyState === "open") out.push(rp.dc);
  }
  if (!out.length && R.dc && R.dc.readyState === "open") out.push(R.dc);
  return out;
}
function addMessage(text, mine) {
  if (!text) return;
  const wrap = $("msgList");
  wrap.hidden = false;
  const div = document.createElement("div");
  div.className = "msg" + (mine ? " mine" : "");
  const span = document.createElement("span");
  span.textContent = (mine ? "You: " : "Peer: ") + text;
  const btn = document.createElement("button");
  btn.className = "btn ghost small-btn";
  btn.textContent = "Copy";
  btn.onclick = async () => {
    try { await navigator.clipboard.writeText(text); btn.textContent = "Copied"; }
    catch { btn.textContent = "Copy failed"; }
  };
  div.append(span, btn);
  wrap.prepend(div);
  if (!mine) notifyDone("text message");
}
$("btnTextSend").onclick = async () => {
  const text = $("textInput").value.trim();
  if (!text) { setStatus($("textStatus"), "Type something first.", "err"); return; }
  if (text.length > 20000) { setStatus($("textStatus"), "Text too long (20,000 chars max).", "err"); return; }
  try {
    const dcs = openDCs();
    if (dcs.length) dcs.forEach((dc) => { try { dc.send(JSON.stringify({ kind: "text", text })); } catch {} });
    else if (activeCode()) await sendSignal({ type: "relay", code: activeCode(), payload: { kind: "relay-text", text } });
    else { setStatus($("textStatus"), "Connect first: sender — Get QR & code; receiver — enter code.", "err"); return; }
    addMessage(text, true);
    $("textInput").value = "";
    setStatus($("textStatus"), "Sent.", "ok");
  } catch (err) {
    setStatus($("textStatus"), "Send failed: " + err.message, "err");
  }
};

// ---------- connection mode + TURN settings UI ----------
(function initConnSettings() {
  const mode = connMode();
  document.querySelectorAll('input[name="cmode"]').forEach((r) => { r.checked = r.value === mode; });
  document.querySelectorAll('input[name="cmode"]').forEach((r) => r.addEventListener("change", () => {
    localStorage.setItem("p2p-mode", r.value);
    log(`Connection mode: ${r.value}. Next transfer uses it (TURN mode = media via TURN).`);
  }));
})();

// ---------- diagnostics panel (advanced, collapsed by default) ----------
let lastDiagT = 0;
function diagRow(box, k, v) {
  const div = document.createElement("div");
  div.className = "diag-row";
  const a = document.createElement("span");
  a.className = "dk"; a.textContent = k;
  const b = document.createElement("span");
  b.className = "dv"; b.textContent = v;
  div.append(a, b);
  box.appendChild(div);
}
function renderDiag() {
  const now = Date.now();
  if (now - lastDiagT < 1000) return;
  lastDiagT = now;
  const sendOpen = $("sendDiag") && $("sendDiag").open;
  const recvOpen = $("recvDiag") && $("recvDiag").open;
  if (sendOpen && $("diagBody")) {
    const box = $("diagBody");
    box.innerHTML = "";
    if (!S.peers.size) box.textContent = "No receivers yet.";
    for (const peer of S.peers.values()) {
      const open = peer.paths.filter((p) => p && p.dc && p.dc.readyState === "open").length;
      diagRow(box, peer.nick + " · paths", `${open}/${peer.paths.length || 0} open`);
      diagRow(box, peer.nick + " · framing", peer.frame === "v2" ? `striped 64 KiB (chunk ${peer.chunkSize / 1024} KiB)` : "legacy ordered 16 KiB");
      diagRow(box, peer.nick + " · route", `${peer.diag.path} / ${peer.diag.transport}${peer.diag.turnHost ? " via " + peer.diag.turnHost : ""}`);
      diagRow(box, peer.nick + " · RTT", peer.diag.rttMs == null ? "n/a" : peer.diag.rttMs + " ms");
      diagRow(box, peer.nick + " · delivered", `${fmtBytes(peer.frame === "v2" && S.file ? Math.min(peer.delivered, S.file.size) : 0)} (ACK-verified)`);
      diagRow(box, peer.nick + " · peak", fmtRate(peer.stats.peak || 0));
      diagRow(box, peer.nick + " · retries", String(peer.diag.retries));
    }
    try {
      const t = JSON.parse(localStorage.getItem("p2p-tune") || "null");
      if (t) diagRow(box, "tune result", `${t.k} conns @ ${fmtRate(t.mbs)} (${t.mode})`);
    } catch {}
  }
  if (recvOpen && $("recvDiagBody")) {
    const box = $("recvDiagBody");
    box.innerHTML = "";
    const open = R.paths.filter((p) => p && p.dc && p.dc.readyState === "open").length;
    diagRow(box, "mode", connMode());
    diagRow(box, "TURN", TURN_STATE.turn ? "configured" : "not configured");
    diagRow(box, "paths", `${open}/${R.paths.length || 0} open`);
    diagRow(box, "framing", R.v2 ? `striped, ${R.v2.total} chunks` : "legacy ordered");
    diagRow(box, "integrity", R.integrity || "pending");
    diagRow(box, "hint", "Full candidate stats are logged on connect.");
  }
}

// ---------- appearance: light / dark / system / custom ----------
function applyTheme() {
  const t = localStorage.getItem("p2p-theme") || "system";
  let dark = false;
  if (t === "dark") dark = true;
  else if (t === "light") dark = false;
  else if (t === "custom") dark = localStorage.getItem("p2p-theme-dark") === "1";
  else dark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
  const root = document.documentElement;
  root.dataset.theme = dark ? "dark" : "light";
  const accent = localStorage.getItem("p2p-accent");
  const bg = localStorage.getItem("p2p-bg");
  if (t === "custom" && accent) root.style.setProperty("--brand", accent);
  else root.style.removeProperty("--brand");
  if (t === "custom" && bg) root.style.setProperty("--bg", bg);
  else root.style.removeProperty("--bg");
}
(function initAppearance() {
  const sel = $("themeSel");
  if (!sel) return;
  sel.value = localStorage.getItem("p2p-theme") || "system";
  if (localStorage.getItem("p2p-accent")) $("accentPick").value = localStorage.getItem("p2p-accent");
  if (localStorage.getItem("p2p-bg")) $("bgPick").value = localStorage.getItem("p2p-bg");
  $("customDark").checked = localStorage.getItem("p2p-theme-dark") === "1";
  sel.addEventListener("change", () => { localStorage.setItem("p2p-theme", sel.value); applyTheme(); });
  $("accentPick").addEventListener("input", (e) => { localStorage.setItem("p2p-accent", e.target.value); applyTheme(); });
  $("bgPick").addEventListener("input", (e) => { localStorage.setItem("p2p-bg", e.target.value); applyTheme(); });
  $("customDark").addEventListener("change", (e) => { localStorage.setItem("p2p-theme-dark", e.target.checked ? "1" : "0"); applyTheme(); });
  $("btnThemeReset").onclick = () => {
    ["p2p-theme", "p2p-accent", "p2p-bg", "p2p-theme-dark"].forEach((k) => localStorage.removeItem(k));
    sel.value = "system";
    applyTheme();
  };
  if (window.matchMedia) {
    try { window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", applyTheme); } catch {}
  }
  applyTheme();
  const cs = $("connSel");
  if (cs) {
    syncConnSel();
    cs.addEventListener("change", () => {
      localStorage.setItem("p2p-conns", cs.value);
      log(`Parallel connections: ${cs.value === "auto" ? "auto (tuned)" : cs.value} (applies to new transfers).`);
      syncConnSel();
    });
  }
  if ($("btnTune")) $("btnTune").onclick = runSpeedTune;
})();

// ---------- deep link: ?code=123456 auto-joins as receiver ----------
(function autoJoin() {
  const params = new URLSearchParams(location.search);
  const code = (params.get("code") || "").trim();
  if (/^\d{6}$/.test(code)) {
    showTab("receive");
    $("codeInput").value = code;
    log(`QR link detected (code ${code}) — press Connect.`);
    // One-click connect for speed:
    setTimeout(() => $("btnJoin").click(), 400);
  }
  if (location.hash === "#receive") showTab("receive");
})();
