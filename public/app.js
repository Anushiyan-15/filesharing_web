/* P2P Share frontend — vanilla JS, no build step.
 * Flow:
 *  Send: pick file -> WS "create" -> show QR+code -> receiver joins ->
 *        WebRTC offer/answer/ICE via WS "signal" -> DataChannel chunks P2P.
 *  Fallback: if P2P can't connect (strict NAT), chunks relay via WS "relay".
 */
"use strict";

const CHUNK_SIZE = 16 * 1024;          // WebRTC DataChannel chunk (safe for all browsers)
const RELAY_CHUNK_SIZE = 32 * 1024;    // WS relay chunk before base64
const MAX_BUFFERED = 4 * 1024 * 1024;  // DataChannel backpressure threshold
const P2P_TIMEOUT_MS = 12000;          // wait this long before suggesting relay

const ICE_SERVERS = [
  // STUN servers
  { urls: "stun:stun.l.google.com:19302" },
  { urls: "stun:stun1.l.google.com:3478" },
  
  // ExpressTURN free server (1000 GB/month free)
  {
    urls: "turn:free.expressturn.com:3478",
    username: "000000002107350926",
    credential: "tOBQTcRCtCQaOz6Qhmf4yIxII+M="
  }
];

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
  startTime: 0,
};

// ---------- receiver state ----------
const R = {
  code: null,
  peerId: null,
  senderNick: "",
  pc: null,
  dc: null,
  pendingIce: [],
  fileId: null,
  meta: null,
  chunks: [],       // Array<ArrayBuffer> indexed by chunk number
  received: 0,
  totalChunks: 0,
  startTime: 0,
  relayMode: false,
  done: false,
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
  S.started = false; S.cancelled = false;
  $("fileRow").hidden = false;
  $("fileName").textContent = f.name;
  $("fileSize").textContent = `${fmtBytes(f.size)} · ${f.type || "unknown type"}`;
  $("btnCreate").disabled = false;
  $("sendProgressWrap").hidden = true;
  $("btnRetrySend").hidden = true;
  $("btnRelay").hidden = true;
  closeAllPeers(false);
  for (const peer of S.peers.values()) { peer.done = false; peer.useRelay = false; peer.sentChunks = 0; peer.totalChunks = 0; peer.left = false; peer.state = "joined"; }
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
  closeAllPeers(false);
  releaseScreen();
  for (const peer of S.peers.values()) { peer.sentChunks = 0; peer.totalChunks = 0; peer.done = false; peer.useRelay = false; peer.state = "joined"; }
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

// ---------- sender: one WebRTC connection per receiver ----------
// Each receiver gets its own PeerConnection + DataChannel with independent
// backpressure, so a slow phone never throttles a fast PC — every receiver
// gets the full speed its own link allows (only the sender's total uplink
// is shared, which no app can avoid).
function addPeer(id, nick) {
  let peer = S.peers.get(id);
  if (!peer) {
    peer = { id, nick: nick || "Receiver", pc: null, dc: null, state: "joined", sentChunks: 0, totalChunks: 0, startTime: 0, p2pTimer: null, pendingIce: [], useRelay: false, done: false, left: false, pumping: false };
    S.peers.set(id, peer);
  } else {
    peer.nick = nick || peer.nick;
    peer.left = false;
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
  if (peer.done) return peer.useRelay ? "done (relay)" : "done (P2P)";
  if (peer.useRelay) return "relaying…";
  if (peer.state === "sending") return "sending…";
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
  const t = p.totalChunks || 1;
  return Math.min(p.sentChunks, t) / t;
}

function updatePeerRow(peer) {
  const st = $("peer-st-" + peer.id), bar = $("peer-bar-" + peer.id);
  const pct = Math.round(peerFrac(peer) * 100) + "%";
  if (st) st.textContent = peerLabel(peer) + (peer.done || peer.left ? "" : ` ${pct}`);
  if (bar) bar.style.width = pct;
}

function makePeerPC(peer) {
  closePeer(peer, false);
  const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
  peer.pc = pc;
  peer.pendingIce = [];
  peer.state = "connecting";

  pc.onicecandidate = (e) => {
    if (e.candidate && S.code) sendSignal({ type: "signal", code: S.code, target: peer.id, payload: { kind: "ice", candidate: e.candidate } });
  };
  pc.onconnectionstatechange = () => {
    const st = pc.connectionState;
    log(`Sender→${peer.nick}: ${st}`);
    if (st === "connected") {
      clearTimeout(peer.p2pTimer);
      peer.state = "connected";
      updatePeerRow(peer);
      if (S.peers.size === 1) setStatus($("senderStatus"), `Connected to ${peer.nick} directly (P2P). Sending…`, "ok");
    } else if (st === "failed") {
      if (S.started && !S.cancelled && !peer.useRelay && !peer.done && !peer.left) {
        log(`Direct P2P to ${peer.nick} failed — auto-switching only that receiver to relay.`);
        startRelayPeer(peer);
      }
    } else if (st === "disconnected") {
      if (S.started && !peer.useRelay && !peer.done && !peer.left) {
        updatePeerRow(peer);
        $("btnRelay").hidden = false;
      }
    }
  };

  const dc = pc.createDataChannel("file", { ordered: true });
  peer.dc = dc;
  dc.binaryType = "arraybuffer";
  dc.bufferedAmountLowThreshold = 1024 * 1024;
  dc.onopen = () => {
    log(`Data channel open to ${peer.nick} — sending.`);
    if (S.started && !S.cancelled && !peer.done && !peer.left && !peer.useRelay) pumpPeerP2P(peer);
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
  dc.onmessage = (e) => {
    if (typeof e.data !== "string") return;
    try {
      const m = JSON.parse(e.data);
      if (m.kind === "cancel-request") { stopPeer(peer, "cancelled"); setStatus($("senderStatus"), `${peer.nick} cancelled. Others continue.`, "err"); return; }
      if (m.kind === "resume") {
        // Receiver tells us how many chunks it already has.
        const have = Math.min(m.received || 0, peer.totalChunks);
        if (have > peer.sentChunks) { peer.sentChunks = have; log(`Resuming ${peer.nick} from chunk ${have}/${peer.totalChunks}.`); }
      }
    } catch {}
  };

  clearTimeout(peer.p2pTimer);
  peer.p2pTimer = setTimeout(() => {
    if (S.started && !S.cancelled && !peer.useRelay && !peer.done && !peer.left && peer.sentChunks === 0 && peer.pc && peer.pc.connectionState !== "connected") {
      log(`P2P to ${peer.nick} taking >12s — auto-switching only that receiver to relay.`);
      startRelayPeer(peer);
    }
  }, P2P_TIMEOUT_MS);
  renderPeerList();
  return pc;
}

function ensurePeerConnection(peer) {
  if (peer.dc && peer.dc.readyState === "open") {
    if (S.started && !S.cancelled && !peer.done && !peer.left && !peer.useRelay) pumpPeerP2P(peer);
    return;
  };
  if (peer.pc && peer.pc.signalingState !== "stable") return; // handshake in flight
  makePeerPC(peer);
  peer.pc.createOffer()
    .then((o) => peer.pc.setLocalDescription(o))
    .then(() => sendSignal({ type: "signal", code: S.code, target: peer.id, payload: { kind: "offer", sdp: peer.pc.localDescription } }))
    .then(() => log(`Offer sent to ${peer.nick}.`))
    .catch((err) => setStatus($("senderStatus"), "Could not start WebRTC: " + err.message, "err"));
}

function closePeer(peer, logIt) {
  if (!peer) return;
  clearTimeout(peer.p2pTimer);
  try { peer.dc && peer.dc.close(); } catch {}
  try { peer.pc && peer.pc.close(); } catch {}
  peer.pc = null; peer.dc = null; peer.pumping = false;
  if (logIt) log(`Connection to ${peer.nick} closed.`);
}

function closeAllPeers(logIt) {
  for (const peer of S.peers.values()) closePeer(peer, false);
  if (logIt) log("All sender connections closed.");
}

// Stop one receiver without touching the others.
function stopPeer(peer, reason) {
  if (!peer || peer.left) return;
  if (reason === "left") peer.left = true;
  else { peer.sentChunks = 0; peer.done = false; peer.state = "joined"; }
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
  let targets = [...S.peers.values()].filter((p) => !p.done && !p.left);
  if (onlyNew) targets = targets.filter((p) => p.sentChunks === 0 && !p.pumping);
  if (!targets.length) {
    if ([...S.peers.values()].some((p) => !p.left)) { setStatus($("senderStatus"), "Everyone already has the file.", "ok"); return; }
    setStatus($("senderStatus"), "No receiver connected yet — share the code first.", "err");
    return;
  }
  S.cancelled = false;
  S.started = true;
  S.startTime = Date.now();
  lockScreen();
  $("sendProgressWrap").hidden = false;
  $("btnRetrySend").hidden = true;
  $("sendLabel").textContent = forceRelay ? "Relaying via server…" : "Sending…";
  setStatus($("senderStatus"), `${forceRelay ? "Relaying" : "Sending"} to ${targets.length} receiver(s)…`);
  for (const peer of targets) {
    if (peer.pumping) continue;
    peer.sentChunks = 0;
    peer.done = false;
    peer.startTime = Date.now();
    if (forceRelay || peer.useRelay) startRelayPeer(peer);
    else ensurePeerConnection(peer);
  }
}

async function pumpPeerP2P(peer) {
  const file = S.file;
  if (!file || !peer.dc || peer.dc.readyState !== "open") return;
  if (peer.pumping) return; // one pump per receiver — others run in parallel
  peer.pumping = true;
  peer.state = "sending";
  peer.totalChunks = Math.max(1, Math.ceil(file.size / CHUNK_SIZE));
  const fileId = S.fileId;
  // Ask receiver where to resume (it replies with {kind:'resume'} if partial).
  peer.dc.send(JSON.stringify({
    kind: "meta", fileId, name: file.name, size: file.size,
    mime: file.type || "application/octet-stream",
    totalChunks: peer.totalChunks, chunkSize: CHUNK_SIZE, nick: myNick(),
  }));
  await sleep(250); // let receiver reply with resume offset before we blast chunks
  if (S.cancelled || peer.left || peer.done) { peer.pumping = false; return; }

  for (let i = peer.sentChunks; i < peer.totalChunks; i++) {
    if (S.cancelled || peer.left) { peer.pumping = false; return; }
    if (peer.dc.readyState !== "open") { peer.sentChunks = i; peer.pumping = false; return; } // onclose handler offers retry
    while (peer.dc.bufferedAmount > MAX_BUFFERED) await sleep(100);
    if (S.cancelled || peer.left) { peer.pumping = false; return; }
    const slice = file.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE);
    const buf = await slice.arrayBuffer();
    try { peer.dc.send(buf); }
    catch { peer.sentChunks = i; peer.pumping = false; return; }
    peer.sentChunks = i + 1;
    if (i % 10 === 0 || i === peer.totalChunks - 1) { updatePeerRow(peer); updateSendProgress(); }
  }
  try { peer.dc.send(JSON.stringify({ kind: "done", fileId })); } catch {}
  peer.pumping = false;
  peer.done = true;
  peer.state = "done";
  updatePeerRow(peer); updateSendProgress(); renderPeerList();
  addHistory("sent", file.name, file.size, null, peer.nick);
  log(`Sent ${file.name} to ${peer.nick} directly (${peer.totalChunks} chunks).`);
  notifyDone(`${file.name} → ${peer.nick}`);
  finishSenderIfAllDone();
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
  const elapsed = Math.max(0.1, (Date.now() - S.startTime) / 1000);
  const doneBytes = peers.reduce((s, p) => s + Math.min(p.sentChunks * (p.useRelay ? RELAY_CHUNK_SIZE : CHUNK_SIZE), S.file ? S.file.size : 0), 0);
  const totalBytes = (S.file ? S.file.size : 0) * Math.max(1, peers.length);
  const who = peers.length > 1 ? `${peers.length} receivers` : (peers[0] ? peers[0].nick : "");
  $("sendStats").textContent = `${fmtBytes(doneBytes)} of ${fmtBytes(totalBytes)} · ${fmtSpeed(doneBytes / elapsed)} · ETA ${fmtETA(elapsed, avg * 100, 100)} · ${who}`;
  for (const p of peers) updatePeerRow(p);
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
  for (let i = peer.sentChunks; i < total; i++) {
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
  try { R.dc && R.dc.readyState === "open" && R.dc.send(JSON.stringify({ kind: "cancel-request" })); } catch {}
  if (R.code) { try { await sendSignal({ type: "relay", code: R.code, payload: { kind: "cancel-request", fileId: R.fileId } }); } catch {} }
  closeReceiverPeer(false);
  resetReceiverFile();
  setStatus($("receiverStatus"), "Cancelled — cleared.", "err");
  log("Receive cancelled and cleared.");
};

function resetReceiverFile() {
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

function makeReceiverPC() {
  closeReceiverPeer(false);
  const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
  R.pc = pc;
  R.pendingIce = [];
  pc.onicecandidate = (e) => {
    if (e.candidate && R.code) sendSignal({ type: "signal", code: R.code, payload: { kind: "ice", candidate: e.candidate } });
  };
  pc.onconnectionstatechange = () => {
    log(`Receiver connection: ${pc.connectionState}`);
    if (pc.connectionState === "connected") setStatus($("receiverStatus"), "Connected directly (P2P). Receiving…", "ok");
    if (pc.connectionState === "failed") setStatus($("receiverStatus"), "Direct path failed — sender auto-switches to relay. Keep this tab open.", "err");
  };
  pc.ondatachannel = (e) => {
    const dc = e.channel;
    R.dc = dc;
    dc.binaryType = "arraybuffer";
    log("Data channel received.");
    dc.onmessage = onP2PMessage(dc);
    dc.onclose = () => {
      if (!R.done && R.meta && R.received < R.totalChunks) {
        setStatus($("receiverStatus"), `Interrupted at ${Math.round(R.received / R.totalChunks * 100)}%. Keep this tab open — pressing Retry on the sender resumes.`, "err");
      }
    };
  };
  return pc;
}

function onP2PMessage(dc) {
  return (e) => {
    if (typeof e.data === "string") {
      let m;
      try { m = JSON.parse(e.data); } catch { return; }
      if (m.kind === "meta") {
        const sameFile = R.fileId === m.fileId && R.meta;
        R.fileId = m.fileId;
        if (!sameFile) { R.meta = m; R.totalChunks = m.totalChunks; R.chunks = new Array(m.totalChunks); R.received = 0; R.startTime = Date.now(); }
        R.done = false;
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
      } else if (m.kind === "done") {
        finishReceive();
      } else if (m.kind === "cancel") {
        resetReceiverFile();
        setStatus($("receiverStatus"), "Sender cancelled — cleared.", "err");
      }
      return;
    }
    // Binary chunk: chunks arrive in order on an ordered channel.
    if (!R.meta) return;
    R.chunks[R.received] = e.data;
    R.received++;
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
  const elapsed = Math.max(0.1, (Date.now() - R.startTime) / 1000);
  const bytes = R.meta ? Math.min(R.meta.size, R.received * (R.meta.chunkSize || CHUNK_SIZE)) : 0;
  const totalB = R.meta ? R.meta.size : 0;
  $("recvStats").textContent = `${fmtBytes(bytes)} of ${fmtBytes(totalB)} · ${fmtSpeed(bytes / elapsed)} · ETA ${fmtETA(elapsed, R.received, total)} · chunk ${R.received}/${total}`;
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
  setStatus($("receiverStatus"), `Received ${R.meta.name} (${fmtBytes(blob.size)}) from ${R.senderNick || "sender"}. Tap Download to save it.`, "ok");
  log(`Transfer complete: ${R.meta.name}.`);
  notifyDone(R.meta.name);
  releaseScreen();
  try { a.click(); } catch {} // best-effort auto-download; button remains regardless
}

function closeReceiverPeer(logIt = true) {
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
    log(`Joined room ${m.code} as ${myNick()} (${m.count}/${m.max} receivers). Sender: ${R.senderNick}.`);
    setStatus($("textStatus"), `Connected to ${R.senderNick} — you can send text here.`, "ok");
    return;
  }
  if (m.type === "peer-joined") {
    const peer = addPeer(m.peerId, m.nick);
    log(`${peer.nick} joined (${m.count}/${m.max}) — auto-starting transfer.`);
    setStatus($("senderStatus"), `${m.count} receiver(s) connected. Starting transfer…`, "ok");
    setStatus($("textStatus"), `${peer.nick} connected — you can send text here.`, "ok");
    $("btnSendStart").disabled = false;
    $("btnRelay").hidden = true;
    ensurePeerConnection(peer);
    if (S.file && !S.started && !S.cancelled) startTransfer(false, true); // only the newcomer
    // If already sending, the new data channel's onopen pumps this late joiner.
    return;
  }
  if (m.type === "signal") {
    const p = m.payload || {};
    if (m.from === "sender" || S.code == null) {
      // Receiver side: handle offer/ice from sender.
      if (!R.pc) makeReceiverPC();
      try {
        if (p.kind === "offer") {
          await R.pc.setRemoteDescription(new RTCSessionDescription(p.sdp));
          for (const c of R.pendingIce) await R.pc.addIceCandidate(new RTCIceCandidate(c)).catch(() => {});
          R.pendingIce = [];
          const answer = await R.pc.createAnswer();
          await R.pc.setLocalDescription(answer);
          sendSignal({ type: "signal", code: R.code, payload: { kind: "answer", sdp: R.pc.localDescription } });
          log("Answer sent.");
        } else if (p.kind === "ice") {
          if (R.pc.remoteDescription) await R.pc.addIceCandidate(new RTCIceCandidate(p.candidate)).catch(() => {});
          else R.pendingIce.push(p.candidate);
        }
      } catch (err) { log("Receiver WebRTC error: " + err.message); }
    } else {
      // Sender side: route answer/ice to the right receiver's PC.
      const peer = S.peers.get(m.from);
      if (!peer || !peer.pc) return;
      try {
        if (p.kind === "answer") {
          await peer.pc.setRemoteDescription(new RTCSessionDescription(p.sdp));
          for (const c of peer.pendingIce) await peer.pc.addIceCandidate(new RTCIceCandidate(c)).catch(() => {});
          peer.pendingIce = [];
          log(`P2P handshake with ${peer.nick} complete.`);
        } else if (p.kind === "ice") {
          if (peer.pc.remoteDescription) await peer.pc.addIceCandidate(new RTCIceCandidate(p.candidate)).catch(() => {});
          else peer.pendingIce.push(p.candidate);
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
  if (R.dc && R.dc.readyState === "open") out.push(R.dc);
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
