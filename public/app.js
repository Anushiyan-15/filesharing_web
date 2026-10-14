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
  { urls: "stun:stun.l.google.com:19302" },
  { urls: "stun:global.stun.twilio.com:3478" },
  // To guarantee cross-network transfers in production, add a TURN server here:
  // { urls: "turn:your-turn-host:3478", username: "user", credential: "pass" }
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
const tabSend = $("tabSend"), tabReceive = $("tabReceive");
const paneSend = $("paneSend"), paneReceive = $("paneReceive");
function showTab(which) {
  const send = which === "send";
  tabSend.classList.toggle("active", send);
  tabReceive.classList.toggle("active", !send);
  paneSend.hidden = !send;
  paneReceive.hidden = send;
}
tabSend.onclick = () => showTab("send");
tabReceive.onclick = () => showTab("receive");

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

// ---------- sender state ----------
const S = {
  file: null,
  fileId: null,
  code: null,
  pc: null,
  dc: null,
  started: false,
  cancelled: false,
  useRelay: false,
  sentChunks: 0,
  totalChunks: 0,
  startTime: 0,
  p2pTimer: null,
  pendingIce: [],
  receiverAcked: 0,
};

// ---------- receiver state ----------
const R = {
  code: null,
  pc: null,
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
  S.started = false; S.cancelled = false; S.sentChunks = 0; S.useRelay = false;
  $("fileRow").hidden = false;
  $("fileName").textContent = f.name;
  $("fileSize").textContent = `${fmtBytes(f.size)} · ${f.type || "unknown type"}`;
  $("btnCreate").disabled = false;
  $("sendProgressWrap").hidden = true;
  $("btnRetrySend").hidden = true;
  $("btnRelay").hidden = true;
  closeSenderPeer(false);
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
    sendSignal({ type: "create" });
    setStatus($("senderStatus"), "Generating code…");
    log("Requesting connection code…");
  } catch (err) {
    setStatus($("senderStatus"), err.message, "err");
    $("btnCreate").disabled = false;
  }
};

function showShare(code) {
  S.code = code;
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

$("btnSendStart").onclick = () => startTransfer(false);
$("btnRelay").onclick = () => {
  log("Switching to server relay (slower, works on any network).");
  startTransfer(true);
};
$("btnCancelSend").onclick = () => {
  S.cancelled = true;
  try { S.dc && S.dc.send(JSON.stringify({ kind: "cancel" })); } catch {}
  if (S.useRelay && S.code) sendSignal({ type: "relay", code: S.code, payload: { kind: "cancel", fileId: S.fileId } });
  closeSenderPeer(true);
  setStatus($("senderStatus"), "Transfer cancelled.");
  $("btnRetrySend").hidden = false;
};
$("btnRetrySend").onclick = () => {
  $("btnRetrySend").hidden = true;
  startTransfer(S.useRelay);
};

// ---------- sender: WebRTC ----------
function makeSenderPC() {
  closeSenderPeer(false);
  const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
  S.pc = pc;
  S.pendingIce = [];

  pc.onicecandidate = (e) => {
    if (e.candidate && S.code) sendSignal({ type: "signal", code: S.code, payload: { kind: "ice", candidate: e.candidate } });
  };
  pc.onconnectionstatechange = () => {
    const st = pc.connectionState;
    log(`Sender connection: ${st}`);
    if (st === "connected") {
      clearTimeout(S.p2pTimer);
      $("btnRelay").hidden = true;
      setStatus($("senderStatus"), "Connected directly (P2P). Sending…", "ok");
    } else if (st === "failed" || st === "disconnected") {
      if (S.started && !S.useRelay) {
        setStatus($("senderStatus"), "Direct connection failed (often strict NAT/firewall). Use relay instead.", "err");
        $("btnRelay").hidden = false;
        $("btnRetrySend").hidden = false;
      }
    }
  };

  const dc = pc.createDataChannel("file", { ordered: true });
  S.dc = dc;
  dc.binaryType = "arraybuffer";
  dc.bufferedAmountLowThreshold = 1024 * 1024;
  dc.onopen = () => {
    log("Data channel open — sending file.");
    pumpP2P();
  };
  dc.onclose = () => {
    if (S.started && !S.cancelled && S.sentChunks < S.totalChunks) {
      setStatus($("senderStatus"), "Connection interrupted. Press Retry to resume.", "err");
      $("btnRetrySend").hidden = false;
      log(`Interrupted at chunk ${S.sentChunks}/${S.totalChunks}. Retry resumes from receiver's progress.`);
    }
  };
  dc.onmessage = (e) => {
    if (typeof e.data !== "string") return;
    try {
      const m = JSON.parse(e.data);
      if (m.kind === "ack") { S.receiverAcked = m.received; }
      if (m.kind === "resume") {
        // Receiver tells us how many chunks it already has.
        const have = Math.min(m.received || 0, S.totalChunks);
        if (have > S.sentChunks) { S.sentChunks = have; log(`Resuming from chunk ${have}/${S.totalChunks}.`); }
      }
    } catch {}
  };
  return pc;
}

async function startTransfer(forceRelay) {
  if (!S.file) { setStatus($("senderStatus"), "Pick a file first.", "err"); return; }
  if (!S.code) { setStatus($("senderStatus"), "Click “Get QR & code” first.", "err"); return; }
  S.cancelled = false;
  S.useRelay = forceRelay;
  S.started = true;
  S.startTime = Date.now();
  S.totalChunks = Math.max(1, Math.ceil(S.file.size / CHUNK_SIZE));
  $("sendProgressWrap").hidden = false;
  $("btnRetrySend").hidden = true;
  $("sendLabel").textContent = forceRelay ? "Relaying via server…" : "Sending…";

  if (forceRelay) {
    setStatus($("senderStatus"), "Relaying through server (fallback)…", "");
    relaySend();
    return;
  }

  // P2P path
  const pc = makeSenderPC();
  setStatus($("senderStatus"), "Connecting directly to receiver…");
  $("btnRelay").hidden = true;
  clearTimeout(S.p2pTimer);
  S.p2pTimer = setTimeout(() => {
    if (S.started && !S.useRelay && S.sentChunks === 0 && pc.connectionState !== "connected") {
      setStatus($("senderStatus"), "Still connecting — devices may be on incompatible networks. You can use relay.", "err");
      $("btnRelay").hidden = false;
      log("P2P is taking >12s: likely symmetric NAT. Relay fallback available.");
    }
  }, P2P_TIMEOUT_MS);

  try {
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    sendSignal({ type: "signal", code: S.code, payload: { kind: "offer", sdp: pc.localDescription } });
    log("Offer sent, waiting for receiver's answer…");
  } catch (err) {
    setStatus($("senderStatus"), "Could not start WebRTC: " + err.message, "err");
  }
}

async function pumpP2P() {
  const { file, fileId, dc } = S;
  if (!file || !dc || dc.readyState !== "open") return;
  // Ask receiver where to resume (it replies with {kind:'resume'} if partial).
  dc.send(JSON.stringify({
    kind: "meta", fileId, name: file.name, size: file.size,
    mime: file.type || "application/octet-stream",
    totalChunks: S.totalChunks, chunkSize: CHUNK_SIZE,
  }));
  await sleep(250); // let receiver reply with resume offset before we blast chunks
  if (S.cancelled) return;

  for (let i = S.sentChunks; i < S.totalChunks; i++) {
    if (S.cancelled) return;
    if (dc.readyState !== "open") { S.sentChunks = i; return; } // onclose handler offers retry
    while (dc.bufferedAmount > MAX_BUFFERED) await sleep(100);
    const slice = file.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE);
    const buf = await slice.arrayBuffer();
    dc.send(buf);
    S.sentChunks = i + 1;
    if (i % 10 === 0 || i === S.totalChunks - 1) updateSendProgress();
  }
  dc.send(JSON.stringify({ kind: "done", fileId }));
  updateSendProgress();
  setStatus($("senderStatus"), `Sent ${file.name} (${fmtBytes(file.size)}) directly.`, "ok");
  log(`P2P transfer complete: ${S.totalChunks} chunks.`);
  S.started = false;
  clearTimeout(S.p2pTimer);
}

function updateSendProgress() {
  const done = S.sentChunks, total = S.totalChunks || 1;
  const pct = Math.round((done / total) * 100);
  $("sendBar").style.width = pct + "%";
  $("sendPct").textContent = pct + "%";
  const elapsed = Math.max(0.1, (Date.now() - S.startTime) / 1000);
  const bytes = Math.min(S.file ? S.file.size : 0, done * CHUNK_SIZE);
  $("sendStats").textContent = `${fmtBytes(bytes)} of ${fmtBytes(S.file.size)} · ${fmtSpeed(bytes / elapsed)} · chunk ${done}/${total}`;
}

// ---------- sender: relay fallback (server-forwarded chunks) ----------
function bufToBase64(buf) {
  const bytes = new Uint8Array(buf);
  let s = "";
  for (let i = 0; i < bytes.length; i += 8192) s += String.fromCharCode(...bytes.subarray(i, i + 8192));
  return btoa(s);
}

async function relaySend() {
  const { file, fileId, code } = S;
  const total = Math.max(1, Math.ceil(file.size / RELAY_CHUNK_SIZE));
  S.totalChunks = total;
  sendSignal({ type: "relay", code, payload: {
    kind: "relay-meta", fileId, name: file.name, size: file.size,
    mime: file.type || "application/octet-stream", totalChunks: total,
  }});
  // If receiver already has part of this file it will answer relay-ack; give it a beat.
  await sleep(400);
  for (let i = S.sentChunks; i < total; i++) {
    if (S.cancelled) return;
    const slice = file.slice(i * RELAY_CHUNK_SIZE, (i + 1) * RELAY_CHUNK_SIZE);
    const buf = await slice.arrayBuffer();
    sendSignal({ type: "relay", code, payload: {
      kind: "relay-chunk", fileId, index: i, base64: bufToBase64(buf), isLast: i === total - 1,
    }});
    S.sentChunks = i + 1;
    if (i % 5 === 0 || i === total - 1) updateSendProgress();
    await sleep(10); // avoid flooding the socket
  }
  updateSendProgress();
  setStatus($("senderStatus"), `Relayed ${file.name} via server.`, "ok");
  log("Relay transfer complete.");
  S.started = false;
}

function closeSenderPeer(logIt) {
  clearTimeout(S.p2pTimer);
  try { S.dc && S.dc.close(); } catch {}
  try { S.pc && S.pc.close(); } catch {}
  S.pc = null; S.dc = null;
  if (logIt) log("Sender peer connection closed.");
}

// ================= RECEIVER UI =================
$("btnJoin").onclick = async () => {
  const code = $("codeInput").value.trim();
  if (!/^\d{6}$/.test(code)) { setStatus($("receiverStatus"), "Enter the 6-digit code.", "err"); return; }
  R.code = code;
  resetReceiverFile();
  try {
    await ensureSocket();
    sendSignal({ type: "join", code });
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

function resetReceiverFile() {
  R.meta = null; R.chunks = []; R.received = 0; R.totalChunks = 0;
  R.done = false; R.relayMode = false; R.fileId = null;
  R.startTime = Date.now();
  $("recvProgressWrap").hidden = true;
  $("downloadLink").hidden = true;
  $("recvBar").style.width = "0%";
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
    if (pc.connectionState === "failed") setStatus($("receiverStatus"), "Direct connection failed. Ask the sender to press “Use relay instead”.", "err");
  };
  pc.ondatachannel = (e) => {
    const dc = e.channel;
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
        $("recvLabel").textContent = `Receiving ${m.name}…`;
        setStatus($("receiverStatus"), `Receiving ${m.name} (${fmtBytes(m.size)}) directly…`, "ok");
        log(`Receiving ${m.name} — ${m.totalChunks} chunks${R.received ? ` (resuming at ${R.received})` : ""}.`);
        // Tell sender where to start (resume support).
        dc.send(JSON.stringify({ kind: "resume", fileId: m.fileId, received: R.received }));
        updateRecvProgress();
      } else if (m.kind === "done") {
        finishReceive();
      } else if (m.kind === "cancel") {
        setStatus($("receiverStatus"), "Sender cancelled the transfer.", "err");
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
  $("recvStats").textContent = `${fmtBytes(bytes)} of ${fmtBytes(totalB)} · ${fmtSpeed(bytes / elapsed)} · chunk ${R.received}/${total}`;
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
  updateRecvProgress();
  setStatus($("receiverStatus"), `Received ${R.meta.name} (${fmtBytes(blob.size)}). Tap Download to save it.`, "ok");
  log(`Transfer complete: ${R.meta.name}.`);
  try { a.click(); } catch {} // best-effort auto-download; button remains regardless
}

function closeReceiverPeer(logIt = true) {
  try { R.pc && R.pc.close(); } catch {}
  R.pc = null;
  if (logIt) log("Receiver peer connection closed.");
}

// ---------- relay receive path ----------
function base64ToBuf(b64) {
  const s = atob(b64);
  const b = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) b[i] = s.charCodeAt(i);
  return b.buffer;
}

async function onRelayPayload(p) {
  if (p.kind === "relay-meta") {
    const sameFile = R.fileId === p.fileId && R.meta;
    R.fileId = p.fileId;
    R.relayMode = true;
    if (!sameFile) { R.meta = p; R.totalChunks = p.totalChunks; R.chunks = new Array(p.totalChunks); R.received = 0; R.startTime = Date.now(); }
    R.done = false;
    $("recvProgressWrap").hidden = false;
    $("recvLabel").textContent = `Receiving ${p.name} (relay)…`;
    setStatus($("receiverStatus"), `Receiving ${p.name} via server relay…`, "ok");
    log(`Relay receive: ${p.name}, ${p.totalChunks} chunks.`);
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
  } else if (p.kind === "relay-ack" && S.started && S.useRelay) {
    // Sender side: receiver reports offset.
    if (p.fileId === S.fileId && (p.received || 0) > S.sentChunks) {
      S.sentChunks = p.received;
      log(`Relay resuming from chunk ${p.received}.`);
    }
  } else if (p.kind === "cancel") {
    setStatus($("receiverStatus"), "Sender cancelled the transfer.", "err");
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
    setStatus($("receiverStatus"), `Joined ${m.code}. Waiting for sender…`, "ok");
    $("btnLeave").hidden = false;
    makeReceiverPC(); // ready before offer arrives; queued ICE handled below
    log(`Joined room ${m.code}.`);
    return;
  }
  if (m.type === "peer-joined") {
    setStatus($("senderStatus"), "Receiver connected! Starting transfer…", "ok");
    log("Receiver joined — auto-starting transfer.");
    $("btnSendStart").disabled = false;
    if (S.file && !S.started) startTransfer(false);
    else if (S.file && S.started) { /* already sending */ }
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
      // Sender side: handle answer/ice from receiver.
      if (!S.pc) return;
      try {
        if (p.kind === "answer") {
          await S.pc.setRemoteDescription(new RTCSessionDescription(p.sdp));
          for (const c of S.pendingIce) await S.pc.addIceCandidate(new RTCIceCandidate(c)).catch(() => {});
          S.pendingIce = [];
          log("P2P handshake complete, establishing data channel…");
        } else if (p.kind === "ice") {
          if (S.pc.remoteDescription) await S.pc.addIceCandidate(new RTCIceCandidate(p.candidate)).catch(() => {});
          else S.pendingIce.push(p.candidate);
        }
      } catch (err) { log("Sender WebRTC error: " + err.message); }
    }
    return;
  }
  if (m.type === "relay") {
    onRelayPayload(m.payload || {});
    return;
  }
  if (m.type === "peer-left") {
    log("The other device disconnected.");
    if (paneSend.hidden === false) setStatus($("senderStatus"), "The other device left. The code stays valid for a short while.", "err");
    else setStatus($("receiverStatus"), "The other device left.", "err");
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
