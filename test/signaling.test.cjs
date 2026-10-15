// Signaling compatibility + validation tests. Top-level WS message types are
// unchanged; parallel-connection data travels inside payloads ({conn}).
const http = require("http");
const { spawn } = require("child_process");
const WebSocket = require("ws");

const wait = (ms) => new Promise((r) => setTimeout(r, ms));
function get(port, path) {
  return new Promise((res, rej) => {
    http.get({ host: "127.0.0.1", port, path }, (r) => {
      let d = "";
      r.on("data", (c) => (d += c));
      r.on("end", () => res({ status: r.statusCode, body: d }));
    }).on("error", rej);
  });
}
async function waitReady(port) {
  for (let i = 0; i < 100; i++) {
    try { await get(port, "/health"); return; } catch { await wait(200); }
  }
  throw new Error("no server on " + port);
}
const msgOnce = (w) => new Promise((res) => w.once("message", (d) => res(JSON.parse(d.toString()))));

let ok = true, n = 0;
function check(name, cond, extra) {
  n++;
  console.log((cond ? "PASS" : "FAIL") + " - " + name + (extra ? " (" + extra + ")" : ""));
  if (!cond) ok = false;
}

(async () => {
  const srv = spawn(process.execPath, ["server.js"], { stdio: "ignore", env: { ...process.env, PORT: "4211" } });
  try {
    await waitReady(4211);
    const open = async () => {
      const w = new WebSocket("ws://127.0.0.1:4211/ws");
      await new Promise((res, rej) => { w.on("open", res); w.on("error", rej); });
      return w;
    };

    // happy path: create / join / nick / signal with conn / relay / leave
    const a = await open();
    a.send(JSON.stringify({ type: "create", nick: "S" }));
    const code = (await msgOnce(a)).code;
    check("create", /^[0-9]{6}$/.test(code));
    const b = await open();
    const pPj = msgOnce(a), pJ = msgOnce(b);
    b.send(JSON.stringify({ type: "join", code, nick: "R" }));
    const [pje, j] = await Promise.all([pPj, pJ]);
    check("join + peer-joined", j.type === "joined" && pje.nick === "R");

    // v2-style signal with conn index passes through untouched
    const sg = msgOnce(b);
    a.send(JSON.stringify({ type: "signal", code, target: j.peerId, payload: { kind: "offer", conn: 2, sdp: { type: "offer", sdp: "x" } } }));
    const sgm = await sg;
    check("signal conn passthrough", sgm.payload.conn === 2 && sgm.payload.kind === "offer" && sgm.from === "sender");

    // hello kind allowed (v2 capability)
    const hl = msgOnce(a);
    b.send(JSON.stringify({ type: "signal", code, payload: { kind: "hello", frame: "v2" } }));
    check("hello allowed", (await hl).payload.kind === "hello");

    // unknown signal kind rejected
    a.send(JSON.stringify({ type: "signal", code, target: j.peerId, payload: { kind: "bogus" } }));
    check("bad signal kind rejected", (await msgOnce(a)).type === "error");

    // relay-meta validation
    b.send(JSON.stringify({ type: "relay", code, payload: { kind: "relay-meta", name: "f", size: -5, totalChunks: 3 } }));
    check("bad meta rejected", (await msgOnce(b)).type === "error");
    const mt = msgOnce(a);
    b.send(JSON.stringify({ type: "relay", code, payload: { kind: "relay-meta", fileId: "f1", name: "ok.bin", size: 100, totalChunks: 4, mime: "application/octet-stream" } }));
    check("good meta forwarded", (await mt).payload.name === "ok.bin");

    // relay-chunk validation
    b.send(JSON.stringify({ type: "relay", code, payload: { kind: "relay-chunk", fileId: "f1", index: 999999999, base64: "eA==" } }));
    check("bad chunk index rejected", (await msgOnce(b)).type === "error");
    const ch = msgOnce(a);
    b.send(JSON.stringify({ type: "relay", code, payload: { kind: "relay-chunk", fileId: "f1", index: 0, base64: Buffer.from("hi").toString("base64"), isLast: false } }));
    check("good chunk forwarded", (await ch).payload.index === 0);

    // invalid JSON doesn't kill the socket
    a.send("{{{not json");
    check("invalid JSON -> error, socket alive", (await msgOnce(a)).type === "error");
    const lv = msgOnce(a);
    b.send(JSON.stringify({ type: "leave" }));
    await msgOnce(b);
    check("leave notifies sender", (await lv).type === "peer-left");

    // oversize message rejected (maxPayload 1 MiB)
    const c = await open();
    const closed = new Promise((res) => { c.on("close", (co, reason) => res(co)); setTimeout(() => res("open"), 3000); });
    c.send(JSON.stringify({ type: "relay", code: "000000", payload: { kind: "relay-text", text: "x".repeat(2 * 1024 * 1024) } }));
    const cc = await closed;
    check("oversize message refused", cc !== "open", "close=" + cc);

    a.close(); b.close(); c.close();
  } finally {
    srv.kill();
  }
  console.log(ok ? `ALL ${n} SIGNALING TESTS PASSED` : "SIGNALING TESTS FAILED");
  process.exit(ok ? 0 : 1);
})().catch((e) => { console.error("TEST FAIL:", e.message); process.exit(1); });
