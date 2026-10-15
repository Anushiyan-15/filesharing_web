// WS relay-path throughput (loopback). Guards against relay regressions.
// NOTE: loopback numbers say nothing about Internet performance.
const crypto = require("crypto");
const http = require("http");
const { spawn } = require("child_process");
const WebSocket = require("ws");

const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const msgOnce = (w) => new Promise((res) => w.once("message", (d) => res(JSON.parse(d.toString()))));
async function waitReady(port) {
  for (let i = 0; i < 100; i++) {
    try {
      await new Promise((res, rej) => {
        http.get({ host: "127.0.0.1", port, path: "/health" }, (r) => { r.resume(); r.on("end", res); }).on("error", rej);
      });
      return;
    } catch { await wait(200); }
  }
  throw new Error("no server");
}

(async () => {
  const srv = spawn(process.execPath, ["server.js"], { stdio: "ignore", env: { ...process.env, PORT: "4212" } });
  try {
    await waitReady(4212);
    const a = new WebSocket("ws://127.0.0.1:4212/ws");
    await new Promise((r) => a.on("open", r));
    a.send(JSON.stringify({ type: "create" }));
    const code = (await msgOnce(a)).code;
    const b = new WebSocket("ws://127.0.0.1:4212/ws");
    await new Promise((r) => b.on("open", r));
    b.send(JSON.stringify({ type: "join", code }));
    await msgOnce(b); await msgOnce(a);

    const SIZE = 4 * 1024 * 1024, CH = 32 * 1024;
    const orig = crypto.randomBytes(SIZE);
    const total = Math.ceil(SIZE / CH);
    const got = new Array(total).fill(null);
    b.on("message", (raw) => {
      const m = JSON.parse(raw.toString());
      if (m.type === "relay" && m.payload.kind === "relay-chunk") got[m.payload.index] = Buffer.from(m.payload.base64, "base64");
    });
    const t0 = process.hrtime.bigint();
    for (let i = 0; i < total; i++) {
      a.send(JSON.stringify({ type: "relay", code, payload: { kind: "relay-chunk", fileId: "bench", index: i, base64: orig.subarray(i * CH, (i + 1) * CH).toString("base64"), isLast: i === total - 1 } }));
      if (i % 8 === 0) await wait(0);
    }
    const secs = Number(process.hrtime.bigint() - t0) / 1e9;
    for (let k = 0; k < 200 && got.some((x) => !x); k++) await wait(50);
    const intact = got.every(Boolean) && Buffer.concat(got).equals(orig);
    const mbs = SIZE / 1e6 / secs;
    console.log(`relay: ${mbs.toFixed(1)} MB/s, intact=${intact}, RSS=${(process.memoryUsage().rss / 1048576).toFixed(0)} MiB`);
    if (!intact) { console.log("FAIL - bytes corrupted"); process.exit(1); }
    if (mbs < 5) { console.log("FAIL - suspiciously slow relay (<5 MB/s loopback)"); process.exit(1); }
    console.log("RELAY BENCH PASSED");
    a.close(); b.close();
  } finally {
    srv.kill();
  }
})().catch((e) => { console.error("BENCH FAIL:", e.message); process.exit(1); });
