// Unit tests for v2 protocol pure helpers, extracted from the real app.js.
const fs = require("fs");
const crypto = require("crypto");
const src = fs.readFileSync("public/app.js", "utf8");

function extractFn(name) {
  const i = src.indexOf("function " + name + "(");
  if (i < 0) throw new Error("not found: " + name);
  const j = src.indexOf("{", i);
  let depth = 0;
  for (let k = j; k < src.length; k++) {
    if (src[k] === "{") depth++;
    else if (src[k] === "}") { depth--; if (depth === 0) return src.slice(i, k + 1); }
  }
  throw new Error("unbalanced: " + name);
}
function extractTable() {
  const i = src.indexOf("const CRC32_TABLE =");
  const j = src.indexOf("})();", i) + "})();".length;
  return src.slice(i, j);
}

const lib = [
  "const CHUNK_SIZE = 64 * 1024;",
  "const CHUNK_SIZE_LEGACY = 16 * 1024;",
  extractTable(),
  extractFn("crc32"),
  extractFn("crc32Combine"),
  "const FRAME_HEADER = 8;",
  extractFn("encodeFrame"),
  extractFn("decodeFrame"),
  extractFn("pickChunkSize"),
  extractFn("fmtRate"),
  extractFn("windowedRate"),
  extractFn("etaState"),
  "globalThis.__lib = { crc32, crc32Combine, encodeFrame, decodeFrame, pickChunkSize, fmtRate, windowedRate, etaState, FRAME_HEADER, CHUNK_SIZE, CHUNK_SIZE_LEGACY };",
].join("\n");
eval(lib);
const L = globalThis.__lib;

let ok = true, n = 0;
function check(name, cond, extra) {
  n++;
  console.log((cond ? "PASS" : "FAIL") + " - " + name + (extra !== undefined ? " (" + extra + ")" : ""));
  if (!cond) ok = false;
}

// CRC32 known-answer test ("123456789" -> 0xCBF43926)
check("crc32 check value", L.crc32(Buffer.from("123456789")) === 0xCBF43926,
  "0x" + L.crc32(Buffer.from("123456789")).toString(16));
// chaining property
{
  const a = crypto.randomBytes(1000), b = crypto.randomBytes(500);
  const chained = L.crc32(b, L.crc32(a));
  const whole = L.crc32(Buffer.concat([a, b]));
  check("crc32 chaining", chained === whole);
}
// combine property on random splits
for (const [la, lb] of [[0, 10], [1, 1], [100, 900], [65536, 65536], [12345, 6789]]) {
  const a = crypto.randomBytes(la), b = crypto.randomBytes(lb);
  const c = L.crc32Combine(L.crc32(a), L.crc32(b), lb);
  check(`combine ${la}+${lb}`, c === L.crc32(Buffer.concat([a, b])));
}
// 1-bit flip detected
{
  const p = crypto.randomBytes(100);
  const c0 = L.crc32(p);
  p[50] ^= 1;
  check("flip detected", L.crc32(p) !== c0);
}
// frame round-trip incl. empty + max index
for (const idx of [0, 1, 70000, 4294967295]) {
  const p = crypto.randomBytes(100);
  const d = L.decodeFrame(L.encodeFrame(idx, p));
  check(`frame idx ${idx}`, !d.error && d.index === idx && Buffer.from(d.payload).equals(p));
}
check("frame empty payload", (() => { const d = L.decodeFrame(L.encodeFrame(7, new Uint8Array(0))); return !d.error && d.index === 7 && d.payload.byteLength === 0; })());
check("frame short rejected", L.decodeFrame(new Uint8Array(3)).error === "short");
check("frame corrupt rejected", (() => {
  const f = new Uint8Array(L.encodeFrame(3, crypto.randomBytes(64)));
  f[20] ^= 0xFF;
  return L.decodeFrame(f.buffer).error === "crc";
})());
// out-of-order reassembly simulation (striped delivery)
{
  const N = 50, SZ = 1000;
  const chunks = Array.from({ length: N }, () => crypto.randomBytes(SZ));
  const frames = chunks.map((c, i) => L.encodeFrame(i, c));
  // shuffle (deterministic PRNG)
  let seed = 42;
  const rnd = () => (seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff;
  for (let i = frames.length - 1; i > 0; i--) { const j = Math.floor(rnd() * (i + 1));[frames[i], frames[j]] = [frames[j], frames[i]]; }
  // duplicate one frame (retransmit)
  frames.push(frames[10]);
  const store = new Array(N).fill(null);
  let dups = 0, bad = 0;
  for (const f of frames) {
    const d = L.decodeFrame(f);
    if (d.error) { bad++; continue; }
    if (d.index >= N) { bad++; continue; }
    if (store[d.index]) { dups++; continue; }
    store[d.index] = Buffer.from(d.payload);
  }
  const allOk = store.every((c, i) => c && c.equals(chunks[i]));
  check("out-of-order + dup reassembly", allOk && dups === 1 && bad === 0, `dups=${dups}`);
  // whole-file CRC via combine in index order
  let acc = 0;
  for (let i = 0; i < N; i++) acc = L.crc32Combine(acc, L.crc32(chunks[i]), SZ);
  const direct = L.crc32(Buffer.concat(chunks));
  check("combine whole-file integrity", acc === direct, "0x" + acc.toString(16));
}
// pickChunkSize
check("pick 64K default", L.pickChunkSize(undefined) === 65536 && L.pickChunkSize(262144) === 65536);
check("pick 16K small cap", L.pickChunkSize(16384) === 16384);
// fmtRate units (decimal)
check("fmtRate", L.fmtRate(0) === "0 B/s" && L.fmtRate(1500) === "1.5 KB/s" && L.fmtRate(2500000) === "2.5 MB/s" && L.fmtRate(3e9) === "3.00 GB/s" && L.fmtRate(NaN) === "—");
// windowedRate
{
  const ev = [{ t: 0, bytes: 1e6 }, { t: 1000, bytes: 1e6 }, { t: 2000, bytes: 1e6 }];
  const r = L.windowedRate(ev, 2500, 3000);
  check("windowedRate ~1.5MB/s", Math.abs(r - 1.5e6) < 1, r);
  check("windowedRate sparse", L.windowedRate([{ t: 0, bytes: 5 }], 100, 3000) === 0);
}
// etaState
check("etaState", L.etaState(1, 9, 0) === "done" && L.etaState(0.5, 1, 0) === "calculating" &&
  L.etaState(0.5, 9, 9) === "stalled" && L.etaState(0.01, 9, 0) === "estimating" &&
  L.etaState(0.99, 9, 0) === "almost" && L.etaState(0.5, 9, 0) === "time");

console.log(ok ? `ALL ${n} UNIT TESTS PASSED` : "UNIT TESTS FAILED");
process.exit(ok ? 0 : 1);
