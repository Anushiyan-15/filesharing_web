// Minimal TURN Allocate probe over TCP (test-only; creds via argv, never logged).
// Usage: node turn-probe.tmp.cjs <host> <port> <user> <pass>
const net = require("net");
const crypto = require("crypto");
const MAGIC = 0x2112A442;
const [HOST, PORT, USER, PASS] = [process.argv[2], parseInt(process.argv[3], 10), process.argv[4], process.argv[5]];

function attr(type, val) {
  const pad = (4 - (val.length % 4)) % 4;
  const h = Buffer.alloc(4);
  h.writeUInt16BE(type, 0); h.writeUInt16BE(val.length, 2);
  return Buffer.concat([h, val, Buffer.alloc(pad)]);
}
function buildAllocate({ username, realm, nonce, password }) {
  const transId = crypto.randomBytes(12);
  const parts = [];
  if (username) parts.push(attr(0x0006, Buffer.from(username, "utf8")));
  if (realm) parts.push(attr(0x0014, Buffer.from(realm, "utf8")));
  if (nonce) parts.push(attr(0x0015, Buffer.from(nonce, "utf8")));
  parts.push(attr(0x0019, Buffer.from([0x11, 0, 0, 0])));
  const body = Buffer.concat(parts);
  const hdr = Buffer.alloc(20);
  hdr.writeUInt16BE(0x0003, 0);
  hdr.writeUInt16BE(body.length + (username ? 24 : 0), 2);
  hdr.writeUInt32BE(MAGIC, 4);
  transId.copy(hdr, 8);
  if (!username) return { transId, msg: Buffer.concat([hdr, body]) };
  const key = crypto.createHash("md5").update(`${username}:${realm}:${password}`).digest();
  const mac = crypto.createHmac("sha1", key).update(Buffer.concat([hdr, body])).digest();
  return { transId, msg: Buffer.concat([hdr, body, attr(0x0008, mac)]) };
}
function parseMessage(buf) {
  const attrs = {};
  const len = buf.readUInt16BE(2);
  let off = 20;
  while (off + 4 <= 20 + len) {
    const at = buf.readUInt16BE(off), al = buf.readUInt16BE(off + 2);
    attrs[at] = buf.subarray(off + 4, off + 4 + al);
    off += 4 + al + ((4 - (al % 4)) % 4);
  }
  return { type: buf.readUInt16BE(0), transId: buf.subarray(8, 20), attrs };
}
function transact(sock, req, timeoutMs) {
  return new Promise((resolve, reject) => {
    let acc = Buffer.alloc(0);
    const to = setTimeout(() => { sock.off("data", onData); reject(new Error("timeout")); }, timeoutMs);
    function onData(chunk) {
      acc = Buffer.concat([acc, chunk]);
      while (acc.length >= 20) {
        const len = acc.readUInt16BE(2);
        if (len > 4096 || acc.length < 20 + len) return;
        const raw = acc.subarray(0, 20 + len);
        acc = acc.subarray(20 + len);
        const p = parseMessage(raw);
        if (p.transId.equals(req.transId)) { clearTimeout(to); sock.off("data", onData); resolve(p); return; }
      }
    }
    sock.on("data", onData);
    sock.write(req.msg);
  });
}
(async () => {
  const t0 = Date.now();
  const sock = new net.Socket();
  await new Promise((res, rej) => { sock.once("error", rej); sock.connect(PORT, HOST, res); setTimeout(() => rej(new Error("connect timeout")), 10000); });
  const rttConnect = Date.now() - t0;
  sock.on("error", () => {});
  const r1 = await transact(sock, buildAllocate({}), 10000);
  const e1 = r1.attrs[0x0009];
  const c1 = e1 ? e1[2] * 100 + e1[3] : -1;
  if (c1 !== 401) throw new Error("expected 401 challenge, got " + r1.type.toString(16) + "/" + c1);
  const realm = r1.attrs[0x0014].toString(), nonce = r1.attrs[0x0015].toString();
  const t1 = Date.now();
  const r2 = await transact(sock, buildAllocate({ username: USER, realm, nonce, password: PASS }), 10000);
  const allocMs = Date.now() - t1;
  if (r2.type !== 0x0103) {
    const e = r2.attrs[0x0009];
    throw new Error("allocate failed: " + (e ? e[2] * 100 + e[3] : r2.type.toString(16)));
  }
  const rel = r2.attrs[0x0016];
  let relay = "unknown";
  if (rel && rel.length >= 8) {
    const port = rel.readUInt16BE(2) ^ (MAGIC >>> 16);
    const m = [(MAGIC >>> 24) & 255, (MAGIC >>> 16) & 255, (MAGIC >>> 8) & 255, MAGIC & 255];
    const ip = [0, 1, 2, 3].map((i) => rel[4 + i] ^ m[i]).join(".");
    relay = `${ip}:${port}`;
  }
  console.log(JSON.stringify({ host: HOST, port: PORT, tcpConnectMs: rttConnect, allocateMs: allocMs, relayedAddress: relay, ok: true }));
  sock.destroy();
})().catch((e) => { console.error("TURN PROBE FAIL:", e.message); process.exit(1); });
