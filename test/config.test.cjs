// Server config tests: /api/ice + /api/turn-check across env configurations.
// Verifies credential handling WITHOUT ever logging secrets.
const http = require("http");
const crypto = require("crypto");
const { spawn } = require("child_process");

const wait = (ms) => new Promise((r) => setTimeout(r, ms));
function get(port, path) {
  return new Promise((res, rej) => {
    http.get({ host: "127.0.0.1", port, path }, (r) => {
      let d = "";
      r.on("data", (c) => (d += c));
      r.on("end", () => res({ status: r.statusCode, headers: r.headers, body: d }));
    }).on("error", rej);
  });
}
async function waitReady(port) {
  for (let i = 0; i < 100; i++) {
    try {
      await get(port, "/health");
      return;
    } catch { await wait(200); }
  }
  throw new Error("server did not start on " + port);
}
function startServer(port, env) {
  return spawn(process.execPath, ["server.js"], { stdio: "ignore", env: { ...process.env, PORT: String(port), ...env } });
}

let ok = true, n = 0;
function check(name, cond, extra) {
  n++;
  console.log((cond ? "PASS" : "FAIL") + " - " + name + (extra ? " (" + extra + ")" : ""));
  if (!cond) ok = false;
}

(async () => {
  // --- Case 1: no TURN configured ---
  let srv = startServer(4201, { TURN_URLS: "", TURN_USER: "", TURN_PASS: "", TURN_SECRET: "" });
  await waitReady(4201);
  let ice = JSON.parse((await get(4201, "/api/ice")).body);
  let tc = JSON.parse((await get(4201, "/api/turn-check")).body);
  check("no-turn: ice STUN only", ice.iceServers.length === 1 && ice.turn === false);
  check("no-turn: turn-check reports missing", tc.configured === false && typeof tc.error === "string");
  check("no-turn: no-store header", (await get(4201, "/api/ice")).headers["cache-control"] === "no-store");
  srv.kill(); await wait(400);

  // --- Case 2: static credentials ---
  srv = startServer(4202, {
    TURN_URLS: "turn:t.example.com:3478?transport=tcp,turn:t.example.com:3478?transport=udp,garbage-no-scheme",
    TURN_USER: "alice", TURN_PASS: "s3cret", TURN_SECRET: "",
  });
  await waitReady(4202);
  ice = JSON.parse((await get(4202, "/api/ice")).body);
  tc = JSON.parse((await get(4202, "/api/turn-check")).body);
  check("static: turn on", ice.turn === true);
  check("static: UDP-first ordering", ice.iceServers[1].urls[0].includes("transport=udp"));
  check("static: malformed URL dropped", !JSON.stringify(ice.iceServers).includes("garbage"));
  check("static: creds present for browser", ice.iceServers[1].username === "alice" && ice.iceServers[1].credential === "s3cret");
  check("static: turn-check leaks nothing", tc.configured === true && !JSON.stringify(tc).includes("s3cret") && !JSON.stringify(tc).includes("alice") && tc.hosts.join() === "t.example.com");
  check("static: ttl 0 (non-expiring)", ice.ttl === 0);
  srv.kill(); await wait(400);

  // --- Case 3: REST shared secret ---
  const SECRET = "test-shared-secret-xyz";
  srv = startServer(4203, {
    TURN_URLS: "turn:r.example.com:3478",
    TURN_SECRET: SECRET, TURN_TTL: "600", TURN_REALM: "testrealm",
  });
  await waitReady(4203);
  ice = JSON.parse((await get(4203, "/api/ice")).body);
  tc = JSON.parse((await get(4203, "/api/turn-check")).body);
  const entry = ice.iceServers[1] || {};
  check("rest: turn on", ice.turn === true && ice.ttl === 600);
  const parts = String(entry.username || "").split(":");
  const exp = parseInt(parts[0], 10);
  const nowS = Math.floor(Date.now() / 1000);
  check("rest: username format expiry:id", parts.length === 2 && exp > nowS && exp <= nowS + 600);
  const expectPass = crypto.createHmac("sha1", SECRET).update(entry.username).digest("base64");
  check("rest: password = HMAC-SHA1(secret, username)", entry.credential === expectPass);
  check("rest: secret never leaves server", !JSON.stringify(ice).includes(SECRET) && !JSON.stringify(tc).includes(SECRET));
  check("rest: turn-check hosts only", tc.configured === true && tc.hosts.join() === "r.example.com");
  // second fetch -> different time-limited credentials (proves rotation)
  const ice2 = JSON.parse((await get(4203, "/api/ice")).body);
  check("rest: credentials rotate", ice2.iceServers[1].username !== entry.username);
  srv.kill(); await wait(400);

  // --- Case 4: URLs but no credentials -> clear error, no turn ---
  srv = startServer(4204, { TURN_URLS: "turn:q.example.com:3478", TURN_USER: "", TURN_PASS: "", TURN_SECRET: "" });
  await waitReady(4204);
  ice = JSON.parse((await get(4204, "/api/ice")).body);
  tc = JSON.parse((await get(4204, "/api/turn-check")).body);
  check("nocreds: turn off with error", ice.turn === false && typeof ice.error === "string" && tc.configured === false);
  srv.kill(); await wait(400);

  // --- Case 5: health still fine everywhere ---
  srv = startServer(4205, {});
  await waitReady(4205);
  const h = JSON.parse((await get(4205, "/health")).body);
  check("health ok", h.ok === true && h.maxReceivers === 5);
  srv.kill();

  console.log(ok ? `ALL ${n} CONFIG TESTS PASSED` : "CONFIG TESTS FAILED");
  process.exit(ok ? 0 : 1);
})().catch((e) => { console.error("CONFIG TEST FAIL:", e.message); process.exit(1); });
