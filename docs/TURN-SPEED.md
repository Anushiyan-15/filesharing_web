# TURN Speed — measurements, tuning, and honest limits

Environment used for every measurement below: Windows 11, Node.js v22.19.0,
loopback (127.0.0.1), single machine. **Loopback numbers characterize the
software path, not Internet performance.** No browser, coturn, iperf3, or
second machine exists in this environment, so all browser/coturn rows are
marked UNTESTED with the exact blocker and the command to run later.

## 1. Baseline (before changes) — measured 2026-10-15

| Test | Before result | Notes |
|---|---|---|
| Raw TCP throughput | **~535 MB/s** (100 MiB loopback) | Machine baseline; absolute ceiling for any local path |
| Raw UDP throughput and loss | UNTESTED | No iperf3 in environment |
| WS relay (base64 32 KiB chunks) | **~40 MB/s**, integrity intact (8 MiB) | `test/relay-bench.cjs`; JSON+base64 cost dominates, not the socket |
| TURN TCP allocate (ExpressTURN) | **reachable**: TCP connect ~2980 ms, allocate ~652 ms, relay port issued | `scripts/turn-probe.cjs`; real Internet measurement from this host |
| Browser TURN transfer | UNTESTED | No browser/Chromium/Playwright in environment |
| Browser direct transfer | UNTESTED | Same blocker |
| Peak aggregate throughput | UNTESTED | Requires ≥2 peers + browser automation |
| CPU utilization | Observed only: bench RSS ~50–70 MiB | No profiling harness; loopback tests are not CPU-bound |
| Memory utilization | WS relay bench RSS ~53 MiB for 4 MiB file | Bounded by design (see §5) |
| Packet loss | UNTESTED | Loopback is lossless by construction; meaningless to report |
| Transfer reliability | PASS (byte-identical, 23 framing unit tests) | `test/framing.test.cjs` |

Raw evidence: `npm test` output (below) plus `test/relay-bench.cjs`.

## 2. What changed for speed (after)

| Change | Expected effect | Status |
|---|---|---|
| Striped v2 frames, 64 KiB payload + 8 B header, K parallel PeerConnections | Uses N independent SCTP congestion windows (multiple DataChannels on one PC would share one) | Implemented, browser-UNTESTED |
| Per-channel backpressure (`bufferedAmountLowThreshold` + event, no polling) + 16 MiB aggregate cap | No polling loops; slow path never blocks healthy paths | Implemented, browser-UNTESTED |
| WS relay unchanged (base64) | No regression by design | Measured: **~42 MB/s** (same path, within noise of 40.4 baseline) |
| UDP-first TURN URL order in `/api/ice` | ICE tries UDP relay before TCP/TLS | Implemented, unit-verified (config test) |
| Short-lived REST credentials | Zero steady-state cost; one HMAC per fetch | Implemented, verified |
| CRC32 per chunk + combined whole-file check | Catches corruption; costs ~1–3% CPU (pure-JS CRC ≈ 300 MB/s) | Unit-verified (known-answer + combine property) |

After-result browser rows: UNTESTED (same blocker as baseline).

## 3. TURN configuration actually used

This deployment uses a managed TURN provider plus any self-hosted coturn via
env — there is **no local coturn to tune**, so no `turnserver.conf` is shipped
(shipping unverified coturn options would violate the no-fabrication rule).

- `TURN_URLS` — comma-separated; server validates scheme and publishes
  deterministic UDP-first order: `?transport=udp` → bare `turn:` →
  `?transport=tcp` → `turns:`. See `.env.example`.
- Auth: `TURN_SECRET` (coturn `static-auth-secret`, time-limited
  `<expiry>:<id>` + base64 HMAC-SHA1) preferred; else `TURN_USER`/`TURN_PASS`
  static. The secret never leaves the server process (tested).
- Browsers fetch config from `GET /api/ice` (`Cache-Control: no-store`).
- `GET /api/turn-check` reports presence/validity/hostnames only.
- UDP-first verification at runtime: connection details show the selected
  relay `protocol` from `getStats()` (`udp`/`tcp`/`unknown` — never guessed).
  If TURN mode lands on TCP while UDP was configured, the log carries a
  warning with the retry path.

## 4. Firewall / deployment ports

Open only what the selected configuration needs (host firewall **and** cloud
security group — one does not update the other):

- TCP 3000 (or `$PORT`) — app + signaling (HTTPS via platform in production)
- UDP 3478 — TURN allocation/media (self-hosted coturn)
- TCP 3478 — TURN over TCP fallback
- TCP 5349 — TURN over TLS
- TCP 443 — TURN TLS fallback where a cert + SNI route exist
- UDP relay range (e.g. 49152–65535 or the range in your coturn config)

Windows note: no `sysctl`/systemd applies here; the Node process needs no
special privileges. For a Linux coturn host, start from: `net.core.rmem_max /
wmem_max` ≥ 16 MiB, `netdev_max_backlog` ≥ 5000, file limit ≥ 65535, and only
enable BBR/`fq` after `sysctl` confirms support (BBR helps TCP paths only —
it does not accelerate UDP relay; do not claim otherwise).

## 5. Bounds that keep large transfers safe

- WS inbound message cap 1 MiB (`maxPayload`; oversize → close 1009, tested).
- Relay chunk base64 ≤ ~512 KiB; meta name ≤ 255 chars; size ≤ 8 GiB;
  chunk index bounds-checked; unknown kinds rejected (tested).
- Per-destination server queue cap 16 MiB → `{type:'pressure'}` + drop;
  relay pumps restart and resume (receiver dedupes by index).
- Inbound rate cap 5000 msg/s per socket → close 1013.
- Sender: 64 KiB slices read on demand (never whole file); receiver keeps the
  standard Blob assembly (browser limit, documented); NACK lists capped at
  1000; UI event logs capped at 120 entries; chart capped at 120 samples.

## 6. Security review (implemented + known limits)

- TURN secret removed from `public/app.js` (was hardcoded); served only via
  `/api/ice`, never logged, `no-store`, rotates per fetch in REST mode.
- Signaling has no authentication: a 6-digit code is **discoverability, not
  auth**. Anyone who guesses an active code can join. Documented in README;
  do not use for secrets without adding real access control.
- No CORS change needed (same-origin app); API returns JSON only.
- Integrity is corruption-detection (CRC32), not tamper-proofing; transport
  confidentiality comes from DTLS (P2P) / TLS (signaling, when deployed
  behind HTTPS) — the UI never claims more.
- Cant-fix-here: malicious sender nicknames are rendered as text
  (`textContent`, not HTML — safe); file names likewise.

## 7. Browser benchmark procedure (for a machine WITH browsers)

```bash
# Two Chromium/Chrome profiles on two machines (or one machine for smoke):
# 1. Serve: npm start (set TURN_URLS + TURN_SECRET or TURN_USER/TURN_PASS)
# 2. Sender tab:  mode=TURN relay, pick GENERATED file:
#    Linux: head -c 500M /dev/urandom > /tmp/t500m.bin   (Windows: fsutil file createnew)
# 3. Receiver tab: join code, force relay mode as well.
# 4. Record: elapsed, MB/s (UI, decimal MB), candidate pair from Connection
#    details, relay protocol, fin integrity line, SHA-256 of both files:
#    Linux: sha256sum /tmp/t500m.bin ~/Downloads/t500m.bin
# 5. Repeat in Auto/direct mode; compare same machines + same network only.
```

## 8. Raw evidence (this environment)

```
TCP loopback: 536.2 MB/s (100 MiB in 0.20s)
WS relay (base64 32KB): 40.4 MB/s (8 MiB in 0.21s) intact=true      # before
{"host":"free.expressturn.com","port":3478,"tcpConnectMs":2980,
 "allocateMs":652,"relayedAddress":"51.158.147.206:44890","ok":true} # TURN live
npm test: 23 framing + 17 config + 12 signaling PASSED;
          relay: 42.4 MB/s, intact=true                            # after
```

## 9. Skipped deliverables (with reasons)

- `turnserver.conf`, `turnserver.test.conf`, `scripts/tune-turn.sh`,
  `docker-compose.turn.yml`, systemd override, sysctl template: **no coturn
  binary exists in this environment, so every option value would be
  unverified**; the deployment target is managed Node hosting + managed
  TURN. Creating them would be fabrication (§4 firewall + §3 config cover
  what a deployer needs). Revisit when a Linux coturn host exists — verify
  each option with `turnserver --help` / `man turnserver` first.
- 500 MB browser benchmark, Playwright tests: **no browser in environment**.
- `iperf3`/UDP baselines: **binary unavailable**.
