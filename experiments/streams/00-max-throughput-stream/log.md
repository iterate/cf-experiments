# Findings

**RPC pipelining (`rpc-pipelined`)** — fire all `stub.append()` without await, `Promise.all` at end — on deployed: **70 → 884/s** (1 runner), **448 → 3,763/s** (8 runners). ~**12×** vs serial. Miniflare unchanged ~10k/s (already colocated). Serial DO→DO ≈ 14 ms; pipelining overlaps in-flight RPCs, not SQLite work.

**BenchmarkRunner fanout** (serial): deployed 8 → 448/s, 20 → 826/s, 50 → 2,152/s aggregate. Per-runner serial ~70/s (~14 ms/append).

---

## 2026-05-22 — RPC pipelined vs serial (BenchmarkRunner)

Mode `rpc-pipelined`: loop `pending.push(stub.append(...))` with no await; `dispatchMs` = time to queue all calls; `elapsedMs` = until all resolve.

1000 appends, 256-byte payload, v `31136118`.

| Environment | Config | Serial | Pipelined | Speedup |
| --- | --- | ---: | ---: | ---: |
| Deployed | 1 runner | 70/s (14.3s) | **884/s** (1.1s, dispatch 0ms) | **12.6×** |
| Deployed | 8 runners | 410/s aggregate | **3,763/s** aggregate | **9.2×** |
| Miniflare | 1 runner | 10,417/s | 10,101/s | ~1× |
| Miniflare | 8 runners | 10,417/s aggregate | 8,511/s aggregate | ~0.8× |

All runs: `committed=1000`, `serverCount=1000` per stream.

**Interpretation:**

- Serial `await` measures full round-trip per append (~14 ms deployed) — artificially limits throughput.
- Pipelining queues hundreds of RPCs instantly (`dispatchMs≈0`); stream DO still processes one append at a time, but caller doesn't wait for each reply before sending the next.
- Deployed pipelined ~884/s ≈ **1.1 ms effective** throughput (1000/1.1s) — not 1.1 ms SQLite; it's overlapped RPC delivery.
- Miniflare already at ~10k/s serial (colocated); pipelining adds nothing locally.
- For max acknowledged throughput on deployed, **pipeline RPCs** (or use fanout + pipelining).

```bash
node scripts/benchmark-runners.ts https://00-max-throughput-stream.iterate-dev-preview.workers.dev \
  --runners 1 --messages 1000 --mode rpc-pipelined --stream pipe-test
```

---

## 2026-05-22 — BenchmarkRunner fanout scaling (8 / 20 / 50 runners)

1000 appends per runner, 256-byte payload, `rpc-serial`, separate stream per runner (`scale-*-N`).

| Runners | Deployed aggregate | Deployed per-runner (median) | Local aggregate | Local per-runner (median) |
| ---:| ---:| ---:| ---:| ---:|
| 8 | 448/s | ~80/s (~12 ms) | 9,816/s | ~1,240/s |
| 20 | **826/s** | ~71/s (~14 ms) | 9,390/s | ~473/s |
| 50 | **2,152/s** | ~72/s (~14 ms) | 8,821/s | ~178/s |

Deployed 50-runner wall time: **23.2s** (50 × 1000 committed). Per-runner range at 50: 47–105/s.

**Interpretation:**

- Deployed aggregate grows sub-linearly (50 runners ≈ 4.8× throughput of 8, not 6.25×) — scheduling/contention.
- Per-runner latency stable ~10–15 ms regardless of fanout size; ~72/s ≈ 14 ms confirms serial RPC math.
- Miniflare aggregate caps ~9k/s; more runners just split that budget (per-runner rate falls).
- Fanout helps **many streams in parallel**, not single-stream speed.

```bash
node scripts/benchmark-runners.ts https://00-max-throughput-stream.iterate-dev-preview.workers.dev \
  --runners 50 --messages 1000 --stream-prefix scale-edge-50
```

---

## 2026-05-22 — BenchmarkRunner (edge-side)

Runner DO calls `SuperSimpleStream.append()` via stub — no HTTP per append, only one trigger request from outside.

| Environment | Config | Per-runner | Aggregate | Notes |
| --- | --- | ---: | ---: | --- |
| Miniflare | 1 runner × 1000 | **9,901/s** | — | 101ms wall |
| Miniflare | 8 runners × 1000 | ~1,240/s each | **9,816/s** | parallel streams `bench-fan-0…7` |
| Deployed | 1 runner × 1000 | **67/s** | — | v `fa2a9a05` |
| Deployed | 8 runners × 1000 | ~65–103/s each | **448/s** | 8000 committed |

Compare to **external** `POST /append` from laptop (deployed): **~20/s** — edge runner is ~3× faster single-stream; fanout scales aggregate throughput without client round-trips per append.

```bash
pnpm benchmark:runners http://localhost:8789 --runners 8 --messages 1000 --stream-prefix bench
pnpm benchmark:runners https://00-max-throughput-stream.iterate-dev-preview.workers.dev --runners 8 --messages 1000 --stream-prefix bench-edge
```

Modes: `rpc-serial` (default), `rpc-batch` (`--batch-size 100`).

---

## 2026-05-22 — compare run (Miniflare vs deployed, external client)

**Worker:** `00-max-throughput-stream` v `96a2bb9c-e1ea-4d08-aed2-ef0ff193ff2d`  
**Command:** `pnpm benchmark <url> --mode compare --messages 5000 --verify --drain-ms 8000` (deployed) / default 3s drain (local)

| Environment | WS `eventsPerSecond` | RPC `eventsPerSecond` | RPC wall time | `serverCount` |
| --- | ---: | ---: | ---: | ---: |
| Miniflare (`localhost:8789`) | 92,919 | **926** | 5.4s | 5000 / 5000 |
| Deployed (`iterate-dev-preview`) | 5,726 | **19.5** | 256s | 5000 / 5000 |

**Notes:**

- RPC is serial `POST /append` with awaited response each time — real acknowledged throughput.
- WS is fire-and-forget client send rate; `--verify` confirms DO SQLite count after drain.
- Deployed RPC ~20/s is the practical ceiling for acknowledged appends from outside Cloudflare on this setup.
- Miniflare colocates worker+DO; deployed pays network + scheduling per hop (cf-ray `9ffc580ffaaa3eb9-LHR` on last RPC).

---

## 2026-05-22 — benchmark smoke (Miniflare)

Environment: `pnpm dev` → `http://localhost:8789`, payload 256 bytes, `SuperSimpleStream` SQLite.

| Mode | Messages | eventsPerSecond | Notes |
| --- | --- | --- | --- |
| `ws-fire-forget` | 500 | **20,955** | `serverCount: 500` after 3s drain — all committed |
| `rpc-serial` | 50 | **442** | round-trip `POST /append` per event |

---

## 2026-05-22 — experiment scaffold

Added `SuperSimpleStream` DO: sync inline SQLite `insert … returning offset`, WebSocket + RPC routes. No sqlfu, no memory mode, minimal schema.
