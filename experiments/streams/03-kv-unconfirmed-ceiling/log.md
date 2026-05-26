# log

Production: `https://03-kv-unconfirmed-ceiling.iterate-dev-preview.workers.dev`  
Worker version: `d0354836-081b-4572-8701-66fd5c21218e` (2026-05-26 extended tests)

## API

| Route | Purpose |
|-------|---------|
| `POST /write-loop?sync=0` | Append loop; **no** `storage.sync()` unless `sync=1` |
| `POST /append` | More appends on **same DO** without sync |
| `POST /flush` | Manual `await storage.sync()` — **you control durability boundary** |
| `POST /pressure` | One-shot max messages until in-DO error |
| `flush-every=N` | Sync every N appends during loop |

`metaCount` reads `stream:meta:nextOffset` from sync KV — includes unconfirmed appends in-process.

---

## 1. No-sync throughput (`sync=0`, kv-unconfirmed)

100k events, no flush until separate `/flush` call:

| payload | wall/s (run 1 / 2) | vs prior test (sync at end) |
|---------|-------------------|----------------------------|
| 256 B | **12,322 / 7,100** | was ~10k all-in with sync |
| 4800 B | **~5,240** | was ~4,925 all-in with sync |

**Without end sync, loop runs at full gate-skipped rate.** Separate `/flush` returned in ~37–100 ms wall (DO `syncMs` reports 0 — timer bug — but metaCount already 100k).

500k @ 256 B in **one RPC**: **1101 crash** (CPU limit on single invocation, not memory).

---

## 2. Controlled flush (`flush-every`, 100k × 4800 B)

| flush-every | loop+sync ms | rate | notes |
|-------------|--------------|------|-------|
| 0 (never during loop) | wall ~19s | **5,239/s** | buffer until manual flush |
| 1,000 | 16,102 | **6,210/s** | 100 syncs |
| 10,000 | 12,651 | **7,905/s** | **sweet spot** — 10 syncs |
| 100,000 | 19,005 | **5,262/s** | same as sync-at-end |

Periodic flush **increases sustained rate** by not letting the unconfirmed buffer grow unbounded through the whole run. **`flush-every=10000`** (~10s of Grok audio @ 10 appends/s) is a good starting point for production.

---

## 3. Memory / buffer limit (same DO, repeated `/append`, no sync)

4800 B payload, 50k per batch, `sync=0`:

| Milestone | totalMeta | wall/s per batch |
|-----------|-----------|------------------|
| batch 1 | 50k | ~5,200/s |
| batch 20 | 1.0M | ~4,900/s |
| batch 41 | **2.05M** | ~5,523/s |
| batch 42 | **1101 crash** | — |

**~2.05 million unconfirmed events** (~9.8 GB logical payload if fully materialised) on one DO before crash. Throughput **did not degrade** until failure — the limit is a cliff (OOM / storage pressure / isolate reset), not gradual slowdown.

After crash, same DO name still had `metaCount: 2,050,000` and `/flush` returned quickly — data likely partially on SRS already; batch 42 could not start.

Single `/pressure` with `max-messages=2M`: **1101** (CPU / wall limit on one invocation).

---

## Findings summary

| Question | Answer |
|----------|--------|
| How fast without end sync? | **~5.2k/s @ 4.8 KB**, **~7–12k/s @ 256 B** (100k batches) |
| Does skipping sync help vs sync-at-end? | **Yes for loop rate**; all-in similar unless you never sync |
| Best controlled flush | **`flush-every=10000`** → **~7.9k/s @ 4.8 KB** |
| How much unconfirmed buffer? | **~2M events @ 4.8 KB** on one DO before 1101 |
| Grok load (~10/s × 6.5 KB) | trivial vs 5k+/s; flush every 10k events ≈ 16 min of Grok audio |

---

## Reproduce

```bash
cd experiments/streams/03-kv-unconfirmed-ceiling
pnpm deploy
pnpm test:no-sync
pnpm test:flush
pnpm test:pressure
```
