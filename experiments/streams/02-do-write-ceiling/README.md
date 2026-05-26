# 02-do-write-ceiling

What is the **absolute maximum append throughput into one Durable Object** when we strip away everything except SQLite writes?

This experiment isolates two ceilings:

1. **`in-do-loop`** — `WriteSink.writeLoop()` runs a tight synchronous loop inside the DO (no cross-DO RPC, no WebSocket framing).
2. **`ws-from-runner`** — `WsBenchmarkRunner` opens a WebSocket to `WriteSink` and pumps fire-and-forget `{ op: "append", event }` frames (same path as experiment 00’s external WS benchmark, but colocated on the edge).

Use this to establish **upper bounds** before adding stream processors, idempotency checks, or multi-DO fanout.

## Context from experiment 00

External laptop WebSocket to one stream DO (256-byte payload):

| Messages | Send rate | Verified committed |
|----------|-----------|-------------------|
| 5,000 | ~5,726/s over ~0.87s | yes (after 8s drain) |

That was **not** a long sustained run — it was a short burst. This experiment sweeps message counts and payload sizes to find where throughput plateaus or falls off.

## Comparison baseline: Grok Voice Agent API

Typical inbound event: `input_audio_buffer.append` with **base64 PCM16 @ 24 kHz**, **~100 ms chunks** (xAI recommendation) → **~6.5 KB WS JSON**, **~10 appends/s**, **~65 KB/s** sustained.

Our old **256 B** benchmark point is ~19× smaller than a real Grok frame. Compare at **`payloadBytes=4800`** (~4.5k/s DO capacity) instead. See [log.md](./log.md).

## Headline result (production, in-DO sync loop)

Using `@cf-experiments/shared` `writeEvent` (`variant=shared`), one DO appends to SQLite at:

- **~15–17k events/s** (256-byte payload, 100k events, repeated runs)
- **~5–5.5 MB/s** stored JSON at that event size

See [log.md](./log.md) for repeated runs, payload sweeps, and schema variants.

## What drives events/s

| Factor | Effect |
|--------|--------|
| Payload size | events/s flat 0–256 B (~16–19k/s shared); knee at 512 B–1 KiB; MB/s plateaus ~16–18 MB/s at 2–4 KiB |
| `select max(offset)` per write | **~3×** — biggest cost in `writeEvent` |
| `json_valid` + multi-column row | Moderate vs bare blob insert |
| `idempotency_key unique` index | Present on `shared`; unused in ceiling runs |

**Sweet spot (shared):** ~**256 B** payload → ~**17k/s, ~5.3 MB/s**. Max volume → 2–4 KiB payloads (~**17 MB/s**, ~4–8k/s). Max events/s → 0–256 B (~**16–19k/s**).

Use `variant=autoinc` or `tiny` in `/write-loop` to isolate overhead — not production schema, but shows headroom (~3× shared).

## Components

| Class | Role |
|-------|------|
| `WriteSink` | Minimal SQLite append sink (`@cf-experiments/shared/event`) |
| `WsBenchmarkRunner` | Edge DO that WS-floods a named `WriteSink` |

## Routes

| Route | Meaning |
|-------|---------|
| `POST /write-loop?name=sink&messages=N&payload-bytes=B&variant=shared` | Sync in-DO write loop (`shared` \| `autoinc` \| `blob` \| `tiny`) |
| `POST /ws-benchmark?name=sink&messages=N&payload-bytes=B&drain-ms=D` | Runner DO → WS → WriteSink |
| `GET /count?name=sink` | SQLite row count |
| `GET /stream?name=sink` | WebSocket upgrade (external client) |

## How to run

```bash
pnpm dev
pnpm ceiling http://localhost:8787 --messages 10000,50000 --payload-bytes 256
pnpm deploy
pnpm ceiling https://02-do-write-ceiling.iterate-dev-preview.workers.dev --messages 10000,50000,100000
```

Record results in [log.md](./log.md).

## How to read results

Each JSON line includes:

- `wallMs` / `wallPerSecond` — **trust these in production** (HTTP round-trip; timers inside CPU-bound DO loops lie)
- `eventsPerSecond` / `commitPerSecond` — in-DO timers (Miniflare OK; production often wrong)
- `serverCount` — committed SQLite rows
- `verified` — all messages committed

For WS mode: `dispatchPerSecond` is send-only; `commitPerSecond` includes drain polling until count matches.

## Open questions

- Does in-DO loop plateau below SQLite’s theoretical max?
- At what message count / payload size does WS throughput drop?
- Is ~5,700/s a burst ceiling or sustainable for 60s+?
- When does SQLite `SQLITE_BUSY` or DO CPU limits kick in?
