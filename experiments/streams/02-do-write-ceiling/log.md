# log

Results for 02-do-write-ceiling. **Production only** — use `wallMs` / `wallPerSecond` from the client script. All sweeps run against `https://02-do-write-ceiling.iterate-dev-preview.workers.dev`.

---

## Comparison baseline: Grok Voice Agent API (`input_audio_buffer.append`)

We compare DO append throughput to **xAI Grok Realtime Voice** — the workload we're building for.

Sources: [xAI Voice Agent API](https://docs.x.ai/developers/model-capabilities/audio/voice-agent), [Voice REST/WebSocket reference](https://docs.x.ai/developers/rest-api-reference/inference/voice), [Pipecat Grok integration](https://github.com/pipecat-ai/pipecat/blob/main/src/pipecat/services/grok/realtime/llm.py).

### Protocol

| | |
|---|---|
| Endpoint | `wss://api.x.ai/v1/realtime` |
| Client event | `input_audio_buffer.append` |
| Audio format | PCM16 (Linear16 LE), mono |
| Default sample rate | **24 kHz** (also 8–48 kHz supported) |
| Encoding on wire | **base64 string** inside JSON (not binary frames) |
| xAI guidance | *"Flush in reasonably sized messages (**100 ms samples** each) for smooth transmission"* |

### Typical frame sizes

Raw PCM16 bytes per chunk = `sample_rate × duration × 2`:

| Chunk duration | Raw PCM @ 24 kHz | Base64 `audio` field | Full WS JSON message | Arrival rate |
|----------------|------------------|----------------------|----------------------|--------------|
| 20 ms | 960 B | ~1,280 B | **~1.3 KB** | **~50/s** |
| 50 ms | 2,400 B | ~3,200 B | **~3.2 KB** | **~20/s** |
| **100 ms (recommended)** | **4,800 B** | **~6,400 B** | **~6.5 KB** | **~10/s** |

Full message shape:

```json
{"type":"input_audio_buffer.append","audio":"<base64 PCM16 chunk>"}
```

Fixed JSON overhead ≈ 47 B + base64 audio.

### Sustained inbound volume (continuous speech)

| Chunking | Events/s | Wire bytes/s (WS JSON) | Raw PCM equivalent |
|----------|----------|------------------------|--------------------|
| 100 ms | ~10 | **~65 KB/s** | 48 KB/s |
| 50 ms | ~20 | ~65 KB/s | 48 KB/s |
| 20 ms | ~50 | ~65 KB/s | 48 KB/s |

Chunk duration trades **event rate** for **event size**; total audio bandwidth is the same (~48 KB/s PCM at 24 kHz mono).

### Mapping to our benchmark `payloadBytes`

Our sweep uses a JSON `StreamEvent` with a `pad` field. Grok-equivalent **`payloadBytes`** values (audio data only, excluding our event envelope):

| Grok scenario | `payloadBytes` to use in sweep | Why |
|---------------|-------------------------------|-----|
| 100 ms chunk (recommended) | **4800** (raw PCM) or **6400** (base64) | Matches one append's audio payload |
| 50 ms chunk | 2400 / 3200 | |
| 20 ms chunk | 960 / 1280 | |

**Previous 256 B baseline was ~19× smaller than a typical Grok append** (~4.8 KB raw / ~6.5 KB on wire). Comparing at 256 B overstated events/s relevance and understated bytes/event.

### DO capacity vs Grok load (production `shared`, from sweep below)

| | Grok (100 ms chunks) | DO capacity @ ~4.8 KB payload | Headroom |
|---|----------------------|-------------------------------|----------|
| Events/s | ~10 | ~4,500 (4096 B point; ~4.8 KB similar) | **~450×** |
| Data rate | ~65 KB/s wire | ~18 MB/s stored | **~280×** |

**Finding:** A single `WriteSink` DO is not the bottleneck for Grok-scale voice ingest. The realistic operating point is **~10 events/s × ~5–7 KB**, not 17k/s × 256 B. Our ceiling experiment's **256 B / 17k/s headline** describes SQLite row throughput for tiny events; the **Grok-relevant point** on the same curve is **~4–5k/s at 4–5 KB (~17–18 MB/s)** — still orders of magnitude above one voice stream.

---

## Headline: in-DO sync loop (`shared` / `@cf-experiments/shared` `writeEvent`)

**One Durable Object can append stream events to SQLite at ~15–17k events/s for small payloads (~256 B), or ~4.5k events/s at Grok-like ~4 KB payloads (~18 MB/s stored).**

For **Grok voice comparison**, use the **~4 KB / ~4.5k/s** point — not the 256 B headline.

This uses the real `writeEvent` path: `select max(offset)` per write, multi-column insert (`offset`, `type`, `idempotency_key`, `raw_event`), `json_valid` check, and the `idempotency_key unique` index (unused in these runs but present).

### Repeated runs (production, 2026-05-26)

100k events, 256-byte payload, `variant=shared`, fresh DO per run:

| Run | wallMs | events/s | MB/s (est.) | verified |
|-----|--------|----------|-------------|----------|
| 1 | 13,050 | 7,663 | 2.46 | yes |
| 2 | 7,206 | 13,877 | 4.45 | yes |
| 3 | 6,750 | 14,814 | 4.75 | yes |
| 4 | 5,796 | 17,254 | 5.53 | yes |
| 5 | 8,130 | 12,299 | 3.94 | yes |

**Summary:** median **~14,800/s**, best **~17,250/s**, mean **~13,200/s**. Run 1 likely cold-start / first SQLite pages; steady state **~13–17k/s**.

At the best run: 100k events × ~336 B stored ≈ **33.6 MB in 5.8 s ≈ 5.5 MB/s**.

---

## Sweet spot: events/s vs data volume (MB/s)

Fine-grained payload sweep (production, 2026-05-26): 100k events × payload 0–4096 B, **2 runs each**, median reported.

Stored size estimate: **~80 + payloadBytes** per event (JSON envelope + pad).

### `shared` (`writeEvent` — production path)

| payload B | events/s (median) | MB/s (median) | total stored |
|-----------|-------------------|---------------|--------------|
| 0 | 18,900 | 1.4 | 7.6 MB |
| 16 | 17,550 | 1.6 | 9.2 MB |
| 64 | 17,530 | 2.4 | 13.7 MB |
| 128 | 18,470 | 3.7 | 19.8 MB |
| **256** | **16,655** | **5.3** | **32.1 MB** |
| 512 | 11,070 | 6.3 | 56.5 MB |
| 1024 | 8,760 | 9.2 | 105 MB |
| 2048 | 7,910 | 16.1 | 203 MB |
| **4096** ≈ Grok 100 ms | **4,497** | **17.9** | 398 MB |

**Events/s sweet spot:** **0–256 B** — flat ~**16–19k/s**. Per-row SQLite overhead dominates; shrinking payload below ~256 B barely helps.

**Data volume sweet spot:** **2–4 KiB** — **~16–18 MB/s plateau**. Events/s falls to **4–8k** as you push more bytes per row.

**Knee (where the tradeoff bites):** **512 B–1 KiB**. Events/s drops ~30–50% from peak; MB/s is still climbing. This is the range where you're paying for bytes without yet reaching max MB/s.

**Practical default for stream events:** **~256 B** if events are small metadata. **~4–5 KB** if mirroring Grok `input_audio_buffer.append` chunks — still **~4.5k/s** on `shared`, far above Grok's **~10/s** arrival rate.

### `autoinc` (no `max(offset)` — headroom reference)

| payload B | events/s (median) | MB/s (median) |
|-----------|-------------------|---------------|
| 0 | 59,200 | 4.5 |
| 256 | 45,600 | 14.6 |
| 1024 | 22,900 | 24.2 |
| 4096 | 11,600 | **46.5** |

Same shape, ~3× higher throughout. Max **~46 MB/s** at 4 KiB; max **~59k events/s** at 0 B. Confirms the bottleneck is mostly per-write logic, not raw SQLite bandwidth.

### How to read the two axes

```
events/s  ▲
          │  ████ flat ~16–19k (shared, 0–256 B)
          │      ╲
          │        ╲___ knee 512 B – 1 KiB
          │              ╲____ plateau ~4–8k at 2–4 KiB
          └──────────────────────────────► payload size

MB/s      ▲
          │                    ████ plateau ~16–18 MB/s (shared)
          │              ╱────
          │        ╱────
          │  ────╱  steep climb 256 B – 2 KiB
          └──────────────────────────────► payload size
```

**Choose by goal:**

| Goal | Payload | Path | Expect |
|------|---------|------|--------|
| Max events/s | 0–256 B | shared | ~16–19k/s, 1.4–5.3 MB/s |
| Balanced stream events | **~256 B** | shared | **~17k/s, ~5.3 MB/s** |
| Max ingest volume | 2–4 KiB | shared | ~16–18 MB/s, 4–8k/s |
| Theoretical headroom | any | autoinc + no max(offset) | ~3× shared |

---

| variant | events/s | MB/s | what it measures |
|---------|----------|------|------------------|
| **shared** | 14,656 | 4.7 | production `writeEvent` — **baseline** |
| autoinc | 41,990 | 13.5 | autoincrement + `raw_event` only; no `max(offset)` per write |
| blob | 35,721 | 8.8 | opaque text column; no `json_valid` |
| tiny | 77,405 | 0.07 | single `"x"` per row — absolute sqlite insert ceiling |

**Biggest win for events/s:** drop the **`select max(offset)` on every append** in `writeEvent` (~3×: 14.6k → 42k/s). Use SQLite `autoincrement` (or cache next offset in DO memory) instead.

Secondary wins: drop `json_valid` check, drop redundant columns / indexes you don't read on the hot path.

The **`idempotency_key unique` index** is on the table but unused in these benchmarks; idempotency lookups would add more per-write cost when used.

Deployed URL: `https://02-do-write-ceiling.iterate-dev-preview.workers.dev`

---

## Other modes (for context)

### WS append (`ws-from-runner`, 256 B)

~**2k events/s** verified commit. Client/runner can send faster; sink SQLite is the bottleneck on the WS path.

### External WS send (experiment 00)

~**5.7k/s client send burst** (5k msgs / ~0.87 s / ~1.6 MB). Not sustained; not in-DO loop.

---

## How to reproduce

```bash
cd experiments/streams/02-do-write-ceiling

# 5× repeat of the headline number
pnpm ceiling https://02-do-write-ceiling.iterate-dev-preview.workers.dev \\
  --modes in-do --messages 100000 --payload-bytes 256 --variants shared --repeats 5

# payload + volume sweet spot sweep
pnpm ceiling <url> --modes in-do --messages 100000 \\
  --payload-bytes 0,16,64,128,256,512,1024,2048,4096 --variants shared,autoinc --repeats 2

# schema / indexing sweep
pnpm ceiling <url> --modes in-do --messages 100000 --payload-bytes 256 \\
  --variants shared,autoinc,blob,tiny
```

Variants: `shared` | `autoinc` | `blob` | `tiny` — see [README.md](./README.md).

---

## Open questions

- Sustained 60 s+ in-DO loop (CPU limit vs steady rate)
- `writeEventFromKv` with `allowUnconfirmed` + batch `sync()` vs SQL path
- Idempotency-enabled writes at scale
- Table size effect (100k rows vs 10M rows — does `max(offset)` degrade further?)
