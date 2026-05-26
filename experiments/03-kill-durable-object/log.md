# Findings

**Memory accounting update:** earlier OOM sweeps used untouched `Uint8Array`s, so `heldBytes` meant logical `byteLength`, not necessarily committed backing memory. That is useful for detecting instance reset, but it is not trustworthy for comparing against Cloudflare's 128 MB/isolate limit.

The memory helper now reports:

| Field | Meaning |
|-------|---------|
| `logicalAllocatedBytes` / `allocatedBytes` | New `Uint8Array.byteLength` retained by this call |
| `totalLogicalHeldBytes` / `totalHeldBytes` | Total retained `byteLength` in the current DO instance |
| `touchedBytes` | Actual writes performed by this call |
| `estimatedCommittedBytes` | Bytes we attempted to force into existence by touching/filling buffers |

Use `touch=fill` or `touch=random` for serious OOM threshold work. `touch=none` is fast but can overstate real committed memory because zero-filled buffers may be lazily backed by V8/workerd. `touch=pages` is a cheaper middle ground: one write per 4 KiB page. These are still experiment-side counters, not runtime heap metrics.

Follow-up production smoke after adding `touch=fill` showed the earlier OOM boundary was **not only** a lazy-zero-buffer artifact: full fills still retained through 192 MiB, silently reset after 208 MiB, and hard-failed at 263 MiB. So the old byte accounting was incomplete, but the observed Cloudflare behavior remains real for this probe.

**Yes — alternating consume + release does NOT crash the DO**, even at per-cycle sizes that would OOM if held cumulatively. Verified in production (`iterate-dev-preview`).

`cycleMemory` RPC (route `POST /memory/cycle?bytes=N&cycles=M`) allocates `bytes` then clears `memoryChunks` each round, asserting `heldBytes === 0` after every release. External `POST /memory` + `DELETE /memory` loops behave the same.

| Test | Result |
|------|--------|
| 64 MiB × 50 cycles | 200, `finalHeldBytes: 0` |
| 192 MiB × 20 cycles | 200 (would ~3.8 GiB if cumulative) |
| 250 MiB × 10 cycles | 200 (above single-shot retention limit) |
| 150 MiB × 30 external POST/DELETE loop | all 200, `finalHeldBytes: 0` |
| 192 MiB × 20 repeat × 3 | 3/3 HTTP 200, recovery pong |

OOM only bites when memory **stays allocated** across calls. Peak per cycle is `bytesPerCycle`, not `bytesPerCycle × cycles`.

**Yes — `ctx.abort` reliably kills the DO instance in both Miniflare (`wrangler dev`) and deployed Workers.** After kill, the same `getByName` recovers: the next `ping` returns 200/pong (new incarnation).

### Code after `ctx.abort` — never observable

`DebugDurableObject.kill()` only calls `this.ctx.abort(reason)`. Any line after that (`console.log`, `throw`, etc.) **never appears** anywhere we measured:

| Observer | Sees abort `reason`? | Sees post-`abort` code? |
|----------|---------------------|-------------------------|
| Client (local) | Yes — `Error: <reason>` | **No** |
| Client (deployed) | No in body (`error code: 1101`) | **No** |
| `wrangler tail` (worker row) | Yes — `exceptions[].message` = reason | **No** |

Only the abort reason propagates. See docstring on `kill()` in `src/worker.ts`.

**Docs vs local:** [Durable Object State `abort`](https://developers.cloudflare.com/durable-objects/api/state/) says abort is *“Not available in local development”*, but **Miniflare does implement it** in our run (errors, 500s, in-flight abort). Treat local behaviour as emulator-specific until re-verified against workerd.

## Miniflare vs production

| | Miniflare (`wrangler dev`) | Deployed (`iterate-dev-preview`) |
|---|---|---|
| Kill works (idle) | Yes — POST `/kill` → 500 | Yes — POST `/kill` → 500 |
| Kill in-flight `ping` | Yes — long ping also 500 (~511ms) | Yes — long ping 500 (~548ms) |
| Recovery (`ping` same `name`) | Yes — 200 pong immediately after | Yes — 200 pong immediately after |
| **HTTP body on kill** | Plain-text **stack trace**; message = abort reason (`Error: <reason>`) | Generic **`error code: 1101`** (16 bytes); reason **not** in body |
| **OOM: retain + ping** | **No limit observed** through 600 MiB single-shot `touch=fill` (2026-05-26 re-run) | Silent reset after **192 MiB** retained; `ping.heldBytes=0` from **208 MiB** upward |
| **OOM: hard fail** | Not observed through 600 MiB | HTTP **500** / `1101` at **264 MiB** single-shot |
| **Abort reason visible** | Wrangler stderr + response body | `wrangler tail` on **worker** `fetch` invocation only |
| DO invocation in tail | Not separate JSON events (bundled into worker log lines) | Separate events: `executionModel: "durableObject"`, `event.rpcMethod: "kill"` / `"ping"` |
| DO `exceptions` in tail JSON | N/A | **Empty** on DO rows; `exceptions` populated on **stateless worker** row with `message: "<reason>"` |
| DO `outcome` on kill | N/A (pretty logs) | `"exception"` on both DO RPC and worker fetch |
| Client correlation | Terminal `[wrangler:info]` / `[wrangler:error]` | Response header **`cf-ray`** (e.g. `9ffbdf400b507761-LHR`) |

Error **1101** = Worker threw an uncaught exception ([Workers errors](https://developers.cloudflare.com/workers/observability/errors/)). Abort propagates to the caller as an `Error` whose **message is the abort reason string**.

## How to discover a kill

### 1. Wrangler dev (Miniflare) — stdout

Run `pnpm dev`, hit routes, watch the terminal:

- **`[wrangler:info]`** — request line + status (`GET /ping 200`, `POST /kill 500`, in-flight `GET /ping 500`).
- **`[wrangler:error]`** — abort surfaces as `Error: <reason>` with stack; frame at `worker.ts` is the **`await stub.kill()`** or **`await stub.ping()`** line.

Filter mentally: two `[wrangler:error]` lines with the **same reason** = kill took down an in-flight ping and the kill request itself.

### 2. Production — `wrangler tail` (real-time)

```bash
cd experiments/03-kill-durable-object
wrangler tail --format json
```

Then reproduce. Per invocation you get JSON with:

- **`executionModel`**: `"stateless"` (edge `fetch`) vs `"durableObject"`.
- **`outcome`**: `"ok"` | `"exception"`.
- **`event.rpcMethod`**: `"ping"` | `"kill"` on DO rows.
- **`event.request.url`** / **`response.status`**: on worker rows.
- **`exceptions[]`**: on worker rows after kill — `{ "name": "Error", "message": "<abort reason>", "stack": "at async Object.fetch (src/worker.ts:…)" }`.
- **`durableObjectId`**: stable per `name` across pings; same id on recovery ping after kill (same logical object, new execution).

Useful filters:

```bash
wrangler tail --format json --status error
wrangler tail --format pretty
```

### 3. Production — Cloudflare dashboard (logs + traces)

Worker URL: `https://03-kill-durable-object.iterate-dev-preview.workers.dev`  
Account: **iterate (dev/preview)** (`376ef7ed81b0573f93524de763666c15`) — set in `wrangler.jsonc`.

1. **Workers & Pages** → **03-kill-durable-object** → **Observability**.
2. **Invocations** / **Workers Logs**: filter **Status = error** or search the abort reason string (e.g. `prod-20260522-kill-idle`).
3. Open a failed invocation; confirm **500** on `POST /kill` or aborted `GET /ping`.
4. **Traces** (enabled in `wrangler.jsonc` with `head_sampling_rate: 1`): open the trace for that invocation. Expect:
   - Root: **fetch handler** span (failed).
   - Child: **Durable Object** binding span → RPC `kill` or `ping` (failed).
   - Abort reason may appear on the worker/error detail, not as a separate “abort” event type.
5. Correlate with **`cf-ray`** from `curl -si` on the failing response → search logs/traces by Ray ID if the UI supports it.

### 4. Client-side quick check

```bash
# Production — generic failure (reason hidden)
curl -si -X POST 'https://03-kill-durable-object.iterate-dev-preview.workers.dev/kill?reason=my-reason'

# Local — reason in body
curl -si -X POST 'http://localhost:<port>/kill?reason=my-reason'
```

---

## Log

### 2026-05-26 — Incarnation IDs added to prove reset/recreation

Added per-instance `incarnationId` and `createdAt` fields to `DebugDurableObject` responses. The OOM sweep now prints whether the allocation response and follow-up ping came from the same DO incarnation.

Production re-run after deploy version `0fcc43c5-7e40-4f71-b19b-da519dffdf1a`:

| Size | Result |
|------|--------|
| 192 MiB | alloc 200, `ping.heldBytes=201326592`, `incarnation same` |
| 208 MiB | alloc 200, `ping.heldBytes=0`, `incarnation changed` |
| 264 MiB | alloc 500 / `1101`, Ray ID `a01dc0c33f183861-LHR` |

This tightens the earlier “silent reset” interpretation: for the 208 MiB probe, the follow-up ping was served by a different DO incarnation, not the same in-memory object with an empty array.

### 2026-05-26 — Miniflare OOM sweep (re-run with self-contained worker)

Same `pnpm test:oom` script, `touch=fill`, fresh DO name per size:

| Size | Miniflare (`localhost:8794`) | Production |
|------|------------------------------|------------|
| 192 MiB | alloc 200, held 201,326,592 | alloc 200, held 201,326,592 |
| 208 MiB | alloc 200, held 218,103,808 | alloc 200, held **0** (silent reset) |
| 264 MiB | alloc 200, held 276,824,064 | alloc **500** / `1101` |
| 280 MiB | alloc 200, held 293,601,280 | alloc **500** / `1101` |
| 600 MiB | alloc 200, held 629,145,600 | — |

Conclusion: **do not trust Miniflare for OOM/limit testing.** Kill/abort behaviour is closer between environments; memory limits are not.

---

Correct API for dashboard-style spans:

```text
POST /accounts/{account_id}/workers/observability/telemetry/query
```

The relevant dataset is `otel`; use `view:"events"` to list raw spans and `view:"traces"` to get trace summaries. This is distinct from GraphQL `workersInvocationsAdaptive`, which only provides aggregate invocation data.

Working query shape:

```json
{
  "queryId": "adhoc-ray-9ffc19796913f668",
  "view": "events",
  "limit": 20,
  "timeframe": { "from": 1779455820000, "to": 1779455840000 },
  "parameters": {
    "datasets": ["otel"],
    "filterCombination": "and",
    "filters": [
      {
        "key": "cloudflare.ray_id",
        "operation": "eq",
        "type": "string",
        "value": "9ffc19796913f668"
      }
    ]
  }
}
```

For the top-level Worker memory failure:

- Ray ID: `9ffc19796913f668`
- Trace ID: `fdb7913a3c332af0c953da82dda7c568`
- Invocation ID: `3fa76d5b144a9d0432ff636193d189c6`
- URL: `/worker-memory?bytes=276824064&touch=fill&reset=1`
- Span: `name:"POST"`, `durationMS:7`
- `cloudflare.outcome:"exceededMemory"`
- `cpu_time_ms:137`, `wall_time_ms:138`
- HTTP status: `503`
- `$metadata.error:"Worker exceeded memory limit."`
- `$metadata.type:"span"`

`view:"traces"` for the same trace returned a one-span trace summary:

```json
{
  "traceId": "fdb7913a3c332af0c953da82dda7c568",
  "services": ["03-kill-durable-object"],
  "traceDurationMs": 7,
  "spans": 1,
  "errors": ["Worker exceeded memory limit."],
  "rootSpanName": "POST",
  "rootTransactionName": "POST https://03-kill-durable-object.iterate-dev-preview.workers.dev/worker-memory"
}
```

Span vs `wrangler tail` delta for this failure: spans include `traceId`, `spanId`, `faas.invocation_id`, `time_to_first_byte_ms`, normalized OTel attributes, `$metadata.fingerprint`, and the compact trace summary. They do **not** appear to add deeper runtime internals beyond the same high-level memory error.

Tooling note: the `user-cloudflare-api` MCP token could query `/workers/observability/telemetry/query` successfully. It could not query `/logs/explorer/query/sql` (`10000 Authentication error`). The local `cf` CLI OAuth token has `workers_observability:read`, and `cf schema --list` shows `cf workers observability telemetry-query`, but this installed `cf` command build does not currently expose the command runner (`Unknown arguments: workers, observability, telemetry-query`). Use the MCP `execute` tool against the endpoint above.

### 2026-05-22 — Top-level Worker memory vs DO memory

Added top-level Worker routes using the same shared memory helper:

- `GET /worker-ping`
- `POST /worker-memory?bytes=N&touch=fill|random&reset=1`
- `DELETE /worker-memory`

Implementation detail: `/worker-memory` stores chunks in a module-level `workerMemoryChunks` array. This is intentionally an experiment probe, not a production pattern. `reset=1` clears the module-level store at the start of the same request so single-shot threshold tests do not accidentally accumulate prior allocations.

Important observation: `GET /worker-ping` is **not** a reliable cross-request persistence check for top-level Worker memory. In production, it often returned `heldBytes:0` immediately after a successful `/worker-memory` request. That likely means the next request hit a fresh/reset isolate (or another isolate), which is allowed for stateless Workers. For the top-level Worker comparison, the reliable signal is whether the allocation request itself returns 200 or fails with a platform error, plus `wrangler tail`.

Clean single-request `touch=fill&reset=1` sweep:

| Top-level Worker allocation | HTTP |
|-----------------------------|------|
| 64 MiB | 200 |
| 96 MiB | 200 |
| 112 MiB | 200 |
| 128 MiB | 200 |
| 144 MiB | 200 |
| 160 MiB | 200 |
| 176 MiB | 200 |
| 192 MiB | 200 |
| 208 MiB | 200 |
| 224 MiB | 200 |
| 240 MiB | 200 |
| 256 MiB | 200 on repeat (one earlier noisy failure before `reset=1` discipline was established) |
| 263 MiB | 200 (3/3 repeat) |
| 264 MiB | 503 (3/3 repeat) |
| 280 MiB | 503 |

`wrangler tail` for top-level Worker failures:

- `executionModel:"stateless"`
- `outcome:"exceededMemory"`
- `exceptions:[{"name":"Error","message":"Worker exceeded memory limit."}]`
- Client sees HTTP 503 with `error code: 1102`

Conclusion: the top-level Worker does **not** die at 128 MiB for this `Uint8Array`/ArrayBuffer allocation shape either. Its hard-fail threshold is also around 264 MiB, but the client/log surface differs from the DO:

| Scope | Hard fail client | Tail outcome | Tail message |
|-------|------------------|--------------|--------------|
| Durable Object | 500 / `1101` | DO row `exceededMemory`; worker row `exception` | `Durable Object's isolate exceeded its memory limit and was reset.` |
| Top-level Worker | 503 / `1102` | worker row `exceededMemory` | `Worker exceeded memory limit.` |

### 2026-05-22 — DO can hold 160-190 MiB and keep responding

Test: allocate random data in a DO, then call `/ping` once per second for 30 seconds on the same DO name.

| DO allocation | Initial alloc | 30s ping loop |
|---------------|---------------|---------------|
| 160 MiB random (`167,772,160` bytes) | 200 | 30/30 pongs, `heldBytes` stayed `167,772,160` |
| 190 MiB random (`199,229,440` bytes) | 200 | 30/30 pongs, `heldBytes` stayed `199,229,440` |

Conclusion: a DO can hold ~160-190 MiB of high-entropy `Uint8Array` data for at least 30 seconds while continuing to service lightweight RPC pings. This is below the observed silent-reset boundary (~208 MiB), so it is consistent with the earlier memory sweep.

### 2026-05-22 — Memory touch modes added

Earlier memory sweeps counted `Uint8Array.byteLength` only. That made the arithmetic internally correct, but the interpretation too strong: allocating untouched zero-filled buffers can reserve logical ArrayBuffer capacity without immediately committing equivalent backing memory. This likely explains why the first OOM numbers appeared higher than the documented 128 MB isolate limit.

New query parameter:

| `touch` | Meaning |
|---------|---------|
| `none` | Allocate and retain buffers without writing. Fast, but not a committed-memory probe. |
| `pages` | Write one byte per 4 KiB page. Cheaper committed-page approximation. |
| `fill` | Write every byte. Slowest, clearest OOM probe. |
| `random` | Write every byte with `crypto.getRandomValues()` in 64 KiB slices. Strictest realism check; defeats repeated-byte/dedup/compression theories but uses more CPU. |

For future OOM sweeps, prefer:

```bash
curl -s -X POST 'https://03-kill-durable-object.iterate-dev-preview.workers.dev/memory?name=oom-fill&bytes=33554432&touch=fill'
curl -s 'https://03-kill-durable-object.iterate-dev-preview.workers.dev/ping?name=oom-fill'
```

Interpretation rule: `totalLogicalHeldBytes` tells us what this DO instance is retaining by JS reference. `estimatedCommittedBytes` tells us what the experiment intentionally touched. Neither is a Cloudflare heap metric; the authoritative crash signal is still the client response plus `wrangler tail` outcomes such as `exceededMemory`.

Production smoke (`iterate-dev-preview`, version `680d3ec1-e8d7-4bb2-b4bb-0b2bafccea0c`):

| Probe | Response fields |
|-------|-----------------|
| 16 MiB `touch=none` | `touchedBytes:0`, `estimatedCommittedBytes:0`, `totalLogicalHeldBytes:16777216` |
| 16 MiB `touch=pages` | `touchedBytes:4096`, `estimatedCommittedBytes:16777216`, `totalLogicalHeldBytes:16777216` |
| 16 MiB `touch=fill` | `touchedBytes:16777216`, `estimatedCommittedBytes:16777216`, `totalLogicalHeldBytes:16777216` |

Mini `touch=fill` OOM sweep:

| Single-shot fill | HTTP | Follow-up `ping.heldBytes` |
|------------------|------|----------------------------|
| 64 MiB | 200 | 67,108,864 |
| 96 MiB | 200 | 100,663,296 |
| 112 MiB | 200 | 117,440,512 |
| 128 MiB | 200 | 134,217,728 |
| 144 MiB | 200 | 150,994,944 |
| 160 MiB | 200 | 167,772,160 |
| 176 MiB | 200 | 184,549,376 |
| 192 MiB | 200 | 201,326,592 |
| 208 MiB | 200 | 0 (silent reset before next RPC) |
| 224 MiB | 200 | 0 |
| 240 MiB | 200 | 0 |
| 256 MiB | 200 | 0 |
| 260 MiB | 200 | 0 |
| 261 MiB | 200 | 0 |
| 262 MiB | 200 | 0 |
| 263 MiB | 500 | 0 |
| 264 MiB | 500 | 0 |
| 272 MiB | 500 | 0 |
| 280 MiB | 500 | 0 |

The filled-buffer threshold matches the earlier untouched-buffer threshold closely. Revised interpretation: `byteLength` was the wrong name for "committed memory", but the weird 192 MiB / 263 MiB thresholds are not explained away by lazy zero pages alone. Possible remaining explanations include Cloudflare's effective isolate memory accounting for external ArrayBuffers, per-isolate baseline/overhead, implementation details in workerd/V8, or the documented 128 MB limit being a nominal billing/platform limit rather than a direct crash threshold for this allocation shape.

### 2026-05-22 — Random data memory probe

Added `touch=random`, which fills every retained byte with `crypto.getRandomValues()` in 64 KiB slices. This tests whether `touch=fill` with a repeated byte (`0xa5`) was getting special treatment from the runtime (deduplication, compression, repeated-page optimization, etc.).

Production (`iterate-dev-preview`, version `87d9dab6-d60b-4609-9fa7-3843b8ce2f61`):

| Single-shot random write | HTTP | Follow-up `ping.heldBytes` |
|--------------------------|------|----------------------------|
| 16 MiB | 200 | 16,777,216 |
| 64 MiB | 200 | 67,108,864 |
| 128 MiB | 200 | 134,217,728 |
| 176 MiB | 200 | 184,549,376 |
| 192 MiB | 200 | 201,326,592 |
| 208 MiB | 200 | 0 (silent reset before next RPC) |
| 240 MiB | 200 | 0 |
| 260 MiB | 200 | 0 |
| 262 MiB | 200 | 0 |
| 263 MiB | 200 | 0 |
| 264 MiB | 500 | 0 |
| 272 MiB | 500 | 0 |
| 280 MiB | 500 | 0 |
| 300 MiB | 500 | 0 |

Conclusion: random data behaves essentially the same as repeated-byte fill. The silent-reset boundary still starts after 192 MiB, and the hard-fail boundary is 264 MiB in this run (within 1 MiB of the `touch=fill` 263 MiB boundary). That makes compression/deduplication of repeated bytes an unlikely explanation.

### 2026-05-22 — Concurrent CPU exceed kills pending RPCs

Production (`iterate-dev-preview`), same DO name:

- Started `GET /ping?timeoutMs=60000`.
- One second later, started three concurrent `POST /burn-cpu?ms=35000` requests.
- Result: the delayed ping and all three CPU requests returned 500 after ~36-37s.
- Immediate recovery ping on the same DO name returned 200 `pong`.

Client observations:

| Request | HTTP | Wall time | Notes |
|---------|------|-----------|-------|
| `ping?timeoutMs=60000` | 500 | 37.204s | Failed early, before 60s timer |
| `burn-cpu` #1 | 500 | 36.199s | `Worker threw exception` page |
| `burn-cpu` #2 | 500 | 36.199s | `Worker threw exception` page |
| `burn-cpu` #3 | 500 | 36.197s | `Worker threw exception` page |
| recovery `ping` | 200 | 0.498s | `{"message":"pong","heldBytes":0}` |

`wrangler tail --format json` observations:

- Only **one** DO `burnCpu` invocation was observed: `executionModel:"durableObject"`, `event.rpcMethod:"burnCpu"`, `outcome:"exceededCpu"`, `wallTime:36079`, `cpuTime:32500`, `exceptions:[]`.
- The delayed DO `ping` also received `outcome:"exceededCpu"` with `cpuTime:0`; it was pending in the same object when the CPU exceed reset happened.
- The three stateless worker `POST /burn-cpu` rows and the stateless worker `GET /ping` row all had `outcome:"exception"` and the same exception message: `Durable Object exceeded its CPU time limit and was reset.`

Conclusion: concurrent CPU soak does not run three CPU loops in parallel inside one DO. One CPU-bound RPC exceeds the DO CPU limit, the DO is reset, and the pending/queued RPCs for that same object fail together with the same reset error.

### 2026-05-22 — Consume/release cycles do not crash

**Assertion:** alternating alloc + release keeps peak heap at one cycle's worth; no OOM.

Production (`https://03-kill-durable-object.iterate-dev-preview.workers.dev`):

- `POST /memory/cycle?bytes=67108864&cycles=50` → 200
- `POST /memory/cycle?bytes=201326592&cycles=20` → 200
- `POST /memory/cycle?bytes=262144000&cycles=10` → 200
- External loop: 150 MiB POST + DELETE × 30 → all 200, ping `heldBytes=0`
- Repeat 192 MiB × 20: 3/3 success

Implementation: `DebugDurableObject.cycleMemory()` throws if held bytes wrong after any alloc/release.

### 2026-05-22 — In-flight client + post-`abort` code

**Does local DO actually die?** Yes. Long `ping?timeoutMs=30000` returns **500 in ~400ms** (not 30s) when killed; next `ping` → 200 pong.

**Client waiting on in-flight `ping` (local):** HTTP **500**, body `Error: <abort reason>` (stack at `await stub.ping`). Same message as the kill request — not pong, not timeout.

**Post-`abort` throw / log:** Never observed; `throwAfterAbort` A/B was identical with and without extra throw.

### 2026-05-22 — First full run (local + iterate-dev-preview)

**Deploy:** `CLOUDFLARE_ACCOUNT_ID=376ef7ed81b0573f93524de763666c15 pnpm run deploy` (wrong account — should be prd)  
**URL:** https://03-kill-durable-object.iterate-dev-preview.workers.dev  
**Migration:** `new_sqlite_classes`.

### 2026-05-22 — `account_id` in wrangler.jsonc

Pinned to **iterate (dev/preview)** (`376ef7ed81b0573f93524de763666c15`).  
**URL:** https://03-kill-durable-object.iterate-dev-preview.workers.dev

**Test matrix** (`RUN=local-20260522` / `RUN=prod-20260522`):

| Step | Local | Production |
|------|-------|------------|
| A ping | 200 `pong` | 200 `pong` |
| B kill idle | 500, body = stack, `Error: …-kill-idle` | 500, body = `error code: 1101` |
| C ping same name | 200 `pong` | 200 `pong` |
| D kill in-flight (ping 10s, kill @ 0.5s) | ping 500 + kill 500, both `…-kill-inflight` | both 500, `error code: 1101` |
| E ping after inflight | 200 `pong` | 200 `pong` |

**Local wrangler dev stdout (excerpt):**

```
[wrangler:info] GET /ping 200 OK (4ms)
[wrangler:error] Error: local-20260522-kill-idle
    at async Object.fetch (.../worker.ts:37:7)
[wrangler:info] POST /kill 500 Internal Server Error (7ms)
[wrangler:info] GET /ping 200 OK (1ms)
[wrangler:error] Error: local-20260522-kill-inflight
    at async Object.fetch (.../worker.ts:31:22)
[wrangler:error] Error: local-20260522-kill-inflight
    at async Object.fetch (.../worker.ts:37:7)
[wrangler:info] GET /ping 500 Internal Server Error (511ms)
[wrangler:info] POST /kill 500 Internal Server Error (4ms)
[wrangler:info] GET /ping 200 OK (2ms)
```

**Production `wrangler tail --format json` (patterns):**

- DO `kill`: `"executionModel":"durableObject"`, `"outcome":"exception"`, `"event":{"rpcMethod":"kill"}`, `"exceptions":[]`.
- Worker `POST /kill`: `"outcome":"exception"`, `"exceptions":[{"message":"prod-20260522-kill-idle",...}]`, `"response":{"status":500}`.
- DO `ping` after kill: `"outcome":"ok"`.
- In-flight: DO `ping` + DO `kill` + worker `GET /ping` + worker `POST /kill` all `exception`; worker exceptions carry `prod-20260522-kill-inflight`.

**Probe kill headers:** `cf-ray: 9ffbdf400b507761-LHR`, `content-length: 16`, body `error code: 1101`.

**Open:** Dashboard trace UI not inspected in this run (no Workers Observability MCP query tool configured). Use Observability → Traces on a tailed `cf-ray` to confirm span shape.
