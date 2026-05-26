# 03-kill-durable-object

**Headline results (production, `iterate-dev-preview`, May 2026):**

- A Durable Object can **retain ~192 MiB** of filled `Uint8Array` data and keep serving pings — **not** the documented 128 MB/isolate limit.
- Above ~208 MiB the instance **silently resets** on the next RPC (`heldBytes` drops to 0, alloc may still return 200).
- Hard OOM at **~263–264 MiB** in a single request → HTTP 500 / `error code: 1101`, tail `exceededMemory`.
- **Alloc + release cycles are fine** at 250 MiB × 10 — OOM only when memory stays retained across calls.
- **Miniflare ≠ production (memory):** local retains 600+ MiB with no reset; production silently resets from ~208 MiB and hard-fails at ~264 MiB. Run `pnpm test:oom` against both URLs.
- **Miniflare ≠ production (kill):** local `POST /kill` returns a stack trace with your abort reason; deployed returns generic `error code: 1101` (reason only in `wrangler tail`).

Everything lives in **`src/worker.ts`** (single file, no shared deps). Reproduce:

```bash
cd experiments/03-kill-durable-object
pnpm install
pnpm dev   # terminal 1

# terminal 2 — OOM threshold table
WORKER_URL=http://localhost:8787 pnpm test:oom

# Miniflare vs prod kill body (run against both URLs)
WORKER_URL=http://localhost:8787 pnpm test:kill-response
WORKER_URL=https://03-kill-durable-object.iterate-dev-preview.workers.dev pnpm test:kill-response
```

Full notes: [log.md](./log.md).

---

Can we reliably kill a Durable Object instance — and what does each kill mode look like in logs?

`DebugDurableObject` is a tiny RPC DO with deliberate crash/kill probes and memory accumulation methods (`consumeMemory`, `releaseMemory`, `cycleMemory`).

## Kill modes (research summary)

| Mode | Mechanism | Expected effect | Observability notes |
|------|-----------|-----------------|---------------------|
| **`ctx.abort(reason)`** | Explicit platform kill | Instance reset; error logged, not catchable in DO code | Docs note abort may differ in `wrangler dev` vs production |
| **OOM / memory pressure** | Allocate until isolate exceeds **128 MB** heap limit | Isolate reset; in-memory state lost; SQLite storage intact | Per-isolate limit, not per-DO — multiple DOs on same isolate share 128 MB. OOM may produce **little or no log signal** ([agents#1285](https://github.com/cloudflare/agents/issues/1285)). Callers may get an exception with `.remote` set |
| **Uncaught exception** | Deliberate `throw` from RPC | Runtime *may* terminate instance; in-memory state lost | Propagates to caller; stub may be broken — create fresh stub on retry |
| **CPU limit** | Busy-loop past default **30s CPU** (configurable to 5 min via `limits.cpu_ms`) | Request fails; instance may survive | Exception message should mention CPU time exceeded |
| **`blockConcurrencyWhile` throw/timeout** | Constructor/init callback throws or exceeds **30s** | DO terminated and reset | Not exposed here yet — another vector to try |
| **Storage op timeout** | Very large `deleteAll()` etc. | Object reset per troubleshooting docs | Not exposed here yet |

References: [Workers limits (128 MB/isolate)](https://developers.cloudflare.com/workers/platform/limits/), [DO state `abort`](https://developers.cloudflare.com/durable-objects/api/state/), [DO error handling](https://developers.cloudflare.com/durable-objects/best-practices/error-handling/), [DO troubleshooting](https://developers.cloudflare.com/durable-objects/observability/troubleshooting/).

## RPC / routes

| Route | Meaning |
|-------|---------|
| `GET /ping` | Pong (+ optional delay). Response includes `heldBytes` from prior `/memory` calls |
| `GET /worker-ping` | Top-level Worker pong. Response includes module-level retained Worker `heldBytes` |
| `POST /worker-memory?bytes=N&touch=fill&reset=1` | Allocate and retain memory in the top-level Worker isolate, not in a DO. `reset=1` clears the module-level store first |
| `DELETE /worker-memory` | Release top-level Worker retained allocations |
| `POST /kill` | `DebugDurableObject.kill()` → `ctx.abort` only (see docstring) |
| `POST /memory?bytes=N&touch=fill` | Allocate **N additional bytes** (1 MiB chunks by default), optionally touch them, retained on the DO instance |
| `DELETE /memory` | Release retained allocations |
| `POST /memory/cycle?bytes=N&cycles=M&touch=fill` | Repeatedly allocate then release, asserting final held bytes are zero |
| `POST /throw` | Uncaught error |
| `POST /burn-cpu?ms=N` | CPU spin (default 60s) |

The `/memory` routes exercise a Durable Object instance. The `/worker-memory` routes exercise the top-level Worker isolate using the same helper and a module-level retained array. That module-level mutable state is intentionally an experiment probe, not a production pattern.

### Memory measurement notes

The memory probe reports two related but different quantities:

| Field | Meaning |
|-------|---------|
| `logicalAllocatedBytes` / `allocatedBytes` | Sum of `Uint8Array.byteLength` allocated by this call. This is capacity retained by JS references, not a platform heap measurement. |
| `totalLogicalHeldBytes` / `totalHeldBytes` | Sum of `byteLength` for all retained chunks in the current DO instance. If this drops to `0` after a successful allocation, the object was likely reset between calls. |
| `touchedBytes` | How many individual bytes this call wrote. |
| `estimatedCommittedBytes` | Best-effort estimate of bytes whose backing pages were forced to exist by our writes. Cloudflare/V8 does not expose actual isolate memory usage. |

`touch` controls how aggressively the probe materializes backing memory:

| `touch` | Behavior | Use |
|---------|----------|-----|
| `none` | Allocate `Uint8Array`s and keep references, but do not write to them. V8/workerd may lazily back zero-filled buffers. | Fast, but **not trustworthy for OOM thresholds**. |
| `pages` | Write one byte per 4 KiB page. | Cheaper approximation of committed pages. |
| `fill` | Write every byte with a non-zero value. | Slowest, but the clearest OOM probe. Prefer this for production threshold sweeps. |
| `random` | Write every byte with high-entropy data from `crypto.getRandomValues()` (64 KiB calls). | Strictest realism check: defeats repeated-byte/dedup/compression theories, but spends extra CPU. |

Important: these are experiment-side counters. They are not Cloudflare runtime memory metrics. Use `touch=fill` when comparing against the Workers 128 MB/isolate limit, and always follow an allocation with `/ping` to check whether the same instance survived.

## Prove Miniflare vs production (two commands)

Same worker, different error surfaces:

```bash
# Local — abort reason in HTTP body
curl -si -X POST 'http://localhost:8787/kill?reason=tweet-demo' | head -20

# Deployed — generic 1101, reason only in wrangler tail
curl -si -X POST 'https://03-kill-durable-object.iterate-dev-preview.workers.dev/kill?reason=tweet-demo' | head -20
```

Or run `pnpm test:kill-response` with each `WORKER_URL`.

## How to run

From this directory:

```bash
pnpm dev
pnpm deploy   # → https://03-kill-durable-object.iterate-dev-preview.workers.dev
```

Parameters (query string on every route):

| Param | Default | Meaning |
|-------|---------|---------|
| `name` | `default` | DO instance (`getByName`) |
| `timeoutMs` | _(none)_ | `/ping` only — ms to wait before returning pong |
| `reason` | _(none)_ | `/kill` only — passed to `ctx.abort` |
| `bytes` | _(required)_ | `/memory` POST — additional bytes to allocate |
| `chunkBytes` | `1048576` | `/memory` POST — allocation chunk size |
| `touch` | `none` | `/memory` and `/memory/cycle` — `none`, `pages`, `fill`, or `random` |
| `reset` | _(none)_ | `/worker-memory` only — set to `1` to clear top-level Worker memory before allocating |
| `cycles` | _(required)_ | `/memory/cycle` POST — consume/release cycle count |
| `message` | _(none)_ | `/throw` only |
| `ms` | `60000` | `/burn-cpu` only |

### Manual OOM probe

```bash
# allocate +32 MiB at a time until failure; always use touch=fill
curl -s -X POST 'http://localhost:8787/memory?name=oom&bytes=33554432&touch=fill'
curl -s 'http://localhost:8787/ping?name=oom'   # heldBytes tells you if instance survived

# or automated sweep
WORKER_URL=http://localhost:8787 pnpm test:oom
```

### Production tail

```bash
wrangler tail --format json --status error
```

`wrangler.jsonc` sets `account_id` to **iterate (dev/preview)** (`376ef7ed81b0573f93524de763666c15`).

## How to evaluate

Record each run in `log.md`. For each environment (Miniflare vs deployed):

1. **OOM ramp** — `pnpm test:oom` or manual curls with increasing `bytes` until failure.
2. **Kill response body** — `pnpm test:kill-response` locally vs deployed.
3. **Alloc/release cycles** — `POST /memory/cycle?bytes=250MiB&cycles=10&touch=fill` should succeed even above single-shot retention limit.
4. **Long-held DO memory** — allocate 160–190 MiB, ping every second for 30s; object keeps responding.

Open questions: whether Miniflare OOM thresholds match production, and what operators actually see in logs.
