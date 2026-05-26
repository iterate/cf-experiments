# 03-kill-durable-object

**Headline results (production, `iterate-dev-preview`, May 2026):**

- For this `Uint8Array` / ArrayBuffer allocation shape, a Durable Object can **retain ~192 MiB** and keep serving pings. That is above the documented 128 MB/isolate limit, but this experiment measures retained JS-visible byte length, not Cloudflare's internal heap accounting.
- In the recorded production runs, follow-up pings from ~208 MiB upward returned `heldBytes:0`, consistent with the instance being reset/recreated before the next RPC. The current worker also returns an `incarnationId` so this can be checked directly.
- Single-request allocation failed around **263–264 MiB** with HTTP 500 / `error code: 1101` and `exceededMemory` telemetry in those runs.
- For this `Uint8Array` cycle probe, **alloc + release cycles are fine** at 250 MiB × 10 — the failure mode appears when memory stays retained across calls.
- **Miniflare ≠ production (memory):** in this local `wrangler dev` run, Miniflare retained 600+ MiB with no reset; production lost retained state from ~208 MiB and hard-failed at ~264 MiB. Run `pnpm test:oom` against both URLs.
- **Miniflare ≠ production (kill):** local `POST /kill` can return a stack trace with your abort reason; deployed returns a generic Cloudflare 1101 error page (reason only in `wrangler tail`).

Everything lives in **`src/worker.ts`** (single file, no shared deps).

## Requirements

```bash
node --version # >=24
pnpm --version # >=10
corepack enable # if pnpm is not already available
```

## Quick Reproduction

```bash
git clone https://github.com/iterate/cf-experiments.git
cd cf-experiments
pnpm install
cd experiments/03-kill-durable-object
pnpm dev
```

Wrangler prints a local URL, usually `http://localhost:8787`, but it may choose another port if 8787 is already busy:

```text
Ready on http://localhost:8794
```

In another terminal, use that exact URL:

```bash
cd experiments/03-kill-durable-object

# OOM threshold table against Miniflare
WORKER_URL=http://localhost:8794 pnpm test:oom

# Kill response body against Miniflare
WORKER_URL=http://localhost:8794 pnpm test:kill-response
```

If you want a fixed local URL, start Wrangler with an explicit port:

```bash
pnpm dev -- --port 8787
```

Expected Miniflare shape: `test:oom` should print successful allocations well past production's OOM point. On my 2026-05-26 run, Miniflare retained 600 MiB and kept responding.

To include bigger local-only probes:

```bash
SWEEP_MIB=64,128,192,208,264,280,400,512,600 WORKER_URL=http://localhost:8794 pnpm test:oom
```

Run the same scripts against the already-deployed copy:

```bash
WORKER_URL=https://03-kill-durable-object.iterate-dev-preview.workers.dev pnpm test:oom
WORKER_URL=https://03-kill-durable-object.iterate-dev-preview.workers.dev pnpm test:kill-response
```

Expected production shape:

```text
192 MiB | alloc 200 | ping heldBytes 201326592 | incarnation same (...)
208 MiB | alloc 200 | ping heldBytes 0         | incarnation changed (...)
264 MiB | alloc 500 | ping heldBytes —         | incarnation — (...)
```

To deploy to your own Cloudflare account, see [Deploy Your Own Copy](#deploy-your-own-copy).

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
| `GET /ping` | Pong (+ optional delay). Response includes `heldBytes` from prior `/memory` calls plus `incarnationId` for the current DO instance |
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
| `totalLogicalHeldBytes` / `totalHeldBytes` | Sum of `byteLength` for all retained chunks in the current DO instance. If this drops to `0` after a successful allocation and `incarnationId` changes, the object was reset/recreated between calls. |
| `touchedBytes` | How many individual bytes this call wrote. |
| `estimatedCommittedBytes` | Best-effort estimate of bytes whose backing pages were forced to exist by our writes. Cloudflare/V8 does not expose actual isolate memory usage. |

`touch` controls how aggressively the probe materializes backing memory:

| `touch` | Behavior | Use |
|---------|----------|-----|
| `none` | Allocate `Uint8Array`s and keep references, but do not write to them. V8/workerd may lazily back zero-filled buffers. | Fast, but **not trustworthy for OOM thresholds**. |
| `pages` | Write one byte per 4 KiB page. | Cheaper approximation of committed pages. |
| `fill` | Write every byte with a non-zero value. | Slowest, but the clearest OOM probe. Prefer this for production threshold sweeps. |
| `random` | Write every byte with high-entropy data from `crypto.getRandomValues()` (64 KiB calls). | Strictest realism check: defeats repeated-byte/dedup/compression theories, but spends extra CPU. |

Important: these are experiment-side counters. They are not Cloudflare runtime memory metrics. Use `touch=fill` when comparing against the Workers 128 MB/isolate limit, and always follow an allocation with `/ping` to check whether the same `incarnationId` survived.

### Limitations

- This probes one allocation shape: retained `Uint8Array` / ArrayBuffer chunks.
- It does not measure RSS, V8 heap, ArrayBuffer external memory, or Cloudflare's internal isolate memory accounting.
- Thresholds may vary by deployment, compatibility date, region, account plan, runtime version, request concurrency, and chunk size.
- The default sweep prints observations; it intentionally does not assert fixed thresholds.

## Prove Miniflare vs Production

Same worker, different memory behavior:

```bash
# Local Miniflare: use the URL printed by `pnpm dev`
WORKER_URL=http://localhost:8794 pnpm test:oom

# Optional: prove the larger local gap
SWEEP_MIB=64,128,192,208,264,280,400,512,600 WORKER_URL=http://localhost:8794 pnpm test:oom

# Production
WORKER_URL=https://03-kill-durable-object.iterate-dev-preview.workers.dev pnpm test:oom
```

Same worker, different `ctx.abort()` error surfaces:

```bash
# Local — abort reason in HTTP body. Use the URL printed by `pnpm dev`.
curl -si -X POST 'http://localhost:8794/kill?reason=tweet-demo' | head -20

# Deployed — generic 1101, reason only in wrangler tail
curl -si -X POST 'https://03-kill-durable-object.iterate-dev-preview.workers.dev/kill?reason=tweet-demo' | head -20
```

Or run `pnpm test:kill-response` with each `WORKER_URL`.

Note: the scripted test sends `Accept: text/plain`. Without that header, Node `fetch()` may receive Miniflare's HTML error page; `curl` receives the plain stack trace.

## Deploy Your Own Copy

Prereqs:

- Node 24+
- pnpm 10+
- A Cloudflare account with Workers + Durable Objects enabled
- Wrangler auth: `pnpm exec wrangler login`

Then edit `wrangler.jsonc`:

```jsonc
{
  "account_id": "your-cloudflare-account-id",
  "name": "your-unique-worker-name",
  // ...
}
```

Deploy:

```bash
pnpm run deploy
```

Wrangler will create the Durable Object class using the existing migration:

```jsonc
"migrations": [{ "tag": "v1", "new_sqlite_classes": ["DebugDurableObject"] }]
```

After deploy, run:

```bash
WORKER_URL=https://your-unique-worker-name.<your-subdomain>.workers.dev pnpm test:oom
WORKER_URL=https://your-unique-worker-name.<your-subdomain>.workers.dev pnpm test:kill-response
```

For production failures, capture Ray IDs from the `test:oom` output and tail logs:

```bash
pnpm exec wrangler tail your-unique-worker-name --format json --status error
```

If you rename `DebugDurableObject`, keep `durable_objects.bindings[].class_name` and `migrations[].new_sqlite_classes` aligned with the exported class name.

## Scripts

| Script | Meaning |
|--------|---------|
| `pnpm dev` | Run locally with Miniflare / `wrangler dev` |
| `pnpm run deploy` | Deploy using `wrangler.jsonc` |
| `pnpm test:oom` | Print allocation + follow-up ping table for `WORKER_URL` |
| `pnpm test:kill-response` | Compare `ctx.abort()` client response for `WORKER_URL` |
| `pnpm typecheck` | Typecheck worker and scripts |

## How to run

From this directory:

```bash
pnpm dev
pnpm run deploy   # → https://03-kill-durable-object.iterate-dev-preview.workers.dev
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
curl -s -X POST 'http://localhost:8794/memory?name=oom&bytes=33554432&touch=fill'
curl -s 'http://localhost:8794/ping?name=oom'   # heldBytes tells you if instance survived

# or automated sweep
WORKER_URL=http://localhost:8794 pnpm test:oom
```

### Production tail

```bash
wrangler tail --format json --status error
```

`wrangler.jsonc` sets `account_id` to **iterate (dev/preview)** (`376ef7ed81b0573f93524de763666c15`).

## How to evaluate

Record each run in `log.md`. For each environment (Miniflare vs deployed):

1. **OOM ramp** — `pnpm test:oom` or manual curls with increasing `bytes` until failure. Check both `heldBytes` and whether `incarnationId` stayed the same.
2. **Kill response body** — `pnpm test:kill-response` locally vs deployed.
3. **Alloc/release cycles** — `POST /memory/cycle?bytes=250MiB&cycles=10&touch=fill` should succeed even above this probe's single-shot retention limit.
4. **Long-held DO memory** — allocate 160–190 MiB, ping every second for 30s; object keeps responding.

Open questions: how stable the thresholds are across chunk sizes, regions, account plans, compatibility dates, runtime releases, and longer retention windows.
