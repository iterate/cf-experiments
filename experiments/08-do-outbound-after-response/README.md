# 08-do-outbound-after-response

What happens in a Durable Object when it starts a long-running outbound `fetch()`, returns to the caller immediately (so the inbound HTTP/RPC request context ends), and the slow origin is still responding?

## Background (docs)

From [Lifecycle of a Durable Object](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/): hibernation requires **no in-progress awaited `fetch()`**. While a fetch is awaited, the object stays **idle, in-memory, non-hibernateable**.

From [Durable Object State — `waitUntil`](https://developers.cloudflare.com/durable-objects/api/state/): **`waitUntil` has no effect in Durable Objects**. The runtime keeps the object active while there is ongoing work or pending I/O; you do not need `ctx.waitUntil()` to finish a fetch after returning a response.

[Kenton Varda — input/output gates](https://blog.cloudflare.com/durable-objects-easy-fast-correct-choose-three/) (2021) is the deeper model: concurrent events vs single-threaded consistency. This experiment does not probe storage races; it only asks whether the **orphaned** outbound fetch still runs to completion.

## What we're trying to find out

1. After `startInline()` returns (RPC done), does the fire-and-forget `fetch()` still complete and persist status in DO storage?
2. Does behavior differ between **Miniflare** (`wrangler dev`) and **deployed** Workers?
3. If inline fire-and-forget is flaky or cancelled in one environment, does scheduling the same fetch from **`alarm()`** (a separate invocation) behave differently?
4. Is the observed failure actually the root Worker returning before a DO RPC is delivered?
5. If the root Worker **awaits** a slow DO RPC and the client disconnects, is the DO's awaited outbound fetch cancelled?

Slow origin: programmatic [captun](https://captun.sh) tunnel (`createCaptunTunnel`) at `/slow?ms=<delay>` — delay per request via query param (enables sweeps).

## How to run

```sh
cd experiments/08-do-outbound-after-response
pnpm install   # from repo root: pnpm install
pnpm dev
```

Another terminal (use Wrangler's "Ready on …" URL):

```sh
WORKER_URL=http://localhost:8787 pnpm test
```

Deployed (needs Cloudflare API token — e.g. Doppler `os` / `dev`):

```sh
doppler run --project os --config dev -- pnpm deploy
WORKER_URL=https://08-do-outbound-after-response.iterate-dev-preview.workers.dev pnpm test
```

Delay sweep (find inline vs alarm divergence):

```sh
WORKER_URL=https://08-do-outbound-after-response.iterate-dev-preview.workers.dev \
  SWEEP_MS=1000,5000,15000,30000,60000,120000 pnpm test:sweep
```

Parent-context mode sweep:

```sh
WORKER_URL=https://08-do-outbound-after-response.iterate-dev-preview.workers.dev \
  MODES=rpc-inline,do-fetch,root-fire-and-forget,root-wait-until,alarm \
  SWEEP_MS=180000,300000 pnpm test:modes
```

Awaited-RPC client-abort probe:

```sh
WORKER_URL=https://08-do-outbound-after-response.iterate-dev-preview.workers.dev \
  SLOW_MS=600000 ABORT_MS=30000 POLL_SLACK_MS=60000 pnpm test:await-cancel
```

| Param | Default | Meaning |
| --- | --- | --- |
| `WORKER_URL` | `http://localhost:8787` | Worker base URL |
| `SLOW_MS` | `8000` | captun `/slow?ms=` for single `pnpm test` |
| `SWEEP_MS` | `1000,3000,8000,…,120000` | Comma-separated delays for `pnpm test:sweep` |
| `MODES` | `rpc-inline,do-fetch,root-fire-and-forget,root-wait-until,alarm` | Modes for `pnpm test:modes` |
| `POLL_SLACK_MS` | `5000` | Added to each sweep delay for status poll budget |
| `ALARM_DELAY_MS` | `0` | Delay before alarm fires |
| `ABORT_MS` | `30000` | Client-side abort delay for `pnpm test:await-cancel` |

## How to evaluate results

Both vitest cases should pass:

- **inline** — `POST /inline` returns quickly; `GET /status` eventually shows `phase: "done"`, `via: "inline"`, body contains `slow-ok`, same `incarnationId`.
- **alarm** — `POST /alarm` returns quickly; status eventually shows `via: "alarm"` with the same checks.

If **inline** fails locally but **alarm** passes (or the reverse on deploy), that gap is the finding — log it in [log.md](./log.md) with `cf-ray` lines from the test output.

Known deployed result (2026-06-01): DO-owned work (`rpc-inline`, `do-fetch`, `root-wait-until`, `alarm`) completed through **10 minutes**. `await-rpc` also completed through **10 minutes** even when the client aborted at 30s and the root Worker observed `request.signal` abort. `root-fire-and-forget` never recorded a run, even at 5s, because the root Worker returned without awaiting the DO RPC or passing it to `ctx.waitUntil`.

Failures print `cf-ray` where available.

Record runs in [log.md](./log.md).
