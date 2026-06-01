# DO memory limit experiment

**Question:** How much memory can a Durable Object retain before Cloudflare kills or resets it?

**Answer (production, May 2026):** A single DO can hold **197 MiB** and answer the next ping on the **same instance**. From **198 MiB**, allocation returns 200 but the follow-up ping hits a **new instance** with zero bytes held. At **264 MiB**, allocation fails with HTTP 500 / `error code: 1101`. That is above the [documented 128 MB/isolate limit](https://developers.cloudflare.com/workers/platform/limits/) — this experiment measures retained JS `Uint8Array` byte length, not Cloudflare's internal heap.

The worker is one file (~70 lines): [`src/worker.ts`](./src/worker.ts).

---

## Reproduce (fastest — no deploy)

You only need Node 24+, pnpm, and network access. Uses our already-deployed worker.

```bash
git clone https://github.com/iterate/cf-experiments.git
cd cf-experiments
corepack enable   # if pnpm is missing
pnpm install
cd experiments/03-kill-durable-object
WORKER_URL=https://03-kill-durable-object.iterate-dev-preview.workers.dev pnpm test:sweep
```

**Expected output** (numbers may vary slightly by account/region; these were stable across 40+ runs in `LHR`):

```text
 197 MiB | stable        | alloc 200 | ping 200 | held 206569472 | inc xxxxxxxx→xxxxxxxx
 198 MiB | replaced      | alloc 200 | ping 200 | held 0         | inc xxxxxxxx→yyyyyyyy
 264 MiB | alloc_failed  | alloc 500 | ...

Summary:
  last stable: 197 MiB
  first replaced: 198 MiB
  first alloc failure: 264 MiB
```

**How to read it**

| Outcome | Meaning |
|---------|---------|
| `stable` | Alloc succeeded, follow-up ping is the **same** DO (`incarnationId` unchanged), bytes still held |
| `replaced` | Alloc returned 200, but ping is a **new** DO — instance was reset between calls |
| `alloc_failed` | Alloc itself failed (500 / Cloudflare 1101) |

Protocol: **fresh DO name per size**, one allocation, one follow-up ping. See [`scripts/memory-threshold-sweep.test.ts`](./scripts/memory-threshold-sweep.test.ts).

---

## Reproduce locally (Miniflare)

Miniflare does **not** enforce the same limits — useful to show the emulator gap, not to find production thresholds.

**Terminal 1 — start the worker:**

```bash
cd experiments/03-kill-durable-object
pnpm dev
```

Wrangler prints a URL. It may not be port 8787 if that port is busy:

```text
Ready on http://localhost:8794
```

**Terminal 2 — run the sweep** (use the exact URL from terminal 1):

```bash
cd experiments/03-kill-durable-object
WORKER_URL=http://localhost:8794 pnpm test:sweep
```

Locally you should see `stable` for all sizes in the default sweep (including 264+ MiB). That contrast is the point.

Fixed port:

```bash
pnpm dev -- --port 8787
```

---

## Reproduce manually (two curls)

Pick a fresh name each time. `bytes` is the size to allocate and retain.

**197 MiB — should survive:**

```bash
NAME="probe-$(uuidgen)"
URL="https://03-kill-durable-object.iterate-dev-preview.workers.dev"
BYTES=$((197 * 1024 * 1024))

curl -s -X POST "$URL/memory?name=$NAME&bytes=$BYTES" | jq .
curl -s "$URL/ping?name=$NAME" | jq .
```

Both responses should share the same `incarnationId`; `heldBytes` should equal `BYTES`.

**198 MiB — should reset:**

```bash
NAME="probe-$(uuidgen)"
BYTES=$((198 * 1024 * 1024))

curl -s -X POST "$URL/memory?name=$NAME&bytes=$BYTES" | jq .
curl -s "$URL/ping?name=$NAME" | jq .
```

Alloc succeeds; ping has a **different** `incarnationId` and `heldBytes: 0`.

---

## Deploy your own copy

```bash
cd experiments/03-kill-durable-object
pnpm exec wrangler login
```

Edit `wrangler.jsonc` — set your `account_id` and a unique `name`.

```bash
pnpm run deploy
```

Then:

```bash
WORKER_URL=https://YOUR-NAME.YOUR-SUBDOMAIN.workers.dev pnpm test:sweep
```

---

## Scripts

| Command | What it does |
|---------|--------------|
| `pnpm dev` | Local Miniflare |
| `pnpm run deploy` | Deploy to Cloudflare |
| `pnpm test:sweep` | Automated threshold sweep (`WORKER_URL` required for non-default host) |
| `pnpm test:kill-response` | `ctx.abort` client body: local shows reason, prod shows generic 1101 page |

Custom sweep sizes:

```bash
SWEEP_MIB=196,197,198,263,264 WORKER_URL=https://... pnpm test:sweep
```

---

## Routes

| Route | Purpose |
|-------|---------|
| `GET /ping?name=X` | Returns `{ message, incarnationId, heldBytes }` |
| `POST /memory?name=X&bytes=N` | Allocate `N` bytes (filled `Uint8Array`), retain on DO |
| `POST /kill?name=X&reason=Y` | `ctx.abort(Y)` — separate Miniflare vs prod observability probe |

`name` selects the DO instance (`getByName`). Default: `default`.

---

## Caveats

- Allocates in **1 MiB filled chunks** (same shape as the original sweeps; one giant buffer behaves differently).
- Not a platform heap metric — only JS-visible retained byte length + instance identity.
- Thresholds may differ by colo, account, runtime version, and compatibility date.
- Miniflare is not trustworthy for OOM testing.

Detailed run history: [`log.md`](./log.md).
