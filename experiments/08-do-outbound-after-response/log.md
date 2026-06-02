# log — 08-do-outbound-after-response

## Findings (top)

- **Deployed (`iterate-dev-preview`, LHR, 2026-06-01):** `void` slow outbound `fetch()` after `startInline()` RPC returns **completes** and writes `phase: done` to storage. `/inline` HTTP response ~600ms for 3s captun delay; fetch finishes ~3s later. Same `incarnationId` throughout.
- **Alarm path:** Same on deploy — `armAlarm()` returns quickly; `alarm()` runs fetch; completes through **120s** captun delay.
- **Inline vs alarm sweep (deploy, 2026-06-01):** **No divergence** from 1s through **120s** — both `done` at every delay tested.
- **Longer production sweep:** DO-owned work completed at **3 min, 5 min, and 10 min** for `rpc-inline`, `do-fetch`, `root-wait-until`, and `alarm`. This supports the docs model: once the DO has accepted the work and has an awaited outbound `fetch()` in progress, it stays active.
- **Parent awaits slow DO RPC:** With `/await-rpc`, the root Worker awaited a DO RPC while the DO awaited the slow outbound `fetch()`. When the client stayed connected, the request completed normally. When the client aborted after 30s, the root Worker observed `request.signal` abort (`rootAbortedAt` recorded), but the DO outbound fetch still completed at **3 min** and **10 min**.
- **Reproduced cancellation / non-delivery shape:** `root-fire-and-forget` (`void stub.startInline(...)` in the root Worker, then immediately return without `ctx.waitUntil`) never recorded a run even with a 5s delay (`phase: missing`). That is likely the class of bug remembered: the parent Worker invocation ends before the outbound DO RPC is guaranteed to be delivered. `ctx.waitUntil(stub.startInline(...))` fixes delivery because the DO RPC itself is short; the long `fetch()` is then owned by the DO.
- **Miniflare:** Same for the DO-owned paths in spot checks; root fire-and-forget needs explicit local rerun if we care about emulator parity.

Deploy: `doppler run --project os --config dev -- pnpm deploy` → `https://08-do-outbound-after-response.iterate-dev-preview.workers.dev`

## Notes

### 2026-06-01 — production deploy + sweep

Version `3e9a42bf-fee9-4b3d-ab05-155a17d649e1`. Sweep `SWEEP_MS=1000,3000,8000,15000,30000,60000,90000,120000`:

| delayMs | inline | alarm | match |
|--------:|--------|-------|-------|
| 1000 | done (~1.7s) | done (~1.5s) | yes |
| 3000 | done (~3.8s) | done (~3.2s) | yes |
| 8000 | done (~8.3s) | done (~8.2s) | yes |
| 15000 | done (~15.2s) | done (~15.1s) | yes |
| 30000 | done (~30.4s) | done (~30.1s) | yes |
| 60000 | done (~60.2s) | done (~60.4s) | yes |
| 90000 | done (~90.2s) | done (~90.1s) | yes |
| 120000 | done (~120.7s) | done (~120.5s) | yes |

`pnpm test` with `SLOW_MS=8000`: inline + alarm both `done`, status 200, body `slow-ok:8000`.

### 2026-06-01 — parent context mode sweep

Version `1f18bc50-f551-479e-ad26-6a93660bced0` adds three more shapes:

- `do-fetch`: HTTP `stub.fetch()` into the DO, where the DO `fetch()` handler starts a `void` slow outbound fetch and returns immediately.
- `root-fire-and-forget`: root Worker does `void stub.startInline(...)` and returns immediately.
- `root-wait-until`: root Worker does `ctx.waitUntil(stub.startInline(...))` and returns immediately.

Short sanity check (`SWEEP_MS=5000`):

| mode | result |
| --- | --- |
| `rpc-inline` | done (~5.7s) |
| `do-fetch` | done (~5.5s) |
| `root-fire-and-forget` | timeout, last `phase: missing` |
| `root-wait-until` | done (~5.8s) |
| `alarm` | done (~5.5s) |

Long production sweep (`SWEEP_MS=180000,300000`, `POLL_SLACK_MS=15000`):

| delayMs | rpc-inline | do-fetch | root-fire-and-forget | root-wait-until | alarm |
| ---: | --- | --- | --- | --- | --- |
| 180000 | done (~180.6s) | done (~180.5s) | timeout, missing | done (~181.1s) | done (~180.5s) |
| 300000 | done (~300.2s) | done (~300.3s) | timeout, missing | done (~300.7s) | done (~300.3s) |

10-minute production point (`SWEEP_MS=600000`, `POLL_SLACK_MS=30000`, without `root-fire-and-forget`):

| mode | result |
| --- | --- |
| `rpc-inline` | done (~600.7s) |
| `do-fetch` | done (~600.8s) |
| `root-wait-until` | done (~601.3s) |
| `alarm` | done (~600.7s) |

### 2026-06-01 — awaited RPC with parent client abort

Version `65f60b32-50db-4f84-a035-a3acc8a5c74a` added `/await-rpc`: root Worker awaits `stub.doSlowStuff()`, and the DO method awaits the slow outbound `fetch()`.

Initial 10s connected control: `await-rpc` returned after the slow fetch completed; status was already `done` by the time the poll ran.

Version `7111004a-782c-49df-a52d-d60cde0efba6` enabled `enable_request_signal` and recorded root request aborts in DO storage.

Abort results:

| slowMs | abortMs | result |
| ---: | ---: | --- |
| 180000 | 30000 | root abort recorded; DO fetch `done` after ~181s total |
| 600000 | 30000 | root abort recorded; DO fetch `done` after ~601s total |

Interpretation: even when the parent Worker is awaiting the DO RPC and the external client disconnects, the DO invocation continues once accepted. The root Worker does observe the client abort, but the DO outbound fetch is not cancelled in these runs.

Docs cross-check:

- [Workers `ctx.waitUntil`](https://developers.cloudflare.com/workers/runtime-apis/context/) has a 30s post-response/client-disconnect cap for the **Worker invocation**. Here it only needs to keep the short DO RPC alive until the DO accepts work.
- [Durable Object lifecycle](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/) says an in-progress awaited `fetch()` makes the DO non-hibernateable. The DO-owned 10-minute results match that.
- [Durable Object State `waitUntil`](https://developers.cloudflare.com/durable-objects/api/state/#waituntil) says DO `waitUntil` has no effect because DOs remain active with ongoing work or pending I/O.

### 2026-06-01 — Miniflare

`WORKER_URL=http://localhost:8818 SLOW_MS=4000` — both paths `done`.

### 2026-05-27 — scaffold

Worker: `startInline` vs `armAlarm` + `alarm()`. captun `/slow?ms=`.
