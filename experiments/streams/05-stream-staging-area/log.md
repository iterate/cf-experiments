# High level findings

None yet.

# Notes

## 2026-06-02

### ~17:00 — Restructured Stream subscription/delivery machinery

Collapsed the six tangled subscription methods in `src/stream.ts`
(`#subscribe`, `#catchUpSubscription`, `#deliverToLiveSubscriptions`, `#deliverBatch`,
`#closeSubscription`, `#reconcileOutboundSubscriptions`) into three
(`#openConnection`, `#reconcile`, `#reconcileOutboundConnections`).

Why it was woozy: the code had grown a `phase: "catching-up" | "live"` state machine
that `design.md` explicitly resolved against ("the runner IS the sink … no
catch-up-vs-live split"). Two delivery paths existed, but live delivery was silently
disabled during catch-up (`if phase !== "live" continue`), so correctness rode entirely
on the catch-up loop re-reading storage. `lastDeliveredOffset` had three writers (one
dead). `appendBatch` fused the atomic commit with both sync delivery and async reconcile.
Reconcile ran on *every* append.

New shape: a connection is a single cursor-driven pump — `getEvents(afterOffset: cursor)`
in a loop until storage is exhausted, then park. Replay and live are the same code path.
`appendBatch` no longer delivers; it calls `connection.wake()`. A `draining` boolean makes
`wake()` idempotent and closes the delivery race (commit happens before `wake()`, so an
in-flight pump's next read always sees the new rows → exactly-once, no drop/double).

Bugs fixed as a side effect:
- **Hibernation gap**: nothing re-established *outbound* connections after a restart
  unless a new append happened to fire reconcile. Constructor now calls `#reconcile()`.
- **Duplicate-dial race**: a `#connecting` Set reserves the key before the
  `await processor.fetch(...)`, so concurrent reconciles can't open two websockets.
- Reconcile moved off the per-append path → runs on boot, on `subscription-configured`
  appends, and on outbound connection loss only.

Consumers updated (no backcompat kept): `runtimeState().runtime.liveSubscriptions` →
`connections` (reshaped); removed dead `Subscription`/`Offset` types from
`stream-types.ts` (folded `Offset` into `StreamCursor`).

Known trade-off + future optimization documented inline in `#openConnection`: the live
path pays one indexed `getEvents` read per batch even when the subscriber is at the head.
Proposal B fast path (hand the in-memory append array straight to the sink when
`cursor === firstNewOffset - 1`) is left as a documented TODO, gated on a benchmark.

Verification:
- `pnpm typecheck`: clean
- `pnpm test`: `10 passed | 9 skipped` (unit)
- `STREAM_STAGING_E2E=true WORKER_URL=http://localhost:5173 pnpm test`: `19 passed`
  — includes the inbound replay→live test and the built-in outbound processor test,
  which exercise the rewritten pump + reconciler end to end.

### Earlier

Added `src/client-libraries/stream-browser.ts` as the first dedicated browser stream client
library. CapnWeb's `newWebSocketRpcSession()` returns synchronously and queues sends while the
browser WebSocket is connecting, so the preferred browser shape is `using stream = withStream({
url })`; `await using stream = await withStream({ url })` also works.

Changed the TanStack Start app into a stream viewer. `/` redirects to `/streams/`, `/streams/`
subscribes to the root stream, and `/streams/*` maps to stream paths under `/`. The old runtime
client helpers moved out of `src/client.ts` because TanStack Start uses that file as its browser
hydration entrypoint.

Verified locally that `/` redirects to `/streams/`, `/streams/` renders the root stream events,
and editing the path to `/anything/else` navigates to `/streams/anything/else` with a new
subscription.

Created `stream-staging-area` as the CapnWeb-only staging version of the handwritten stream
experiment.

Converted the worker entrypoint into a minimal TanStack Start React app using Vite and the
Cloudflare Vite plugin. The same worker still dispatches `/stream/*` and
`/stream-processor-runner/*` to the stream Durable Objects before falling back to the React app.

Verification:
- local `vite dev` root page returned `200 OK`
- local `STREAM_STAGING_E2E=true WORKER_URL=http://localhost:5173 ... stream-capnweb.test.ts`:
  `7 passed`
- deployed `https://stream-staging-area.iterate-dev-preview.workers.dev/` returned `200 OK`
- deployed `STREAM_STAGING_E2E=true WORKER_URL=https://stream-staging-area.iterate-dev-preview.workers.dev ... stream-capnweb.test.ts`:
  `7 passed`
