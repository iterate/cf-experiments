# High level findings

## SOLVED: sqlite-wasm OPFS froze in production builds because cross-origin isolation triggered the async-proxy VFS auto-install

**Symptom:** The browser SQLite mirror (SQLocal → `@sqlite.org/sqlite-wasm`) worked under
`vite dev` but **the SQLocal worker froze on the first SQL statement in every production
build** (`vite preview`, `wrangler dev`, `wrangler deploy`). The page subscribed and the
browser-hosted processor received events (`__receivedEventCount > 0`), but `Events` stayed
`0` and `Storage` stayed `pending` forever.

**Root cause (confirmed):** `@sqlite.org/sqlite-wasm`'s default "opfs" VFS uses a nested
**classic** async-proxy worker (`sqlite3-opfs-async-proxy.js`) + a `SharedArrayBuffer` /
`Atomics.wait` handshake. SQLocal's vite plugin forces `worker.format:'es'` on ALL workers,
so in a production build Rollup emits that proxy as an **ES module** while sqlite-wasm still
instantiates it as a **classic** `new Worker(...)` → parse error → the proxy never signals
ready → the SQLite worker **deadlocks in `Atomics.wait`** (synchronous; no `setTimeout`
guard can fire). Under `vite dev` the proxy is served unbundled/classic so it parses — hence
the dev-vs-prod split. Crucially, `sqlite3InitModule()` **auto-installs that async "opfs"
VFS during init whenever `SharedArrayBuffer` exists** — so even after patching SQLocal to the
proxy-free `opfs-sahpool` VFS, it still froze, because we had *added* COOP/COEP → the page was
cross-origin-isolated → SAB present → the broken proxy spun up *before* the SAH-pool code ran.

**Fix (confirmed on `vite preview` AND `wrangler deploy`, 2026-06-02):** **Remove
cross-origin isolation.** With no COOP/COEP there is no `SharedArrayBuffer`, so
`sqlite3InitModule()`'s OPFS auto-install early-rejects cleanly without ever spawning the
proxy worker, and the patched `opfs-sahpool` VFS (which needs neither SAB nor isolation) runs.
Counterintuitively, **adding** COOP/COEP (the usual "fix" for OPFS) is what *caused* the
deadlock here. Concretely:
- `src/worker.ts` — return the SSR response unmodified (no COOP/COEP).
- removed `public/_headers` and `vite.config.ts` `preview.headers`.
- `vite.config.ts` — `sqlocal({ coi: false })` so dev matches prod (no SAB anywhere).
- kept `patches/sqlocal@0.18.0.patch` (switches SQLocal to `installOpfsSAHPoolVfs` /
  `OpfsSAHPoolDb`).

After the fix, a fresh deployed stream page shows `Events: 2`, `Storage: opfs`,
`DB file size: 32 KB`, `crossOriginIsolated: false`, with 2 event rows rendered.
**Note:** verify in a *fresh* browser profile — a profile that previously loaded the COI
build reports stale `crossOriginIsolated: true` from cache and re-freezes.

Hypotheses ruled out before finding the real cause (all on the production build):

| Hypothesis | Verdict | Evidence |
|---|---|---|
| Missing / 404 assets | ✗ | `sqlite3-*.wasm`, `sqlite3-opfs-async-proxy-*.js`, `sqlite3-worker1-*.js` all emitted + served `200` |
| Wrong MIME | ✗ | wasm `application/wasm`, proxy/worker `text/javascript` |
| Asset COEP/CORP missing | ✗ | added via `public/_headers`; still froze |
| `opfs-sahpool` VFS itself | ✗ | patch alone (with COI still on) still froze — the async-proxy auto-install during init deadlocks first |
| Minifier mangling the glue | ✗ | `build.minify:false` build still froze |

The real differentiator was cross-origin isolation (SAB presence), not bundling/MIME/minify.
Repro of the fix: `scripts/browser-inbound-proof.sh` + a fresh-profile headless Chrome on the
deployed URL now shows `Events` growing and rows rendering.

First-party sources:
- sqlite-wasm OPFS persistence + SAH-pool VFS: <https://sqlite.org/wasm/doc/trunk/persistence.md>
- sqlite-wasm COOP/COEP requirement: <https://sqlite.org/wasm/doc/trunk/index.md>
- SQLocal (the wrapper): <https://sqlocal.dev/guide/setup>
- Cloudflare Workers static-asset response headers (`_headers`): <https://developers.cloudflare.com/workers/static-assets/headers/>
- crossOriginIsolated + COOP/COEP (MDN): <https://developer.mozilla.org/en-US/docs/Web/API/crossOriginIsolated>

# Notes

## 2026-06-03

### Two more browser non-linearities (both size-dependent → now constant)

1. **"kill stream" → slow `woken` on large streams.** On reconnect the browser re-subscribed
   at `afterOffset = -1` (the runner's no-op storage had no cursor), so the Stream DO replayed
   the WHOLE stream before the newly-appended `woken` arrived — delay grew with stream size.
   Fix: the runner's `storage.load` now returns the local SQLite mirror's `maxOffset()` as the
   resume cursor (the events table IS the durable cursor), with a fallback to `-1` if the DB
   read fails. Verified on deployed: kill on a 3002-event stream → only **1** event replayed
   (`recvDelta=1`), constant regardless of size.
2. **Flicker on every append in a long list.** The windowed row query sized itself to the live
   last index (`max(lastIndex, …)`), so every append re-keyed the query; a fresh reactive query
   starts empty, blanking the visible window for a frame. Fix: a FIXED-size, bin-aligned window
   (key changes only once per ~1000 rows of scrolling) + a bounded retained-rows cache so the
   occasional window shift repaints from cache instead of blanking. Verified on deployed: 60
   rapid samples while appending near the tail of a 1200+ event stream → `minRendered=11`,
   `maxPending=0` (never blanks). Both provisional (not yet repeated across sessions).

### Perf: `PRAGMA busy_timeout` causes a ~5s first-open stall with OPFSCoopSyncVFS

First page load sat on "opening sqlite DB" for ~5s (independent of event count). Cause:
OPFSCoopSyncVFS acquires its OPFS `FileSystemSyncAccessHandle` **asynchronously** — its
`jLock` returns `SQLITE_BUSY` and pushes the acquisition onto wa-sqlite's `Module.retryOps`,
which `sqlite3.exec`/`statements` `await` and then retry (resolves in ~one event-loop turn).
Setting `PRAGMA busy_timeout = 5000` **breaks this**: SQLite's core busy handler retries
*synchronously*, blocking the event loop so the async acquisition can never resolve, and it
spins for the entire timeout before yielding to wa-sqlite's `retry()`. Measured init on a
fresh stream: **5081ms → 64ms** after removing the pragma (`wasm 7 / vfs.create 9 / open 4 /
schema 10`). Genuine cross-connection (multi-tab) contention is handled instead by a bounded
JS-layer retry-on-`SQLITE_BUSY` with backoff (`withBusyRetry` in `stream-db.worker.ts`).

Confirmed on the production build: reopening an 808 KB DB (3002 events) → 137ms init, ~253ms
to first rendered rows; deployed cold load → ~1.2s to first render (dominated by page/JS/wasm
download + the capnweb subscribe round-trip, not SQLite). Init is size-independent — the
count is special-cased (O(1)) and the row query is LIMIT-windowed — so thousands of events
load just as fast. NOT yet repeated across sessions — provisional.

### Migrated the browser SQLite mirror from SQLocal → wa-sqlite OPFSCoopSyncVFS

Replaced SQLocal/`@sqlite.org/sqlite-wasm` (opfs-sahpool, kept working only by removing
cross-origin isolation) with **wa-sqlite's `OPFSCoopSyncVFS`** (the `@journeyapps/wa-sqlite`
fork). Why: opfs-sahpool is **single-connection** — only one tab can open the DB — so
multi-tab reactive reads were impossible. OPFSCoopSyncVFS allows **multiple cooperative
per-tab connections** against one OPFS file, needs **no SharedArrayBuffer, no async-proxy
worker, and no COOP/COEP**, and is the VFS PowerSync recommends as the cross-browser
default (Chrome / Edge / Safari 16.4+ / mobile Safari 16.4+).

Architecture (all in `src/client-libraries/`):
- `stream-db.worker.ts` — per-tab dedicated worker owning one wa-sqlite connection; generic
  `exec`/`batch`/`export`. `FileSystemSyncAccessHandle` only exists in a dedicated worker
  (never main thread / SharedWorker), hence one worker per tab.
- `stream-leader.ts` — single-writer election via the **Web Locks API**. The lock holder is
  the writer (subscribes + writes); it auto-releases on tab close for seamless failover.
- `stream-browser-db.ts` — worker client + an **offset-range-aware reactive query layer**:
  a query declares the offset range its result depends on; an append announces the range it
  wrote; only intersecting queries re-run. A fixed historical window is immutable under
  append-only and never re-runs. The **event count is special-cased** — advanced straight
  from the writer's cross-tab change broadcast (O(1), zero per-append SQL), since the row
  count is all TanStack Virtual needs. Cross-tab freshness rides a `BroadcastChannel`.
- `use-stream-query.ts` — React 19 `useSyncExternalStore` hooks (cached stable snapshot;
  async re-run triggered in `subscribe`). Not `use()`+Suspense: React docs warn against
  suspending on a useSyncExternalStore value. oxlint `--deny-warnings` clean.
- Added a `processor_state(processor_slug, state)` table mirroring the Stream DO's, so the
  browser can host multiple reducing processors next (same SQLite snapshot shape per runtime).

Verified on the **production build** (`vite preview`) AND **deployed Cloudflare**, headless
Chrome for Testing: events render and update live; a **second tab that never subscribed
(`__receivedEventCount === 0`)** reflects the writer's appends reactively via the shared OPFS
file; 24/24 e2e pass. Multi-tab quirk worth noting: the first cross-tab propagation after an
append can lag a few seconds while OPFSCoopSyncVFS hands the single `SyncAccessHandle` between
the two connections (covered by `PRAGMA busy_timeout`); it then converges. NOT yet repeated
across sessions — provisional.

First-party sources:
- PowerSync, "Current State of SQLite Persistence on the Web" (May 2026): <https://powersync.com/blog/sqlite-persistence-on-the-web>
- wa-sqlite (`OPFSCoopSyncVFS`): <https://github.com/rhashimoto/wa-sqlite> ; PowerSync fork: `@journeyapps/wa-sqlite`
- React `useSyncExternalStore`: <https://react.dev/reference/react/useSyncExternalStore>
- Web Locks API (MDN): <https://developer.mozilla.org/en-US/docs/Web/API/Web_Locks_API>
- OPFS `createSyncAccessHandle` (MDN): <https://developer.mozilla.org/en-US/docs/Web/API/FileSystemFileHandle/createSyncAccessHandle>

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
