# High level findings

None yet.

# Notes

## 2026-06-02

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
