# stream-staging-area

This experiment is a staging area for the production stream implementation.

It keeps only the pieces we want to graduate:

- a stream processor abstraction with typed event contracts and reducer state
- the stream Durable Object
- the stream processor runner Durable Object
- CapnWeb-over-WebSocket RPC between streams and subscribers
- browser, Node.js, and Workers client entry points
- a tiny TanStack Start React app served by the same Worker
- end-to-end fixtures for append, replay, outbound processors, and one-way batch delivery

It intentionally does not include the old handwritten WebSocket protocol, benchmark runners,
ORPC/minimal-stream comparisons, or measurement-specific storage delay knobs.

## Run

```sh
pnpm --filter @cf-experiments/05-stream-staging-area typecheck
pnpm --filter @cf-experiments/05-stream-staging-area build
pnpm --filter @cf-experiments/05-stream-staging-area test
pnpm --filter @cf-experiments/05-stream-staging-area test:e2e
```

Run the local TanStack Start + Cloudflare dev server:

```sh
pnpm --filter @cf-experiments/05-stream-staging-area dev
```

Then run end-to-end tests against it:

```sh
WORKER_URL=http://localhost:5173 STREAM_STAGING_E2E=true pnpm --filter @cf-experiments/05-stream-staging-area test -- src/stream-capnweb.test.ts
```

Run browser Playwright tests against local Miniflare:

```sh
pnpm --filter @cf-experiments/05-stream-staging-area test:e2e
```

The browser suite covers append + local mirror updates, same-stream split views,
multi-stream split views, split-pane disposal/handoff, multi-tab leadership handoff, large
stream virtualization and scrolling, raw SQLite download/query, kill/reconnect, and
reset/reconcile behavior.

Run the same browser tests against a deployed worker:

```sh
WORKER_URL=https://stream-staging-area.iterate-dev-preview.workers.dev pnpm --filter @cf-experiments/05-stream-staging-area test:e2e
```

Use the browser client library with a full stream URL:

```ts
import { withStream } from "./src/client-libraries/stream-browser.js";

using stream = withStream({ url: "wss://stream-staging-area.iterate-dev-preview.workers.dev/stream/example" });
const event = await stream.rpc.append({ event: { type: "example", payload: {} } });
```

CapnWeb's `newWebSocketRpcSession()` returns the RPC stub synchronously and queues sends while
the browser WebSocket is connecting, so this does not need an async connect step.

The React app serves one stream viewer:

- `/` redirects to `/streams/`
- `/streams/` shows the root stream with path `/`
- `/streams/anything/else` shows the stream with path `/anything/else`
- `/split-stream?left=/a&right=/b` shows two stream viewers side by side

The top bar has an editable stream path input, a `Go to stream` button when the input differs
from the current route, and the current browser capnweb connection status. The browser viewer
uses Web Locks so only one mounted runtime subscribes for a stream path, mirrors delivered
events into a per-stream OPFS SQLite database, and renders the raw events with TanStack
Virtual. The important reads are ordinary SQL in the route code: a row count and a visible
`local_index` range query. The tools include a raw SQLite database download.

Deploy with Wrangler through the TanStack Start Vite build:

```sh
doppler run --project os --config dev -- pnpm --filter @cf-experiments/05-stream-staging-area run deploy
```

Run the same end-to-end tests against the deployed worker:

```sh
WORKER_URL=https://stream-staging-area.iterate-dev-preview.workers.dev STREAM_STAGING_E2E=true pnpm --filter @cf-experiments/05-stream-staging-area test -- src/stream-capnweb.test.ts
```

## Evaluate

This experiment is successful when the staging API stays small and clear enough to port into the
main repo:

- appends are expressed as event batches
- subscribers consume event batches through a `processEventBatch({ events })` RPC method
- stream delivery does not await each subscriber's `processEventBatch` result
- stream state is reduced by the core stream processor contract
- outbound built-in subscribers are reconciled from `subscription-configured` events
