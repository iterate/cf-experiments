# High level findings

- Deployed hibernatable Durable Object WebSockets can keep returning
  `setWebSocketAutoResponse()` replies after `ctx.abort()` / OOM even though real
  messages on that old socket no longer reach the restarted Durable Object.
- A short-lived auto-response lease gives bounded detection: cheap auto-pongs are
  allowed while fresh, but an expired lease must be renewed with a real message.

# Detailed notes

## 2026-05-27

- Simplified worker deployed as version `f7298c36-2bc5-41dd-871b-c065ce69f3ae`.
- Deployed test run passed with 2 tests and OOM skipped:
  `WORKER_URL=https://06-hibernation-restarts.iterate-dev-preview.workers.dev pnpm --filter @cf-experiments/06-hibernation-restarts test`.
- Deployed OOM run passed with all 3 tests:
  `RUN_OOM_PROBE=true OOM_BYTES=536870912 WORKER_URL=https://06-hibernation-restarts.iterate-dev-preview.workers.dev pnpm --filter @cf-experiments/06-hibernation-restarts test`.
- Reduced the repro to the smallest protocol surface:
  - literal `"ping"` receives the hibernation auto-response;
  - JSON `{ "op": "app-ping" }` must reach `webSocketMessage()`;
  - `incarnationId` proves which constructor created the response.
- Normal deployed hibernation:
  - after the idle wait, literal `"ping"` returns a stale auto-response from the
    old constructor;
  - JSON `app-ping` wakes the Durable Object and returns a new `incarnationId`.
- `ctx.abort()` and OOM reset:
  - literal `"ping"` still returns the stale auto-response from the old
    incarnation;
  - JSON `app-ping` on the old socket times out;
  - a fresh WebSocket reaches a new `incarnationId`.
- This makes plain `setWebSocketAutoResponse("ping", "pong")` unsuitable as a
  correctness heartbeat. It is only transport/socket-state evidence.
