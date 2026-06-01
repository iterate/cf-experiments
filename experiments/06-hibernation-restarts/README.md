# 06-hibernation-restarts

Tiny deployed-only repro for hibernatable Durable Object WebSockets after the
object is restarted.

## What we're trying to find out

Can a stream processor safely hold a hibernatable WebSocket connection to a
Durable Object?

The specific scary case is:

1. Client opens a hibernatable WebSocket with `ctx.acceptWebSocket(server)`.
2. Durable Object crashes or is reset.
3. Client-side WebSocket still looks open.
4. The old socket no longer reaches the restarted Durable Object.

That is different from normal hibernation. During normal hibernation, a real
message from the client should wake the Durable Object, rerun the constructor,
and deliver the message to `webSocketMessage()`.

## What we observed

On the deployed worker, normal idle hibernation behaves as expected:

- The client WebSocket stays open across an idle wait.
- A real application message wakes the Durable Object.
- The response contains a new `incarnationId`, proving the constructor reran.

After `ctx.abort()` and after an OOM-style memory reset, the old client socket is
different:

- A literal `"ping"` can still receive the hibernation auto-response.
- A real application message times out and does not reach the restarted Durable
  Object.
- A fresh WebSocket connection reaches a new `incarnationId`.

So a plain `setWebSocketAutoResponse("ping", "pong")` is not a correctness
heartbeat. It can prove that Cloudflare still has some socket state, but it does
not prove that the socket is attached to the current Durable Object incarnation.

## Minimal mitigation

Make the auto-response a short-lived lease:

```ts
this.ctx.setWebSocketAutoResponse(
  new WebSocketRequestResponsePair(
    "ping",
    JSON.stringify({ op: "auto-pong", incarnationId, expiresAt }),
  ),
);
```

The client can use cheap literal `"ping"` messages while the lease is fresh.
When `expiresAt` is in the past, the client must send a real application message
to renew the lease.

- If the Durable Object merely hibernated, the real message wakes it and gets a
  fresh response.
- If the old socket is a post-crash ghost, the real message times out and the
  client reconnects.

This gives bounded crash detection without keeping the Durable Object awake all
the time. The lease needs to be longer than the hibernation idle threshold; this
experiment uses `1s` only to make the test fast.

## How to run

Local tests intentionally skip because Miniflare does not prove deployed
hibernation behavior:

```sh
pnpm --filter @cf-experiments/06-hibernation-restarts test
```

Deploy and run the non-OOM probes:

```sh
pnpm --filter @cf-experiments/06-hibernation-restarts run deploy
WORKER_URL=https://06-hibernation-restarts.iterate-dev-preview.workers.dev \
  pnpm --filter @cf-experiments/06-hibernation-restarts test
```

Run the opt-in OOM probe:

```sh
RUN_OOM_PROBE=true \
OOM_BYTES=536870912 \
WORKER_URL=https://06-hibernation-restarts.iterate-dev-preview.workers.dev \
  pnpm --filter @cf-experiments/06-hibernation-restarts test
```

Parameters:

- `WORKER_URL`: deployed or local worker URL. Deployed-only probes skip on localhost.
- `HIBERNATION_WAIT_MS`: idle wait before proving hibernation. Default `15000`.
- `RUN_OOM_PROBE=true`: enables the memory-reset probe.
- `OOM_BYTES`: bytes allocated by the Durable Object. Default `536870912`.

## How to evaluate results

The hibernation test passes only if the same client WebSocket can send a real
message after the idle wait and receives a different `incarnationId`.

The reset tests pass only if:

- the old socket still receives an auto-response with the old `incarnationId`;
- that auto-response can be observed after its `expiresAt` is stale;
- a real application message on the old socket times out;
- a fresh socket reaches a different `incarnationId`.
