# 06-hibernation-restarts

When cancelling a Durable Object using `this.ctx.abort()`, WebSocket clients for
hibernatable WebSockets can keep receiving auto-responses even though real
messages no longer reach the restarted object. This is a tiny deployed-only
repro for that behavior.

## Question

Is the observed behavior intended, or is it a platform bug?

The minimal scenario is:

1. A client opens a hibernatable WebSocket to a Durable Object.
2. The Durable Object accepts it with `ctx.acceptWebSocket(server)`.
3. The Durable Object is later restarted, either by normal hibernation or by a
   reset such as `ctx.abort()` / memory pressure.
4. The client sends messages on the original WebSocket.

For normal hibernation, the expected behavior is clear: the client WebSocket
continues to work. A real message from the client wakes the Durable Object, the
constructor reruns, and the message is delivered to `webSocketMessage()`.

This experiment asks whether reset/crash recovery has the same client-visible
behavior.

## Expected Behavior

The best behavior would be the same as normal hibernation:

- the client WebSocket remains open;
- after the Durable Object boots again, the existing WebSocket is attached to
  the new Durable Object incarnation;
- real messages from the client reach `webSocketMessage()`;
- server messages can still be sent to that client.

That would make restart recovery and hibernation equivalent from the client's
point of view.

## Observed Behavior

The deployed worker does behave correctly for normal idle hibernation:

- the client WebSocket stays open across the idle wait;
- a real application message wakes the Durable Object;
- the response contains a new `incarnationId`, proving the constructor reran.

After `ctx.abort()` and after an OOM-style memory reset, we observed a worse
state:

- the client WebSocket still appears open;
- a literal `"ping"` can still receive the configured hibernation
  auto-response;
- a real application message on the same WebSocket times out and does not reach
  the restarted Durable Object;
- a fresh WebSocket connection reaches a new `incarnationId`.

That means the client appears to have no passive way to learn that the
connection is no longer useful. It can only discover the problem by sending a
real application message through the WebSocket and checking whether the Durable
Object answers. That seems to undermine a major purpose of hibernatable
WebSockets: the client cannot rely on the open WebSocket or the auto-response to
prove that the Durable Object can still send it application events.

## Better Behaviors

If the observed behavior is not the intended one, the ideal behavior would be:

- after reset, the restarted Durable Object picks up the waiting client
  WebSocket, just like it does after normal hibernation.

A less ideal but still better behavior would be:

- after reset, the platform closes the client WebSocket, so the client can
  reconnect instead of silently waiting on a dead connection.

The observed behavior is worse than both: the WebSocket can stay open-looking
and can still receive auto-responses, while real application messages no longer
reach the Durable Object.

## Workaround Found

The only workaround we found is to make the auto-response a short-lived lease:

```ts
this.ctx.setWebSocketAutoResponse(
  new WebSocketRequestResponsePair(
    "ping",
    JSON.stringify({ op: "auto-pong", incarnationId, expiresAt }),
  ),
);
```

This gives bounded reset detection without keeping the Durable Object awake all
the time:

- while the lease is fresh, the client can use cheap literal `"ping"` messages
  that do not wake the Durable Object;
- once the lease is expired, the client must send a real application message;
- if the Durable Object merely hibernated, that real message wakes it and gets a
  fresh response;
- if the WebSocket is in the observed post-reset state, the real message times
  out and the client reconnects.

It is not clear whether this workaround should be necessary. The lease needs to
be longer than the hibernation idle threshold; this experiment uses `1s` only to
make the test fast.

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
