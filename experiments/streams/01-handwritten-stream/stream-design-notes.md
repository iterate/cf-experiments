# Stream design notes

These notes describe the current `Stream` Durable Object design. The shorter goal statement lives in
[`design-goals.md`](./design-goals.md); this file is for implementation reasoning.

## First-party platform semantics

The implementation depends on these Cloudflare primitives:

- SQLite-backed Durable Objects have a synchronous KV API at `ctx.storage.kv`:
  https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#synchronous-kv-api
- Async KV writes support `allowUnconfirmed`. By default, outgoing network messages from the DO wait
  for previous writes to flush; `allowUnconfirmed: true` opts out:
  https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#supported-options
- `storage.sync()` resolves once pending writes, including writes submitted with `allowUnconfirmed`,
  have persisted:
  https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#sync
- `blockConcurrencyWhile()` blocks other delivered events while its async callback runs:
  https://developers.cloudflare.com/durable-objects/api/state/#blockconcurrencywhile
- Workers RPC supports passing `ReadableStream` values across RPC calls, but this experiment runs
  Cap'n Web inside the `Stream` DO so stream chunks are Cap'n Web object chunks rather than a DO-stub
  byte-stream boundary:
  https://developers.cloudflare.com/workers/runtime-apis/rpc/#readablestream-writeablestream-request-and-response

## Append contract

`append()` is async at the Cap'n Web RPC boundary.

Offset allocation still happens in a synchronous critical section before the first `await`:

1. Check the idempotency index.
2. Read the current meta offset.
3. Allocate the next offset.
4. Enqueue the event, idempotency index, and meta writes.

All modes use `allowUnconfirmed: true` for those writes. This avoids the platform's global output gate:
an unrelated subscriber or RPC response should not be held just because a new append is waiting for its
own durability acknowledgement.

After allocation, modes diverge:

- `confirmed`: wait for `storage.sync()`, then broadcast the new event and resolve the append RPC.
- `best-effort`: broadcast and resolve immediately.
- `checkpointed`: broadcast and resolve immediately, but start periodic `storage.sync()` barriers once
  enough unconfirmed appends have accumulated.

The important causal rule is: in confirmed mode, bytes about the new offset must not leave before the
explicit durability barrier completes. Bytes unrelated to the new offset should continue to flow.

## Why not use normal output gates for confirmed append?

Normal output gates can make "remote caller observed the offset after durability" true, but they are
too broad for the stream shape we want.

If a stream has many subscribers, some may be draining older already-durable history while one writer
is appending a new event. Those subscribers should be allowed to keep receiving old events while the
new append waits for durability. Likewise, unrelated RPCs such as `ping()` should be able to resolve.

Using `allowUnconfirmed: true` plus an explicit `storage.sync()` inside the append RPC gives a narrower
causal gate: only the append result and the new event's fan-out wait.

## Broadcast timing

Confirmed mode broadcasts after `storage.sync()`.

Best-effort and checkpointed modes broadcast before durability is confirmed. This is intentional:
those modes optimize for throughput and lowest egress latency. A crash before a later platform flush or
explicit sync may lose events that subscribers already saw.

Tests assert this split:

- unrelated `ping()` resolves while confirmed append is delayed;
- subscribers can drain old durable history while confirmed append is delayed;
- the new confirmed event does not arrive until after append resolves;
- best-effort appends contribute to `unconfirmedWriteCount` until `sync()`.

## Checkpoints

Checkpointed mode is not confirmed append.

It is a stream-level throttle for best-effort writes: once `checkpointEveryUnconfirmedAppends` has been
accepted, the DO starts `storage.sync()` inside `blockConcurrencyWhile()`. This intentionally blocks
later delivered events while the checkpoint catches up.

The append that triggers a checkpoint still resolves before the checkpoint completes. Therefore a
checkpointed offset means "accepted locally and inside a bounded unconfirmed window", not "confirmed
durable".

## Backpressure and buffering

`stream({ desiredBufferedEvents })` passes that value to the DO-created `ReadableStream` as the Web
Streams `highWaterMark`. Because chunks are `StreamEvent` objects and no custom `size()` function is
provided, the unit is events, not bytes.

This is only the DO-side stream queue. There are also Cap'n Web pipe buffers, client-side stream
buffers, and WebSocket/runtime transport buffers. `desiredBufferedEvents` is a signal observed via
`controller.desiredSize`; it is not a hard cap. Current append/fan-out code is push-based and does not
stop enqueueing when `desiredSize <= 0`.

Pressure and crash behavior still needs deployed stress testing.

## Debug hooks

The following RPCs exist to make the experiment observable:

- `ping()` proves unrelated RPC egress can flow while confirmed append is waiting.
- `durabilityDebug()` exposes in-memory counters and `incarnationId`.
- `streamDebug()` exposes subscriber queue signals.
- `sync()` is an explicit durability barrier for best-effort/checkpointed modes.
- `kill()` calls `ctx.abort()` and is intended for deployed reset/recovery probes.

These are experiment tools, not product API.
