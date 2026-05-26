# Stream design

This experiment asks whether a `Stream` Durable Object can expose a Cap'n Web append/read API with
explicit durability and egress trade-offs.

## Platform semantics

- SQLite-backed Durable Objects have synchronous KV reads/writes at `ctx.storage.kv`:
  https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#synchronous-kv-api
- Async KV writes support `allowUnconfirmed`. By default, outgoing messages from the DO wait for prior
  writes to flush; `allowUnconfirmed: true` opts out:
  https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#supported-options
- `storage.sync()` resolves after pending writes, including `allowUnconfirmed` writes, have persisted:
  https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#sync
- `blockConcurrencyWhile()` blocks later delivered events while its async callback runs:
  https://developers.cloudflare.com/durable-objects/api/state/#blockconcurrencywhile
- Workers RPC can pass `ReadableStream` values, but this experiment runs Cap'n Web inside the `Stream`
  DO so `StreamEvent` chunks do not cross a DO-stub byte-stream boundary:
  https://developers.cloudflare.com/workers/runtime-apis/rpc/#readablestream-writeablestream-request-and-response

## Append contract

`append()` is async at the remote RPC boundary. Offset allocation still happens before the first
`await`: idempotency lookup, next-offset allocation, and the event/idempotency/meta writes are all
issued first.

All modes write with `allowUnconfirmed: true` so unrelated egress is not held by the platform's global
output gate.

| Mode | RPC resolves | Subscribers see new event | Durability meaning |
| --- | --- | --- | --- |
| `confirmed` | after `storage.sync()` | after `storage.sync()` | returned offset is confirmed by the explicit barrier |
| `best-effort` | immediately after local acceptance | immediately after local acceptance | may be lost on crash before platform flush |
| `checkpointed` | like best-effort | like best-effort | periodically starts `storage.sync()` to bound the unconfirmed window |

The key causal rule: confirmed mode must not send bytes about the new offset before the durability
barrier completes, but unrelated bytes should continue flowing. For example, a subscriber draining old
history and a `ping()` RPC should not be blocked by a new confirmed append waiting for `storage.sync()`.

## Checkpoints

Checkpointed mode is a throttle for best-effort writes, not confirmed append. Once
`checkpointEveryUnconfirmedAppends` locally accepted appends have accumulated, the DO starts
`storage.sync()` inside `blockConcurrencyWhile()`. That broad gate is intentional for checkpointed
mode: later delivered events wait while the checkpoint catches up.

The append that triggers a checkpoint can still resolve before the checkpoint completes.

## Backpressure

The current implementation does not manage stream backpressure. `append()` pushes each new event into
each subscriber's DO-created `ReadableStream` with `controller.enqueue()` and does not pause when the
controller's `desiredSize` reaches zero or below.

There are several queues that would need separate investigation before making this a real flow-control
contract: the DO-created `ReadableStream`, Cap'n Web's pipe into WebSocket frames, the client-side
`ReadableStream`, and WebSocket/runtime transport buffers between isolates/processes. `debug()` exposes
the DO-side `desiredSize` signal for observation only.

## Debug hooks

The experiment keeps a small debug surface:

- `debug()` exposes settings, counters, subscriber queue signals, and `incarnationId`.
- `ping()` proves unrelated RPC egress can flow while confirmed append is waiting.
- `sync()` is an explicit durability barrier for best-effort/checkpointed modes.
- `kill()` calls `ctx.abort()` for deployed reset/recovery probes.

These are experiment tools, not product API.
