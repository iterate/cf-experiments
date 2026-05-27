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

## Subscriber shape: stream, not callback

Subscribers receive a returned `ReadableStream<StreamEvent>`. They do not pass an `onEvent()`
callback capability into the DO.

That shape is intentional. The stream is one-directional fan-out: after the initial `stream()` RPC
sets up the pipe, event delivery does not require subscriber return traffic, per-event method calls,
or acknowledgements. A callback-shaped API would make every event a DO-to-client RPC call with a
return path; this experiment wants append fan-out to be just stream chunks flowing out.

The isolated websocket-frame proof is "pure subscribers do not originate per-event websocket
traffic" in `scripts/stream-capnweb.test.ts`: after subscription, the subscriber receives a burst and
its recorded websocket frames contain no outbound `pull` or `push` frames.

## Design-space guardrails

These are the sharp edges currently protected by tests. The point is not only that the happy path
passes, but that a competing implementation choice should fail a named probe.

| Design decision | If changed | Guarding test |
| --- | --- | --- |
| Use `allowUnconfirmed: true` for all writes, then explicitly `sync()` confirmed appends | Platform output gate would hold unrelated RPC/stream egress behind confirmed appends | "lets unrelated RPC resolve while confirmed append waits for durability" and source sentinel "append uses the allowUnconfirmed write fast path" |
| Validate append argument/event input before idempotency or durability handling | Malformed runtime calls could throw incidental property-access errors, hide behind an idempotent retry, silently drop unknown event envelope fields, accept malformed event types, offset preconditions, idempotency keys, metadata envelopes, or source processor fields, silently ignore misspelled append arguments, or reach later append logic | "rejects malformed append args before reading event or durability", "rejects malformed append events before idempotency or durability handling", "rejects non-string event types at the append envelope boundary", "rejects non-integer event offsets at the append envelope boundary", "rejects non-positive event offsets at the append envelope boundary", "rejects non-string idempotency keys before idempotency lookup", "rejects scalar metadata at the append envelope boundary", "rejects unknown top-level append event fields instead of dropping them", "rejects unknown source envelope fields instead of dropping them", "rejects malformed source processor fields at the append envelope boundary", "rejects unknown source object fields instead of dropping them", "rejects unknown append argument fields before allocating an offset", and "rejects malformed idempotent retries before reading the idempotency index" |
| Keep top-level event validation strict while leaving payload/metadata arbitrary | Audio frame payloads or user metadata could be narrowed while validating the event envelope | "preserves audio-shaped payload and metadata while rejecting only top-level event fields" |
| Broadcast confirmed events only after `storage.sync()` | Subscribers could observe a confirmed event before the append has crossed its durability barrier | "lets subscribers drain old events but not the new confirmed event before durability" |
| Broadcast best-effort events before an explicit sync barrier | Best-effort mode would collapse into confirmed semantics and lose the latency/throughput distinction | "best-effort appends fan out while write debt is still unconfirmed" |
| Resolve and validate per-call durability/settings before allocating an offset | Invalid modes, invalid thresholds, unknown option/setting fields, malformed runtime values like `null`, primitive numbers, non-string object `mode`, non-number thresholds, missing object `mode`, or wrong checkpoint threshold fallback could partially mutate the stream, silently fall back to settings, checkpoint at the wrong cadence, or fail with incidental errors | "rejects invalid per-call durability modes before allocating an offset", "rejects invalid per-call checkpoint thresholds", "rejects non-integer checkpoint thresholds before allocating an offset", "rejects non-number checkpoint thresholds before allocating an offset", "rejects null per-call durability before allocating an offset", "rejects object durability without a mode before allocating an offset", "rejects non-string object durability modes before falling back to stream settings", "rejects unknown durability option fields before allocating an offset", "rejects primitive per-call durability before falling back to stream settings", "uses stream checkpoint threshold for checkpointed string overrides", "uses stream checkpoint threshold for checkpointed object overrides without a threshold", and "rejects invalid stream settings without changing append defaults" |
| Return idempotent retries before durability validation/accounting/fan-out, and fail if the idempotency index points at missing history | Retries could throw on irrelevant invalid retry arguments, duplicate events, write debt, checkpoints, or silently allocate a second offset for one idempotency key | "idempotent append returns the original event and emits once to live subscribers", "does not count idempotent best-effort retries as new unconfirmed writes", "idempotent retries return before conflicting validation can reject them", and "fails corrupted idempotent retries before conflicting validation can reject them" |
| Fire-and-forget checkpoint `blockConcurrencyWhile()` from the triggering append, then re-arm checkpoint scheduling after the sync completes | Awaiting it would turn checkpointed append into a confirmed append at the boundary; omitting the gate would let later RPC run through a checkpoint; failing to clear the in-progress flag would make later checkpoint windows silently stop syncing | "checkpointed appendBatch returns after scheduling but before awaiting the checkpoint", "checkpointed append schedules a delayed checkpoint that gates later RPC", and "checkpointed appends can schedule a second checkpoint after the first completes" |
| Keep checkpointed delivery best-effort while confirmed delivery waits for durability | The two durability modes would become indistinguishable at the subscriber boundary | "checkpointed passes the live-before-durability probe that confirmed intentionally fails" |
| Capture one replay boundary, register each stream once, and use one enqueue path for replay/live fan-out | Replay/live ordering, subscriber accounting, or multi-subscriber fan-out could skip, duplicate, leak, or reorder events | "replays committed history before switching to live appends", "accounts replayed events through the same subscriber enqueue path as live fan-out", "delivers global offset order to multiple subscribers with no per-event RPC from readers or writers", and "removes subscribers whose stream controller rejects enqueue" |
| Treat `maxOffset` as a contiguous committed-history claim and clean up failed replay subscribers | A missing event key could be silently skipped, or a failed replay could leave a dead subscriber in live fan-out | "fails replay loudly when committed history has a missing event key" and "removes replay subscribers when committed history is corrupt" |
| Model subscriptions as returned `ReadableStream`, not `onEvent()` callback RPC | A pure subscriber would originate per-event return traffic/acks | "pure subscribers do not originate per-event websocket traffic" |
| Reject stream arguments because the subscription has no cursor/options surface | Runtime callers could pass `fromOffset`-style options and silently receive the default full replay subscription | "rejects stream arguments instead of silently ignoring subscription options" |
| Keep session-owned stream internals off the Cap'n Web surface | A client could call `streamForSession()` directly and create a subscriber that session disposal does not own | "does not expose session-owned stream internals over capnweb" |
| Do not await per-subscriber delivery in `#broadcast()` and keep iterating after subscriber-local failures | One unread or broken subscriber could slow active subscribers, or prevent later subscribers from seeing the current event | "delivers to an active subscriber while another subscriber does not read" and "continues fan-out to later subscribers after removing a broken subscriber" |
| Remove a subscriber when its stream controller rejects `enqueue()` | One broken stream sink could remain registered and be retried on every append | "removes subscribers whose stream controller rejects enqueue" and "continues fan-out to later subscribers after removing a broken subscriber" |
| Release subscribers on local stream cancel and Cap'n Web session disposal | Dead sessions, including sessions that opened more than one stream, could stay in fan-out forever. In capnweb@0.8.0, client `ReadableStreamDefaultReader.cancel()` alone is not a prompt server cleanup boundary; the subscriber remains until session disposal or later pipe teardown. | "removes locally cancelled streams from live fan-out", "removes cancelled subscribers from live fan-out", "removes every stream opened by a disposed capnweb session", and "documents that capnweb reader cancel does not release the server subscriber" |

## Checkpoints

Checkpointed mode is a throttle for best-effort writes, not confirmed append. Once
`checkpointEveryUnconfirmedAppends` locally accepted appends have accumulated, the DO starts
`storage.sync()` inside `blockConcurrencyWhile()`. That broad gate is intentional for checkpointed
mode: later delivered events wait while the checkpoint catches up.

The append handler that triggers a checkpoint does not await that checkpoint internally; the
`appendBatchDebug()` probe can capture `checkpointInProgress: true` before completion. However,
because the checkpoint runs under `blockConcurrencyWhile()`, later delivered RPC result pulls can be
gated by the checkpoint. The stronger user-visible split is that checkpointed live stream delivery
happens before the delayed checkpoint barrier, while confirmed delivery waits for its explicit
`storage.sync()`.

## Backpressure

The current implementation does not manage stream backpressure. `append()` pushes each new event into
each subscriber's DO-created `ReadableStream` with `controller.enqueue()` and does not pause when the
controller's `desiredSize` reaches zero or below.

There are several queues that would need separate investigation before making this a real flow-control
contract: the DO-created `ReadableStream`, Cap'n Web's pipe into WebSocket frames, the client-side
`ReadableStream`, and WebSocket/runtime transport buffers between isolates/processes. `debug()` exposes
the DO-side `desiredSize` signal for observation only.

The current audio-shaped benchmark suggests this simple one-DO fan-out design is not sufficient for a
10 publisher / 36 active subscriber / 24 kHz PCM16 base64 / 20 ms frame workload if sub-second p95
delivery to every subscriber is required. In the 2026-05-26 deployed run, p95 append-to-all-subscribers
was about 1.08-2.28 s depending on durability mode/run, and p95 same-session publisher self-echo was
about 550 ms-2.19 s under load.

The same benchmark with one publisher and one subscriber does not show that pathology: p95
same-session self-echo was about 32 ms for best-effort, 82 ms for checkpointed, and 38 ms for
confirmed in one deployed run. So the high read-your-own-append latency is not explained simply by
awaiting `storage.sync()`; it appears with many subscribers/publishers and the resulting fan-out /
transport pressure.

With `--measure-append-ack`, the full-load benchmark also records publisher 0's append RPC
acknowledgement for the same events it reads back from its own stream. In best-effort and
checkpointed runs, the stream echo usually arrived at or before the append acknowledgement
(`publisherAckToSelfEchoLatencyMs.p95` near zero / tens of ms), while the append acknowledgement
itself had hundreds of milliseconds of p95 latency. That means the slow read-your-own path is mostly
"time until the append work is serviced under fan-out pressure", not "event was appended but stream
delivery was delayed behind an unnecessary await".

To separate platform/runtime behavior from local WiFi or client machine effects, the worker also
exposes `/benchmark/audio-chaos`. That route starts one orchestrator `BenchmarkRunner` DO plus
separate publisher/subscriber `BenchmarkRunner` DOs, and those runner DOs connect to the `Stream` DO
through the same Cap'n Web WebSocket endpoint external clients use. The DO-side benchmark reports
`*CreatedAtLatencyMs`, measured from the stream DO's committed `createdAt` timestamp to delivery in
the runner DO. That removes local WiFi, but it still compares wall-clock timestamps from different
DOs, so sub-100 ms conclusions can be polluted by clock skew or isolate scheduling. For publisher
self-echo, `publisherAppendStartToSelfEchoLatencyMs` is the sharper metric because append start and
own-stream delivery are both timestamped in the same publisher runner DO. This is not a substitute
for the websocket-frame tests, but it is the better latency probe when the question is whether the
user's local network is polluting the result.

## Debug hooks

The experiment keeps a small debug surface:

- `debug()` exposes settings, counters, subscriber queue signals, and `incarnationId`.
- `ping()` proves unrelated RPC egress can flow while confirmed append is waiting.
- `sync()` is an explicit durability barrier for best-effort/checkpointed modes.
- `kill()` calls `ctx.abort()` for deployed reset/recovery probes.

These are experiment tools, not product API.
