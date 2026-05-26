# Stream Durability / Throughput Design Goals

This experiment is about finding the right stream append contract for Durable Objects when
durability, throughput, and outbound network timing are all first-class concerns.

## Primary Goals

`append()` is now async at the remote RPC boundary. The Durable Object should allocate offsets in a
synchronous critical section, but a remote caller should receive the `StreamEvent` only when the
selected durability mode says the offset is safe to observe.

Streams must be configurable across an active durability/throughput spectrum:

- **Durable append:** if `append()` returns a new offset, that offset is safe to treat as durable
  across Cloudflare's storage replication boundary. The caller can rely on the write having made it
  through the platform's confirmation path before externally observing success.
- **Fast append:** `append()` returns as soon as the Durable Object has accepted the event locally and
  enqueued the storage writes. Durability is best-effort until a later checkpoint.
- **Checkpointed append:** the stream permits locally accepted, unconfirmed appends, then forces a
  durability checkpoint after a configured number of appends.

Outbound network timing should be an explicit part of the contract. If the caller wants an observed
offset to mean "confirmed durable", `confirmed` mode waits on an explicit `storage.sync()` before
resolving the append or broadcasting the new event. If the caller wants the fastest possible
acknowledgement, `best-effort` and `checkpointed` expose offsets before an explicit durability barrier
completes.

The behavior we would prefer for most streams is more specific than the current implementation:

- `append()` should return to its remote caller only after the appended offset is confirmed durable.
- That durability wait should apply only to the append result and any work causally downstream of that
  append.
- Unrelated egress should keep flowing while the append write is being confirmed. For example, if a
  stream has 100 subscribers and some are far behind the front of the log, those subscribers should be
  able to keep receiving already-stored events while a new append is waiting for durability.
- Other RPC calls into the same Durable Object that do not depend on the unconfirmed append should
  still be able to receive responses.

In other words: the desired primitive is a per-append / per-causal-chain durability gate, not the
Durable Object's global output gate for all outgoing messages after a storage write.

## Platform Semantics This Design Depends On

For SQLite-backed Durable Objects, async KV writes through `ctx.storage.put()` support
`allowUnconfirmed: true`. With that option, outgoing network messages are not held by the write's
output gate.

Multiple async `put()` calls with no intervening `await` are automatically coalesced and submitted
atomically. If the append writes the event, idempotency index, and meta offset this way, those keys
should be committed as one storage batch.

`ctx.storage.sync()` is the explicit durability barrier. It resolves only after pending writes in the
write buffer, including writes submitted with `allowUnconfirmed`, have persisted.

`ctx.blockConcurrencyWhile()` is not a durability acknowledgement to the current caller. It is useful
for preventing later events from entering the Durable Object while a checkpoint is in progress, but it
is the wrong primitive for `confirmed` append acknowledgement because it blocks unrelated delivered
events too broadly.

## Open Design Tension

The old sync-append shape could not satisfy all three desirable properties:

- `append()` returns a concrete event to the remote caller immediately.
- durable mode means "returned offset is already durably confirmed."
- durability waits only gate the append result and causally downstream work, not unrelated outbound
  bytes from the same Durable Object.

Making `append()` async is the escape hatch:

- allocate the offset and write storage keys before the first `await`;
- for `confirmed`, wait on `storage.sync()` before resolving the append or broadcasting the new event;
- for `best-effort` / `checkpointed`, return and broadcast without waiting for that barrier.

This distinction matters for the API contract. "Run a checkpoint every N unconfirmed appends" is
weaker than "this append's returned offset is confirmed before the caller observes it."

## Implemented Configuration Shape

The setting should describe the acknowledgement contract rather than a storage implementation detail.

```ts
type AppendDurabilityMode = "confirmed" | "best-effort" | "checkpointed";

type StreamDoSettings = {
  defaultAppendDurabilityMode: AppendDurabilityMode;
  checkpointEveryUnconfirmedAppends: number;
};
```

Per-call overrides use the same language:

```ts
append({ event, durability: "best-effort" });
append({ event, durability: { mode: "checkpointed", checkpointEveryUnconfirmedAppends: 10 } });
```

The intentionally sharp edge is `confirmed`: this is the only mode where observing the returned offset
over RPC can mean "Cloudflare confirmed the writes durable". It gets that guarantee by writing with
`allowUnconfirmed: true`, then awaiting `storage.sync()` before resolving or broadcasting.
`best-effort` and `checkpointed` use the same unconfirmed write path, but their returned offsets only
mean local acceptance until an explicit barrier completes.

This means the implemented API can provide all three desired properties for `confirmed`, with one
important caveat: while `storage.sync()` is actually pending, Cloudflare input-gate behavior may still
limit which later events are delivered. The debug delay creates a crisp test window before `sync()` so
we can prove the causal contract around our own code.

The API keeps the remaining trade-offs explicit instead of naming a periodic checkpoint "confirmed".

## Test Strategy

Unit/integration tests should separate three claims that are easy to conflate.

### 1. Append API Shape

Assert that `append()` and `appendBatch()` allocate offsets in order, but that confirmed append RPCs do
not resolve until the durability barrier completes.

### 2. Output-Gate Independence

Use the Cap'n Web frame recorder to compare wire timing for:

- unconfirmed async `put(..., { allowUnconfirmed: true })`;
- default-gated async `put()`;
- async `put(..., { allowUnconfirmed: true })` followed by explicit `storage.sync()`.

The test should prove that ordinary append fan-out / RPC bytes are not being delayed by storage writes
unless the chosen policy explicitly waits.

### 3. Durability Barriers

Add a `sync()` or `checkpoint()` method that awaits `ctx.storage.sync()` and returns a marker. Tests
can append, call the barrier, then assert that later reads see all offsets. This verifies the local
barrier contract, but does not by itself prove multi-machine durability under failure.

### 4. Kill / Reset Probes

Add a `kill()` method that calls `ctx.abort(reason)` and a fresh-instance marker such as
`incarnationId`. This lets tests distinguish "same in-memory object still had the write buffered" from
"the object restarted and recovered from storage."

Useful probes:

- append without checkpoint, kill immediately, reconnect, and count/replay;
- append with checkpoint, kill after checkpoint returns, reconnect, and count/replay;
- append enough events to fill a window, trigger checkpoint, kill during or immediately after the
  checkpoint, reconnect, and verify the recovered offset sequence;
- repeat against local Miniflare and deployed Workers because failure/restart behavior may differ.

### 5. Deployed Fault Tests

The strongest durability claim needs deployed tests. Local Miniflare can check API sequencing, but it
does not prove SRS quorum durability. A deployed harness should log Cloudflare ray IDs, stream name,
append offsets, checkpoint timings, kill timing, and recovered offsets after reconnect.

## Current Hypothesis

The current implementation path is:

1. `confirmed`: shared `writeEventFromKv(..., { allowUnconfirmedWrites: false })`.
2. `best-effort`: shared `writeEventFromKv(..., { allowUnconfirmedWrites: true })`.
3. `checkpointed`: same as best-effort, plus `storage.sync()` under `blockConcurrencyWhile()` once the
   unconfirmed-append count reaches the configured threshold.
4. `sync()` is an explicit async durability barrier for tests and callers that want to separate fast
   offset allocation from later durability confirmation.
5. `kill()` exists as a reset probe for deployed fault tests; local Miniflare can check API sequencing
   but not SRS quorum durability.
