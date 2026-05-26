# Stream Durability / Throughput Design Goals

This experiment is about finding the right stream append contract for Durable Objects when
durability, throughput, and outbound network timing are all first-class concerns.

## Primary Goals

`append()` must remain synchronous from the caller's point of view. It should return a committed
`StreamEvent` with a new offset directly, not a `Promise<StreamEvent>`.

Streams must be configurable across an active durability/throughput spectrum:

- **Durable append:** if `append()` returns a new offset, that offset is safe to treat as durable
  across Cloudflare's storage replication boundary. The caller can rely on the write having made it
  through the platform's confirmation path before externally observing success.
- **Fast append:** `append()` returns as soon as the Durable Object has accepted the event locally and
  enqueued the storage writes. Durability is best-effort until a later checkpoint.
- **Windowed append:** the stream permits up to `N` locally accepted, unconfirmed appends, then forces
  a durability checkpoint before accepting more append work.

Outbound network bytes must not be held by ordinary Durable Object storage writes. Most stream
instances are expected to be durable and should not allow any unconfirmed writes, but stream fan-out
and Cap'n Web/RPC bytes should not accidentally depend on the default Durable Object output gate.

That last point rules out relying on default-gated storage writes as the durable mode. Durable mode
should be explicit: write with `allowUnconfirmed: true` to avoid the output gate, then use an explicit
durability barrier when the selected policy requires one.

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
for preventing later events from entering the Durable Object while a checkpoint is in progress, but a
synchronous `append()` that starts `blockConcurrencyWhile(async () => storage.sync())` and then returns
has still returned before the checkpoint has resolved.

## Open Design Tension

There are three desirable properties that may not all be compatible in one method shape:

- `append()` is synchronous.
- durable mode means "returned offset is already durably confirmed."
- durable writes do not hold outbound network bytes via the platform output gate.

If `append()` cannot be `async`, then `await storage.sync()` cannot happen inside the returned method.
The likely escape hatch is to move the durability wait to an earlier boundary:

- a stream-level policy can block entering the next append until the previous checkpoint has finished;
- `append()` can synchronously return only after observing that no unconfirmed writes are outstanding;
- the call that fills a durability window may trigger a checkpoint that delays later appends, but it
  cannot truthfully claim its own returned offset was confirmed unless the method blocks somehow.

This distinction matters for the API contract. "No more than N unconfirmed appends will be admitted"
is weaker than "this append's returned offset is confirmed before the caller observes it."

## Implemented Configuration Shape

The setting should describe the acknowledgement contract rather than a storage implementation detail.

```ts
type AppendDurabilityMode = "confirmed" | "best-effort" | "checkpointed";

type StreamDoSettings = {
  defaultAppendDurabilityMode: AppendDurabilityMode;
  checkpointEveryUnconfirmedWrites: number;
};
```

Per-call overrides use the same language:

```ts
append({ event, durability: "best-effort" });
append({ event, durability: { mode: "checkpointed", checkpointEveryUnconfirmedWrites: 10 } });
```

The intentionally sharp edge is `confirmed`: this is the only mode where observing the returned offset
over RPC can mean "Cloudflare confirmed the writes durable", and it gets that guarantee from normal
Durable Object output gates. `best-effort` and `checkpointed` use `allowUnconfirmed: true`, so outbound
bytes are not held by storage writes, but their returned offsets only mean local acceptance until an
explicit barrier completes.

This means there is no mode that simultaneously provides all three properties:

- `append()` is synchronous;
- outbound bytes are never held by storage writes;
- the returned offset is already durably confirmed when observed by a remote caller.

The API keeps those trade-offs explicit instead of naming a periodic checkpoint "confirmed".

## Test Strategy

Unit/integration tests should separate three claims that are easy to conflate.

### 1. Append API Shape

Assert that `append()` and `appendBatch()` still return synchronously from the RPC target's point of
view. If the implementation needs an async method for confirmed durability, that should be an explicit
new API rather than an accidental change to `append()`.

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
   unconfirmed-write count reaches the configured threshold.
4. `sync()` is an explicit async durability barrier for tests and callers that want to separate fast
   offset allocation from later durability confirmation.
5. `kill()` exists as a reset probe for deployed fault tests; local Miniflare can check API sequencing
   but not SRS quorum durability.
