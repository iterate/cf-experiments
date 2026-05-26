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

## Candidate Configuration Shape

The setting should describe the acknowledgement contract rather than a storage implementation detail.

```ts
type AppendDurability =
  | { mode: "confirmed" }
  | { mode: "windowed"; maxUnconfirmedAppends: number }
  | { mode: "best-effort" };
```

Per-call overrides can use the same language:

```ts
append({ event, durability: { mode: "best-effort" } });
```

Questions to resolve before implementing this shape:

- Does `confirmed` require changing `append()` to async, or can the stream arrange a pre-append
  checkpoint that makes the returned offset honestly confirmed?
- Does `windowed: 0` mean "confirmed append" or "do not admit a second append until the first is
  confirmed"?
- Are subscribers allowed to receive an event before the event is confirmed in `confirmed` mode, or is
  subscriber delivery part of the acknowledgement contract?

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

The safe implementation path is likely:

1. Always use async KV writes with `allowUnconfirmed: true` for append storage so outbound bytes are
   never accidentally held by the platform output gate.
2. Track a local unconfirmed append count.
3. Use `storage.sync()` as the only explicit durability barrier.
4. Treat `blockConcurrencyWhile()` as an input-throttling tool during checkpoints, not as proof that
   the current append's returned offset was durable.
5. Be precise in naming: "confirmed", "windowed", and "best-effort" should describe what the caller is
   allowed to believe after observing an offset.
