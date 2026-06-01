# 04-capnweb-rawws-parity

This experiment is a minimal in-memory reproduction of Cap'n Web stream fan-out overhead.

## What we're trying to find out

Can we use Cap'n Web for the data plane and get close to raw WebSocket fan-out latency when storage,
replay, idempotency, and durability are completely removed?

The experiment compares:

- `raw`: one JSON WebSocket `event` frame per subscriber per append.
- `capnweb-event`: client-main Cap'n Web callback, one unawaited `afterAppend({ event })` RPC per
  subscriber per append.
- `capnweb-process-events`: client-main Cap'n Web callback, one unawaited
  `processEvents({ events: [event] })` RPC per subscriber per append. This isolates method shape
  and array payload shape from actual timed batching.
- `capnweb-batch`: client-main Cap'n Web callback, one unawaited
  `afterAppendBatch({ events })` RPC per subscriber per shared stream-level flush. The flush window
  is controlled by `batch-ms` and defaults to zero.
- `capnweb-process-events-batch`: client-main Cap'n Web callback, one unawaited
  `processEvents({ events })` RPC per subscriber per shared stream-level flush. This is the same
  batching strategy as `capnweb-batch`, but with the subscriber API shape we expect to use.

`capnweb-event` proves whether removing returned `ReadableStream` write/resolve traffic is enough.
`capnweb-process-events` tests whether a single array-shaped handler changes the cost without
coalescing. `capnweb-batch` tests the next plausible lever: reduce Cap'n Web RPC call count while
still using Cap'n Web for event delivery. `capnweb-process-events-batch` checks whether that winning
coalescing shape also holds with the intended `processEvents({ events })` method.

## How to run

Start a local worker:

```sh
pnpm wrangler dev --port 8794
```

Run wire-shape tests:

```sh
WORKER_URL=http://localhost:8794 pnpm vitest run scripts/wire.test.ts
```

Run the DO-orchestrated benchmark locally:

```sh
curl -sS 'http://localhost:8794/benchmark?mode=raw&publishers=10&subscribers=36&frames-per-publisher=50&pace-ms=20'
curl -sS 'http://localhost:8794/benchmark?mode=capnweb-event&publishers=10&subscribers=36&frames-per-publisher=50&pace-ms=20'
curl -sS 'http://localhost:8794/benchmark?mode=capnweb-process-events&publishers=10&subscribers=36&frames-per-publisher=50&pace-ms=20'
curl -sS 'http://localhost:8794/benchmark?mode=capnweb-batch&publishers=10&subscribers=36&frames-per-publisher=50&pace-ms=20&batch-ms=12'
curl -sS 'http://localhost:8794/benchmark?mode=capnweb-process-events-batch&publishers=10&subscribers=36&frames-per-publisher=50&pace-ms=20&batch-ms=12'
```

Run against the deployed worker:

```sh
WORKER_URL=https://04-capnweb-rawws-parity.iterate-dev-preview.workers.dev \
  pnpm vitest run scripts/wire.test.ts
```

## How to evaluate

Use `allSubscribersCreatedAtLatencyMs.p95`, `appendAckLatencyMs.p95`, `elapsedMs`, and
`serverDebug.fanout`:

- `raw` is the baseline to beat.
- `capnweb-event` should have no subscriber-originated per-event frames, but still pays one Cap'n Web
  RPC call per event per subscriber.
- `capnweb-process-events` should have the same one-way wire property and the same call count as
  `capnweb-event`; if it is faster, the handler/payload shape matters separately from batching.
- `capnweb-batch` should reduce Cap'n Web call count. Compare `serverDebug.fanout.capnwebBatchCalls`
  with `serverDebug.fanout.capnwebBatchEvents`; an ideal 10-publisher, 36-subscriber, 50-frame run
  with `pace-ms=20` batches roughly one call per subscriber per frame tick, or about 1,800 calls.
- `capnweb-process-events-batch` should have a similar call count to `capnweb-batch`, but its
  subscriber wire method is `processEvents`.
  If it approaches rawws latency, the practical answer is batching/coalescing. If it does not,
  Cap'n Web RPC framing remains too costly for this data-plane shape even when one-way and batched.
