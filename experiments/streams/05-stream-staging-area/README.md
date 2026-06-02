# stream-staging-area

This experiment is a staging area for the production stream implementation.

It keeps only the pieces we want to graduate:

- a stream processor abstraction with typed event contracts and reducer state
- the stream Durable Object
- the stream processor runner Durable Object
- CapnWeb-over-WebSocket RPC between streams and subscribers
- browser, Node.js, and Workers client entry points
- end-to-end fixtures for append, replay, outbound processors, and one-way batch delivery

It intentionally does not include the old handwritten WebSocket protocol, benchmark runners,
ORPC/minimal-stream comparisons, or measurement-specific storage delay knobs.

## Run

```sh
pnpm --filter @cf-experiments/05-stream-staging-area typecheck
pnpm --filter @cf-experiments/05-stream-staging-area test
```

Run end-to-end tests against a local worker:

```sh
cd experiments/streams/05-stream-staging-area
pnpm exec wrangler dev --port 8793
```

In another shell:

```sh
WORKER_URL=http://localhost:8793 STREAM_STAGING_E2E=true pnpm --filter @cf-experiments/05-stream-staging-area test -- src/stream-capnweb.test.ts
```

## Evaluate

This experiment is successful when the staging API stays small and clear enough to port into the
main repo:

- appends are expressed as event batches
- subscribers consume event batches through a `consumeEvents({ events })` RPC method
- stream delivery does not await each subscriber's `consumeEvents` result
- stream state is reduced by the core stream processor contract
- outbound built-in subscribers are reconciled from `subscription-configured` events

