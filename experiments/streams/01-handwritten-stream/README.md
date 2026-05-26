# 01-handwritten-stream

This experiment explores a Cap'n Web RPC interface directly on a `Stream` Durable Object.

See [design-goals.md](./design-goals.md) for the durability/throughput contract we are trying to make
precise, and [stream-design-notes.md](./stream-design-notes.md) for implementation notes.

## What we're trying to find out

Can a `Stream` DO expose `append()` over Cap'n Web while making the durability and egress trade-off
explicit?

The current API separates three modes:

- `confirmed`: `append()` waits for `storage.sync()` before resolving or broadcasting the new event,
  while unrelated RPC/stream egress should keep flowing.
- `best-effort`: `allowUnconfirmed: true` for fastest offset allocation and fan-out.
- `checkpointed`: best-effort writes plus periodic `storage.sync()` barriers to bound the
  unconfirmed window.

The experiment also checks stream fan-out, replay, idempotency, backpressure signals, and Cap'n Web
wire round trips.

## How to run

Start a local worker:

```sh
pnpm wrangler dev --port 8787
```

Run the Cap'n Web integration tests against local Miniflare:

```sh
pnpm vitest run scripts/stream-capnweb.test.ts
```

Run the same tests against the deployed worker:

```sh
WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev \
  pnpm vitest run scripts/stream-capnweb.test.ts
```

Deploy current code:

```sh
pnpm run deploy
```

## How to evaluate results

- Tests should pass both locally and against the deployed worker.
- Cap'n Web wire assertions should show no avoidable `pull` / `push` RPCs while subscribers receive
  live events.
- Durability tests should keep the contracts distinct: `confirmed` waits before exposing the new
  offset/event, while `best-effort` and `checkpointed` expose offsets before an explicit `sync()`
  barrier completes.
- Deployed-only fault probes should log the stream path, offsets, checkpoint timings, and Cloudflare
  ray IDs where available so failures can be traced in Cloudflare observability.

