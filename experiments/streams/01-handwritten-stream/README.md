# 01-handwritten-stream

This experiment explores a Cap'n Web RPC interface directly on a `Stream` Durable Object.

See [design.md](./design.md) for the durability/throughput contract and implementation notes.

## What we're trying to find out

Can a `Stream` DO expose `append()` over Cap'n Web while making the durability and egress trade-off
explicit?

The current API separates three modes:

- `confirmed`: `append()` waits for `storage.sync()` before resolving or broadcasting the new event,
  while unrelated RPC/stream egress should keep flowing.
- `best-effort`: `allowUnconfirmed: true` for fastest offset allocation and fan-out.
- `checkpointed`: best-effort writes plus periodic `storage.sync()` barriers to bound the
  unconfirmed window.

The experiment also checks stream fan-out, replay, idempotency, durability modes, and Cap'n Web wire
round trips.

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

Run the audio-shaped fan-out benchmark:

```sh
node scripts/audio-chaos-benchmark.ts \
  https://01-handwritten-stream.iterate-dev-preview.workers.dev \
  --publishers 10 \
  --subscribers 36 \
  --frames-per-publisher 50 \
  --slow-subscribers 1 \
  --pace-ms 20 \
  --durability best-effort \
  --measure-append-ack
```

Run the same audio-shaped benchmark from Durable Objects, so local WiFi/browser/Node networking is
not in the publisher/subscriber timing path:

```sh
curl -sS 'https://01-handwritten-stream.iterate-dev-preview.workers.dev/benchmark/audio-chaos?publishers=10&subscribers=36&slow-subscribers=1&frames-per-publisher=50&pace-ms=20&durability=best-effort&checkpoint-every=100&measure-append-ack=true'
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
- Audio-shaped benchmark output should be read as latency evidence, not just pass/fail:
  `allSubscribersLatencyMs` measures append-to-all-active-subscribers, and
  `publisherSelfEchoLatencyMs` measures same-session append-to-own-stream delivery under load.
- The `/benchmark/audio-chaos` route reports `*CreatedAtLatencyMs` from the stream DO's committed
  event timestamp to delivery inside runner DOs. Use it when local network quality could contaminate
  the external WebSocket benchmark.
- Deployed-only fault probes should log the stream path, offsets, checkpoint timings, and Cloudflare
  ray IDs where available so failures can be traced in Cloudflare observability.
