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

Run the clean transport comparison tests:

```sh
pnpm wrangler dev --port 8788
WORKER_URL=http://localhost:8788 pnpm vitest run scripts/clean-stream.test.ts
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

Set `stream-kind=volatile` on `/benchmark/audio-chaos` to keep the same Stream DO, Cap'n Web
WebSocket transport, and `ReadableStream<StreamEvent>` fan-out while bypassing persistence/replay:

```sh
curl -sS 'https://01-handwritten-stream.iterate-dev-preview.workers.dev/benchmark/audio-chaos?stream-kind=volatile&publishers=10&subscribers=36&frames-per-publisher=50&pace-ms=20&measure-append-ack=true'
```

Set `stream-kind=raw-volatile` to keep the same Stream DO and audio payloads but replace Cap'n Web
returned streams with a small JSON-over-WebSocket protocol:

```sh
curl -sS 'https://01-handwritten-stream.iterate-dev-preview.workers.dev/benchmark/audio-chaos?stream-kind=raw-volatile&publishers=10&subscribers=36&frames-per-publisher=50&pace-ms=20&measure-append-ack=true'
```

Set `stream-kind=minimal-ws` to use a separate `MinimalStream` DO that implements only WebSocket
upgrade, `subscribe`, `append`, broadcast, and append `ack`. This is the clean raw-WebSocket baseline:
subscribers send one `subscribe` frame and then receive events without per-event return traffic.

```sh
curl -sS 'https://01-handwritten-stream.iterate-dev-preview.workers.dev/benchmark/audio-chaos?stream-kind=minimal-ws&publishers=10&subscribers=36&frames-per-publisher=50&pace-ms=20&measure-append-ack=true'
```

Set `stream-kind=json-volatile` to keep Cap'n Web returned streams but make each stream chunk a
pre-serialized JSON string instead of a pass-by-value event object:

```sh
curl -sS 'https://01-handwritten-stream.iterate-dev-preview.workers.dev/benchmark/audio-chaos?stream-kind=json-volatile&publishers=10&subscribers=36&frames-per-publisher=50&pace-ms=20&measure-append-ack=true'
```

Set `stream-kind=batched-json-volatile` to keep Cap'n Web returned streams but coalesce events into
JSON-array chunks on a zero-delay timer. This probes whether the expensive unit is one returned-stream
chunk rather than one event payload:

```sh
curl -sS 'https://01-handwritten-stream.iterate-dev-preview.workers.dev/benchmark/audio-chaos?stream-kind=batched-json-volatile&publishers=10&subscribers=36&frames-per-publisher=50&pace-ms=20&measure-append-ack=true'
```

Set `stream-kind=orpc-durable-iterator` to use a separate volatile `OrpcDurableStream` DO, where
subscribers connect through ORPC's Durable Iterator WebSocket path:

```sh
curl -sS 'https://01-handwritten-stream.iterate-dev-preview.workers.dev/benchmark/audio-chaos?stream-kind=orpc-durable-iterator&publishers=10&subscribers=36&frames-per-publisher=50&pace-ms=20&measure-append-ack=true'
```

The clean comparison surface is `/clean/:name?transport=capnweb|orpc|rawws`. It keeps one minimal
in-memory stream application and swaps only the transport adapter. Use `src/clean/client.ts` from
Vitest or from another Durable Object; it accepts either a URL endpoint or a `fetch(request)`
endpoint. The explicit client constructors are `connectCleanCapnwebStream`,
`connectCleanOrpcStream`, and `connectCleanRawwsStream`; each returns the same `CleanStreamClient`
interface.

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
  the external WebSocket benchmark, but treat sub-100 ms differences carefully because that metric
  crosses DO clocks. For publisher self-echo, prefer `publisherAppendStartToSelfEchoLatencyMs`,
  which measures append call start to own-stream delivery inside the same publisher runner DO.
- For `/benchmark/audio-chaos`, also check `framesFullyDelivered`, `framesMissingFullDelivery`,
  `minFrameDeliveries`, and `maxFrameDeliveries`; these make partial fan-out coverage explicit
  instead of relying on a percentile sample count.
- Deployed-only fault probes should log the stream path, offsets, checkpoint timings, and Cloudflare
  ray IDs where available so failures can be traced in Cloudflare observability.
