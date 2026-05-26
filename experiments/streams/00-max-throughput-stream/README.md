# 00-max-throughput-stream

What is the maximum append throughput we can get into a Durable Object event log?

`SuperSimpleStream` is a thin SQLite DO: sync inline SQL via `@cf-experiments/shared/event`, no sqlfu. RPC methods are `append`, `appendBatch`, and `count`.

## Design

| Piece | Choice |
| --- | --- |
| Storage | SQLite via sync `ctx.storage.sql.exec` (in-thread, no await) |
| Schema | `events(offset, type, idempotency_key, raw_event)` — see `packages/shared/src/event.ts` |
| Hot path | Synchronous RPC `append(event) → StreamEvent` / fire-and-forget WebSocket `{ op, event }` |

## Wire protocol

RPC / HTTP POST body is the event directly (not wrapped):

```json
{ "type": "benchmark.append", "payload": { "n": 1 } }
```

WebSocket frames still use an envelope:

```json
{ "op": "append", "event": { "type": "benchmark.append", "payload": { "n": 1 } } }
```

```json
{ "op": "appendBatch", "events": [{ "type": "a" }, { "type": "b" }] }
```

WebSocket connect: `GET /stream?name=my-path` (upgrade). No server frames on the hot path.

## How to run

From this directory:

```bash
pnpm dev
```

| Param | Default | Meaning |
| --- | --- | --- |
| `name` | `default` | DO instance (`getByName`) |

### Benchmark append throughput

With `pnpm dev` running in another terminal:

```bash
# WebSocket fire-and-forget (client send rate — usually highest)
pnpm benchmark http://localhost:8787 --mode ws --messages 10000 --verify

# Serial RPC round-trips (POST /append awaited each time)
pnpm benchmark http://localhost:8787 --mode rpc --messages 1000

# Both modes back-to-back
pnpm benchmark http://localhost:8787 --mode compare --messages 5000 --verify
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--mode` | `ws` | `ws`, `rpc`, or `compare` |
| `--messages` | `10000` | Appends per run |
| `--payload-bytes` | `256` | Padding inside `payload.pad` |
| `--name` | random | DO instance name (use a fresh name per run when verifying) |
| `--verify` | off | After drain, `GET /count` and include `serverCount` |
| `--drain-ms` | `3000` | Wait before verify (WS only) |

Output is JSON on stdout with `eventsPerSecond`. WS mode measures client send rate; RPC mode measures round-trip appends/sec.

### Miniflare (local)

```bash
# single append via RPC — returns committed StreamEvent
curl -s -X POST 'http://localhost:8787/append?name=bench' \
  -H 'content-type: application/json' \
  -d '{"type":"test","payload":{"n":1}}'

# count
curl -s 'http://localhost:8787/count?name=bench'

# WebSocket fire-and-forget (use wscat, websocat, or a benchmark script)
websocat 'ws://localhost:8787/stream?name=bench'
# then send: {"op":"append","event":{"type":"benchmark.append","payload":{"n":1}}}
```

### Production

```bash
pnpm run deploy
# https://00-max-throughput-stream.iterate-dev-preview.workers.dev
```

## How to evaluate

Record runs in [log.md](./log.md). Compare:

1. **RPC serial** — `POST /append` in a loop; measure round-trip appends/sec.
2. **WebSocket fire-and-forget** — pump `{ op: "append", event }` without reading replies; verify with `GET /count`.
3. **Miniflare vs deployed** — same harness, note Ray IDs from response headers.
4. **Payload size sweep** — vary `payload` JSON size and record where throughput cliffs.

Open questions: how close does inline sync SQL get to the sqlfu v2 sink? Does batching (`appendBatch`) beat serial `append` on the same connection?
