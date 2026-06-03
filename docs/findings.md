# Findings

This file records platform-level findings that have reproducible experiments behind them.

## Hibernatable WebSocket auto-responses can outlive DO reset

Experiment: `experiments/06-hibernation-restarts`

Status: confirmed against deployed Workers on `2026-05-27`, worker version
`f7298c36-2bc5-41dd-871b-c065ce69f3ae`, including `ctx.abort()` and a 512 MiB allocation reset
probe.

For hibernatable Durable Object WebSockets accepted with `ctx.acceptWebSocket()`, a configured
`setWebSocketAutoResponse()` reply is not proof that the client socket is attached to the current
Durable Object incarnation.

The deployed repro observed:

- normal idle hibernation keeps the client WebSocket usable, and a real application message wakes a
  new DO incarnation;
- after `ctx.abort()`, the old client WebSocket can still receive the auto-response, but a real
  application message on that same socket times out and does not reach the restarted DO;
- the same pattern was observed after an OOM-style reset triggered by retaining 512 MiB in the DO;
- a fresh WebSocket connection reaches the new DO incarnation.

Practical consequence: plain `"ping"` / `"pong"` auto-response heartbeats are transport liveness
only. They can hide a stale post-reset socket. For stream processors that must know whether a
connection still reaches the DO, use a leased auto-response: include an `incarnationId` and
`expiresAt` in the auto-response, and require a real application message to renew once the lease is
expired. Normal hibernation still works because the real message wakes the DO; a ghost socket after
reset times out and can be closed/reconnected by the client.

## capnweb@0.8.0 returned `ReadableStream` chunks are not wire-one-way

Experiment: `experiments/streams/01-handwritten-stream`

Status: confirmed by frame-level tests and repeated deployed benchmark comparisons in the experiment
log.

Cap'n Web can expose a pleasant API where a subscriber receives a returned
`ReadableStream<StreamEvent>` instead of passing an `onEvent()` callback to the Durable Object. But in
capnweb@0.8.0, that returned stream is not one-way on the WebSocket wire. Each stream chunk is encoded
as a pipe write to a remote `WritableStream`, and the subscriber sends a write-completion frame:

```txt
in  ["stream",["pipeline",1,["write"],[event]]]
out ["resolve",2,["undefined"]]
```

The outbound `resolve undefined` frame is not an application-level acknowledgement, but it is still
per-chunk subscriber-originated return traffic. In the audio-shaped fan-out benchmark, this
pipe/write/resolve machinery is the central difference between slow Cap'n Web returned streams and
much faster raw WebSocket fan-out.

Evidence:

- `experiments/streams/01-handwritten-stream/scripts/stream-capnweb.test.ts` contains
  "documents the concrete Cap'n Web returned-stream pipe frames", which asserts the exact WebSocket
  frame shape above.
- `experiments/streams/01-handwritten-stream/log.md` records deployed runs where raw WebSocket paths
  (`raw-volatile`, `minimal-ws`) delivered 18,000 fan-out messages around `140-150 ms` p95, while
  unbatched Cap'n Web returned streams were much slower.
- The same log records `batched-json-volatile`, which kept Cap'n Web returned streams but reduced
  chunk count from `18000` to `6842` and moved all-subscriber p95 from `2439 ms` to `461 ms`. That
  isolates returned-stream chunk/write/resolve count as the expensive unit rather than storage,
  JSON serialization, or raw WebSocket egress from one Durable Object.

Practical consequence: for high-frequency audio-frame fan-out, raw WebSocket is currently the data
plane baseline. Cap'n Web remains useful for typed control-plane RPC, but capnweb@0.8.0 returned
streams need batching/coalescing or a different one-way consumption shape before they are suitable as
the data-plane stream transport for this workload.

## capnweb@0.8.0 unawaited method calls can be one-way after setup

Experiment: `experiments/streams/01-handwritten-stream`

Status: confirmed locally by `scripts/clean-stream.test.ts`.

The returned-stream issue above is avoidable without leaving Cap'n Web. The clean transport
comparison includes `transport=capnweb-oneway`, where the subscriber passes a sink capability during
setup:

```ts
subscribeOneWay(sink)
```

The stream DO stores a duplicated sink stub, calls `sink.event(event)` for each event, deliberately
does not await the returned Cap'n Web thenable, and immediately disposes the ignored result. The
post-subscribe subscriber WebSocket traffic is server-to-subscriber only:

```txt
in ["push",["pipeline",sinkId,["event"],[event]]]
in ["release",resultId,refcount]
```

There are no subscriber-originated `resolve undefined` frames for each event. The cleanup `release`
frame travels from the stream DO to the subscriber, not from the subscriber back to the stream DO.

How Cap'n Web can tell: its `RpcPromise` is a custom thenable. `await`, `.then()`, `.catch()`, or
`.finally()` call the internal pull path, which sends `["pull", id]` and asks the peer to resolve the
result. If the caller never observes the thenable and instead disposes it, no pull is sent, so the
callee does not send a result frame. The receiving side still runs the method; the result is just
ignored.

Important ownership rule: a sink passed as an RPC argument is disposed when the setup call finishes
unless the receiver keeps a duplicate. `transport=capnweb-oneway` calls `sink.dup()` before storing
the subscriber, and disposes that duplicate when the Cap'n Web session is disposed.

Evidence:

- `scripts/clean-stream.test.ts` includes "capnweb-oneway subscriber originates no websocket frames
  per event", which records subscriber WebSocket frames after setup and asserts zero outbound frames
  while events are delivered.
- `scripts/stream-capnweb.test.ts` includes "client-main afterAppend subscriber originates no
  websocket frames per event", which proves the same trick works with a client main object exposing
  `afterAppend({ event })`.
- The same clean client API can consume `capnweb`, `capnweb-oneway`, `orpc`, and `rawws`, so the
  transport comparison isolates the consumption shape rather than changing the app contract.

Benchmark consequence: the trick removes per-event subscriber-originated frames, but it does not make
Cap'n Web as fast as raw WebSocket for the audio-shaped data plane. In the deployed storage-free
BenchmarkRunner DO matrix on `2026-05-27`, `capnweb-after-append` delivered all 18,000 fan-outs at
`622 ms` then `928 ms` all-subscriber p95, while `raw-volatile` delivered the same matrix at
`145 ms` then `146 ms`.

## capnweb@0.8.0 can approach raw WebSocket fan-out with one-way shared timed batches

Experiment: `experiments/streams/04-capnweb-rawws-parity`

Status: confirmed by deployed wire tests and repeated DO-orchestrated deployed benchmarks on
`2026-05-27`, worker version `428aec51-384b-453e-81aa-e31df38a246a`.

The one-way callback trick is necessary but not sufficient when every event remains its own Cap'n Web
RPC. In the clean in-memory reproduction, `capnweb-event` uses a client-main callback:

```ts
afterAppend({ event })
```

The Stream DO does not await the returned thenable and disposes it. The subscriber-originated traffic
problem is gone, but the deployed 10-publisher, 36-subscriber, 50-frame benchmark still showed raw
WebSocket at `131 ms` all-subscriber p95 versus `capnweb-event` at `298 ms`.

The winning shape in the clean repro is a shared stream-level timed batch:

```ts
afterAppendBatch({ events })
```

The Stream DO keeps one pending event array and one flush timer for the stream, then sends one
unawaited client-main callback per subscriber per flush. With `batch-ms=10-12`, repeated deployed
runs delivered all 500 events to all 36 subscribers with all-subscriber p95 close to raw WebSocket:

```txt
raw:                         148 ms, 130 ms
capnweb-batch batch-ms=12:   146 ms, 147 ms
capnweb-batch batch-ms=10:   195 ms, 150 ms
```

Zero-delay batching is not enough on the deployed edge. It produced `11520` Cap'n Web batch calls for
`18000` event-subscriber deliveries in one run and had `438 ms` all-subscriber p95. The batch window
needs to be explicit; otherwise event-loop scheduling does not reliably coalesce enough publisher
traffic.

Practical consequence: for an audio-shaped stream, Cap'n Web can stay on the data plane if the API is
one-way after setup and the data-plane unit is a small timed batch, not a returned `ReadableStream`
chunk and not one RPC per frame per subscriber. The latency/cadence tradeoff is now an application
decision: a `10-12 ms` batch window was enough to get near raw WebSocket in this workload while
preserving append acknowledgements around `9-12 ms` p95.

## `allowUnconfirmed` is useful for few-subscriber read-your-own latency, not a blanket stream win

Experiment: `experiments/streams/01-handwritten-stream`

Status: confirmed by deployed `BenchmarkRunner` runs on `2026-06-02` against the clean Stream DO
(`src/clean/stream-do.ts`, worker version `30732d8c-1a7c-44a3-8a9c-37cd15f80ba7`). The benchmark
compares:

- `best-effort`: `ctx.storage.put(..., { allowUnconfirmed: true })`, no `storage.sync()`;
- `confirmed-sync`: the same async write, then `await storageWrite` and `await ctx.storage.sync()`;
- `output-gated`: normal synchronous `ctx.storage.kv.put(...)`, which uses the SQLite-backed storage
  facade and closes the Durable Object output gate.

For the production-shaped stream target — high append volume, few subscribers, and fast read-your-own
append delivery — `allowUnconfirmed` was the best hot path in deployed tests. With 5 audio publishers,
2 subscribers, 100 frames per publisher, no artificial delay:

```txt
pace=20ms
best-effort      143.8/s, self-echo p95  8 ms, all-subscriber p95 167 ms
confirmed-sync   183.1/s, self-echo p95 32 ms, all-subscriber p95 239 ms
output-gated     176.4/s, self-echo p95 17 ms, all-subscriber p95 266 ms

pace=0 burst
best-effort      633.7/s, self-echo p95 87 ms, all-subscriber p95 225 ms
confirmed-sync   404.2/s, self-echo p95 90 ms, all-subscriber p95 304 ms
output-gated     384.9/s, self-echo p95 86 ms, all-subscriber p95 389 ms
```

The same experiment also shows the boundary where `allowUnconfirmed` stops being a clear win. With a
much heavier 10-publisher / 36-subscriber fan-out, rankings were noisy and sync writes could be
competitive or better. In addition, the `simulatedStorageSyncDelayMs` probe is **not** a faithful
storage-replication model: it inserts an awaited timer on the append hot path before subscriber
delivery, so all modes collapse toward `1 / delay` throughput. Do not use that synthetic delay alone
to reason about Cloudflare storage latency.

Practical consequence: do not simplify stream storage to synchronous writes only for the few-subscriber
audio-stream shape. Keep an `allowUnconfirmed` no-sync hot path for fast append/self-echo, and use
sync writes (or explicit sync waits) when the workload needs durability before delivery or behaves more
like heavy fan-out than read-your-own audio.

## `ctx.id.name` is set for `getByName` on RPC and alarm (current compat)

Experiment: `experiments/07-do-ctx-id-name`

Status: confirmed by vitest on Miniflare (`wrangler dev`) and deployed Workers
(`iterate-dev-preview`, `compatibility_date: 2026-05-01`) on `2026-05-27`.

When a Durable Object is addressed with `env.PROBE.getByName(name)`, `this.ctx.id.name` inside the
object equals that `name` on:

- a normal RPC handler (`getName()`), and
- a subsequent `alarm()` fired after `setAlarm()` from the same instance.

The property is `ctx.id.name` on `DurableObjectId`, not `ctx.name` on `DurableObjectState`.

Evidence:

- `experiments/07-do-ctx-id-name/scripts/ctx-id-name.test.ts` — RPC and alarm cases with fresh random
  names per run.
- `experiments/07-do-ctx-id-name/log.md` — local and deployed passes with Ray IDs
  `a0255161c9143784-LHR`, `a0255164ba453784-LHR`.

```sh
cd experiments/07-do-ctx-id-name
pnpm dev
WORKER_URL=http://localhost:<port> pnpm test
WORKER_URL=https://07-do-ctx-id-name.iterate-dev-preview.workers.dev pnpm test
```

**Not proven here** (and documented as `undefined` by Cloudflare): `newUniqueId()`, access via
`idFromString()` even when the ID came from a name, names longer than 1024 bytes, and alarms
scheduled before the platform stored names (pre-2026-03-15) until rescheduled from a handler where
name is available.

Practical consequence: for DOs that are **only** ever addressed by `getByName`, application code can
read `this.ctx.id.name` (or throw `"this should never happen"` if missing) instead of threading the
name through every RPC argument or persisting it in storage — including in `alarm()` handlers with no
incoming request.

