# High level findings

- No high-level Cloudflare platform finding yet. The current work is shaping a reproducible contract
  for Cap'n Web streams, append durability modes, and deployed-vs-local correctness tests.

# Notes

## 2026-05-26 22:56 UTC+1

- Added `/benchmark/audio-chaos`, a worker route that runs the audio-shaped benchmark from Durable
  Objects rather than from the local Node/WebSocket client. One orchestrator `BenchmarkRunner` DO
  starts separate publisher/subscriber/passive-subscriber `BenchmarkRunner` DOs, and each runner DO
  connects to the `Stream` DO through the normal Cap'n Web WebSocket endpoint. This keeps local WiFi
  and laptop scheduling out of the publisher/subscriber timing path while preserving the stream
  transport shape under test.
- The DO-side benchmark keeps the same audio event format as `scripts/audio-chaos-benchmark.ts`:
  `benchmark.audio-frame`, 24 kHz PCM16 mono, 20 ms frames, 960 raw bytes / 1280 base64 chars.
- Fixed the DO-side `publisherAckToSelfEchoLatencyMs` measurement to use absolute timestamps inside
  publisher runner 0. The initial implementation subtracted two latency values with different bases.
- Local verification:
  - `pnpm --filter @cf-experiments/01-handwritten-stream typecheck`
  - `pnpm --filter @cf-experiments/01-handwritten-stream test`
  - Result: 24 tests passed.
- Deployed version `e261db43-d227-42a6-a440-b141ad284fab`.
- DO-side smoke command:
  `curl -sS --fail 'https://01-handwritten-stream.iterate-dev-preview.workers.dev/benchmark/audio-chaos?publishers=2&subscribers=2&frames-per-publisher=5&pace-ms=20&durability=best-effort&measure-append-ack=true'`
  Result: `allSubscribersCreatedAtLatencyMs.p95 = 22`, `publisherSelfEchoCreatedAtLatencyMs.p95 =
  19`, `publisherAppendAckLatencyMs.p95 = 17`, `publisherAckToSelfEchoLatencyMs.p95 = 0`.
- First full DO-side comparison, all with 10 publishers / 36 active subscribers / 1 passive
  subscriber / 50 frames per publisher / 20 ms pacing / `--measure-append-ack=true`:
  - best-effort: `allSubscribersCreatedAtLatencyMs.p95 = 1388`,
    `publisherSelfEchoCreatedAtLatencyMs.p95 = 980`, `publisherAppendAckLatencyMs.p95 = 664`,
    `publisherAckToSelfEchoLatencyMs.p95 = 0`, unconfirmed writes at end `500`.
  - checkpointed (`checkpoint-every = 100`): `allSubscribersCreatedAtLatencyMs.p95 = 2735`,
    `publisherSelfEchoCreatedAtLatencyMs.p95 = 1836`, `publisherAppendAckLatencyMs.p95 = 797`,
    `publisherAckToSelfEchoLatencyMs.p95 = 982`, checkpoints started/completed `5/5`.
  - confirmed: `allSubscribersCreatedAtLatencyMs.p95 = 1087`,
    `publisherSelfEchoCreatedAtLatencyMs.p95 = 771`, `publisherAppendAckLatencyMs.p95 = 446`,
    `publisherAckToSelfEchoLatencyMs.p95 = 4`.
- Interpretation: moving clients into DOs does not make the full fan-out shape look real-time; the
  bottleneck is still visible without local WiFi. However, these are single DO-side samples and the
  relative ordering varied from the local-client runs, so this is not ready for `docs/findings.md`.

## 2026-05-26 22:35 UTC+1

- Documented the stream-vs-callback subscription decision in `design.md` and `stream.ts`: subscribers
  get a one-directional `ReadableStream<StreamEvent>` rather than passing an `onEvent()` callback
  capability, because event delivery should not require per-event return traffic or acknowledgements.
- Added "pure subscribers do not originate per-event websocket traffic", an isolated websocket-frame
  test proving that after the initial `stream()` setup a subscriber receives a burst without outbound
  `pull`/`push` frames.
- Added "delivers to an active subscriber while another subscriber does not read" to make the slow
  consumer isolation decision explicit at the integration-test level.
- Added "checkpointed passes the live-before-durability probe that confirmed intentionally fails", a
  paired sharp-edge test: with `debugConfirmedSyncDelayMs`, confirmed mode does not deliver the new
  event before the durability window, while checkpointed mode delivers within that same window and
  checkpoints afterward.
- Added `scripts/audio-chaos-benchmark.ts`, which uses xAI/Grok Voice's documented default PCM shape
  (24 kHz Linear16, base64 realtime chunks; 20 ms mono frame = 960 raw bytes / 1280 base64 chars) to
  measure append-to-subscriber fan-out and same-session publisher self-echo.
- Deployed correctness result: `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm vitest run scripts/stream-capnweb.test.ts`
  passed with 24 tests.
- Deployed benchmark command:
  `node scripts/audio-chaos-benchmark.ts https://01-handwritten-stream.iterate-dev-preview.workers.dev --publishers 10 --subscribers 36 --frames-per-publisher 50 --slow-subscribers 1 --pace-ms 20 --timeout-ms 60000`
- Deployed benchmark result: 500 audio-shaped events, 36 active subscribers, 1 passive slow
  subscriber, 10 publishers, 20 ms pacing. Reader websocket framing stayed optimal
  (`readerOutboundPullPushFrames.max = 0` after subscription), but latency was not acceptable for
  real-time audio fan-out: `allSubscribersLatencyMs.p95 = 1268.9`, `allSubscribersLatencyMs.p99 =
  1306.6`, and under-load `publisherSelfEchoLatencyMs.p95 = 911.8`.
- Decision: the current one-DO `ReadableStream` fan-out shape is good for proving RPC framing and
  durability semantics, but it is not hunky-dory for the requested 10-publisher / few-dozen-subscriber
  audio-call workload. Next design candidates should reduce per-event fan-out work in the DO, e.g.
  partition subscribers, send smaller/binary frames, or separate hot audio media transport from the
  durable event log.
- Follow-up durability comparison, same deployed worker and same audio event format:
  - Full shape, 10 publishers / 36 active subscribers / 1 passive subscriber / 500 total events /
    20 ms pacing:
    - best-effort: `publisherSelfEchoLatencyMs.p95 = 2189.7`, `allSubscribersLatencyMs.p95 =
      2279.2`, reader outbound frames after subscription still `0`;
    - checkpointed (`checkpoint-every = 100`): `publisherSelfEchoLatencyMs.p95 = 549.3`,
      `allSubscribersLatencyMs.p95 = 1081.6`, reader outbound frames after subscription still `0`;
    - confirmed: `publisherSelfEchoLatencyMs.p95 = 1905.8`, `allSubscribersLatencyMs.p95 = 1930.8`,
      reader outbound frames after subscription still `0`.
  - Isolated shape, 1 publisher / 1 active subscriber / 100 total events / 20 ms pacing:
    - best-effort: `publisherSelfEchoLatencyMs.p95 = 32.2`;
    - checkpointed: `publisherSelfEchoLatencyMs.p95 = 82.0`;
    - confirmed: `publisherSelfEchoLatencyMs.p95 = 38.4`.
- Interpretation: the high read-your-own-append latency is not fundamentally caused by awaiting
  durability; in the isolated shape confirmed and best-effort are both tens of milliseconds. The
  bad latency appears under fan-out/connection pressure. Checkpointed was the best full-shape run in
  this sample, but still missed real-time audio expectations by a wide margin.
- Added `--measure-append-ack` to the audio benchmark. This keeps publisher 0's append result for
  timing while other publishers remain fire-and-forget. It adds two diagnostics:
  `publisherAppendAckLatencyMs` and `publisherAckToSelfEchoLatencyMs`.
- Follow-up full-shape runs with ack timing:
  - checkpointed (`checkpoint-every = 100`, one passive slow subscriber):
    `publisherSelfEchoLatencyMs.p95 = 813.1`, `publisherAppendAckLatencyMs.p95 = 832.5`,
    `publisherAckToSelfEchoLatencyMs.p95 = 33.3`;
  - best-effort: `publisherSelfEchoLatencyMs.p95 = 778.9`, `publisherAppendAckLatencyMs.p95 =
    783.7`, `publisherAckToSelfEchoLatencyMs.p95 = -0.04`;
  - confirmed: `publisherSelfEchoLatencyMs.p95 = 840.8`, `publisherAppendAckLatencyMs.p95 =
    776.5`, `publisherAckToSelfEchoLatencyMs.p95 = 429.4`.
- Follow-up checkpointed run without the passive slow subscriber was not materially better:
  `publisherSelfEchoLatencyMs.p95 = 1236.9`, `allSubscribersLatencyMs.p95 = 1294.1`,
  `publisherAckToSelfEchoLatencyMs.p95 = -0.07`.
- Interpretation update: for best-effort/checkpointed, own stream echo is generally delivered before
  or almost immediately after append acknowledgement. The high read-your-own latency under the full
  shape is therefore mostly before append acknowledgement: DO event scheduling / fan-out / WebSocket
  transport pressure, not an event that was already appended and then blocked behind an unnecessary
  durability await.

## 2026-05-26 22:22 UTC+1

- Added `debugOpenAndCancelLocalStream()` so the Web Streams `cancel` callback inside
  `#openStream()` is directly covered, separately from Cap'n Web session disposal.
- Added "removes locally cancelled streams from live fan-out", which verifies local stream cancel
  removes the subscriber and later appends do not fan out to a dead subscriber.
- Local result: `pnpm typecheck` and `pnpm vitest run scripts/stream-capnweb.test.ts` passed
  with 21 tests.
- Deployed version `334ff2ae-8ef0-4e9e-8613-e4b79d4a60e6`.
- Ran `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm vitest run scripts/stream-capnweb.test.ts`.
- Result: 21 tests passed in one test file.

## 2026-05-26 22:17 UTC+1

- Tightened `append()` / `stream()` implementation comments so each non-obvious branch names the
  Cap'n Web test that should fail if the behavior changes.
- Added stream lifecycle coverage:
  - best-effort appends fan out to subscribers while unconfirmed write debt is still visible;
  - checkpointed `appendBatchDebug()` proves the triggering append returns after scheduling, not
    awaiting, the checkpoint;
  - invalid per-call durability modes are rejected before allocating offsets;
  - per-session RPC disposal removes live subscribers after WebSocket teardown.
- Fixed a subscriber leak found by the new lifecycle test: Cap'n Web session teardown did not
  promptly call the returned `ReadableStream` cancel hook, so the per-connection `StreamRpcTarget`
  now owns its subscribers and releases them from `[Symbol.dispose]()`.
- Local result: `pnpm typecheck` and `pnpm vitest run scripts/stream-capnweb.test.ts` passed
  with 20 tests.
- Deployed version `00c87e34-0ee6-4ead-bd9f-c22da4753b61`.
- Ran `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm vitest run scripts/stream-capnweb.test.ts`.
- Result: 20 tests passed in one test file.

## 2026-05-26 21:11 UTC+1

- Added detailed implementation comments to `Stream.append()` and checkpoint scheduling, with first-party
  Cloudflare docs links for `allowUnconfirmed`, `storage.sync()`, and `blockConcurrencyWhile()`.
- Simplified documentation and settings structure after review:
  - merged `design-goals.md` and `stream-design-notes.md` into `design.md`;
  - moved stream settings out of `packages/shared` and into `stream.ts`;
  - collapsed separate debug RPCs into `debug()` and removed the mid-flight `appendBatchDebug()` probe.
- Deployed version `5430fce7-bf06-4d9d-a441-81e708e6c72a`.
- Ran `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm vitest run scripts/stream-capnweb.test.ts`.
- Result: 17 tests passed in one test file.

## 2026-05-26 20:36 UTC+1

- Changed `confirmed` append semantics to be async at the RPC boundary: write with
  `allowUnconfirmed: true`, optionally wait through `debugConfirmedSyncDelayMs`, `await storage.sync()`,
  then broadcast and resolve the append.
- Added causal-gating tests:
  - unrelated `ping()` RPC resolves while confirmed append is waiting on durability;
  - subscribers can drain old durable backlog while the confirmed append is pending;
  - the newly appended confirmed event is not delivered to subscribers until after the append resolves.
- Deployed version `b3572ab6-10fa-4967-abd1-99ba405b2ee4`.
- Ran `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm vitest run scripts/stream-capnweb.test.ts`.
- Result: 19 tests passed in one test file.

## 2026-05-26 20:04 UTC+1

- Renamed checkpoint settings/API from unconfirmed writes to unconfirmed appends to match the actual
  accounting unit.
- Added validation for per-call checkpoint thresholds and a debug test that observes checkpoint
  scheduling before the follow-up RPC observes checkpoint completion.
- Deployed version `7176c2ef-0db9-43e4-bebc-62c1ea9166f1`.
- Ran `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm vitest run scripts/stream-capnweb.test.ts`.
- Result: 17 tests passed in one test file.

## 2026-05-26 19:53 UTC+1

- Deployed `01-handwritten-stream` to `https://01-handwritten-stream.iterate-dev-preview.workers.dev`
  after adding explicit append durability modes.
- Ran `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm vitest run scripts/stream-capnweb.test.ts`.
- Result: 15 tests passed in one test file.
- Current API distinction:
  - `confirmed` uses normal Durable Object output-gate semantics.
  - `best-effort` uses `allowUnconfirmed: true`.
  - `checkpointed` uses `allowUnconfirmed: true` plus explicit `storage.sync()` checkpoints.

## 2026-05-26 19:22 UTC+1

- Added stream correctness coverage for replay, idempotency, offset preconditions, multi-subscriber
  fan-out, and a delayed-reader buffering probe.
- Local result: `pnpm typecheck` and `pnpm vitest run scripts/stream-capnweb.test.ts` passed.
