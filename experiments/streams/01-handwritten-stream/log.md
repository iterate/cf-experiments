# High level findings

- No high-level Cloudflare platform finding yet. The current work is shaping a reproducible contract
  for Cap'n Web streams, append durability modes, and deployed-vs-local correctness tests.

# Notes

## 2026-05-27 02:46 UTC+1

- Added "rejects malformed append events before idempotency or durability handling". The test sends
  `event: null` plus invalid durability, then asserts no offset or checkpoint state changes.
- Red result before the fix: append rejected with `Cannot read properties of null (reading
  'idempotencyKey')`, proving the append boundary dereferenced the event before validation.
- Fixed `append()` to validate `StreamEventInput` at the top and use the parsed event for
  idempotency lookup and `writeEventFromKv()`.
- Verification: root `pnpm typecheck`, local `pnpm vitest run scripts/stream-capnweb.test.ts`, and
  deployed `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm vitest run scripts/stream-capnweb.test.ts`
  passed with 40 tests. Deployed version `6c1db589-85ad-4c48-ab55-48a678cb0fb0`.

## 2026-05-27 02:43 UTC+1

- The DO-side audio benchmark route rejected `subscribers=0` with Worker 1101 because the HTTP
  parser required positive subscriber counts. Changed that parameter to allow zero active subscribers
  and made the delivery-coverage fields report zero active delivery cleanly for that case.
- Ran a best-effort active-subscriber scaling sweep with 10 publishers, 50 frames/publisher, 20 ms
  pacing, 24 kHz mono PCM16, 960 raw bytes / 1280 base64 chars per frame:
  - 0 active subscribers: self-echo p95 92 ms, append-ack p95 9 ms.
  - 1 active subscriber: all-subs p95 176 ms, self-echo p95 167 ms, append-ack p95 23 ms.
  - 10 active subscribers: all-subs p95 230 ms, self-echo p95 194 ms, append-ack p95 31 ms.
  - 36 active subscribers: all-subs p95 685 ms, self-echo p95 523 ms, append-ack p95 266 ms.
- All nonzero-subscriber runs fully delivered 500/500 frames to all active subscribers. This points
  at active fan-out pressure as the main contributor: ten publishers by themselves do not create the
  second-scale read-your-own latency.
- Verification after the benchmark-route change: root `pnpm typecheck`, local
  `pnpm vitest run scripts/stream-capnweb.test.ts`, and deployed
  `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm vitest run scripts/stream-capnweb.test.ts`
  passed with 39 tests. Deployed version `5c186d7b-7fdd-45d9-a77a-a0a2104bf8ff`.

## 2026-05-27 02:39 UTC+1

- Repeated the DO-side full audio workload three times in best-effort mode after deploying
  `91cb5c60-9586-4b8a-9e72-ce074d77d2b9`:
  10 publishers, 36 active subscribers, 50 frames/publisher, 20 ms pacing, 24 kHz mono PCM16,
  960 raw bytes / 1280 base64 chars per frame, `measureAppendAck=true`.
- All three runs fully delivered all 500 frames to all 36 subscribers:
  - run 1: all-subs p95 1674 ms, self-echo p95 1148 ms, append-ack p95 780 ms.
  - run 2: all-subs p95 1472 ms, self-echo p95 1155 ms, append-ack p95 421 ms.
  - run 3: all-subs p95 1406 ms, self-echo p95 1098 ms, append-ack p95 691 ms.
- This is stronger evidence that the current single Stream DO design is correct but not
  "hunky-dory" for the target audio-call latency shape. Running publishers/subscribers from DOs
  removes local WiFi from the timing path, and best-effort mode removes intentional durability waits
  from live fan-out, yet p95 delivery is still well over 1 second under 10x36 fan-out.

## 2026-05-27 02:37 UTC+1

- Added "rejects primitive per-call durability before falling back to stream settings". Runtime
  clients can send `durability: 1` or `true`; those are not valid object/string durability choices.
- Red result before the fix: a numeric durability rejected with `'' is not a function.` instead of a
  stream-level validation error. The test sets default durability to `checkpointed` first so a
  silent fallback would allocate an offset and schedule a checkpoint.
- Fixed `#resolveAppendDurability()` to reject non-string, non-object, non-undefined durability
  values explicitly before any persisted-setting fallback.
- Verification: root `pnpm typecheck`, local `pnpm vitest run scripts/stream-capnweb.test.ts`, and
  deployed `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm vitest run scripts/stream-capnweb.test.ts`
  passed with 39 tests. Deployed version `91cb5c60-9586-4b8a-9e72-ce074d77d2b9`.

## 2026-05-27 02:33 UTC+1

- Added "rejects object durability without a mode before allocating an offset". A runtime Cap'n Web
  caller can send an object that the TypeScript API would not allow, such as
  `{ checkpointEveryUnconfirmedAppends: 1 }`.
- Red result before the fix: the malformed object rejected with `'' is not a function.` instead of a
  stream-level validation error.
- Fixed `#resolveAppendDurability()` to reject object durability options without `mode` explicitly,
  so they do not silently inherit persisted stream settings or fail with transport-shaped errors.
- Verification: root `pnpm typecheck`, local `pnpm vitest run scripts/stream-capnweb.test.ts`, and
  deployed `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm vitest run scripts/stream-capnweb.test.ts`
  passed with 38 tests. Deployed version `8f9baf53-c5fe-456d-a39b-8b7e97b7107f`.

## 2026-05-27 02:31 UTC+1

- Added "rejects null per-call durability before allocating an offset" for malformed runtime RPC
  input that TypeScript callers cannot express but a Cap'n Web client can send.
- Red result before the fix: append rejected with `Cannot read properties of null (reading
  'checkpointEveryUnconfirmedAppends')`, proving the resolver had an incidental TypeError path.
- Fixed `#resolveAppendDurability()` to reject `null` explicitly before mode/threshold handling.
- Verification: root `pnpm typecheck`, local `pnpm vitest run scripts/stream-capnweb.test.ts`, and
  deployed `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm vitest run scripts/stream-capnweb.test.ts`
  passed with 37 tests. Deployed version `c2eefee1-70ff-40b4-8e55-0db228783d12`.

## 2026-05-27 00:50 UTC+1

- Added "removes every stream opened by a disposed capnweb session". A single Cap'n Web WebSocket can
  call `stream()` more than once; all resulting subscribers must be owned by that session and released
  when the session disposes.
- Mutation check: temporarily removing `sessionSubscribers?.add(subscriber)` made the new test fail
  with both subscribers still present after disconnect.
- While rerunning deployed tests, "checkpointed passes the live-before-durability probe that
  confirmed intentionally fails" failed once because its checkpointed half used a 100 ms deployed
  delivery timeout without a delayed checkpoint. Tightened the probe by setting
  `debugCheckpointSyncDelayMs: 2000` and asserting live stream delivery within 1000 ms.
- Attempted to also assert the checkpointed append RPC acknowledgement within that same 1000 ms
  window. That failed locally: even when the append result is observed immediately, Cap'n Web result
  delivery can still wait behind the `blockConcurrencyWhile()` checkpoint gate. The implementation
  still does not `await` the checkpoint inside `append()` (covered by `appendBatchDebug()`), but the
  external append result pull is not the right proof of live-before-checkpoint behavior. The stream
  event delivery is.
- Verification after the test/comment update: root `pnpm typecheck`, local
  `pnpm vitest run scripts/stream-capnweb.test.ts`, and deployed
  `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm vitest run scripts/stream-capnweb.test.ts`
  passed with 36 tests. Deployed version `048a0bfe-ecc8-4518-a1d3-2e5b8a338c19`.

## 2026-05-26 23:44 UTC+1

- Added explicit DO-side audio benchmark delivery coverage fields:
  `framesFullyDelivered`, `framesMissingFullDelivery`, `minFrameDeliveries`, and
  `maxFrameDeliveries`. This keeps partial fan-out coverage visible instead of burying it in
  `allSubscribersCreatedAtLatencyMs.count`.
- Deployed version `d768c0aa-8db9-4a1d-9924-54dfa176f605`.
- Verification: root `pnpm typecheck` passed.
- Deployed Cap'n Web verification:
  `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm vitest run scripts/stream-capnweb.test.ts`
  passed with 35 tests.
- Smoke run:
  `/benchmark/audio-chaos?publishers=10&subscribers=36&slow-subscribers=0&frames-per-publisher=50&frame-ms=20&pace-ms=20&sample-rate=24000&channels=1&bytes-per-sample=2&timeout-ms=60000&durability=best-effort&measure-append-ack=true`
  returned `framesFullyDelivered: 500`, `framesMissingFullDelivery: 0`,
  `minFrameDeliveries: 36`, `maxFrameDeliveries: 36`,
  `allSubscribersCreatedAtLatencyMs.p95: 1032`,
  `publisherSelfEchoCreatedAtLatencyMs.p95: 732`, and
  `publisherAppendAckLatencyMs.p95: 429`.

## 2026-05-26 23:42 UTC+1

- Ran DO-side audio-shaped benchmarks from `/benchmark/audio-chaos`, with 24 kHz mono PCM16,
  20 ms frames, 960 raw bytes / 1280 base64 chars per event, `measureAppendAck=true`, 10
  publishers, 36 active subscribers, 50 frames per publisher, and 20 ms publisher pacing.
- Mode comparison from one run:
  - `best-effort`: `allSubscribersCreatedAtLatencyMs.p95` 1248 ms,
    `publisherSelfEchoCreatedAtLatencyMs.p95` 748 ms,
    `publisherAppendAckLatencyMs.p95` 469 ms.
  - `checkpointed`: `allSubscribersCreatedAtLatencyMs.p95` 939 ms,
    `publisherSelfEchoCreatedAtLatencyMs.p95` 798 ms,
    `publisherAppendAckLatencyMs.p95` 530 ms.
  - `confirmed`: `allSubscribersCreatedAtLatencyMs.p95` 709 ms,
    `publisherSelfEchoCreatedAtLatencyMs.p95` 682 ms,
    `publisherAppendAckLatencyMs.p95` 326 ms.
- Ran a best-effort passive-subscriber comparison with the same active load:
  - 0 passive subscribers: all-subs p95 2014 ms, self-echo p95 1440 ms, append-ack p95 568 ms.
  - 10 passive subscribers: all-subs p95 1042 ms, self-echo p95 820 ms, append-ack p95 475 ms.
  - 36 passive subscribers: all-subs p95 1882 ms, self-echo p95 1079 ms, append-ack p95 690 ms.
  The run is noisy and does not show a clean "one unread consumer stalls active consumers" effect;
  the bigger signal remains active fan-out / append service pressure.
- Ran a 1 publisher / 1 subscriber DO-side baseline:
  - `best-effort`: all-subs p95 38 ms, self-echo p95 32 ms, append-ack p95 20 ms.
  - `checkpointed`: all-subs p95 24 ms, self-echo p95 20 ms, append-ack p95 11 ms.
  - `confirmed`: all-subs p95 22 ms, self-echo p95 22 ms, append-ack p95 12 ms.
  This reinforces that the high read-your-own latency is not simply "we awaited storage sync";
  under low load it is tens of milliseconds, while under 10x36 fan-out it is hundreds of
  milliseconds to seconds.

## 2026-05-26 23:37 UTC+1

- Re-ran the `allowUnconfirmedWrites: true` mutation check locally by temporarily changing it to
  `false`.
- Result: the source sentinel "append uses the allowUnconfirmed write fast path" failed, but the
  strongest local behavioral probe, "lets unrelated RPC resolve while confirmed append waits for
  durability", still passed. This confirms the current reason for keeping a source-level sentinel:
  local runtime behavior still does not make the platform output-gate difference crisp enough to
  rely on as the only regression test.
- Also deployed the same temporary mutation as version `091869c8-3eb8-4c3c-9823-11a23563a43a` and
  ran the three strongest behavioral probes:
  - "lets unrelated RPC resolve while confirmed append waits for durability"
  - "lets subscribers drain old events but not the new confirmed event before durability"
  - "best-effort appends fan out while write debt is still unconfirmed"
  They all passed, so the source sentinel remains necessary for this exact implementation choice.
  Restored and redeployed the intended implementation as
  `50bb84f2-a679-41b4-a152-81423f58138e`.
- Verification after restore: root `pnpm typecheck`, local
  `pnpm vitest run scripts/stream-capnweb.test.ts`, and deployed
  `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm vitest run scripts/stream-capnweb.test.ts`
  all passed with 35 tests.

## 2026-05-26 23:35 UTC+1

- Added "removes replay subscribers when committed history is corrupt". This covers the failure path
  introduced by the explicit replay-gap invariant: if replay throws after registering the subscriber,
  the failed stream must be removed from live fan-out.
- Red result before the fix: `debug().subscribers` still contained one subscriber with
  `desiredSize: null` after the read failed with `Missing stream event ...`.
- Fixed cleanup by routing session disposal, stream cancel, enqueue failure, and replay-start failure
  through the same small `#removeSubscriber()` helper.
- Local verification: root `pnpm typecheck` and
  `pnpm vitest run scripts/stream-capnweb.test.ts` passed with 35 tests.
- Deployed version `e95981a3-6ea1-4ab3-a44a-d68742534eaa`; deployed verification
  `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm vitest run scripts/stream-capnweb.test.ts`
  passed with 35 tests.

## 2026-05-26 23:32 UTC+1

- Added "fails corrupted idempotent retries before conflicting validation can reject them". The test
  deletes the original event while leaving the idempotency index, then retries with both the same key
  and invalid retry options.
- Red result before the fix: the retry failed with `Unknown append durability mode: not-a-mode`,
  proving the corrupted idempotency index was not treated as the first-class append invariant.
- Fixed the stream boundary and shared KV writer to throw
  `Idempotency index points at missing stream event offset ...` instead of falling through to later
  validation or allocating a second offset.
- Local verification: root `pnpm typecheck` and
  `pnpm vitest run scripts/stream-capnweb.test.ts` passed with 34 tests.
- Deployed version `7ddcdef7-5d10-47b7-bffc-a5ea380dcf0f`; deployed verification
  `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm vitest run scripts/stream-capnweb.test.ts`
  passed with 34 tests.

## 2026-05-26 23:29 UTC+1

- Added "fails replay loudly when committed history has a missing event key". The test uses a
  test-only corruption hook to delete `event:1` while `maxOffset` remains 2, then asserts the
  Cap'n Web stream errors instead of replaying sparse history.
- Red result before the fix: the first read resolved with offset 2, proving the old replay loop
  silently skipped the missing offset.
- Fixed `#openStream()` to treat `maxOffset` as a contiguous committed-history claim and throw
  `Missing stream event at offset ...` when an event key is absent.
- Local verification: `pnpm typecheck` and `pnpm vitest run scripts/stream-capnweb.test.ts` passed
  with 33 tests.
- Deployed version `8d387da1-633a-45bd-a3aa-a5bbee4ed4fc`; deployed verification
  `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm vitest run scripts/stream-capnweb.test.ts`
  passed with 33 tests.

## 2026-05-26 23:25 UTC+1

- Tightened the replay/live fan-out language in `stream.ts` and `design.md`. The previous wording
  treated "register before replay" as a sharp race boundary, but replay is synchronous and contains no
  `await`, so another append cannot interleave in the middle of replay. The real invariant is:
  capture one replay boundary, register each stream once for later live fan-out, and use the same
  enqueue/error-cleanup path for replay and live events.
- Local verification: `pnpm --filter @cf-experiments/01-handwritten-stream typecheck` and
  `pnpm --filter @cf-experiments/01-handwritten-stream test` passed with 32 tests.
- Deployed version `8e527595-9e4a-4594-b8d2-77a0bc4f64e5`; deployed verification
  `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm --filter @cf-experiments/01-handwritten-stream test`
  passed with 32 tests.

## 2026-05-26 23:23 UTC+1

- Mutation-checked the two checkpoint scheduling early-return guards:
  - replacing `this.#checkpointInProgress` with `false` made the checkpointed appendBatch tests fail
    by starting four checkpoints in a five-event batch instead of one;
  - replacing `this.#unconfirmedWriteCount < checkpointEveryUnconfirmedAppends` with `false` made
    "uses checkpointed stream settings when append does not pass a per-call override" fail by
    checkpointing after the first append even though the threshold was two.
- No code change needed; existing tests already pin both branches.
- Local verification after restoring mutations: `pnpm --filter @cf-experiments/01-handwritten-stream typecheck`
  and `pnpm --filter @cf-experiments/01-handwritten-stream test` passed with 32 tests.

## 2026-05-26 23:21 UTC+1

- Added "rejects non-websocket requests at the stream durable object boundary" to cover the
  `Stream.fetch()` guard that keeps the stream DO's public transport surface to Cap'n Web over
  WebSocket only.
- Updated the `Stream.fetch()` comment to state that plain HTTP must fail closed rather than expose
  an accidental second stream protocol.
- Local verification: `pnpm --filter @cf-experiments/01-handwritten-stream typecheck` and
  `pnpm --filter @cf-experiments/01-handwritten-stream test` passed with 32 tests.
- Deployed version `272f56fa-40cf-4889-a791-ab5fa7e12e50`; deployed verification
  `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm --filter @cf-experiments/01-handwritten-stream test`
  passed with 32 tests.

## 2026-05-26 23:19 UTC+1

- Added "rejects invalid checkpoint thresholds even on non-checkpointed object durability" to cover
  the `#resolveAppendDurability()` branch that validates a present
  `checkpointEveryUnconfirmedAppends` field even when `mode` is `best-effort` or `confirmed`.
- Mutation check: changing the resolver to validate the threshold only for `mode === "checkpointed"`
  made the new test fail before any offset was allocated.
- Local verification: `pnpm --filter @cf-experiments/01-handwritten-stream typecheck` and
  `pnpm --filter @cf-experiments/01-handwritten-stream test` passed with 31 tests.
- Deployed version `8c1f6110-b4b3-46f6-b27b-1055cff4a9ac`; deployed verification
  `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm --filter @cf-experiments/01-handwritten-stream test`
  passed with 31 tests.

## 2026-05-26 23:16 UTC+1

- Simplified checkpoint scheduling by replacing the `while` loop with a single guarded sync. Under
  `blockConcurrencyWhile()`, later delivered events cannot enter while the checkpoint is awaiting, so
  no new unconfirmed append window can appear inside that callback. The existing checkpoint tests are
  the relevant guard:
  - "checkpointed appendBatch drains the whole same-event unconfirmed window"
  - "checkpointed appendBatch returns after scheduling but before awaiting the checkpoint"
  - "checkpointed append schedules a delayed checkpoint that gates later RPC"
- Local verification: `pnpm --filter @cf-experiments/01-handwritten-stream typecheck` and
  `pnpm --filter @cf-experiments/01-handwritten-stream test` passed with 30 tests.
- Deployed version `cf4a2cf9-0e0b-4e70-9aac-3b01033df75f`; deployed verification
  `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm --filter @cf-experiments/01-handwritten-stream test`
  passed with 30 tests.

## 2026-05-26 23:13 UTC+1

- Added "removes subscribers whose stream controller rejects enqueue" to cover the defensive
  `#enqueueToSubscriber()` catch path. The test installs an errored local stream controller through a
  debug hook, appends once, and asserts the broken subscriber is removed.
- Mutation check: commenting out `this.#streamSubscribers.delete(subscriber)` inside the
  `#enqueueToSubscriber()` catch made the new test fail with the errored subscriber still present.
- Updated `design.md` and the `stream.ts` catch comment to tie the cleanup behavior to the test.
- Local verification: `pnpm --filter @cf-experiments/01-handwritten-stream typecheck` and
  `pnpm --filter @cf-experiments/01-handwritten-stream test` passed with 30 tests.
- Deployed version `8ee25586-748a-44cb-a815-0c4d94614a15`; deployed verification
  `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm --filter @cf-experiments/01-handwritten-stream test`
  passed with 30 tests.

## 2026-05-26 23:10 UTC+1

- Ran a deployed A/B mutation for the remaining weak `allowUnconfirmedWrites` behavioral question.
  Current intended code (`allowUnconfirmedWrites: true`) was deployed as version
  `7efc3173-d007-4321-b90d-a712412602be`; the one-line mutation
  `allowUnconfirmedWrites: false` was deployed temporarily as
  `81a6e55e-9a36-478e-98d6-08e631e20308` for the small probe and
  `fe5cbdb2-7472-4c13-a59e-65608cd5afd9` for the full-shape probe, then restored.
- Small DO-side probe, 1 publisher / 1 subscriber / 200 frames / no pacing / best-effort:
  - intended: `allSubscribersCreatedAtLatencyMs.p95 = 126`, `publisherSelfEchoCreatedAtLatencyMs.p95 = 233`,
    `publisherAppendAckLatencyMs.p95 = 206`;
  - mutated false: `allSubscribersCreatedAtLatencyMs.p95 = 153`, `publisherSelfEchoCreatedAtLatencyMs.p95 = 172`,
    `publisherAppendAckLatencyMs.p95 = 131`.
- Full DO-side probe, 10 publishers / 36 active subscribers / 1 passive subscriber / 50 frames each /
  20 ms pacing / best-effort:
  - intended: `allSubscribersCreatedAtLatencyMs.p95 = 2054`, `publisherSelfEchoCreatedAtLatencyMs.p95 = 804`,
    `publisherAppendAckLatencyMs.p95 = 557`;
  - mutated false: `allSubscribersCreatedAtLatencyMs.p95 = 2581`, `publisherSelfEchoCreatedAtLatencyMs.p95 = 912`,
    `publisherAppendAckLatencyMs.p95 = 719`.
- Interpretation: the full-shape deployed mutation was worse with gated writes, but the small probe
  was mixed and the latency distributions are noisy. This is useful evidence, not a crisp correctness
  test. The source sentinel remains the only deterministic guard for that exact option.
- Added deterministic settings-path coverage:
  - "rejects invalid stream settings without changing append defaults" covers settings validation and
    protects the default confirmed append path from invalid persisted config.
  - "persists stream settings across durable object restart" uses `kill()` to prove settings are read
    in the constructor after restart and still drive default checkpointed append behavior.
- Local verification: `pnpm --filter @cf-experiments/01-handwritten-stream typecheck` and
  `pnpm --filter @cf-experiments/01-handwritten-stream test` passed with 29 tests.
- Deployed version `88be5303-e324-45d0-8657-e30cb7e67699`; deployed verification
  `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm --filter @cf-experiments/01-handwritten-stream test`
  passed with 29 tests.

## 2026-05-26 23:02 UTC+1

- Mutation-checked `append()` around the two most suspicious design choices:
  - flipping `allowUnconfirmedWrites: true` to `false` did **not** fail the behavioral tests in the
    local runtime, so added the source-level sentinel "append uses the allowUnconfirmed write fast
    path" and left the limitation explicit in the `stream.ts` comment. This still needs a stronger
    deployed behavioral probe before we can claim it is fully covered by runtime behavior.
  - replacing checkpoint `blockConcurrencyWhile()` with an ungated async task failed "checkpointed
    append schedules a delayed checkpoint that gates later RPC".
  - awaiting checkpoint scheduling from `append()` failed the checkpointed append tests, including
    "checkpointed appendBatch returns after scheduling but before awaiting the checkpoint".
- Added `debugCheckpointSyncDelayMs`, a test-only lever that widens the checkpoint window without
  depending on natural storage-sync latency.
- Added "idempotent retries return before conflicting validation can reject them", proving the
  idempotency fast path runs before durability-mode validation and offset precondition checks for
  retries.
- Local verification: `pnpm --filter @cf-experiments/01-handwritten-stream typecheck` and
  `pnpm --filter @cf-experiments/01-handwritten-stream test` passed with 27 tests.
- Deployed version `29ef0dab-cb05-41f4-9f0b-9c0914d788c9`.
- Deployed verification:
  `WORKER_URL=https://01-handwritten-stream.iterate-dev-preview.workers.dev pnpm --filter @cf-experiments/01-handwritten-stream test`
  passed with 27 tests.
- DO-side benchmark smoke after deploy:
  `publishers=2`, `subscribers=2`, `frames-per-publisher=5`, `pace-ms=20`, best-effort with ack
  timing. Result: `allSubscribersCreatedAtLatencyMs.p95 = 40`,
  `publisherSelfEchoCreatedAtLatencyMs.p95 = 19`, `publisherAppendAckLatencyMs.p95 = 16`,
  `publisherAckToSelfEchoLatencyMs.p95 = 0`.

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
