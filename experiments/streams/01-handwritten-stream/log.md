# High level findings

- No high-level Cloudflare platform finding yet. The current work is shaping a reproducible contract
  for Cap'n Web streams, append durability modes, and deployed-vs-local correctness tests.

# Notes

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
