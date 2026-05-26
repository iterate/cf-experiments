# High level findings

- No high-level Cloudflare platform finding yet. The current work is shaping a reproducible contract
  for Cap'n Web streams, append durability modes, and deployed-vs-local correctness tests.

# Notes

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
