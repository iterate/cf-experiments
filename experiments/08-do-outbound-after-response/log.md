# log — 08-do-outbound-after-response

## Findings (top)

- **Miniflare (`wrangler dev`, 2026-06-01):** Both **inline** fire-and-forget and **alarm** slow-fetch paths complete after the inbound RPC returns (~4s captun delay, same `incarnationId`). Matches docs: in-progress `fetch()` keeps the DO non-hibernateable; `waitUntil` not needed.
- **Deployed:** not run here (wrangler auth). Use `pnpm deploy` then `WORKER_URL=https://08-do-outbound-after-response.iterate-dev-preview.workers.dev pnpm test`.

## Notes

### 2026-05-27 — experiment scaffolded

- Worker: `startInline` (void slow fetch + immediate RPC return) vs `armAlarm` + `alarm()` doing the same fetch.
- Test: captun tunnel with `/slow` delayed by `SLOW_MS`; polls `/status` until `done` or timeout.
- Docs expectation: pending `fetch()` keeps DO non-hibernateable; `waitUntil` unnecessary.

### 2026-06-01 — Miniflare run

`WORKER_URL=http://localhost:8818 SLOW_MS=4000 pnpm test` — inline waited ~4.9s, alarm ~4.7s; both `phase: done`, status 200, body `slow-ok`.
