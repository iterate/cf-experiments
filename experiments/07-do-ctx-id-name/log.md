# High level findings

- **`ctx.id.name` works for `getByName` on RPC and alarm** with `compatibility_date: 2026-05-01`, on Miniflare (`wrangler dev`) and deployed (`iterate-dev-preview`), 2026-05-27.
- Scope: only `getByName` addressing; not `newUniqueId`, `idFromString`, or pre-2026-03-15 alarms.

# Detailed notes

## 2026-05-27 — first runs

Local (`http://localhost:8792`, wrangler dev):

- RPC: 200, `body.name` matched query param.
- Alarm: `lastAlarm.name` and `lastAlarm.armedName` matched within ~60ms.

Deployed (`https://07-do-ctx-id-name.iterate-dev-preview.workers.dev`):

- RPC: 200, `cf-ray=a0255161c9143784-LHR`.
- Alarm: 200, `lastAlarm` matched, `cf-ray=a0255164ba453784-LHR`.

## 2026-05-27

- Created minimal `NameProbe` DO + vitest harness for RPC and alarm paths.
