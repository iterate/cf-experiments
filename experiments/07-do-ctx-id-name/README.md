# 07-do-ctx-id-name

Can we depend on `this.ctx.id.name` when a Durable Object is addressed via `getByName()` — in Miniflare and deployed Workers with a current compatibility date?

## What we're trying to find out

When the worker calls `env.PROBE.getByName(name)`:

1. Does `this.ctx.id.name` inside the DO equal that `name` on a normal RPC call?
2. Does `this.ctx.id.name` still equal that `name` inside `alarm()` after scheduling from the same instance?

If `ctx.id.name` is missing, the DO throws `this should never happen`.

This experiment intentionally does **not** cover `newUniqueId()`, `idFromString()`, or legacy alarms scheduled before the platform stored names.

## How to run

```sh
pnpm dev
```

In another terminal (use the URL Wrangler prints):

```sh
WORKER_URL=http://localhost:8787 pnpm test
```

Deployed:

```sh
pnpm deploy
WORKER_URL=https://07-do-ctx-id-name.iterate-dev-preview.workers.dev pnpm test
```

| Param | Default | Meaning |
| --- | --- | --- |
| `WORKER_URL` | `http://localhost:8787` | Worker base URL |
| `ALARM_DELAY_MS` | `50` | Alarm delay passed to the DO |
| `ALARM_POLL_MS` | `2000` | Max wait for alarm snapshot |

## How to evaluate results

Both vitest cases pass:

- `GET /rpc?name=…` → `{ name }` matches the query param.
- `POST /alarm` then poll `GET /alarm` → `lastAlarm.name` and `lastAlarm.armedName` match.

Failures print `cf-ray` from responses for Cloudflare log lookup.

Record runs in [log.md](./log.md).
