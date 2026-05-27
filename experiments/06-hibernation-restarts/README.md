# 06-hibernation-restarts

Small Durable Object WebSocket hibernation/restart repro.

## What we're trying to find out

When a Durable Object has a hibernatable WebSocket accepted with `ctx.acceptWebSocket()`:

- Does the WebSocket stay connected across normal deployed hibernation?
- What does the client observe when the DO is reset with `ctx.abort()`?
- What does the client observe when the DO is killed by memory pressure?
- Can a hibernation auto-response be used as a cheap heartbeat without hiding a dead DO?

The expected baseline is:

- Hibernation is an idle eviction path. The raw WebSocket should remain usable, and the next message
  should run in a new DO incarnation.
- `ctx.abort()` is not hibernation. The old raw WebSocket may close, error, or become stale, but it
  should not keep participating in the restarted DO's WebSocket set.
- OOM behavior is intentionally treated as an opt-in deployed probe because it can crash the isolate
  and may vary by runtime.
- A plain auto-response is not enough for crash detection: the runtime can keep answering it after
  `ctx.abort()` / OOM. This experiment makes the auto-response a short-lived lease. When the lease
  expires, the client must send a real application message to renew it. That wakes a normally
  hibernated DO, but times out on a stale ghost connection after reset.

## How to run

Local test run, with deployed lifecycle probes skipped:

```sh
pnpm --filter @cf-experiments/06-hibernation-restarts test
```

Deploy and run the deployed hibernation probe:

```sh
pnpm --filter @cf-experiments/06-hibernation-restarts deploy
WORKER_URL=https://06-hibernation-restarts.iterate-dev-preview.workers.dev \
  pnpm --filter @cf-experiments/06-hibernation-restarts test
```

Run the opt-in OOM probe:

```sh
RUN_OOM_PROBE=true \
WORKER_URL=https://06-hibernation-restarts.iterate-dev-preview.workers.dev \
  pnpm --filter @cf-experiments/06-hibernation-restarts test
```

Parameters:

- `WORKER_URL`: deployed or local worker URL. Hibernation test skips on localhost.
- `HIBERNATION_WAIT_MS`: idle wait before sending the wake message. Default `15000`.
- `OOM_BYTES`: bytes retained by the DO during the OOM probe. Default `268435456`.
- `RUN_OOM_PROBE=true`: enables the OOM test.

## How to evaluate results

The deployed hibernation test passes only if the same client WebSocket receives a `pong` after the
idle wait and the returned `incarnationId` differs from the pre-wait HTTP ping.

The auto-response lease test passes only if literal `ping` receives an expired `auto-pong` after the
DO has been idle or reset. A real JSON `ping` should renew against normal hibernation and should not
respond on the old socket after abort/OOM.

The abort and OOM probes report the old socket as `closed`, `errored`, `stale`, or `responded`.
`responded` means the old socket still reached a live DO after reset and is unexpected for abort/OOM.
