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
