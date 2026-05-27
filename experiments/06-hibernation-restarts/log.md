# High level findings

- None yet.

# Detailed notes

## 2026-05-27

- Deployed probe showed a plain `setWebSocketAutoResponse("ping", "pong")` can keep returning
  `pong` on the old client socket after `ctx.abort()`, even though a real JSON message no longer
  reaches the restarted DO. Treat plain auto-response as transport liveness only.
- Switched the auto-response to a short-lived lease containing `incarnationId` and `expiresAt`.
  Once expired, the client must renew with a real WebSocket message. That still allows normal
  hibernation, but gives bounded detection for abort/OOM ghost sockets.
- Created a minimal Durable Object hibernation/restart repro.
- The experiment keeps protocol surface deliberately small: HTTP controls for `ping`, `kill`, and
  allocation; JSON raw WebSocket for `ping`.
