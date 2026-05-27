# High level findings

- None yet.

# Detailed notes

## 2026-05-27

- Created a minimal Durable Object hibernation/restart repro.
- The experiment keeps protocol surface deliberately small: HTTP controls for `ping`, `kill`, and
  allocation; JSON raw WebSocket for `ping`.

