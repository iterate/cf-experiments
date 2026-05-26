# 01-handwritten-stream

This folder contains my best guess at where we'll land in the near future.

It has 

A `Stream` durable object
- Capnweb interface, so we can connect 
- Use KV API to store events, so we can decide whether or not to block network output gates wait for durable appends
- Stream only persists events and reduced state - no offsets
- `kill()` method

A `StreamProcessor` durable object
- Client connects into `StreamProcessor` durable object via WS to `fetch()` 
- `kill()` method

Test harness
- capnweb client in vitest test harness

Things we are interested in

- Websocket traffic between StreamProcessor and 
- "Read your own write" latency in stream processor

