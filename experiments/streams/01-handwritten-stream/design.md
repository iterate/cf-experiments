# Stream and StreamProcessorRunner design

This document describes the design of the two principal abstractions in our stream processing system:

1. Stream (currently called JonasStream)
2. StreamProcessor

We'll cover
- The core data model
- The durable object design in cloudflare
- API surfaces for interacting with streams and stream processors
- Client libraries
- How to write stream processors

# Taxonomy / Shared vocabulary

Stream: a stream is a sequence of events

Event: an event is a single item in a stream

Subscriber: any program across the network from a stream that consumes events from the stream.

Outbound subscriber: A subscriber that that the stream connects into

Inbound subscriber: A subscriber that connects into the stream. E.g. a browser tab or stream processor in an e2e test.

Subscriber transport: The protocol used by an inbound or outbound subscriber. For now we only support websockets (inbound and outbound) and capnweb and workers rpc (both inbound only). Eventually, we'll support outbound webhooks.

Websocket subscriber: A subscriber that uses the websocket protocol to consume events from the stream and append to them. Could be inbound or outbound.

Stream processor: A consumer that uses our library to define a well defined manifest that declares
 - the schemas of events it owns, consumes and emits. 
 - the processor manifests it depends on
 - its state schema
 - its reducer function (pure function of the current state and new event - safe to import anywhere)
 - a separate implementation function for side effects (e.g. appending more events)

Stream processor "runner": A program that connects a stream processor to a stream subscription. For example, we might have a nodejs stream processor runner that creates an inbound websocket subscription on a stream and then runs the stream processor against it. In production the main stream processor runner we use is a durable object called StreamProcessorRunner, which streams connect to via an outbound websocket subscription.


# Requirements

## Core data model

Each stream is uniquely identified by a "path" that must start with a slash and contain only lowercase letters, numbers, and hyphens (aka sluggable path components)

Each stream contains an append-only log of events with 

- `offset` (unique autoincrementing integer > 0)
- `type` (string)
- `idempotencyKey?` (optional unique string) - if provided must be unique within the stream
- `payload?` (optional object) - shape determined by the `type`
- `metadata?` (optional object) - arbitrary metadata associated with the event
- `createdAt` (string) - ISO 8601 timestamp

## Subscriptions

- The websocket protocol and client libraries should not care which side initiated a subscriber websocket connection
- We should be able to write e2e tests where we run stream processors in our vitest processes via inbound websocket subscription
- Outbound websocket subscriptions need to survive the calling request context expiring. For example, if the inbound HTTP request that caused a "subscription-configured" append gets closed, the outbound websocket connection to the subscriber should continue to work.
- Inbound websocket subscriptions need to resume after hibernation

## Stream processor
- The core API should be runtime-agnostic
- Need to separate out reducer and schemas / metadata from implementation
- Implementation consists of single `afterAppend` function
  - `afterAppend` should be synchronous to force processor author to think about what they want to do
  - `afterAppend` should be a pure function of its arguments. It needs to be passed
    - the new event
    - previous state
    - new state (because reducer is run for us)
    - an `append({event})` function that appends an event to the stream and returns a promise that resolves when the event is confirmed. 
    - a `blockProcessorUntil` function that tells the processor runer to not process any more events until the promise returned from the callback completes
    - a function that appends but DOESN'T stop consuming events - maybe called `appendAndWaitForConfirmation`
    - a function `waitUntil` to say "i'm going to do some long running work - please wait for it" (this is mostly relevant in a durable object context)
    - 




## Instrumentation

We need to be able to track all this information
- in workers analytics engine
- on-demand from a live durable object via RPC
- from client libraries (the websocket client needs to emit these metrics)

For any stream
- All active subscriptions with direction (inbound vs outbound), protocol (websocket), status (connected or not)
- age
- number of events
- storage size
- append volume (events per second)
- data in/out rate (bytes per second)

For any stream processor
- Delay betweeen `append-requested` and `append-confirmed` for each append
- last processed event offset
- size of "buffer"
- events processed
- events per second throughput
- data in/out rate (bytes per second)

For any stream processor subscription (i.e. the live websocket connection), we need to track
- ping time to stream
- direction
- connection status 


# Design philosophy

### Make `Stream` itself very small 

It should be a short piece of well instrumented high performance code. I

It should only care about:
- storage/retrieval of events
- subscription transports (inbound and outbound)
- any stream processing that can STOP events from being appended (e.g. circuit breaker, rate limiting, access control, etc)

Everything else should be implemented in separate builtin processors.

The primary optimisation goals for the stream are:
- performance (throughput and latency to subscribers)
- scalability in size of events, number of events, throughput, number of subscribers, etc
- simplicity
- observability

### Minimise blast radius

The failure of any one processor should not affect other processors. This means, for example, that whenever there is a need for in-memory event buffers, we prefer to buffer on the processor side than inside the `Stream` durable object.



# Durable object design in cloudflare 

## Stream

- Use the async KV API for storage because it allows us manual control over when writes are persisted to other edge locations (use `allowUnconfirmed: true` for this)

- In general, DO NOT block durable object output gates on writes. Only block egress about the specific event that was just appended

## Capnweb API

The primary way to use 

### Capnweb RPC

This is used heavily in e2e tests to call privileged debug APIs like `.kill()` or `.simulateStorageSyncDelay()`

### Workers RPC

We use the workers RPC API from other cloudflare workers. For example from the ingress worker which calls `.fetch()` on the durable object stub. We might also use this from a wrapping orpc API.

Not a big deal, though, as this is an almost complete implementation of capnweb.

### Custom websocket protocol

This is the main high-performance, low-latency API surface for subscribers.


The entire design of our stream processing system hinges on a high-throughput, low-latency connection between processors and stream.

The key performance constraint is that the websocket protocol must have truly one-directional flow of data on the network in most cases. No acknowledgement traffic going back to the subscriber for each event. We did make this work with capnweb using a subscriber-provided RPC target: the subscriber passed in a sink capability with an `event()` callback, and the Stream DO called `sink.event(event)` for each event without awaiting the returned thenable, then immediately disposed the ignored result. But performance was still worse than expected. In order to properly troubleshoot this, we had to make our own protocol.


High level design for outbound websocket subscribers:
- The `Stream` knows at all times which subscribers it should be connected to

Once a websocket connection has been established, neither side cares about who started it. The protocol should be identical.

Websocket protocol messages: (THIS NEEDS A LOT MORE WORK)

- `stream-requested` (subscriber -> stream) with optional start offset and filter (the name is a bit confusing maybe)
- `append-requested` 
- `append-confirmed` 

What else?


# Stream processor 

TODO

# Future work
- Batching appends and  acknolwedgements to reduce network chatter
- YOLO mode with configurable storage.sync() timing - we should be able to say globally or on a per-event basis that "we're okay with losing 100 events or maybe 30s worth of events", with individually overridable policies in .append()
- Permissions / access control
- Loop detection - like permissions / access control this MUST be in the stream durable object, as it _blocks_ appends
- Split events across multiple kv sqlite rows to avoid 2mb limit
- Store older events in R2
- Different types of subscriptions - including those where the server keeps track of the offset for each consumer

