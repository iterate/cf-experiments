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

Subscription: the relationship between a stream and a subscriber. Streams and subscribers are nodes;
subscriptions are edges. `subscriptionKey` identifies this edge within a stream.

Outbound subscriber: A subscriber that that the stream connects into. Direction is always from the `Stream` durable object's perspective.

Inbound subscriber: A subscriber that connects into the stream. Direction is always from the `Stream` durable object's perspective. E.g. a browser tab or stream processor in an e2e test.

Subscriber transport: How a subscriber connects to the stream. This is a property of the subscriber configuration, not a separate sibling object. For now we only support CaptainWeb-WebSocket (`captainweb-websocket`) for both inbound and outbound subscriptions. Eventually, we'll support other transports such as outbound webhooks.

CaptainWeb subscriber: A subscriber that uses a CaptainWeb-WebSocket connection to consume events from the stream and append to it. Could be inbound or outbound.

Subscriber RpcTarget: The RPC target provided by a subscriber. The stream stores this target in memory for either inbound or outbound subscriptions, and calls methods on it to deliver batches of events to the subscriber.

Stream snapshot: The current reduced state of the stream that is useful to a subscriber at subscription start. This includes things like when the stream was created, the current event count / max offset, and stream metadata such as event schemas.

Stream reduced state: The stream durable object's persisted projection over its own event log. The
stream uses this state for core bookkeeping such as created time, max offset / event count, event
schemas, and the latest `subscription-configured` event for each `subscriptionKey`.

Core stream processor: The built-in reducer that belongs to the stream itself. It runs synchronously
inside the stream durable object after offset allocation. Its reduced state is the stream reduced
state.

Subscription reconciler: The stream-owned process that compares the stream reduced state with runtime
connection state and opens any outbound subscriber connections that should exist. For now it only
needs to add missing outbound connections; unsubscribe / disconnect policy can come later.

CaptainWeb session: A live CaptainWeb connection to the stream. Some sessions are subscriber sessions,
but debug/control sessions can also exist without being subscribers.

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

- `offset` (unique autoincrementing integer >= 0)
- `type` (string)
- `idempotencyKey?` (optional unique string) - if provided must be unique within the stream
- `payload?` (optional object) - shape determined by the `type`
- `metadata?` (optional object) - arbitrary metadata associated with the event
- `createdAt` (string) - ISO 8601 timestamp

## Core stream events

### `events.iterate.com/stream/subscription-configured`

Configures an outbound subscriber for the stream. The event is part of the stream history, and the
exact event is passed to the subscriber during outbound subscription init so the subscriber can
configure itself from committed stream state.

```ts
{
  offset: 0,
  type: "events.iterate.com/stream/subscription-configured",
  idempotencyKey: "subscription:transcribe-audio",
  payload: {
    subscriptionKey: "transcribe-audio",
    subscriber: {
      type: "built-in",
      transport: "captainweb-websocket",
      processorSlug: "transcribe-audio",
    },
  },
  createdAt: "2026-06-01T12:00:00.000Z",
}
```

`subscriptionKey` is the durable identity of this stream's configured subscription and must be unique
within a stream. If a later `subscription-configured` event uses the same `subscriptionKey`, it replaces
the previous configuration for that subscription. The same subscriber implementation can appear in
multiple subscriptions.
`subscriber` describes what kind of subscriber should be connected and how to connect to it. The
initial subscriber type is `built-in`, which uses `processorSlug` to select a built-in stream
processor runner. Future subscriber types might look like:

```ts
{
  type: "events.iterate.com/stream/subscription-configured",
  payload: {
    subscriptionKey: "summarize-transcript",
    subscriber: {
      type: "dynamic-worker",
      transport: "captainweb-websocket",
      workerName: "customer-summary-worker",
      entrypoint: "TranscriptSummarizer",
    },
  },
}
```

```ts
{
  type: "events.iterate.com/stream/subscription-configured",
  payload: {
    subscriptionKey: "crm-webhook",
    subscriber: {
      type: "external-url",
      transport: "https-webhook",
      url: "https://example.com/events",
    },
  },
}
```

## Subscriptions

- The CaptainWeb API should not care which side initiated a subscriber websocket connection
- Subscription direction is always named from the stream's perspective: inbound means the subscriber connected into the stream, and outbound means the stream connected out to the subscriber.
- Event delivery should be framed as batches from day one, even when the batch contains a single event.
- We should be able to write e2e tests where we run stream processors in our vitest processes via inbound websocket subscription
- Outbound websocket subscriptions need to survive the calling request context expiring. For example, if the inbound HTTP request that caused a `subscription-configured` append gets closed, the outbound websocket connection to the subscriber should continue to work.
- Inbound websocket subscriptions need to resume after hibernation

## Stream processor
- The core API should be runtime-agnostic
- Need to separate out reducer and schemas / metadata from implementation
- `consumes` can include `"*"` for processors that need to reduce every event in a stream. The core
  stream processor uses this to maintain event count, max offset, and subscription configuration.
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
- All active subscriptions with direction (inbound vs outbound), transport (`captainweb-websocket`), status (connected or not)
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

- The stream durable object has its own formal reducer for core stream events. This reducer maintains
  the stream reduced state, including the latest subscription configuration for each `subscriptionKey`.
  Subscription management is stream-owned core behavior, not a separate user processor.

- The stream has a subscription reconciler that uses the stream reduced state to know which outbound
  subscriber connections should exist, and uses runtime state to know which CaptainWeb sessions /
  subscriber RpcTargets are currently connected.

## CaptainWeb API

The primary way to interact with the stream is via a CaptainWeb API. 

The subscription handshake should be symmetrical:

- For inbound subscriptions, the subscriber calls `initInboundSubscription()` on the stream and passes its `subscriberRpcTarget`.
- For outbound subscriptions, the stream calls `initOutboundSubscription()` on the subscriber and passes its stream RPC target, the `subscription-configured` event, and the stream snapshot. The subscriber returns the same request shape used by inbound subscriptions, so the stream can start delivery without another round trip.
- In both cases, the stream ends up storing a `subscriberRpcTarget` in memory and delivering event batches to it.

The subscription request shape is:

```ts
{
  subscriberRpcTarget,
  afterOffset?,
}
```

`afterOffset` is owned by the subscriber and is optional. If omitted, the stream treats it as `-1`,
meaning "start before the first event". For inbound subscriptions, the subscriber sends it directly
to `initInboundSubscription()`. For outbound subscriptions, the subscriber returns it from
`initOutboundSubscription()` after looking at the stream snapshot and the `subscription-configured`
event. The stream then starts replay/live delivery from `afterOffset + 1`.

Not every CaptainWeb session is a subscription. Debug and control clients can open CaptainWeb
sessions and call RPC methods without providing a subscriber RpcTarget. A CaptainWeb subscriber is a
session that completes the subscription handshake and gives the stream a subscriber RpcTarget to store
for event delivery.

`debug()` should return the stream reduced state plus runtime state, including active CaptainWeb
sessions, active subscribers, subscription keys, directions, transports, and connection status.

The subscriber RpcTarget should expose a batch-shaped delivery method:

```ts
consumeEvents({ events })
```

`consumeEvents()` has no meaningful return value. The stream must not use it for acknowledgement,
backpressure, offset tracking, or error reporting.

The most important performance constraint is to avoid back-and-forth network round trips for each
consumed batch. When the stream durable object delivers a batch, it must call
`subscriberRpcTarget.consumeEvents({ events })`, not await the returned CaptainWeb thenable, and then
immediately dispose the ignored result.

The experiment showed why this matters. CaptainWeb returned `ReadableStream` values are encoded as
remote writable stream writes, and each chunk produces return traffic:

```txt
in  ["stream",["pipeline",1,["write"],[eventBatch]]]
out ["resolve",2,["undefined"]]
```

The subscriber RpcTarget shape avoids that write/resolve pair when the caller does not observe the
result. The expected post-init wire shape is one-way event delivery from stream to subscriber:

```txt
in ["push",["pipeline",subscriberId,["consumeEvents"],[{ "events": [event] }]]]
in ["release",resultId,refcount]
```

This is the main reason to keep trying the CaptainWeb API before falling back to a custom WebSocket
protocol.




### CaptainWeb RPC

This is used heavily in e2e tests to call privileged debug APIs like `.kill()` or `.simulateStorageSyncDelay()`

### Workers RPC

We use the workers RPC API from other cloudflare workers. For example from the ingress worker which calls `.fetch()` on the durable object stub. We might also use this from a wrapping orpc API.

Not a big deal, though, as this is an almost complete implementation of capnweb.

# Stream processor 

TODO

# Future work
- YOLO mode with configurable storage.sync() timing - we should be able to say globally or on a per-event basis that "we're okay with losing 100 events or maybe 30s worth of events", with individually overridable policies in .append()
- Permissions / access control
- Loop detection - like permissions / access control this MUST be in the stream durable object, as it _blocks_ appends
- Split events across multiple kv sqlite rows to avoid 2mb limit
- Store older events in R2
- Different types of subscriptions - including those where the server keeps track of the offset for each consumer
