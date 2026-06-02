# Stream and StreamProcessorRunner design

This document describes the design of the two principal abstractions in our stream processing system:

1. Stream
2. StreamProcessorRunner

We'll cover
- The core data model
- The durable object design in cloudflare
- API surfaces for interacting with streams and stream processors
- Client libraries
- How to write stream processors

# Taxonomy / Shared vocabulary

Stream: a durable node that owns an append-only sequence of events.

Event: a single item in a stream.

Subscriber: a node outside the stream that can consume event batches from the stream. A subscriber is
not a websocket, not a live connection, and not necessarily unique to one stream.

Subscriber spec: the `subscriber` object inside a `subscription-configured` event. It tells the stream
what kind of subscriber should exist and how to connect to it. The transport is a property of this
object. For now we only support CaptainWeb-WebSocket (`captainweb-websocket`), but later subscriber
specs can describe dynamic workers, external URLs, webhooks, etc.

Subscription: the configured edge from a stream node to a subscriber node. `subscriptionKey` identifies
this edge within one stream. The same subscriber implementation can appear behind many subscriptions.

Subscription configuration: the latest `events.iterate.com/stream/subscription-configured` event for a
given `subscriptionKey`. The core stream processor stores this exact event in stream reduced state.

Subscription connection: a live runtime connection used to deliver events for a subscription. It has a
direction, a transport, an optional `subscriptionKey`, and a `SubscriptionRpcTarget`. It is not
persisted; it can be recreated from stream reduced state and runtime handshakes.

Inbound subscription connection: a subscriber connects into the stream and passes the stream a
`SubscriptionRpcTarget`. Direction is always from the `Stream` durable object's perspective. Browser
tabs and vitest-hosted stream processors use this in tests.

Outbound subscription connection: the stream connects out to a subscriber described by a persisted
subscription configuration. Direction is always from the `Stream` durable object's perspective. Built-in
stream processors normally use this.

SubscriptionRpcTarget: the RPC capability provided by the subscriber side for one live subscription
connection. The stream stores this target in memory and calls `consumeEvents({ events })` on it to
deliver batches.

CaptainWeb session: a live CaptainWeb connection to the stream. A session can become a subscription
connection if the subscription handshake yields a `SubscriptionRpcTarget`, but debug/control sessions
can exist without being subscriptions.

Stream snapshot: The current reduced state of the stream that is useful to a subscriber at subscription start. This includes things like when the stream was created, the current event count / max offset, and stream metadata such as event schemas.

Stream reduced state: The stream durable object's persisted projection over its own event log. The
stream uses this state for core bookkeeping such as created time, max offset / event count, event
schemas, and the latest `subscription-configured` event for each `subscriptionKey`.

Core stream processor: The built-in reducer that belongs to the stream itself. It runs synchronously
inside the stream durable object after offset allocation. Its reduced state is the stream reduced
state.

Subscription reconciler: the stream-owned process that compares stream reduced state with runtime
subscription connections and opens any outbound connections that should exist. For now it only needs to
add missing outbound connections; unsubscribe / disconnect policy can come later.

Stream processor: A consumer that uses our library to define a well defined manifest that declares
 - the schemas of events it owns, consumes and emits. 
 - the processor manifests it depends on
 - its state schema
 - its reducer function (pure function of the current state and new event - safe to import anywhere)
 - a separate implementation function for side effects (e.g. appending more events)

Stream processor runner: A program that connects a stream processor to a subscription connection. For example, we might have a nodejs stream processor runner that creates an inbound subscription connection on a stream and then runs the stream processor against it. In production the main stream processor runner we use is a durable object called StreamProcessorRunner, which streams connect to via an outbound subscription connection.


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

### `events.iterate.com/stream/created`

The first event in every stream. It has offset `0`, records the stream namespace/path, and lets the
core stream processor reduce `createdAt` from the event timestamp.

```ts
{
  offset: 0,
  type: "events.iterate.com/stream/created",
  payload: {
    namespace: "stream",
    path: "/audio/uploads/123",
  },
  createdAt: "2026-06-01T12:00:00.000Z",
}
```

### `events.iterate.com/stream/woken`

Appended whenever the stream Durable Object constructor runs. It records the current incarnation so
debug output can distinguish persisted stream state from the currently running object instance.

```ts
{
  offset: 1,
  type: "events.iterate.com/stream/woken",
  payload: {
    incarnationId: "81e7f2f0-8f2d-47e6-a9d9-1df5a4ad33f0",
  },
  createdAt: "2026-06-01T12:00:00.001Z",
}
```

### `events.iterate.com/stream/configured`

Updates stream-level configuration that belongs in reduced state.

```ts
{
  offset: 2,
  type: "events.iterate.com/stream/configured",
  payload: {
    config: {
      simulatedStorageSyncDelayMs: 25,
    },
  },
  createdAt: "2026-06-01T12:00:00.002Z",
}
```

### `events.iterate.com/stream/subscription-configured`

Configures an outbound subscriber for the stream. The event is part of the stream history, and the
exact event is passed to the subscriber during outbound subscription init so the subscriber can
configure itself from committed stream state.

```ts
{
  offset: 3,
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
  createdAt: "2026-06-01T12:00:00.003Z",
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

- The CaptainWeb API should not care which side initiated a subscription connection.
- Subscription direction is always named from the stream's perspective: inbound means the subscriber connected into the stream, and outbound means the stream connected out to the subscriber.
- Event delivery should be framed as batches from day one, even when the batch contains a single event.
- We should be able to write e2e tests where we run stream processors in our vitest processes via inbound subscription connections.
- Outbound subscription connections need to survive the calling request context expiring. For example, if the inbound HTTP request that caused a `subscription-configured` append gets closed, the outbound connection to the subscriber should continue to work.
- Inbound subscription connections need to resume after hibernation.

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
- All active subscription connections with direction (inbound vs outbound), transport (`captainweb-websocket`), status (connected or not)
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

For any stream processor subscription connection, we need to track
- ping time to stream
- direction
- connection status 


# Design philosophy

### Make `Stream` itself very small 

It should be a short piece of well instrumented high performance code. I

It should only care about:
- storage/retrieval of events
- subscription connection transports (inbound and outbound)
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
  subscription connections should exist, and uses runtime state to know which CaptainWeb sessions /
  `SubscriptionRpcTarget`s are currently connected.

## CaptainWeb API

The primary way to interact with the stream is via a CaptainWeb API. 

The subscription handshake should be symmetrical:

- For inbound subscriptions, the subscriber calls `initInboundSubscription()` on the stream and passes its `subscriptionRpcTarget`.
- For outbound subscriptions, the stream calls `initOutboundSubscription()` on the subscriber and passes its stream RPC target, the `subscription-configured` event, and the stream snapshot. The subscriber returns the same request shape used by inbound subscriptions, so the stream can start delivery without another round trip.
- In both cases, the stream ends up storing a `SubscriptionRpcTarget` in memory and delivering event batches to it.

The subscription connection lifecycle is:

1. The stream reduced state says which durable subscriptions should exist, or an inbound caller asks to
   subscribe directly.
2. One side opens a CaptainWeb session.
3. The initiating side calls the appropriate init method.
4. The subscriber side provides a `SubscriptionRpcTarget` and optional `afterOffset`.
5. The stream stores a subscription connection in memory and starts replay/live delivery from
   `afterOffset + 1`.
6. When the CaptainWeb session breaks, the stream forgets the runtime connection. The durable
   subscription configuration remains in stream reduced state.

The subscription request shape is:

```ts
{
  subscriptionRpcTarget,
  afterOffset?,
}
```

`afterOffset` is owned by the subscriber and is optional. If omitted, the stream treats it as `-1`,
meaning "start before the first event". For inbound subscriptions, the subscriber sends it directly
to `initInboundSubscription()`. For outbound subscriptions, the subscriber returns it from
`initOutboundSubscription()` after looking at the stream snapshot and the `subscription-configured`
event. The stream then starts replay/live delivery from `afterOffset + 1`.

Not every CaptainWeb session is a subscription connection. Debug and control clients can open
CaptainWeb sessions and call RPC methods without providing a `SubscriptionRpcTarget`. A CaptainWeb
session becomes a subscription connection only when the handshake gives the stream a
`SubscriptionRpcTarget` to store for event delivery.

`debug()` should return the stream reduced state plus runtime state, including active CaptainWeb
sessions, active subscription connections, subscription keys, directions, transports, and connection
status.

The `SubscriptionRpcTarget` should expose a batch-shaped delivery method:

```ts
consumeEvents({ events })
```

`consumeEvents()` has no meaningful return value. The stream must not use it for acknowledgement,
backpressure, offset tracking, or error reporting.

The most important performance constraint is to avoid back-and-forth network round trips for each
consumed batch. When the stream durable object delivers a batch, it must call
`subscriptionRpcTarget.consumeEvents({ events })`, not await the returned CaptainWeb thenable, and then
immediately dispose the ignored result.

The experiment showed why this matters. CaptainWeb returned `ReadableStream` values are encoded as
remote writable stream writes, and each chunk produces return traffic:

```txt
in  ["stream",["pipeline",1,["write"],[eventBatch]]]
out ["resolve",2,["undefined"]]
```

The `SubscriptionRpcTarget` shape avoids that write/resolve pair when the caller does not observe the
result. The expected post-init wire shape is one-way event delivery from stream to subscriber:

```txt
in ["push",["pipeline",subscriberId,["consumeEvents"],[{ "events": [event] }]]]
in ["release",resultId,refcount]
```

This is the main reason to keep trying the CaptainWeb API before falling back to a custom WebSocket
protocol.




### CaptainWeb RPC

This is used heavily in e2e tests to call privileged debug APIs like `.kill()`.

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




# Next steps scratchpad

## Client libraries

### Level 1: Connect to capnweb stream library / useStream disposable from a a node program or browser or workerd

This is the only layer that is probably runtime-specific.

What the user has
- A URL and headers
- An optional fetch function?

What the user wants 
- A Disposable & RpcStub<StreamRpc>
- Some helper methods to e.g. measure ping time etc

Example usage 

```ts

await using streamRpcStub = withStream({
  url,
  headers,
  fetch? // needed for cloudflare i think
});

```

TODO: 
- is stream disposal async?
- can we make this runtime agnostic / have a node client and a workers client?

###: Level 2: Subscriber 

Calls initInboundSubscription on the stream rpc stub, which calls initOutboundSubscription on the 

```ts

// calls streamRpcStub.subscribe({ startingOffset, processEventBatch }) or similar 
// processEventBatch then calls onEvent? on the subscription
// We want the `subscription` fixture to be a bonafide javascript event emitter if that makes sense - open question
await using subscription = await using withStreamSubscription({
  streamRpcStub,
  startingOffset,
  onEvent?
})

// it should be possible to do this 
const event = subscription.waitForEvent({predicate})


```

### Level 3: Stream processor runner for _inbound_ subscribers

What the user has
- A stream processor instance with reduce and afterAppend functions
- A stream rpc stub

What the user wants
- set up some initial starting state and offset
- subscribe to stream and receive events
- some way to access metrics / ping times


```ts

// this would call withSubscription internally
await using processorSubscription = withStreamProcessor({
  streamRpcStub,
  initialState?,
  afterOffset?,
  // not sure here - processor or processor + contract or what? what even _is_ this thing?
  processor,
  
})
```

// Could maybe also allow 

# Design decisions
- For inbound subscribers, we should not need to provide any initOutboundSubscription() on our capnweb main object or anything
- For inbound subscribers, it is the client that initiates ping and everything else 
- Durable object does not care about inbound capnweb subscribers - it only cares the capnweb peer subscribes
