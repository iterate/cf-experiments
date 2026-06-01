# High level findings

- A one-way Cap'n Web client-main callback fixes the returned-`ReadableStream` wire problem but is
  still about 2x slower than raw WebSocket when every event is its own Cap'n Web RPC. Deployed
  in-memory DO benchmark on version `428aec51-384b-453e-81aa-e31df38a246a`: raw all-subscriber p95
  `131 ms`, `capnweb-event` `298 ms`, same 10 publishers, 36 subscribers, 50 frames each,
  `pace-ms=20`, `payload-bytes=1280`.
- Cap'n Web can get close to raw WebSocket for this in-memory fan-out shape if the data plane uses
  unawaited client-main callbacks plus a shared stream-level timed batch. Repeated deployed runs with
  `batch-ms=10-12` delivered all 500 events to all 36 subscribers and landed around raw p95:
  raw `148 ms` / `130 ms`; `capnweb-batch&batch-ms=12` `146 ms` / `147 ms`.
- The intended subscriber API shape is `processEvents({ events })` with actual batching. Deployed
  runs on version `96a8690a-765a-483a-9188-187588c7d374` showed
  `capnweb-process-events-batch&batch-ms=12` at `157 ms` / `151 ms` all-subscriber p95 against raw
  `140 ms` / `139 ms`.
- Zero-delay batching is not enough on the deployed edge. It still produced thousands of Cap'n Web
  calls (`11520` calls for `18000` event-subscriber deliveries in one run) and was slower than the
  one-event callback in that run (`438 ms` all-subscriber p95).

# Notes

## 2026-05-27 12:41 UTC+1

- Added `capnweb-process-events-batch`, the intended shape:

```ts
processEvents({ events })
```

- The Stream DO still uses one stream-level pending event array and one flush timer. On flush, it
  calls each subscriber's `processEvents({ events })` once and immediately disposes the ignored
  Cap'n Web thenable.
- Deployed wire tests passed on version `96a8690a-765a-483a-9188-187588c7d374`.
- Concrete subscriber data-plane frame after setup:

```txt
in ["push",["pipeline",0,["processEvents"],[{"events":[[event1,event2,...]]}]]]
```

- Deployed DO-orchestrated benchmark, 10 publishers, 36 subscribers, 50 frames each, `pace-ms=20`,
  `payload-bytes=1280`, `timeout-ms=30000`:

| mode | batch-ms | all-subs p95 | subscriber p95 | append ack p95 | fanout calls |
| --- | ---: | ---: | ---: | ---: | ---: |
| raw | 0 | 181 ms | 173 ms | 30 ms | 18000 raw frames |
| capnweb-process-events | 0 | 447 ms | 439 ms | 62 ms | 18000 `processEvents` calls |
| capnweb-batch | 12 | 223 ms | 219 ms | 9 ms | 2772 `afterAppendBatch` calls |
| capnweb-process-events-batch | 12 | 153 ms | 147 ms | 11 ms | 3060 `processEvents` calls |

- Repeat:

| mode | batch-ms | repeat | all-subs p95 | subscriber p95 | append ack p95 | fanout calls |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| raw | 0 | 1 | 140 ms | 132 ms | 11 ms | 18000 raw frames |
| capnweb-batch | 12 | 1 | 147 ms | 141 ms | 12 ms | 3132 `afterAppendBatch` calls |
| capnweb-process-events-batch | 12 | 1 | 157 ms | 147 ms | 9 ms | 2808 `processEvents` calls |
| raw | 0 | 2 | 139 ms | 129 ms | 15 ms | 18000 raw frames |
| capnweb-batch | 12 | 2 | 281 ms | 276 ms | 14 ms | 2520 `afterAppendBatch` calls |
| capnweb-process-events-batch | 12 | 2 | 151 ms | 148 ms | 11 ms | 3096 `processEvents` calls |

- Conclusion: this is the shape we wanted to test. `processEvents({ events })` only works when the
  `events` array contains a real coalesced batch. With `batch-ms=12`, it repeatedly landed near raw
  WebSocket p95 and avoided the slow per-event `processEvents({ events: [event] })` behavior.

## 2026-05-27 12:34 UTC+1

- Added `capnweb-process-events`, where each subscriber exposes one method:

```ts
processEvents({ events })
```

- In this mode the Stream DO calls the peer once per committed event with a single-element array:

```ts
subscriber.client.processEvents({ events: [event] })
```

- The method call is still unawaited and the returned Cap'n Web thenable is immediately disposed, so
  the subscriber wire shape remains one-way after setup. Deployed wire tests passed on version
  `79c75d1f-cd47-4f7a-bdee-35a9124d19a3`.
- Concrete subscriber data-plane frame:

```txt
in ["push",["pipeline",0,["processEvents"],[{"events":[[event]]}]]]
```

- Deployed DO-orchestrated benchmark, 10 publishers, 36 subscribers, 50 frames each, `pace-ms=20`,
  `payload-bytes=1280`, `timeout-ms=30000`:

| mode | all-subs p95 | subscriber p95 | append ack p95 | fanout calls |
| --- | ---: | ---: | ---: | ---: |
| raw | 166 ms | 162 ms | 29 ms | 18000 raw frames |
| capnweb-event | 327 ms | 308 ms | 29 ms | 18000 `afterAppend` calls |
| capnweb-process-events | 370 ms | 351 ms | 34 ms | 18000 `processEvents` calls |
| capnweb-batch, `batch-ms=12` | 155 ms | 148 ms | 11 ms | 2952 batch calls |

- Repeat:

| mode | all-subs p95 | subscriber p95 | append ack p95 | fanout calls |
| --- | ---: | ---: | ---: | ---: |
| raw | 133 ms | 125 ms | 16 ms | 18000 raw frames |
| capnweb-event | 286 ms | 270 ms | 21 ms | 18000 `afterAppend` calls |
| capnweb-process-events | 345 ms | 323 ms | 30 ms | 18000 `processEvents` calls |
| capnweb-batch, `batch-ms=12` | 245 ms | 242 ms | 14 ms | 2664 batch calls |

- Conclusion: changing from `afterAppend({ event })` to `processEvents({ events: [event] })` does
  not solve the performance issue. It preserves the one-way wire property, but because it still makes
  one Cap'n Web RPC per event per subscriber, it remains in the slow class. The improvement comes
  from reducing Cap'n Web call count with actual coalescing, not from naming the method differently
  or wrapping each event in an array.

## 2026-05-27 11:52 UTC+1

- Deployed version `428aec51-384b-453e-81aa-e31df38a246a`.
- Verified deployed wire tests:
  - `capnweb-event`: after subscribe, subscriber receives only inbound
    `["push",["pipeline",0,["afterAppend"],[...]]]` frames for events.
  - `capnweb-batch`: after subscribe, subscriber receives only inbound
    `["push",["pipeline",0,["afterAppendBatch"],[...]]]` frames for event batches.
  - No subscriber-originated per-event frames after setup in either mode.
- Deployed benchmark, all runs DO-orchestrated:

| mode | batch-ms | all-subs p95 | subscriber p95 | append ack p95 | fanout calls |
| --- | ---: | ---: | ---: | ---: | ---: |
| raw | 0 | 131 ms | 128 ms | 13 ms | 18000 raw frames |
| capnweb-event | 0 | 298 ms | 282 ms | 30 ms | 18000 Cap'n Web calls |
| capnweb-batch | 0 | 438 ms | 430 ms | 13 ms | 11520 batch calls |
| capnweb-batch | 5 | 303 ms | 295 ms | 12 ms | 5040 batch calls |
| capnweb-batch | 6 | 160 ms | 156 ms | 10 ms | 5436 batch calls |

- Repeated raw versus the best batch windows:

| mode | batch-ms | repeat | all-subs p95 | subscriber p95 | append ack p95 | batch calls |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| raw | 0 | 1 | 148 ms | 139 ms | 22 ms | 0 |
| capnweb-batch | 10 | 1 | 195 ms | 143 ms | 11 ms | 3348 |
| capnweb-batch | 12 | 1 | 146 ms | 141 ms | 9 ms | 2952 |
| raw | 0 | 2 | 130 ms | 123 ms | 14 ms | 0 |
| capnweb-batch | 10 | 2 | 150 ms | 144 ms | 12 ms | 3204 |
| capnweb-batch | 12 | 2 | 147 ms | 142 ms | 12 ms | 2808 |

- Design change: `capnweb-batch` now uses one stream-level pending event array and one flush timer.
  The previous per-subscriber timer/pending-array design worked but retained avoidable scheduling and
  object churn. New subscribers record `subscribedAfterOffset` so a subscriber that joins mid-batch
  does not receive events from before it subscribed.

## 2026-05-27 11:44 UTC+1

- Deployed initial version `74c89c0b-f36f-440c-b90f-76c03d358f34`.
- First deployed benchmark, 10 publishers, 36 subscribers, 50 frames each, `pace-ms=20`,
  `payload-bytes=1280`, `timeout-ms=30000`:

| mode | all-subs p95 | subscriber p95 | append ack p95 | fanout |
| --- | ---: | ---: | ---: | --- |
| raw | 147 ms | 139 ms | 15 ms | 18000 raw frames |
| capnweb-event | 317 ms | 309 ms | 28 ms | 18000 Cap'n Web calls |
| capnweb-batch, zero-delay per-subscriber timers | 286 ms | 273 ms | 13 ms | 9591 batch calls |

- The deployed wire test showed zero-delay batching is not deterministic: two concurrent appends can
  arrive as two `afterAppendBatch` pushes. The wire invariant we can assert is one-way subscriber
  traffic; batching requires an explicit non-zero batch window.

## 2026-05-27 11:34 UTC+1

- Created the experiment as a clean in-memory repro for raw WebSocket versus Cap'n Web data-plane
  fan-out.
- Planned modes:
  - `raw`: JSON WebSocket fan-out.
  - `capnweb-event`: unawaited client-main `afterAppend({ event })`.
  - `capnweb-batch`: unawaited client-main `afterAppendBatch({ events })`.
