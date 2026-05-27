import { newWebSocketRpcSession } from "capnweb";
import { DurableObject } from "cloudflare:workers";
import {
  StreamEventInput as StreamEventInputSchema,
  writeEventFromKv,
  type StreamEvent,
  type StreamEventInput,
} from "@cf-experiments/shared/event";
import { makeRpcTargetClass } from "@cf-experiments/shared/rpc-target";

const STREAM_SETTINGS_KEY = "settings";

type AppendDurabilityMode = "confirmed" | "best-effort" | "checkpointed";

const APPEND_EVENT_INPUT_SCHEMA = StreamEventInputSchema.strict();

type StreamSettings = {
  defaultAppendDurabilityMode: AppendDurabilityMode;
  checkpointEveryUnconfirmedAppends: number;
  debugConfirmedSyncDelayMs: number;
  debugCheckpointSyncDelayMs: number;
};

const defaultSettings = (): StreamSettings => ({
  defaultAppendDurabilityMode: "confirmed",
  checkpointEveryUnconfirmedAppends: 100,
  debugConfirmedSyncDelayMs: 0,
  debugCheckpointSyncDelayMs: 0,
});

type StreamSubscriber = {
  controller: ReadableStreamDefaultController<StreamEvent>;
  enqueuedEvents: number;
  sessionSubscribers?: Set<StreamSubscriber>;
};

type RawAppendMessage = {
  op: "append";
  requestId: string;
  event: StreamEventInput;
};

type AppendDurability =
  | AppendDurabilityMode
  | {
      mode: AppendDurabilityMode;
      checkpointEveryUnconfirmedAppends?: number;
    };

export class Stream extends DurableObject {
  #incarnationId = crypto.randomUUID();
  #settings = defaultSettings();
  #unconfirmedWriteCount = 0;
  #checkpointInProgress = false;
  #checkpointStartedCount = 0;
  #checkpointCompletedCount = 0;
  #streamSubscribers = new Set<StreamSubscriber>();
  #volatileOffset = 0;
  #volatileSubscribers = new Set<StreamSubscriber>();
  #durableWritePlanMs: number[] = [];
  #durableBroadcastMs: number[] = [];
  #durableAppendMs: number[] = [];
  #volatileBroadcastMs: number[] = [];
  #volatileAppendMs: number[] = [];
  #durableFanoutAttempts = 0;
  #volatileFanoutAttempts = 0;
  #rawVolatileOffset = 0;
  #rawVolatileSubscribers = new Set<WebSocket>();
  #rawVolatileFanoutAttempts = 0;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.#settings = this.#readSettings();
  }

  /**
   * Merge patch, validate, persist to sync KV, update in-memory copy.
   *
   * This is on the append path because `#resolveAppendDurability()` falls back
   * to persisted settings when a caller omits per-call durability. If invalid
   * settings were accepted, future appends could start allocating offsets under
   * an unknown mode or threshold. Unknown runtime setting keys must also fail
   * explicitly; otherwise typoed settings such as
   * `checkpointEveryUnconfirmedAppend` can be persisted and then silently
   * ignored by later `append()` durability resolution. If settings were not
   * persisted/read in the constructor, a DO restart would silently reset stream
   * behavior. See "rejects invalid stream settings without changing append
   * defaults" and "persists stream settings across durable object restart" in
   * `scripts/stream-capnweb.test.ts`.
   */
  patchSettings(settings: Partial<StreamSettings>): StreamSettings {
    this.#settings = this.#parseSettings({ ...this.#readSettings(), ...settings });
    this.ctx.storage.kv.put(STREAM_SETTINGS_KEY, this.#settings);
    return this.#settings;
  }

  async append(args: {
    event: StreamEventInput;
    durability?: AppendDurability;
  }): Promise<StreamEvent> {
    /**
     * Runtime Cap'n Web callers are not constrained by this TypeScript signature.
     * Validate the argument object and event before idempotency lookup or
     * durability resolution so malformed inputs do not produce incidental
     * property-access errors, consult idempotency keys, or allocate offsets. See
     * "rejects malformed append args before reading event or durability" and
     * "rejects malformed append events before idempotency or durability handling",
     * "rejects non-string event types at the append envelope boundary",
     * "rejects non-integer event offsets at the append envelope boundary",
     * "rejects non-positive event offsets at the append envelope boundary",
     * "rejects non-string idempotency keys before idempotency lookup",
     * "rejects scalar metadata at the append envelope boundary",
     * "rejects unknown top-level append event fields instead of dropping them",
     * "preserves audio-shaped payload and metadata while rejecting only top-level
     * event fields",
     * "rejects unknown source envelope fields instead of dropping them",
     * "rejects malformed source processor fields at the append envelope boundary",
     * "rejects unknown source object fields instead of dropping them",
     * "rejects unknown append argument fields before allocating an offset",
     * plus "rejects malformed idempotent retries before reading the idempotency
     * index", in `scripts/stream-capnweb.test.ts`.
     */
    if (args === null || typeof args !== "object" || !("event" in args)) {
      throw new Error("append args must be an object with event");
    }
    const unknownArgFields = Object.keys(args).filter(
      (field) => field !== "event" && field !== "durability",
    );
    if (unknownArgFields.length > 0) {
      throw new Error(`Unknown append argument field: ${unknownArgFields.join(", ")}`);
    }
    const parsedEvent = APPEND_EVENT_INPUT_SCHEMA.safeParse(args.event);
    if (!parsedEvent.success) {
      throw new Error("append event must be a valid StreamEventInput");
    }
    const event = parsedEvent.data;
    const kv = this.ctx.storage.kv;
    if (event.idempotencyKey !== undefined) {
      /**
       * Idempotency retries must be a read-only fast path at the Stream boundary.
       *
       * `writeEventFromKv()` also protects the storage write, but doing this
       * before resolving durability is observable stream behavior: a retry must
       * not broadcast a duplicate event, increment unconfirmed write debt, or
       * schedule a checkpoint. See:
       *
       * - "rejects malformed append events before idempotency or durability handling"
       * - "idempotent append returns the original event and emits once to live subscribers"
       * - "does not count idempotent best-effort retries as new unconfirmed writes"
       * - "fails corrupted idempotent retries before conflicting validation can reject them"
       *
       * in `scripts/stream-capnweb.test.ts`.
       */
      const existingOffset = kv.get<number>(`idempotency:${event.idempotencyKey}`);
      if (existingOffset !== undefined) {
        const existing = kv.get<StreamEvent>(`event:${existingOffset}`);
        if (existing !== undefined) return existing;
        throw new Error(`Idempotency index points at missing stream event offset ${existingOffset}`);
      }
    }

    const durability = this.#resolveAppendDurability(args.durability);
    const appendStartedAt = performance.now();
    const writePlanStartedAt = performance.now();
    const committed = writeEventFromKv({
      storage: this.ctx.storage,
      input: event,
      allowUnconfirmedWrites: true,
    });
    this.#recordTiming(this.#durableWritePlanMs, performance.now() - writePlanStartedAt);

    /**
     * Durability/egress contract for append.
     *
     * Every mode writes with `allowUnconfirmed: true`. That is deliberate: the
     * default Durable Object output gate is global to outgoing messages after a
     * write, so using it would also hold unrelated RPC responses and subscriber
     * stream chunks behind this append. Cloudflare documents the default gate and
     * the `allowUnconfirmed` opt-out here:
     * https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#supported-options
     *
     * The confirmed mode rebuilds a narrower, append-causal acknowledgement:
     *
     * 1. Allocate the offset and enqueue the KV writes synchronously before the
     *    first await (`writeEventFromKv` does the multi-key append plan).
     * 2. Await `storage.sync()`, which Cloudflare documents as resolving once
     *    pending writes, including `allowUnconfirmed` writes, have persisted:
     *    https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#sync
     * 3. Only after that barrier resolves do we broadcast the new event and
     *    resolve the append RPC.
     *
     * This means unrelated DO work can still make progress while this async RPC
     * method is suspended: for example a subscriber draining already-persisted
     * backlog, or a `ping()` RPC that does not depend on this new offset. What we
     * do NOT allow in confirmed mode is bytes about this just-appended event to
     * leave before the explicit durability barrier completes.
     *
     * `best-effort` and `checkpointed` intentionally choose the opposite trade:
     * broadcast and resolve immediately, accepting that a crash before a later
     * platform flush / explicit `sync()` may lose those offsets. `checkpointed`
     * adds periodic barriers to bound the window, but the append that triggers a
     * checkpoint still returns before that checkpoint resolves.
     *
     * Tests covering this contract in `scripts/stream-capnweb.test.ts`:
     *
     * - "lets unrelated RPC resolve while confirmed append waits for durability"
     *   fails if confirmed append work holds the DO's global output gate instead
     *   of awaiting an explicit append-local barrier.
     * - "lets subscribers drain old events but not the new confirmed event before
     *   durability" fails if we broadcast before the confirmed barrier, or if old
     *   replay bytes are unnecessarily held behind the pending append.
     * - "allows a best-effort per-call override and clears it with an explicit
     *   sync barrier" and "uses checkpointed stream settings when append does
     *   not pass a per-call override" fail if non-confirmed modes do not count
     *   and expose unconfirmed append debt.
     * - "best-effort appends fan out while write debt is still unconfirmed"
     *   fails if the non-confirmed branch stops broadcasting live events before
     *   an explicit `sync()` clears that debt.
     * - "append uses the allowUnconfirmed write fast path" is a source-level
     *   sentinel for this exact option. The local runtime does not make the
     *   platform output-gate difference crisp enough for a reliable behavioral
     *   assertion when this one value is flipped.
     */
    if (durability.mode === "confirmed") {
      await this.#delayForConfirmedAppendDebug();
      await this.ctx.storage.sync();
      const broadcastStartedAt = performance.now();
      this.#broadcast(committed);
      this.#recordTiming(this.#durableBroadcastMs, performance.now() - broadcastStartedAt);
      this.#recordTiming(this.#durableAppendMs, performance.now() - appendStartedAt);
      return committed;
    }

    const broadcastStartedAt = performance.now();
    this.#broadcast(committed);
    this.#recordTiming(this.#durableBroadcastMs, performance.now() - broadcastStartedAt);

    /**
     * Only non-confirmed appends accrue stream-level unconfirmed debt. Confirmed
     * appends already paid their explicit `storage.sync()` barrier before
     * broadcasting. If this increment moves before the confirmed branch, the
     * default-mode test observes write debt after a confirmed append; if it is
     * removed or moved after checkpoint scheduling, the best-effort and
     * checkpointed durability tests stop seeing the intended mode split.
     */
    this.#unconfirmedWriteCount += 1;
    /**
     * A checkpoint threshold is only meaningful when the selected mode is
     * checkpointed. We still validate positive thresholds on best-effort object
     * options so runtime callers cannot hide malformed input, but that option
     * must not change the mode's semantics. See "best-effort object thresholds
     * are validated but do not schedule checkpoints" in
     * `scripts/stream-capnweb.test.ts`.
     */
    if (durability.mode === "checkpointed") {
      this.#scheduleCheckpointIfNeeded(durability.checkpointEveryUnconfirmedAppends);
    }

    this.#recordTiming(this.#durableAppendMs, performance.now() - appendStartedAt);
    return committed;
  }

  appendVolatile(args: { event: StreamEventInput }): StreamEvent {
    /**
     * Message-only append path for latency diagnosis. It deliberately keeps the
     * same Cap'n Web WebSocket transport and `ReadableStream<StreamEvent>`
     * chunks as `append()` / `stream()`, but removes storage, replay,
     * idempotency, offset preconditions, and durability. If this path is fast
     * while durable best-effort is slow, storage/write bookkeeping is suspect;
     * if both are slow under fan-out, the bottleneck is transport/fan-out work.
     * See the `/benchmark/audio-chaos?stream-kind=volatile` runs in `log.md`.
     */
    if (args === null || typeof args !== "object" || !("event" in args)) {
      throw new Error("append args must be an object with event");
    }
    const parsedEvent = APPEND_EVENT_INPUT_SCHEMA.safeParse(args.event);
    if (!parsedEvent.success) {
      throw new Error("append event must be a valid StreamEventInput");
    }
    const appendStartedAt = performance.now();
    this.#volatileOffset += 1;
    const committed: StreamEvent = {
      ...parsedEvent.data,
      offset: this.#volatileOffset,
      createdAt: new Date().toISOString(),
    };
    const broadcastStartedAt = performance.now();
    this.#broadcastVolatile(committed);
    this.#recordTiming(this.#volatileBroadcastMs, performance.now() - broadcastStartedAt);
    this.#recordTiming(this.#volatileAppendMs, performance.now() - appendStartedAt);
    return committed;
  }

  async appendBatch(args: {
    events: StreamEventInput[];
    durability?: AppendDurability;
  }): Promise<StreamEvent[]> {
    const events: StreamEvent[] = [];
    for (const event of args.events) {
      events.push(await this.append({ event, durability: args.durability }));
    }
    return events;
  }

  async appendBatchDebug(args: {
    events: StreamEventInput[];
    durability?: AppendDurability;
  }): Promise<{ events: StreamEvent[]; debug: ReturnType<Stream["debug"]> }> {
    return {
      events: await this.appendBatch(args),
      debug: this.debug(),
    };
  }

  maxOffset() {
    return this.ctx.storage.kv.get<number>("maxOffset") ?? 0;
  }

  async sync() {
    await this.ctx.storage.sync();
    this.#unconfirmedWriteCount = 0;
    return this.debug();
  }

  /** Test/experiment introspection; not a product API. */
  debug() {
    return {
      settings: this.#settings,
      incarnationId: this.#incarnationId,
      unconfirmedWriteCount: this.#unconfirmedWriteCount,
      checkpointInProgress: this.#checkpointInProgress,
      checkpointStartedCount: this.#checkpointStartedCount,
      checkpointCompletedCount: this.#checkpointCompletedCount,
      volatileOffset: this.#volatileOffset,
      subscribers: Array.from(this.#streamSubscribers, (subscriber) => ({
        desiredSize: subscriber.controller.desiredSize,
        enqueuedEvents: subscriber.enqueuedEvents,
      })),
      volatileSubscribers: Array.from(this.#volatileSubscribers, (subscriber) => ({
        desiredSize: subscriber.controller.desiredSize,
        enqueuedEvents: subscriber.enqueuedEvents,
      })),
      rawVolatileSubscribers: this.#rawVolatileSubscribers.size,
      timings: {
        durableWritePlanMs: this.#timingSummary(this.#durableWritePlanMs),
        durableBroadcastMs: this.#timingSummary(this.#durableBroadcastMs),
        durableAppendMs: this.#timingSummary(this.#durableAppendMs),
        volatileBroadcastMs: this.#timingSummary(this.#volatileBroadcastMs),
        volatileAppendMs: this.#timingSummary(this.#volatileAppendMs),
      },
      fanoutAttempts: {
        durable: this.#durableFanoutAttempts,
        volatile: this.#volatileFanoutAttempts,
        rawVolatile: this.#rawVolatileFanoutAttempts,
      },
    };
  }

  async debugOpenAndCancelLocalStream() {
    const readable = this.stream();
    const beforeCancel = this.debug();
    await readable.cancel("debug local stream cancel");
    return {
      beforeCancel,
      afterCancel: this.debug(),
    };
  }

  debugInstallErroredLocalSubscriber() {
    let controller: ReadableStreamDefaultController<StreamEvent> | undefined;
    void new ReadableStream<StreamEvent>({
      start(streamController) {
        controller = streamController;
      },
    });
    if (controller === undefined) throw new Error("debug stream controller was not created");
    const subscriber: StreamSubscriber = { controller, enqueuedEvents: 0 };
    controller.error(new Error("debug enqueue failure"));
    this.#streamSubscribers.add(subscriber);
    return this.debug();
  }

  debugDeleteEventForReplay(args: { offset: number }) {
    // Test-only corruption hook: lets the replay invariant test prove that a
    // maxOffset/event-key gap fails loudly instead of producing sparse history.
    this.ctx.storage.kv.delete(`event:${args.offset}`);
    return this.debug();
  }

  ping() {
    return {
      incarnationId: this.#incarnationId,
      t: Date.now(),
    };
  }

  kill(args?: { reason?: string }): never {
    const reason = args?.reason ?? "kill requested";
    this.ctx.abort(reason);
    throw new Error(reason);
  }

  /**
   * Live event feed; replays committed history, then pushes each new append.
   *
   * Cap'n Web runs on this DO (`fetch()` → `newWebSocketRpcSession(server, getCapability())`),
   * so chunks are RPC pass-by-value `StreamEvent` objects — no NDJSON byte encoding.
   *
   * The API is deliberately modeled as a returned `ReadableStream`, not as a
   * subscriber passing an `onEvent(event)` callback capability into the DO. This
   * stream is one-directional: after the initial `stream()` RPC, delivery does
   * not require per-event subscriber acknowledgements, return values, or callback
   * method calls from the DO to the subscriber. See "pure subscribers do not
   * originate per-event websocket traffic" in `scripts/stream-capnweb.test.ts`,
   * which records the subscriber WebSocket and asserts no outbound pull/push
   * frames after subscription while events are delivered.
   *
   * The subscription intentionally has no options/cursor argument. Runtime
   * callers must not be allowed to pass `fromOffset`-style objects and silently
   * receive the default full replay stream. See "rejects stream arguments
   * instead of silently ignoring subscription options".
   */
  stream(args?: unknown): ReadableStream<StreamEvent> {
    if (args !== undefined) throw new Error("stream does not accept arguments");
    return this.#openStream();
  }

  streamForSession(sessionSubscribers: Set<StreamSubscriber>): ReadableStream<StreamEvent> {
    return this.#openStream(sessionSubscribers);
  }

  streamVolatileForSession(sessionSubscribers: Set<StreamSubscriber>): ReadableStream<StreamEvent> {
    return this.#openVolatileStream(sessionSubscribers);
  }

  releaseSessionSubscribers(sessionSubscribers: Set<StreamSubscriber>): void {
    for (const subscriber of sessionSubscribers) {
      this.#removeSubscriber(subscriber);
    }
    sessionSubscribers.clear();
  }

  #openStream(sessionSubscribers?: Set<StreamSubscriber>): ReadableStream<StreamEvent> {
    const kv = this.ctx.storage.kv;
    /**
     * Capture the history boundary once, before replay. The stream contract is
     * "replay committed history, then live appends"; if we re-read maxOffset
     * during replay, or started from the wrong key prefix, replay could skip,
     * duplicate, or reorder events. See "replays committed history before
     * switching to live appends" and "rejects offset precondition failures
     * without advancing the stream" in `scripts/stream-capnweb.test.ts`.
     */
    const latestOffset = this.maxOffset();

    let subscriber: StreamSubscriber | undefined;

    return new ReadableStream<StreamEvent>({
      start: (streamController) => {
        subscriber = {
          controller: streamController,
          enqueuedEvents: 0,
          sessionSubscribers,
        };
        /**
         * Keep one subscriber object for replay and live fan-out. Replay is
         * synchronous, so "before vs after replay registration" is not itself a
         * meaningful race boundary; the observable contract is that each stream
         * is registered exactly once for later live broadcasts and that replay
         * uses the same enqueue/error-accounting path as live delivery. The
         * multi-subscriber, replay/live, "accounts replayed events through the
         * same subscriber enqueue path as live fan-out", and enqueue-error
         * tests in `scripts/stream-capnweb.test.ts` fail if those properties
         * change.
         * A single Cap'n Web session can also open more than one stream; each
         * subscriber must be tracked in that session-owned set so disposing the
         * WebSocket releases all of them. See "removes every stream opened by a
         * disposed capnweb session".
         *
         * `maxOffset` is also a contiguity claim: every offset from 1 through
         * that boundary must have an `event:${offset}` value. Silently skipping
         * a missing key would turn storage corruption into a sparse stream. See
         * "fails replay loudly when committed history has a missing event key"
         * in `scripts/stream-capnweb.test.ts`.
        */
        this.#streamSubscribers.add(subscriber);
        sessionSubscribers?.add(subscriber);

        for (let offset = 1; offset <= latestOffset; offset++) {
          const event = kv.get<StreamEvent>(`event:${offset}`);
          if (event === undefined) {
            this.#removeSubscriber(subscriber);
            throw new Error(
              `Missing stream event at offset ${offset} while replaying through ${latestOffset}`,
            );
          }
          this.#enqueueToSubscriber(subscriber, event);
        }
      },
      cancel: () => {
        if (subscriber !== undefined) {
          /**
           * Cancellation must remove the subscriber immediately. Otherwise later
           * broadcasts keep trying to enqueue into a dead stream and `debug()`
           * reports leaked subscribers.
           *
           * Cap'n Web session teardown reaches this experiment through
           * `StreamRpcTarget[Symbol.dispose]()`. A client-side
           * `ReadableStreamDefaultReader.cancel()` does not promptly invoke this
           * hook while the WebSocket session stays open; under capnweb@0.8.0 the
           * subscriber remains until the session is disposed or the pipe is torn
           * down after a later write. See "removes locally cancelled streams from
           * live fan-out", "removes cancelled subscribers from live fan-out",
           * and "documents that capnweb reader cancel does not release the
           * server subscriber" in `scripts/stream-capnweb.test.ts`.
           *
           * Replay-start errors must also remove the subscriber. Otherwise a
           * stream that failed before subscription completed would remain in
           * live fan-out with `desiredSize: null`. See "removes replay
           * subscribers when committed history is corrupt".
           */
          this.#removeSubscriber(subscriber);
        }
      },
    });
  }

  #openVolatileStream(sessionSubscribers?: Set<StreamSubscriber>): ReadableStream<StreamEvent> {
    let subscriber: StreamSubscriber | undefined;

    return new ReadableStream<StreamEvent>({
      start: (streamController) => {
        subscriber = {
          controller: streamController,
          enqueuedEvents: 0,
          sessionSubscribers,
        };
        this.#volatileSubscribers.add(subscriber);
        sessionSubscribers?.add(subscriber);
      },
      cancel: () => {
        if (subscriber !== undefined) {
          this.#removeSubscriber(subscriber);
        }
      },
    });
  }

  getCapability(_policy?: unknown) {
    return new StreamRpcTarget(this);
  }

  async fetch(request: Request) {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      /**
       * The stream DO fetch surface is only the Cap'n Web WebSocket transport.
       * Plain HTTP requests must fail closed instead of accidentally exposing a
       * second protocol with different stream framing/backpressure semantics.
       * See "rejects non-websocket requests at the stream durable object
       * boundary" in `scripts/stream-capnweb.test.ts`.
       */
      return new Response("This endpoint only accepts WebSocket requests.", { status: 400 });
    }

    const url = new URL(request.url);
    if (url.searchParams.get("transport") === "raw-volatile") {
      return this.#fetchRawVolatile();
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    newWebSocketRpcSession(server, this.getCapability());
    return new Response(null, { status: 101, webSocket: client });
  }

  #fetchRawVolatile() {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    server.addEventListener("message", (message) => {
      const data = JSON.parse(String(message.data)) as { op?: unknown };
      if (data.op === "subscribe") {
        this.#rawVolatileSubscribers.add(server);
        server.send(JSON.stringify({ op: "subscribed" }));
        return;
      }
      if (data.op !== "append") {
        throw new Error("raw volatile message op must be subscribe or append");
      }
      const append = data as RawAppendMessage;
      const parsedEvent = APPEND_EVENT_INPUT_SCHEMA.safeParse(append.event);
      if (!parsedEvent.success) {
        throw new Error("raw volatile append event must be a valid StreamEventInput");
      }
      this.#rawVolatileOffset += 1;
      const committed: StreamEvent = {
        ...parsedEvent.data,
        offset: this.#rawVolatileOffset,
        createdAt: new Date().toISOString(),
      };
      this.#broadcastRawVolatile(committed);
      server.send(JSON.stringify({ op: "ack", requestId: append.requestId, event: committed }));
    });
    server.addEventListener("close", () => {
      this.#rawVolatileSubscribers.delete(server);
    });
    server.addEventListener("error", () => {
      this.#rawVolatileSubscribers.delete(server);
    });

    return new Response(null, { status: 101, webSocket: client });
  }

  #broadcast(event: StreamEvent): void {
    this.#durableFanoutAttempts += this.#streamSubscribers.size;
    for (const subscriber of this.#streamSubscribers) {
      /**
       * Fan-out is deliberately synchronous and per-subscriber independent: one
       * slow client can build up its own stream/WebSocket queues, but there is
       * no await or shared backpressure point here that can stall delivery to
       * the next subscriber. See "delivers to an active subscriber while another
       * subscriber does not read" and "continues fan-out to later subscribers
       * after removing a broken subscriber" in
       * `scripts/stream-capnweb.test.ts`.
      */
      this.#enqueueToSubscriber(subscriber, event);
    }
  }

  #broadcastVolatile(event: StreamEvent): void {
    this.#volatileFanoutAttempts += this.#volatileSubscribers.size;
    for (const subscriber of this.#volatileSubscribers) {
      this.#enqueueToSubscriber(subscriber, event);
    }
  }

  #broadcastRawVolatile(event: StreamEvent): void {
    const message = JSON.stringify({ op: "event", event });
    this.#rawVolatileFanoutAttempts += this.#rawVolatileSubscribers.size;
    for (const subscriber of this.#rawVolatileSubscribers) {
      try {
        subscriber.send(message);
      } catch (error) {
        console.error("Error sending raw volatile event", event, error);
        this.#rawVolatileSubscribers.delete(subscriber);
      }
    }
  }

  #enqueueToSubscriber(subscriber: StreamSubscriber, event: StreamEvent): void {
    try {
      subscriber.controller.enqueue(event);
      subscriber.enqueuedEvents += 1;
    } catch (error) {
      /**
       * A broken stream controller must be isolated to its own subscriber. If we
       * keep it registered after `enqueue()` throws, every later append keeps
       * re-hitting the same dead sink and `debug()` reports a leaked subscriber;
       * if we stop iterating after that removal, later healthy subscribers miss
       * the current event. See "removes subscribers whose stream controller
       * rejects enqueue" and "continues fan-out to later subscribers after
       * removing a broken subscriber" in `scripts/stream-capnweb.test.ts`.
       */
      console.error("Error enqueuing event to subscriber", event, error, subscriber);
      this.#removeSubscriber(subscriber);
    }
  }

  #removeSubscriber(subscriber: StreamSubscriber): void {
    this.#streamSubscribers.delete(subscriber);
    this.#volatileSubscribers.delete(subscriber);
    subscriber.sessionSubscribers?.delete(subscriber);
  }

  #recordTiming(samples: number[], value: number): void {
    samples.push(value);
    if (samples.length > 2_000) samples.shift();
  }

  #timingSummary(samples: number[]) {
    const sorted = [...samples].sort((a, b) => a - b);
    const percentile = (p: number) => {
      if (sorted.length === 0) return 0;
      return sorted[Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * p))] ?? 0;
    };
    return {
      count: sorted.length,
      min: sorted[0] ?? 0,
      p50: percentile(0.5),
      p95: percentile(0.95),
      max: sorted[sorted.length - 1] ?? 0,
      avg:
        sorted.length === 0
          ? 0
          : sorted.reduce((sum, value) => sum + value, 0) / sorted.length,
    };
  }

  #resolveAppendDurability(durability: AppendDurability | undefined): {
    mode: AppendDurabilityMode;
    checkpointEveryUnconfirmedAppends: number;
  } {
    if (
      durability !== undefined &&
      typeof durability !== "string" &&
      typeof durability !== "object"
    ) {
      throw new Error("append durability must be a mode string or options object");
    }
    if (durability === null) {
      throw new Error("append durability must be a mode string or options object");
    }
    if (typeof durability === "object" && !("mode" in durability)) {
      throw new Error("append durability options must include mode");
    }
    if (typeof durability === "object") {
      const unknownFields = Object.keys(durability).filter(
        (field) => field !== "mode" && field !== "checkpointEveryUnconfirmedAppends",
      );
      if (unknownFields.length > 0) {
        throw new Error(`Unknown append durability option: ${unknownFields.join(", ")}`);
      }
    }
    const mode =
      typeof durability === "string"
        ? durability
        : typeof durability === "object"
          ? durability.mode
          : this.#settings.defaultAppendDurabilityMode;
    /**
     * Per-call durability wins over persisted stream settings, but object-form
     * modes and checkpoint thresholds still need validation before any write is
     * allocated. We validate a present threshold even when the mode is not
     * checkpointed so malformed option objects do not get silently accepted.
     * Runtime RPC callers can also send `null`; that must fail explicitly
     * instead of falling through to an incidental property-access TypeError.
     * Runtime object durability values without `mode` must also fail explicitly
     * instead of silently falling back to persisted stream settings.
     * Runtime object durability values with a present but non-string `mode`
     * must also fail instead of being treated as omitted.
     * Unknown object fields must fail explicitly too, otherwise typoed runtime
     * options like `checkpointEveryUnconfirmedAppend` are ignored and the call
     * falls back to the stream's default threshold after allocating an offset.
     * String-form `"checkpointed"` overrides also use the persisted stream
     * checkpoint threshold; otherwise the convenient mode-only override would
     * silently checkpoint at a different cadence than the stream was configured
     * for. The same applies to object-form `{ mode: "checkpointed" }` without
     * an explicit threshold; otherwise adding an options object would change
     * checkpoint cadence. See "uses stream checkpoint threshold for checkpointed
     * string overrides" and "uses stream checkpoint threshold for checkpointed
     * object overrides without a threshold".
     * The default/override/settings/invalid-mode/invalid-threshold tests,
     * including "rejects invalid checkpoint thresholds even on non-checkpointed
     * object durability", "rejects non-integer checkpoint thresholds before
     * allocating an offset", "rejects non-number checkpoint thresholds before
     * allocating an offset", "rejects null per-call durability before
     * allocating an offset", "rejects object durability without a mode before
     * allocating an offset", "rejects non-string object durability modes before
     * falling back to stream settings", "rejects unknown durability option
     * fields before allocating an offset", and "rejects primitive per-call
     * durability before falling back to stream settings" in
     * `scripts/stream-capnweb.test.ts` cover each branch here.
     */
    this.#validateDurabilityMode(mode);
    return {
      mode,
      checkpointEveryUnconfirmedAppends:
        typeof durability === "object" && durability.checkpointEveryUnconfirmedAppends !== undefined
          ? this.#validateCheckpointEveryUnconfirmedAppends(
              durability.checkpointEveryUnconfirmedAppends,
            )
          : this.#settings.checkpointEveryUnconfirmedAppends,
    };
  }

  #scheduleCheckpointIfNeeded(checkpointEveryUnconfirmedAppends: number): void {
    if (
      this.#checkpointInProgress ||
      this.#unconfirmedWriteCount < checkpointEveryUnconfirmedAppends
    ) {
      return;
    }

    this.#checkpointInProgress = true;
    this.#checkpointStartedCount += 1;

    void this.ctx.blockConcurrencyWhile(async () => {
      /**
       * `blockConcurrencyWhile()` prevents later delivered events from entering
       * the Durable Object while this callback is awaiting. Cloudflare calls out
       * this use case for async operations where state must not change while the
       * event loop yields:
       * https://developers.cloudflare.com/durable-objects/api/state/#blockconcurrencywhile
       *
       * We only use that broad gate for checkpointed mode, not for confirmed
       * append acknowledgement. A checkpoint is a stream-level throttle: once the
       * unconfirmed append window is full, pause later delivered events until the
       * explicit durability barrier has caught up.
       *
       * The call is intentionally fire-and-forget from `append()`. Awaiting it
       * inside this method would make the handler wait until the checkpoint
       * completes, hiding the mode difference this experiment is measuring. The
       * checkpoint can still affect later delivered RPC result pulls through the
       * broad DO gate; the sharper stream contract is that live event fan-out
       * happens before the checkpoint barrier. See
       * "checkpointed appendBatch returns after scheduling but before awaiting
       * the checkpoint", "checkpointed append schedules a delayed checkpoint
       * that gates later RPC", and "checkpointed passes
       * the live-before-durability probe that confirmed intentionally fails" in
       * `scripts/stream-capnweb.test.ts`.
       */

      // No zero-count guard is needed here. `#scheduleCheckpointIfNeeded()`
      // only starts this callback after the threshold is reached, and
      // `blockConcurrencyWhile()` prevents a later delivered `sync()` RPC from
      // clearing the count before this checkpoint runs. Appends still executing
      // in the same handler can only increase the count. The first await below
      // still gives an appendBatch() handler room to finish its same-turn
      // appends before sync snapshots the window. See the checkpointed
      // appendBatch tests in `scripts/stream-capnweb.test.ts`.
      await this.#delayForCheckpointDebug();
      await this.ctx.storage.sync();
      this.#unconfirmedWriteCount = 0;
      this.#checkpointCompletedCount += 1;

      // Re-arm future checkpoint windows. See "checkpointed appends can
      // schedule a second checkpoint after the first completes" in
      // `scripts/stream-capnweb.test.ts`.
      this.#checkpointInProgress = false;
    });
  }

  async #delayForConfirmedAppendDebug(): Promise<void> {
    const delayMs = this.#settings.debugConfirmedSyncDelayMs;
    if (delayMs === 0) return;
    // Test-only delay: widens the gap between "event locally accepted" and
    // "storage.sync() completed" so causal-egress tests can assert ordering
    // without depending on natural SRS latency.
    await new Promise((resolve) => setTimeout(resolve, delayMs));
  }

  async #delayForCheckpointDebug(): Promise<void> {
    const delayMs = this.#settings.debugCheckpointSyncDelayMs;
    if (delayMs === 0) return;
    // Test-only delay: makes the checkpoint gate observable without relying on
    // natural storage-sync latency.
    await new Promise((resolve) => setTimeout(resolve, delayMs));
  }

  #readSettings(): StreamSettings {
    return this.#parseSettings(this.ctx.storage.kv.get(STREAM_SETTINGS_KEY) ?? {});
  }

  #parseSettings(settings: Partial<StreamSettings>): StreamSettings {
    const defaults = defaultSettings();
    const unknownFields = Object.keys(settings).filter((field) => !(field in defaults));
    if (unknownFields.length > 0) {
      throw new Error(`Unknown stream setting: ${unknownFields.join(", ")}`);
    }
    const next = { ...defaults, ...settings };
    this.#validateDurabilityMode(next.defaultAppendDurabilityMode);
    this.#validateCheckpointEveryUnconfirmedAppends(next.checkpointEveryUnconfirmedAppends);
    if (!Number.isInteger(next.debugConfirmedSyncDelayMs) || next.debugConfirmedSyncDelayMs < 0) {
      throw new Error("debugConfirmedSyncDelayMs must be a non-negative integer");
    }
    if (!Number.isInteger(next.debugCheckpointSyncDelayMs) || next.debugCheckpointSyncDelayMs < 0) {
      throw new Error("debugCheckpointSyncDelayMs must be a non-negative integer");
    }
    return next;
  }

  #validateDurabilityMode(mode: unknown): asserts mode is AppendDurabilityMode {
    if (mode !== "confirmed" && mode !== "best-effort" && mode !== "checkpointed") {
      throw new Error(`Unknown append durability mode: ${mode}`);
    }
  }

  #validateCheckpointEveryUnconfirmedAppends(value: number): number {
    if (!Number.isInteger(value) || value <= 0) {
      throw new Error("checkpointEveryUnconfirmedAppends must be a positive integer");
    }
    return value;
  }
}

export type StreamRpc = Omit<
  Stream,
  | keyof DurableObject
  | "getCapability"
  | "fetch"
  | "stream"
  | "streamForSession"
  | "streamVolatileForSession"
  | "releaseSessionSubscribers"
> & {
  stream(args?: unknown): ReadableStream<StreamEvent>;
  streamVolatile(args?: unknown): ReadableStream<StreamEvent>;
};

type BaseStreamRpc = Omit<StreamRpc, "stream" | "streamVolatile">;

const BaseStreamRpcTarget = makeRpcTargetClass<BaseStreamRpc, Stream>(Stream, {
  /**
   * `streamForSession()` and `releaseSessionSubscribers()` are session-internal.
   * If a client can call `streamForSession()` directly, it can create a stream
   * without the `StreamRpcTarget` subscriber set that is released on WebSocket
   * disposal. See "does not expose session-owned stream internals over capnweb"
   * in `scripts/stream-capnweb.test.ts`.
   */
  exclude: [
    "getCapability",
    "fetch",
    "stream",
    "streamForSession",
    "streamVolatileForSession",
    "releaseSessionSubscribers",
  ],
});

export class StreamRpcTarget extends BaseStreamRpcTarget {
  #stream: Stream;
  #subscribers = new Set<StreamSubscriber>();

  constructor(stream: Stream) {
    super(stream);
    this.#stream = stream;
  }

  stream(args?: unknown): ReadableStream<StreamEvent> {
    if (args !== undefined) throw new Error("stream does not accept arguments");
    return this.#stream.streamForSession(this.#subscribers);
  }

  streamVolatile(args?: unknown): ReadableStream<StreamEvent> {
    if (args !== undefined) throw new Error("stream does not accept arguments");
    return this.#stream.streamVolatileForSession(this.#subscribers);
  }

  [Symbol.dispose](): void {
    this.#stream.releaseSessionSubscribers(this.#subscribers);
  }
}
