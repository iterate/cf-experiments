import { newWebSocketRpcSession } from "capnweb";
import { DurableObject } from "cloudflare:workers";
import {
  writeEventFromKv,
  type StreamEvent,
  type StreamEventInput,
} from "@cf-experiments/shared/event";
import { makeRpcTargetClass } from "@cf-experiments/shared/rpc-target";

const STREAM_SETTINGS_KEY = "settings";

type AppendDurabilityMode = "confirmed" | "best-effort" | "checkpointed";

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
   * an unknown mode or threshold. If settings were not persisted/read in the
   * constructor, a DO restart would silently reset stream behavior. See
   * "rejects invalid stream settings without changing append defaults" and
   * "persists stream settings across durable object restart" in
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
    const kv = this.ctx.storage.kv;
    if (args.event.idempotencyKey !== undefined) {
      /**
       * Idempotency retries must be a read-only fast path at the Stream boundary.
       *
       * `writeEventFromKv()` also protects the storage write, but doing this
       * before resolving durability is observable stream behavior: a retry must
       * not broadcast a duplicate event, increment unconfirmed write debt, or
       * schedule a checkpoint. See:
       *
       * - "idempotent append returns the original event and emits once to live subscribers"
       * - "does not count idempotent best-effort retries as new unconfirmed writes"
       * - "fails corrupted idempotent retries before conflicting validation can reject them"
       *
       * in `scripts/stream-capnweb.test.ts`.
       */
      const existingOffset = kv.get<number>(`idempotency:${args.event.idempotencyKey}`);
      if (existingOffset !== undefined) {
        const existing = kv.get<StreamEvent>(`event:${existingOffset}`);
        if (existing !== undefined) return existing;
        throw new Error(`Idempotency index points at missing stream event offset ${existingOffset}`);
      }
    }

    const durability = this.#resolveAppendDurability(args.durability);
    const committed = writeEventFromKv({
      storage: this.ctx.storage,
      input: args.event,
      allowUnconfirmedWrites: true,
    });

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
      this.#broadcast(committed);
      return committed;
    }

    this.#broadcast(committed);

    /**
     * Only non-confirmed appends accrue stream-level unconfirmed debt. Confirmed
     * appends already paid their explicit `storage.sync()` barrier before
     * broadcasting. If this increment moves before the confirmed branch, the
     * default-mode test observes write debt after a confirmed append; if it is
     * removed or moved after checkpoint scheduling, the best-effort and
     * checkpointed durability tests stop seeing the intended mode split.
     */
    this.#unconfirmedWriteCount += 1;
    if (durability.mode === "checkpointed") {
      this.#scheduleCheckpointIfNeeded(durability.checkpointEveryUnconfirmedAppends);
    }

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
      subscribers: Array.from(this.#streamSubscribers, (subscriber) => ({
        desiredSize: subscriber.controller.desiredSize,
        enqueuedEvents: subscriber.enqueuedEvents,
      })),
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
   */
  stream(): ReadableStream<StreamEvent> {
    return this.#openStream();
  }

  streamForSession(sessionSubscribers: Set<StreamSubscriber>): ReadableStream<StreamEvent> {
    return this.#openStream(sessionSubscribers);
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
         * uses the same enqueue/error-cleanup path as live delivery. The
         * multi-subscriber, replay/live, and enqueue-error tests in
         * `scripts/stream-capnweb.test.ts` fail if those properties change.
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
           * Cap'n Web session teardown currently reaches this experiment through
           * `StreamRpcTarget[Symbol.dispose]()` rather than a prompt client-side
           * `ReadableStream.cancel()`, so this local Web Streams cancel hook and
           * the session disposer both remove from the same sets. See "removes
           * locally cancelled streams from live fan-out" and "removes cancelled
           * subscribers from live fan-out" in `scripts/stream-capnweb.test.ts`.
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

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    newWebSocketRpcSession(server, this.getCapability());
    return new Response(null, { status: 101, webSocket: client });
  }

  #broadcast(event: StreamEvent): void {
    for (const subscriber of this.#streamSubscribers) {
      /**
       * Fan-out is deliberately synchronous and per-subscriber independent: one
       * slow client can build up its own stream/WebSocket queues, but there is
       * no await or shared backpressure point here that can stall delivery to
       * the next subscriber. See "delivers to an active subscriber while another
       * subscriber does not read" in `scripts/stream-capnweb.test.ts`.
       */
      this.#enqueueToSubscriber(subscriber, event);
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
       * re-hitting the same dead sink and `debug()` reports a leaked subscriber.
       * See "removes subscribers whose stream controller rejects enqueue" in
       * `scripts/stream-capnweb.test.ts`.
       */
      console.error("Error enqueuing event to subscriber", event, error, subscriber);
      this.#removeSubscriber(subscriber);
    }
  }

  #removeSubscriber(subscriber: StreamSubscriber): void {
    this.#streamSubscribers.delete(subscriber);
    subscriber.sessionSubscribers?.delete(subscriber);
  }

  #resolveAppendDurability(durability: AppendDurability | undefined): {
    mode: AppendDurabilityMode;
    checkpointEveryUnconfirmedAppends: number;
  } {
    if (durability === null) {
      throw new Error("append durability must be a mode string or options object");
    }
    if (typeof durability === "object" && !("mode" in durability)) {
      throw new Error("append durability options must include mode");
    }
    const mode =
      typeof durability === "string"
        ? durability
        : (durability?.mode ?? this.#settings.defaultAppendDurabilityMode);
    /**
     * Per-call durability wins over persisted stream settings, but object-form
     * modes and checkpoint thresholds still need validation before any write is
     * allocated. We validate a present threshold even when the mode is not
     * checkpointed so malformed option objects do not get silently accepted.
     * Runtime RPC callers can also send `null`; that must fail explicitly
     * instead of falling through to an incidental property-access TypeError.
     * Runtime object durability values without `mode` must also fail explicitly
     * instead of silently falling back to persisted stream settings.
     * The default/override/settings/invalid-mode/invalid-threshold tests,
     * including "rejects invalid checkpoint thresholds even on non-checkpointed
     * object durability" and "rejects null per-call durability before allocating
     * an offset", and "rejects object durability without a mode before
     * allocating an offset" in `scripts/stream-capnweb.test.ts` cover each
     * branch here.
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

      // Let the current handler reach its next turn before deciding which
      // unconfirmed append window this checkpoint should cover. In practice this
      // lets a single appendBatch() finish allocating its offsets before the
      // checkpoint snapshots the unconfirmed append count.
      await Promise.resolve();

      if (this.#unconfirmedWriteCount > 0) {
        await this.#delayForCheckpointDebug();
        await this.ctx.storage.sync();
        this.#unconfirmedWriteCount = 0;
        this.#checkpointCompletedCount += 1;
      }

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
    const next = { ...defaultSettings(), ...settings };
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

  #validateDurabilityMode(mode: string): asserts mode is AppendDurabilityMode {
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
  keyof DurableObject | "getCapability" | "fetch" | "streamForSession" | "releaseSessionSubscribers"
>;

type BaseStreamRpc = Omit<StreamRpc, "stream">;

const BaseStreamRpcTarget = makeRpcTargetClass<BaseStreamRpc, Stream>(Stream, {
  exclude: ["getCapability", "fetch", "stream", "streamForSession", "releaseSessionSubscribers"],
});

export class StreamRpcTarget extends BaseStreamRpcTarget {
  #stream: Stream;
  #subscribers = new Set<StreamSubscriber>();

  constructor(stream: Stream) {
    super(stream);
    this.#stream = stream;
  }

  stream(): ReadableStream<StreamEvent> {
    return this.#stream.streamForSession(this.#subscribers);
  }

  [Symbol.dispose](): void {
    this.#stream.releaseSessionSubscribers(this.#subscribers);
  }
}
