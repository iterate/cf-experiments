import { newWebSocketRpcSession } from "capnweb";
import { DurableObject } from "cloudflare:workers";
import { writeEventFromKv, type StreamEvent, type StreamEventInput } from "@cf-experiments/shared/event";
import { makeRpcTargetClass } from "@cf-experiments/shared/rpc-target";
import {
  type AppendDurabilityMode,
  StreamDoSettings as StreamDoSettingsSchema,
  type StreamDoSettings,
  readStreamDoSettingsFromKv,
  streamDoSettingsDefaults,
  writeStreamDoSettingsToKv,
} from "@cf-experiments/shared/stream-config";

type StreamSubscriber = {
  controller: ReadableStreamDefaultController<StreamEvent>;
  desiredBufferedEvents: number;
  enqueuedEvents: number;
};

type AppendDurability =
  | AppendDurabilityMode
  | {
      mode: AppendDurabilityMode;
      checkpointEveryUnconfirmedAppends?: number;
    };

export class Stream extends DurableObject {
  #incarnationId = crypto.randomUUID();
  #settings = streamDoSettingsDefaults();
  #unconfirmedWriteCount = 0;
  #checkpointInProgress = false;
  #checkpointStartedCount = 0;
  #checkpointCompletedCount = 0;
  #streamSubscribers = new Set<StreamSubscriber>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.#settings = readStreamDoSettingsFromKv({ kv: this.ctx.storage.kv });
  }

  get settings(): StreamDoSettings {
    return this.#settings;
  }

  /** Reload from sync KV (e.g. another writer updated `stream:settings`). */
  reloadSettings(): StreamDoSettings {
    this.#settings = readStreamDoSettingsFromKv({ kv: this.ctx.storage.kv });
    return this.#settings;
  }

  /** Merge patch, persist to sync KV, update in-memory copy. */
  patchSettings(settings: Partial<StreamDoSettings>): StreamDoSettings {
    this.#settings = writeStreamDoSettingsToKv({ kv: this.ctx.storage.kv, settings });
    return this.#settings;
  }

  async append(args: { event: StreamEventInput; durability?: AppendDurability }): Promise<StreamEvent> {
    const kv = this.ctx.storage.kv;
    if (args.event.idempotencyKey !== undefined) {
      const existingOffset = kv.get<number>(`stream:idem:${args.event.idempotencyKey}`);
      if (existingOffset !== undefined) {
        const existing = kv.get<StreamEvent>(`stream:evt:${existingOffset}`);
        if (existing !== undefined) return existing;
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
     */
    if (durability.mode === "confirmed") {
      await this.#delayForConfirmedAppendDebug();
      await this.ctx.storage.sync();
      this.#broadcast(committed);
      return committed;
    }

    this.#broadcast(committed);

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

  async appendBatchDebug(args: { events: StreamEventInput[]; durability?: AppendDurability }) {
    return {
      events: await this.appendBatch(args),
      durability: this.durabilityDebug(),
    };
  }

  count() {
    return this.ctx.storage.kv.get<number>("stream:meta:nextOffset") ?? 0;
  }

  async sync(): Promise<{
    unconfirmedWriteCount: number;
    checkpointStartedCount: number;
    checkpointCompletedCount: number;
  }> {
    await this.ctx.storage.sync();
    this.#unconfirmedWriteCount = 0;
    return this.durabilityDebug();
  }

  durabilityDebug() {
    return {
      settings: this.settings,
      incarnationId: this.#incarnationId,
      unconfirmedWriteCount: this.#unconfirmedWriteCount,
      checkpointInProgress: this.#checkpointInProgress,
      checkpointStartedCount: this.#checkpointStartedCount,
      checkpointCompletedCount: this.#checkpointCompletedCount,
    };
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
   * ## Backpressure / buffering model
   *
   * There are several queues in play:
   *
   * 1. This DO-created `ReadableStream`, observed through `streamDebug().desiredSize`.
   * 2. Cap'n Web's pipe from this stream into WebSocket frames.
   * 3. The client-side `ReadableStream` returned by Cap'n Web.
   * 4. The WebSocket/runtime transport buffers between both isolates/processes.
   *
   * `desiredBufferedEvents` only controls queue #1. Internally it becomes the
   * Web Streams `highWaterMark`, whose unit here is `StreamEvent` objects, not
   * bytes: one enqueued event counts as 1 because there is no custom `size()`
   * function.
   *
   * "Reports backpressure" means the controller's `desiredSize` becomes zero or
   * negative. In pull-based streams an underlying source would normally stop
   * producing until `desiredSize` rises again. Here, `append()` is push-based and
   * currently ignores that signal: `controller.enqueue()` does not throw merely
   * because `desiredSize <= 0`, so bursts can exceed `desiredBufferedEvents`.
   * The backpressure test documents that observed behavior.
   *
   * We intentionally accept only an event count, not a full `QueuingStrategy`.
   * A user-defined `size()` function is not meaningful over Cap'n Web RPC here.
   */
  stream(
    options: {
      /**
       * Desired number of `StreamEvent` objects buffered inside the DO-created
       * ReadableStream before it reports backpressure.
       *
       * This is not bytes. Because each chunk in this stream is one `StreamEvent`
       * and we do not pass a custom `size()` function to `ReadableStream`, every
       * event counts as size 1.
       */
      desiredBufferedEvents?: number;
    } = {},
  ): ReadableStream<StreamEvent> {
    const kv = this.ctx.storage.kv;
    const latestOffset = this.count();
    const desiredBufferedEvents = options.desiredBufferedEvents ?? 1;

    let subscriber: StreamSubscriber | undefined;

    return new ReadableStream<StreamEvent>(
      {
        start: (streamController) => {
          subscriber = {
            controller: streamController,
            desiredBufferedEvents,
            enqueuedEvents: 0,
          };
          this.#streamSubscribers.add(subscriber);

          for (let offset = 1; offset <= latestOffset; offset++) {
            const event = kv.get<StreamEvent>(`stream:evt:${offset}`);
            if (event !== undefined) this.#enqueueToSubscriber(subscriber, event);
          }
        },
        cancel: () => {
          if (subscriber !== undefined) {
            this.#streamSubscribers.delete(subscriber);
          }
        },
      },
      { highWaterMark: desiredBufferedEvents },
    );
  }

  /** Introspection for experiments/tests; not part of the Stream product API. */
  streamDebug() {
    return {
      subscribers: Array.from(this.#streamSubscribers, (subscriber) => ({
        desiredSize: subscriber.controller.desiredSize,
        enqueuedEvents: subscriber.enqueuedEvents,
        desiredBufferedEvents: subscriber.desiredBufferedEvents,
      })),
    };
  }

  getCapability(_policy?: unknown) {
    return new StreamRpcTarget(this);
  }

  async fetch(request: Request) {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
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
      this.#enqueueToSubscriber(subscriber, event);
    }
  }

  #enqueueToSubscriber(subscriber: StreamSubscriber, event: StreamEvent): void {
    try {
      subscriber.controller.enqueue(event);
      subscriber.enqueuedEvents += 1;
    } catch (error) {
      console.error("Error enqueuing event to subscriber", event, error, subscriber);
      this.#streamSubscribers.delete(subscriber);
    }
  }

  #resolveAppendDurability(durability: AppendDurability | undefined): {
    mode: AppendDurabilityMode;
    checkpointEveryUnconfirmedAppends: number;
  } {
    const mode =
      typeof durability === "string"
        ? durability
        : durability?.mode ?? this.settings.defaultAppendDurabilityMode;
    return {
      mode,
      checkpointEveryUnconfirmedAppends:
        typeof durability === "object" &&
        durability.checkpointEveryUnconfirmedAppends !== undefined
          ? StreamDoSettingsSchema.shape.checkpointEveryUnconfirmedAppends.parse(
              durability.checkpointEveryUnconfirmedAppends,
            )
          : this.settings.checkpointEveryUnconfirmedAppends,
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
       */

      // Let the current handler reach its next turn before deciding which
      // unconfirmed append window this checkpoint should cover. In practice this
      // lets a single appendBatch() finish allocating its offsets before the
      // checkpoint snapshots the unconfirmed append count.
      await Promise.resolve();

      while (this.#unconfirmedWriteCount > 0) {
        await this.ctx.storage.sync();
        this.#unconfirmedWriteCount = 0;
        this.#checkpointCompletedCount += 1;

        if (this.#unconfirmedWriteCount < checkpointEveryUnconfirmedAppends) {
          break;
        }
      }

      this.#checkpointInProgress = false;
    });
  }

  async #delayForConfirmedAppendDebug(): Promise<void> {
    const delayMs = this.settings.debugConfirmedSyncDelayMs;
    if (delayMs === 0) return;
    // Test-only delay: widens the gap between "event locally accepted" and
    // "storage.sync() completed" so causal-egress tests can assert ordering
    // without depending on natural SRS latency.
    await new Promise((resolve) => setTimeout(resolve, delayMs));
  }

}

export type StreamRpc = Omit<Stream, keyof DurableObject | "getCapability" | "fetch">;

export const StreamRpcTarget = makeRpcTargetClass<StreamRpc, Stream>(Stream, {
  exclude: ["getCapability", "fetch"],
});
export type StreamRpcTarget = InstanceType<typeof StreamRpcTarget>;
