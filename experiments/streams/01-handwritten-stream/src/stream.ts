import { newWebSocketRpcSession } from "capnweb";
import { DurableObject } from "cloudflare:workers";
import { writeEventFromKv, type StreamEvent, type StreamEventInput } from "@cf-experiments/shared/event";
import { makeRpcTargetClass } from "@cf-experiments/shared/rpc-target";
import {
  type AppendDurabilityMode,
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
      checkpointEveryUnconfirmedWrites?: number;
    };

export class Stream extends DurableObject {
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

  append(args: { event: StreamEventInput; durability?: AppendDurability }): StreamEvent {
    const kv = this.ctx.storage.kv;
    if (args.event.idempotencyKey !== undefined) {
      const existingOffset = kv.get<number>(`stream:idem:${args.event.idempotencyKey}`);
      if (existingOffset !== undefined) {
        const existing = kv.get<StreamEvent>(`stream:evt:${existingOffset}`);
        if (existing !== undefined) return existing;
      }
    }

    const durability = this.#resolveAppendDurability(args.durability);
    const allowUnconfirmedWrites = durability.mode !== "confirmed";
    const committed = writeEventFromKv({
      storage: this.ctx.storage,
      input: args.event,
      allowUnconfirmedWrites,
    });

    this.#broadcast(committed);

    /**
     * IMPORTANT: `append()` is intentionally synchronous. Do not make it `async`.
     *
     * The durability modes are intentionally named by what the caller may
     * believe after observing the returned offset:
     *
     * - `confirmed`: sync KV writes (`allowUnconfirmedWrites: false`) use normal
     *   Durable Object output-gate semantics. DO code gets a `StreamEvent`
     *   synchronously, but RPC/WebSocket bytes that expose the offset may be held
     *   until Cloudflare confirms the writes durable.
     * - `best-effort`: async KV writes use `allowUnconfirmed: true`; outgoing
     *   bytes are not held by these writes, so the offset is only locally
     *   accepted until a later platform flush or explicit `sync()`.
     * - `checkpointed`: same fast egress as `best-effort`, plus periodic
     *   explicit `storage.sync()` barriers to bound the unconfirmed window. The
     *   append that fills the window still returns before the barrier resolves.
     *
     * Cloudflare docs:
     * - `allowUnconfirmed`: by default outgoing network messages are paused
     *   until previous writes are flushed; `allowUnconfirmed: true` opts out:
     *   https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#supported-options
     * - `sync()`: resolves once pending writes, including unconfirmed writes,
     *   have been persisted:
     *   https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#sync
     * - `blockConcurrencyWhile()`: runs an async callback while blocking other
     *   events from being delivered to this DO:
     *   https://developers.cloudflare.com/durable-objects/api/state/#blockconcurrencywhile
     *
     * A checkpoint is a later durability barrier, not an acknowledgement for the
     * current append. We enter `blockConcurrencyWhile()` synchronously, but do
     * not await it here. That keeps `append()` sync while making later delivered
     * events wait behind the checkpoint callback.
     */
    if (durability.mode !== "confirmed") {
      this.#unconfirmedWriteCount += 1;
      if (durability.mode === "checkpointed") {
        this.#scheduleCheckpointIfNeeded(durability.checkpointEveryUnconfirmedWrites);
      }
    }

    return committed;
  }

  appendBatch(args: { events: StreamEventInput[]; durability?: AppendDurability }): StreamEvent[] {
    return args.events.map((event) => this.append({ event, durability: args.durability }));
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
      unconfirmedWriteCount: this.#unconfirmedWriteCount,
      checkpointInProgress: this.#checkpointInProgress,
      checkpointStartedCount: this.#checkpointStartedCount,
      checkpointCompletedCount: this.#checkpointCompletedCount,
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
    checkpointEveryUnconfirmedWrites: number;
  } {
    const mode =
      typeof durability === "string"
        ? durability
        : durability?.mode ?? this.settings.defaultAppendDurabilityMode;
    return {
      mode,
      checkpointEveryUnconfirmedWrites:
        typeof durability === "object" && durability.checkpointEveryUnconfirmedWrites !== undefined
          ? durability.checkpointEveryUnconfirmedWrites
          : this.settings.checkpointEveryUnconfirmedWrites,
    };
  }

  #scheduleCheckpointIfNeeded(checkpointEveryUnconfirmedWrites: number): void {
    if (
      this.#checkpointInProgress ||
      this.#unconfirmedWriteCount < checkpointEveryUnconfirmedWrites
    ) {
      return;
    }

    this.#checkpointInProgress = true;
    this.#checkpointStartedCount += 1;

    void this.ctx.blockConcurrencyWhile(async () => {
      while (this.#unconfirmedWriteCount > 0) {
        const writesIncludedInThisSync = this.#unconfirmedWriteCount;
        await this.ctx.storage.sync();
        this.#unconfirmedWriteCount = Math.max(
          0,
          this.#unconfirmedWriteCount - writesIncludedInThisSync,
        );
        this.#checkpointCompletedCount += 1;

        if (this.#unconfirmedWriteCount < checkpointEveryUnconfirmedWrites) {
          break;
        }
      }

      this.#checkpointInProgress = false;
    });
  }

}

export type StreamRpc = Omit<Stream, keyof DurableObject | "getCapability" | "fetch">;

export const StreamRpcTarget = makeRpcTargetClass<StreamRpc, Stream>(Stream, {
  exclude: ["getCapability", "fetch"],
});
export type StreamRpcTarget = InstanceType<typeof StreamRpcTarget>;
