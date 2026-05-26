import { newWebSocketRpcSession } from "capnweb";
import { DurableObject } from "cloudflare:workers";
import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import { makeRpcTargetClass } from "@cf-experiments/shared/rpc-target";
import {
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

export class Stream extends DurableObject {
  #settings = streamDoSettingsDefaults();
  #unconfirmedWriteCount = 0;
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

  append(args: { event: StreamEventInput }): StreamEvent {
    const event = args.event;
    const kv = this.ctx.storage.kv;

    if (event.idempotencyKey !== undefined) {
      const existingOffset = kv.get<number>(`stream:idem:${event.idempotencyKey}`);
      if (existingOffset !== undefined) {
        const existing = kv.get<StreamEvent>(`stream:evt:${existingOffset}`);
        if (existing !== undefined) return existing;
      }
    }

    const latest = this.count();
    const nextOffset = latest + 1;

    if (event.offset !== undefined && event.offset !== nextOffset) {
      throw new Error(`Offset precondition failed: expected ${nextOffset}, got ${event.offset}`);
    }

    const { offset: _precondition, ...input } = event;
    const committed = {
      ...input,
      offset: nextOffset,
      createdAt: new Date().toISOString(),
    };

    this.ctx.storage.put(`stream:evt:${nextOffset}`, committed, {
      allowUnconfirmed: true,
      noCache: true,
    });

    if (event.idempotencyKey !== undefined) {
      this.ctx.storage.put(`stream:idem:${event.idempotencyKey}`, nextOffset, {
        allowUnconfirmed: true,
        noCache: true,
      });
    }
    this.ctx.storage.put("stream:meta:nextOffset", nextOffset, {
      allowUnconfirmed: true,
      noCache: false,
    });

    this.#broadcast(committed);

    /**
     * IMPORTANT: `append()` is intentionally synchronous. Do not make it `async`.
     *
     * This experiment is measuring the fast path where event appends use async
     * `ctx.storage.put(..., { allowUnconfirmed: true })` and immediately return
     * / fan out without waiting for the Durable Object output gate.
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
     * Checkpoint policy:
     * 1. If `maxUnconfirmedWrites` is `null`, never explicitly sync.
     * 2. If the window is not full yet, do nothing.
     * 3. If the window is full, synchronously enter `blockConcurrencyWhile()`
     *    and await `storage.sync()` inside its async callback. We intentionally
     *    do not `await` the returned Promise here, because doing so would force
     *    `append()` to become async. The runtime still blocks later events until
     *    the callback completes.
     */
    const maxUnconfirmedWrites = this.settings.maxUnconfirmedWrites;
    this.#unconfirmedWriteCount += 1;

    if (maxUnconfirmedWrites === null || this.#unconfirmedWriteCount < maxUnconfirmedWrites) {
      return committed;
    }

    void this.ctx.blockConcurrencyWhile(async () => {
      await this.ctx.storage.sync();
      this.#unconfirmedWriteCount = 0;
    });

    return committed;
  }

  appendBatch(args: { events: StreamEventInput[] }): StreamEvent[] {
    return args.events.map((event) => this.append({ event }));
  }

  count() {
    return this.ctx.storage.kv.get<number>("stream:meta:nextOffset") ?? 0;
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

}

export type StreamRpc = Omit<Stream, keyof DurableObject | "getCapability" | "fetch">;

export const StreamRpcTarget = makeRpcTargetClass<StreamRpc, Stream>(Stream, {
  exclude: ["getCapability", "fetch"],
});
export type StreamRpcTarget = InstanceType<typeof StreamRpcTarget>;
