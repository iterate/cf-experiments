import { newWebSocketRpcSession } from "capnweb";
import { DurableObject } from "cloudflare:workers";
import {
  countStreamEventsFromKv,
  readEventByIdempotencyKeyFromKv,
  readEventByOffsetFromKv,
  STREAM_EVENTS_META_NEXT_OFFSET_KEY,
  type StreamEvent,
  type StreamEventInput,
  streamEventIdempotencyKvKey,
  streamEventInputToCommitted,
  streamEventKvKey,
} from "@cf-experiments/shared/event";
import { makeRpcTargetClass } from "@cf-experiments/shared/rpc-target";
import {
  type StreamDoSettings,
  readStreamDoSettingsFromKv,
  streamDoSettingsDefaults,
  writeStreamDoSettingsToKv,
} from "@cf-experiments/shared/stream-config";

export class Stream extends DurableObject {
  #settings = streamDoSettingsDefaults();
  #unconfirmedWrites = 0;
  #streamControllers = new Set<ReadableStreamDefaultController<StreamEvent>>();

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

    if (args.event.idempotencyKey !== undefined) {
      const existing = readEventByIdempotencyKeyFromKv({
        kv: this.ctx.storage.kv,
        idempotencyKey: args.event.idempotencyKey,
      });
      if (existing !== null) return existing;
    }

    const latest = countStreamEventsFromKv({ kv: this.ctx.storage.kv });
    const nextOffset = latest + 1;

    if (event.offset !== undefined && event.offset !== nextOffset) {
      throw new Error(`Offset precondition failed: expected ${nextOffset}, got ${event.offset}`);
    }

    const committed = streamEventInputToCommitted({
      input: event,
      offset: nextOffset,
      createdAt: new Date().toISOString(),
    });

    this.ctx.storage.put(streamEventKvKey(nextOffset), committed, {
      allowUnconfirmed: true,
      noCache: true,
    });

    if (event.idempotencyKey !== undefined) {
      this.ctx.storage.put(streamEventIdempotencyKvKey(event.idempotencyKey), nextOffset, {
        allowUnconfirmed: true,
        noCache: true,
      });
    }
    this.ctx.storage.put(STREAM_EVENTS_META_NEXT_OFFSET_KEY, nextOffset, {
      allowUnconfirmed: true,
      noCache: false,
    });

    this.#broadcast(committed);

    this.checkpointIfUnconfirmedWindowIsFull();
    return committed;
  }

  appendBatch(args: { events: StreamEventInput[] }): StreamEvent[] {
    return args.events.map((event) => this.append({ event }));
  }

  count() {
    return { kv: countStreamEventsFromKv({ kv: this.ctx.storage.kv }) };
  }

  /**
   * Live event feed; replays committed history, then pushes each new append.
   *
   * Cap'n Web runs on this DO (`fetch()` → `newWebSocketRpcSession(server, getCapability())`),
   * so chunks are RPC pass-by-value `StreamEvent` objects — no NDJSON byte encoding.
   */
  stream(): ReadableStream<StreamEvent> {
    const kv = this.ctx.storage.kv;
    const latestOffset = countStreamEventsFromKv({ kv });

    let controller: ReadableStreamDefaultController<StreamEvent> | undefined;

    return new ReadableStream<StreamEvent>({
      start: (streamController) => {
        controller = streamController;
        this.#streamControllers.add(streamController);
        for (let offset = 1; offset <= latestOffset; offset++) {
          const event = readEventByOffsetFromKv({ kv, offset });
          if (event !== null) streamController.enqueue(event);
        }
      },
      cancel: () => {
        if (controller !== undefined) {
          this.#streamControllers.delete(controller);
        }
      },
    });
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
    for (const controller of this.#streamControllers) {
      try {
        controller.enqueue(event);
      } catch {
        this.#streamControllers.delete(controller);
      }
    }
  }

  private checkpointIfUnconfirmedWindowIsFull(): void {
    const maxUnconfirmedWrites = this.settings.maxUnconfirmedWrites;
    this.#unconfirmedWrites += 1;

    if (maxUnconfirmedWrites === null || this.#unconfirmedWrites < maxUnconfirmedWrites) {
      return;
    }

    void this.ctx.storage.sync();
    this.#unconfirmedWrites = 0;
  }
}

type StreamRpcApi = Omit<Stream, keyof DurableObject | "getCapability" | "fetch">;

export type StreamRpc = StreamRpcApi;

export const StreamRpcTarget = makeRpcTargetClass<StreamRpcApi, Stream>(Stream, {
  exclude: ["getCapability", "fetch"],
});
export type StreamRpcTarget = InstanceType<typeof StreamRpcTarget>;
