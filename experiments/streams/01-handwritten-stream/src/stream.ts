import { DurableObject } from "cloudflare:workers";
import {
  countStreamEventsFromKv,
  readEventByIdempotencyKeyFromKv,
  STREAM_EVENT_UNCONFIRMED_KV_PUT,
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

    // Return previously appended event if idempotency key is provided
    if (args.event.idempotencyKey !== undefined) {
      const existing = readEventByIdempotencyKeyFromKv({
        kv: this.ctx.storage.kv,
        idempotencyKey: args.event.idempotencyKey,
      });
      if (existing !== null) return existing;
    }

    const latest = countStreamEventsFromKv({ kv: this.ctx.storage.kv });
    const nextOffset = latest + 1;

    // Treat offset as an expected next offset: append only if the stream
    // has not advanced since the caller chose that offset.
    if (event.offset !== undefined && event.offset !== nextOffset) {
      throw new Error(`Offset precondition failed: expected ${nextOffset}, got ${event.offset}`);
    }

    const committed = {
      input: event,
      offset: nextOffset,
      createdAt: new Date().toISOString(),
    };

    // Write to sqlite table
    // We use the lower level kv API because it allows us to set { allowUnconfirmed: true } .
    // That way we can decide when to call .sync() to persist the writes to other machines.
    // This is controlled by settings.maxUnconfirmedWrites.
    this.ctx.storage.put(streamEventKvKey(nextOffset), committed, {
      // allowUnconfirmed: true means output gates won't be blocked until the write is confirmed.
      // we will manually block them later with .sync()
      allowUnconfirmed: true,
      // noCache: true keeps the in-memory cache from filling with old events we'll never read again
      noCache: true,
    });

    // Since we're using the lower level kv API, we have to write the idempotencyKey => offset lookup
    // manually. Since we're in a synchronous context, all this will be persised atomically
    if (event.idempotencyKey !== undefined) {
      this.ctx.storage.put(streamEventIdempotencyKvKey(event.idempotencyKey), nextOffset, {
        allowUnconfirmed: true,
        noCache: true,
      });
    }
    // Similarly, update the high water mark offset
    this.ctx.storage.put(STREAM_EVENTS_META_NEXT_OFFSET_KEY, nextOffset, {
      allowUnconfirmed: true,
      noCache: false,
    });

    // This is where I think we'd immediately broadcast to all websocket listeners

    this.checkpointIfUnconfirmedWindowIsFull();
    return {
      ...committed,
      type: event.type,
    };
  }

  appendBatch(args: { events: StreamEventInput[] }): StreamEvent[] {
    return args.events.map((event) => this.append({ event }));
  }

  count() {
    return { kv: countStreamEventsFromKv({ kv: this.ctx.storage.kv }) };
  }

  getCapability(_policy?: unknown) {
    return new StreamRpcTarget(this);
  }

  private checkpointIfUnconfirmedWindowIsFull(): void {
    const maxUnconfirmedWrites = this.settings.maxUnconfirmedWrites;
    this.#unconfirmedWrites += 1;

    if (maxUnconfirmedWrites === null || this.#unconfirmedWrites < maxUnconfirmedWrites) {
      return;
    }

    /**
     * Append is sync and uses unconfirmed puts, so we fire `sync()` without awaiting — same
     * non-blocking egress story as the puts themselves. `sync()` waits for pending writes:
     * https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#sync
     *
     * Checkpoint policy only — not a precise view of the write buffer.
     * `maxUnconfirmedWrites: 0` syncs every append; `null` never explicitly syncs.
     */
    void this.ctx.storage.sync();
    this.#unconfirmedWrites = 0;
  }
}

type StreamRpcApi = Omit<Stream, keyof DurableObject | "getCapability">;

export const StreamRpcTarget = makeRpcTargetClass<StreamRpcApi, Stream | DurableObjectStub<Stream>>(
  Stream,
  {
    exclude: ["getCapability"],
  },
);
export type StreamRpcTarget = InstanceType<typeof StreamRpcTarget>;
