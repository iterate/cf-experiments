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
  #streamControllers = new Set<ReadableStreamDefaultController<Uint8Array>>();
  #textEncoder = new TextEncoder();

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

    const committed = streamEventInputToCommitted({
      input: event,
      offset: nextOffset,
      createdAt: new Date().toISOString(),
    });

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

    this.#broadcast(this.#textEncoder.encode(`${JSON.stringify(committed)}\n`));

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
   * ## Wire format: NDJSON `Uint8Array` chunks, not `StreamEvent` objects
   *
   * Cap'n Web docs say ReadableStreams can carry "arbitrarily-typed chunks" — and they
   * can, **when the stream never crosses Workers native RPC**. Our call path has two hops:
   *
   *   vitest client  --capnweb websocket-->  worker (`ProjectCapability`)
   *                                           --DO stub (Workers RPC)-->  `Stream` DO
   *
   * 1. **Cap'n Web** (client ↔ worker): JSON pipe per chunk; object chunks work if created
   *    in the worker isolate. See capnweb `__tests__/index.test.ts` "supports complex chunk
   *    types" and protocol.md (`write(chunk)` accepts any RPC-compatible value).
   *
   * 2. **Workers native RPC** (worker ↔ DO stub): `ReadableStream` is serialized as a
   *    Cap'n Proto **ByteStream** — bytes only. Docs:
   *    https://developers.cloudflare.com/workers/runtime-apis/rpc/#readablestream-writeablestream-request-and-response
   *    workerd enforces this in `ReadableStream::serialize()` → `pumpTo()`; object chunks
   *    throw `This ReadableStream did not return bytes`, and open byte pipes that end early
   *    throw `ReadableStream received over RPC disconnected prematurely`.
   *
   * Because `StreamRpcTarget` wraps `DurableObjectStub<Stream>` (`makeRpcTargetClass`),
   * `stream()` runs inside the DO but the **returned** `ReadableStream` is proxied back
   * through the stub before Cap'n Web can forward it to the client. That middle hop is
   * Workers RPC, not Cap'n Web — so chunks must be `Uint8Array` (or other byte views).
   *
   * We encode each `StreamEvent` as one NDJSON line (`JSON.stringify(event)\n`). Cap'n Web
   * then forwards `ReadableStream<Uint8Array>` to websocket clients unchanged; clients
   * decode with `TextDecoder` + `JSON.parse`. Stream flow-control `resolve` acks on the
   * websocket are normal; what we avoid is per-event `pull`/`push` RPC round trips.
   *
   * ## In-memory fan-out
   *
   * `#streamControllers` holds every open subscription on this DO. `append()` broadcasts
   * the same encoded chunk to all of them so a writer on connection A can push events to
   * a reader that called `stream()` on connection B (same `projectId` + stream path).
   *
   * To use object chunks end-to-end you'd need `stream()` to live in the **worker** isolate
   * (same isolate as the Cap'n Web session), with a separate DO→worker notification path
   * that does not return a `ReadableStream` across the stub.
   */
  stream(): ReadableStream<Uint8Array> {
    const kv = this.ctx.storage.kv;
    const latestOffset = countStreamEventsFromKv({ kv });

    let controller: ReadableStreamDefaultController<Uint8Array> | undefined;

    return new ReadableStream<Uint8Array>({
      start: (streamController) => {
        controller = streamController;
        this.#streamControllers.add(streamController);
        for (let offset = 1; offset <= latestOffset; offset++) {
          const event = readEventByOffsetFromKv({ kv, offset });
          if (event !== null) {
            streamController.enqueue(this.#textEncoder.encode(`${JSON.stringify(event)}\n`));
          }
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

  #broadcast(chunk: Uint8Array): void {
    for (const controller of this.#streamControllers) {
      try {
        controller.enqueue(chunk);
      } catch (error) {
        console.error("Error broadcasting event to controller", error);
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
