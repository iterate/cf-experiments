import { newWebSocketRpcSession, newWorkersRpcResponse, type RpcStub } from "capnweb";
import { DurableObject } from "cloudflare:workers";
import {
  StreamEventInput as StreamEventInputSchema,
  type StreamEvent,
  type StreamEventInput,
} from "@cf-experiments/shared/event";
import { makeRpcTargetClass } from "@cf-experiments/shared/rpc-target";
import { coreStreamProcessorContract, type CoreStreamState } from "./core-stream-processor.js";
import type {
  AppendDurability,
  StreamCursor,
  StreamProcessorRunnerRpc,
  StreamRpc,
  Subscription,
  SubscriptionKey,
  SubscriptionSink,
} from "./stream-types.js";

export class Stream extends DurableObject<Env> implements StreamRpc {
  state: CoreStreamState;

  #subscriptions = new Map<string, LiveSubscription>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);

    // Hydrate state from KV storage, but layer it over the current core initial state.
    // Local experiments often wake Durable Objects created before the latest reducer field existed.
    const initialState = coreStreamProcessorContract.stateSchema.parse(
      coreStreamProcessorContract.initialState,
    );
    const storedState = this.ctx.storage.kv.get<Partial<CoreStreamState>>("state");
    this.state = coreStreamProcessorContract.stateSchema.parse({
      ...initialState,
      ...storedState,
      config: {
        ...initialState.config,
        ...storedState?.config,
      },
      subscriptionsByKey: storedState?.subscriptionsByKey ?? initialState.subscriptionsByKey,
    });

    // When the durable object boots up the _first time_, we add a
    // events.iterate.com/stream/created event to the stream.
    //
    // And every time it's woken up for any reason (inbound fetch, rpc or alarm),
    // we append a "woken" event to the stream.
    const startupEvents: StreamEventInput[] = [];
    if (this.state.eventCount === 0) {
      // stream durable objects have names like "namespace:/some/stream/path"
      if (!ctx.id.name) throw new Error("ctx.id.name is falsey - this should never happen");
      const [namespace, path] = ctx.id.name.split(":");
      startupEvents.push({
        type: "events.iterate.com/stream/created",
        payload: { namespace, path },
      });
    }
    // each time the durable object wakes up, we append this event
    startupEvents.push({
      type: "events.iterate.com/stream/woken",
      payload: { incarnationId: crypto.randomUUID() },
    });

    // Startup should not wait for a persistent storage write before connecting outbound consumers.
    this.appendBatch({
      events: startupEvents,
      durability: { waitForStorageSync: false, closeOutputGate: false },
    }).then(
      (events) => console.log("Stream startup events appended", events),
      (error: unknown) => console.error("Stream startup append failed", error),
    );
  }

  /** Opens the capnweb RPC API for this stream Durable Object. */
  async fetch(request: Request) {
    return newWorkersRpcResponse(request, new StreamRpcTarget(this));
  }

  /**
   * Convenience RPC for appending one event.
   *
   * Uses `appendBatch()`, so it supports the same durability options.
   * Cloudflare docs: https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#put
   */
  async append(args: { event: StreamEventInput; durability?: AppendDurability }) {
    const events = await this.appendBatch({
      events: [args.event],
      durability: args.durability,
    });
    return events[0];
  }

  /**
   * Coordinates append phases: beforeAppend, durability-specific persistence, afterAppend.
   *
   * `closeOutputGate` uses sync KV writes. Otherwise this uses `storage.put(..., { allowUnconfirmed: true })`.
   * Set `waitForStorageSync` to await `storage.sync()` before `afterAppend`.
   * Cloudflare docs: https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#sync
   */
  async appendBatch(args: {
    events: StreamEventInput[];
    durability?: AppendDurability;
  }): Promise<StreamEvent[]> {
    const closeOutputGate = args.durability?.closeOutputGate ?? false;
    const waitForStorageSync = args.durability?.waitForStorageSync ?? true;
    if (closeOutputGate && !waitForStorageSync) {
      throw new Error("closeOutputGate requires waitForStorageSync for public appends");
    }

    const batch = this.#beforeAppend({ events: args.events });
    if (batch.newEvents.length === 0) return batch.events;

    if (closeOutputGate) {
      // This is the normal Durable Object sync KV API: simple and predictable, but it closes
      // the output gate until the write is durable. The async branch below is deliberately
      // weird: deployed benchmarks showed `allowUnconfirmed` is useful for high append volume
      // with a few subscribers and fast read-your-own appends, while sync writes can still win
      // under heavier fan-out. Keep both until this experiment has a narrower production shape.
      this.#persistAppendSync(batch);
    } else {
      const storageWrite = this.ctx.storage.put(this.#storageWritesForAppend(batch), {
        allowUnconfirmed: true,
        noCache: true,
      });
      this.state = batch.newState;

      if (this.state.config.simulatedStorageSyncDelayMs !== null) {
        await new Promise((resolve) =>
          setTimeout(resolve, this.state.config.simulatedStorageSyncDelayMs ?? 0),
        );
      }

      if (waitForStorageSync) {
        await storageWrite;
        await this.ctx.storage.sync();
      }
    }

    await this.#afterAppend(batch);
    return batch.events;
  }

  getEvent(
    args: { offset: number; idempotencyKey?: never } | { idempotencyKey: string; offset?: never },
  ): StreamEvent | undefined {
    if (args.idempotencyKey !== undefined) {
      const existingOffset = this.ctx.storage.kv.get<number>(`idempotency:${args.idempotencyKey}`);
      if (existingOffset === undefined) return undefined;
      return this.getEvent({ offset: existingOffset });
    }
    const event = this.ctx.storage.kv.get<StreamEvent>(`event:${args.offset}`);
    if (event === undefined) throw new Error(`No stream event found at offset ${args.offset}.`);
    return event;
  }

  getEvents(
    args: {
      afterOffset?: StreamCursor;
      beforeOffset?: StreamCursor;
      limit?: number;
    } = {},
  ): StreamEvent[] {
    const limit = args.limit;
    if (limit !== undefined && (!Number.isInteger(limit) || limit <= 0)) {
      throw new Error("getEvents limit must be a positive integer.");
    }

    const afterOffset = eventOffsetAfterCursor({
      cursor: args.afterOffset ?? "start",
      maxOffset: this.state.maxOffset,
    });
    const beforeOffset = eventOffsetBeforeCursor({
      cursor: args.beforeOffset ?? "end",
      maxOffset: this.state.maxOffset,
    });
    const endOffset = Math.min(
      beforeOffset,
      eventOffsetBeforeCursor({ cursor: "end", maxOffset: this.state.maxOffset }),
    );
    const events: StreamEvent[] = [];

    for (let offset = afterOffset + 1; offset < endOffset; offset++) {
      const event = this.ctx.storage.kv.get<StreamEvent>(`event:${offset}`);
      if (event !== undefined) events.push(event);
      if (limit !== undefined && events.length === limit) break;
    }

    return events;
  }

  subscribe(args: {
    subscriptionKey: string;
    sink: RpcStub<SubscriptionSink>;
    afterOffset?: StreamCursor;
  }): { unsubscribe(): void } {
    return this.#subscribe({
      ...args,
      direction: "inbound",
    });
  }

  runtimeState() {
    return {
      state: this.state,
      runtime: {
        liveSubscriptions: Object.fromEntries(
          [...this.#subscriptions].map(([subscriptionKey, subscription]) => [
            subscriptionKey,
            {
              direction: subscription.direction,
              phase: subscription.phase,
              startedAt: subscription.startedAt,
              afterOffset: subscription.afterOffset,
              lastDeliveredOffset: subscription.lastDeliveredOffset,
              batchesSent: subscription.batchesSent,
              eventsSent: subscription.eventsSent,
              lastDeliveredAt: subscription.lastDeliveredAt,
            },
          ]),
        ),
      },
    };
  }

  /**
   * Prepares an append synchronously: idempotency reads, offset allocation, reducer, and return order.
   */
  #beforeAppend(args: { events: StreamEventInput[] }): PreparedBatchWrite {
    const preparedAppend: PreparedBatchWrite = {
      events: [],
      newEvents: [],
      newState: this.state,
    };

    for (const eventInput of args.events) {
      const input = StreamEventInputSchema.strict().parse(eventInput);

      if (input.idempotencyKey !== undefined) {
        const existing = this.getEvent({ idempotencyKey: input.idempotencyKey });
        if (existing !== undefined) {
          preparedAppend.events.push(existing);
          continue;
        }
      }

      const offset = preparedAppend.newState.maxOffset + 1;
      if (input.offset !== undefined && input.offset !== offset) {
        throw new Error(`expected offset ${offset}, got ${input.offset}`);
      }

      const event = { ...input, offset, createdAt: new Date().toISOString() };
      // The core reducer intentionally sees every committed event. TypeScript
      // cannot model "*" as "any event except the named ones" without breaking
      // payload narrowing in the reducer's named switch cases.
      preparedAppend.newState = (coreStreamProcessorContract.reduce as any)({
        contract: coreStreamProcessorContract,
        state: preparedAppend.newState,
        event,
      });
      preparedAppend.events.push(event);
      preparedAppend.newEvents.push(event);
    }

    return preparedAppend;
  }

  /**
   * Persists with sync KV writes. This output-gated path is for benchmarking experiments.
   * Cloudflare docs: https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#put
   */
  #persistAppendSync(args: PreparedBatchWrite): void {
    this.state = args.newState;
    for (const [key, value] of Object.entries(this.#storageWritesForAppend(args))) {
      this.ctx.storage.kv.put(key, value);
    }
  }

  // Returns a record of storage writes for the given append batch.
  #storageWritesForAppend(batch: PreparedBatchWrite) {
    const writes: Record<string, unknown> = { state: batch.newState };
    for (const event of batch.newEvents) writes[`event:${event.offset}`] = event;
    for (const event of batch.newEvents) {
      if (event.idempotencyKey !== undefined) {
        writes[`idempotency:${event.idempotencyKey}`] = event.offset;
      }
    }
    return writes;
  }

  /** Runs subscriber delivery and outbound reconciliation after new events have been persisted. */
  async #afterAppend(batch: PreparedBatchWrite): Promise<void> {
    for (const subscription of this.#subscriptions.values()) {
      if (subscription.phase === "live") this.#deliverBatch(subscription, batch.newEvents);
    }
    await this.#reconcileOutboundSubscriptions();
  }

  async #reconcileOutboundSubscriptions() {
    for (const [subscriptionKey, subscription] of this.#subscriptions) {
      if (
        subscription.direction === "outbound" &&
        this.state.subscriptionsByKey[subscriptionKey] === undefined
      ) {
        this.#closeSubscription(subscription);
      }
    }

    for (const [subscriptionKey, configuredSubscription] of Object.entries(
      this.state.subscriptionsByKey,
    )) {
      if (this.#subscriptions.has(subscriptionKey)) continue;

      const processor = this.env.STREAM_PROCESSOR_RUNNER.getByName(
        `${this.state.namespace}:${this.state.path}:${subscriptionKey}`,
      );
      const response = await processor.fetch(
        new Request("https://stream-processor.local/", {
          headers: { Upgrade: "websocket" },
        }),
      );
      const webSocket = response.webSocket;
      if (webSocket === null) throw new Error("expected stream processor websocket");

      webSocket.accept();
      const runner = newWebSocketRpcSession<StreamProcessorRunnerRpc>(webSocket);
      const request = await runner.subscribe({
        stream: new StreamRpcTarget(this),
        subscriptionConfiguredEvent: configuredSubscription.latestConfiguredEvent,
        streamRuntimeState: this.runtimeState(),
      });

      this.#subscribe({
        ...request,
        direction: "outbound",
        subscriptionKey,
        onClose: () => runner[Symbol.dispose](),
      });
      runner.onRpcBroken(() => {
        const subscription = this.#subscriptions.get(subscriptionKey);
        if (subscription?.direction === "outbound") this.#closeSubscription(subscription);
      });
    }
  }

  #subscribe(args: {
    direction: "inbound" | "outbound";
    subscriptionKey: string;
    sink: RpcStub<SubscriptionSink>;
    afterOffset?: StreamCursor;
    onClose?: () => void;
  }): { unsubscribe(): void } {
    const subscriptionKey = args.subscriptionKey.trim();
    if (subscriptionKey.length === 0) throw new Error("subscriptionKey must not be blank.");

    const existing = this.#subscriptions.get(subscriptionKey);
    if (existing !== undefined) this.#closeSubscription(existing);

    const afterOffset = args.afterOffset ?? "start";
    const subscription: LiveSubscription = {
      direction: args.direction,
      subscriptionKey,
      phase: "catching-up",
      startedAt: new Date().toISOString(),
      afterOffset,
      lastDeliveredOffset: deliveredOffsetForCursor({
        cursor: afterOffset,
        maxOffset: this.state.maxOffset,
      }),
      batchesSent: 0,
      eventsSent: 0,
      sink: args.sink.dup(),
      onClose: args.onClose,
    };
    this.#subscriptions.set(subscriptionKey, subscription);
    subscription.sink.onRpcBroken(() => this.#closeSubscription(subscription));
    void this.#catchUpSubscription(subscription);

    return {
      unsubscribe: () => {
        if (this.#subscriptions.get(subscriptionKey) === subscription) {
          this.#closeSubscription(subscription);
        }
      },
    };
  }

  async #catchUpSubscription(subscription: LiveSubscription) {
    while (this.#subscriptions.get(subscription.subscriptionKey) === subscription) {
      const events = this.getEvents({
        afterOffset: subscription.lastDeliveredOffset ?? subscription.afterOffset,
        limit: 100, // hardcoded for now
      });

      if (events.length === 0) {
        subscription.phase = "live";
        return;
      }

      this.#deliverBatch(subscription, events);
      await Promise.resolve();
    }
  }

  #closeSubscription(subscription: LiveSubscription) {
    if (this.#subscriptions.get(subscription.subscriptionKey) !== subscription) return;
    this.#subscriptions.delete(subscription.subscriptionKey);
    subscription.sink[Symbol.dispose]();
    subscription.onClose?.();
  }

  #deliverBatch(subscription: LiveSubscription, events: StreamEvent[]) {
    if (events.length === 0) return;

    subscription.batchesSent += 1;
    subscription.eventsSent += events.length;
    subscription.lastDeliveredOffset = events.at(-1)?.offset ?? subscription.lastDeliveredOffset;
    subscription.lastDeliveredAt = new Date().toISOString();

    const result = subscription.sink.processEventBatch({ events });
    result[Symbol.dispose]();
  }
}

// Wraps the Stream Durable Object in an RpcTarget that can be passed
// across workers rpc and capnweb rpc boundaries
export const StreamRpcTarget = makeRpcTargetClass(Stream);

/** A live capnweb subscription edge from this stream to a batch consumer. */
type LiveSubscription = Subscription & {
  subscriptionKey: SubscriptionKey;
  sink: RpcStub<SubscriptionSink>;
  onClose?: () => void;
};

/** The result of validating a requested append batch before storage writes begin. */
type PreparedBatchWrite = {
  /** One output event for each input event, including idempotency hits that will not be written again. */
  events: StreamEvent[];
  /** Only events that were newly assigned offsets and need persistence plus delivery. */
  newEvents: StreamEvent[];
  /** The reducer state after applying every event in `newEvents`. */
  newState: CoreStreamState;
};

/** Resolves a cursor used as an exclusive lower bound into the last skipped event offset. */
function eventOffsetAfterCursor(args: { cursor: StreamCursor; maxOffset: number }): number {
  const { cursor, maxOffset } = args;
  if (cursor === "start") return -1;
  if (cursor === "end") return maxOffset;
  if (!Number.isInteger(cursor)) throw new Error("Stream cursor offset must be an integer.");
  return cursor;
}

/** Resolves a cursor used as an exclusive upper bound into the first omitted event offset. */
function eventOffsetBeforeCursor(args: { cursor: StreamCursor; maxOffset: number }): number {
  const { cursor, maxOffset } = args;
  if (cursor === "start") return 0;
  if (cursor === "end") return maxOffset + 1;
  if (!Number.isInteger(cursor)) throw new Error("Stream cursor offset must be an integer.");
  return cursor;
}

/** Resolves a subscription cursor into the last event already considered delivered. */
function deliveredOffsetForCursor(args: {
  cursor: StreamCursor;
  maxOffset: number;
}): number | undefined {
  const { cursor, maxOffset } = args;
  if (cursor === "start") return undefined;
  if (cursor === "end") return maxOffset;
  if (!Number.isInteger(cursor)) throw new Error("Stream cursor offset must be an integer.");
  return cursor;
}
