import { newWebSocketRpcSession, newWorkersRpcResponse, type RpcStub } from "capnweb";
import { DurableObject } from "cloudflare:workers";
import {
  StreamEventInput as StreamEventInputSchema,
  type StreamEvent,
  type StreamEventInput,
} from "@cf-experiments/shared/event";
import { makeRpcTargetClass } from "@cf-experiments/shared/rpc-target";
import { coreStreamProcessorContract, type CoreStreamState } from "./core-stream-processor.js";
import type { StreamProcessorRunnerRpc } from "./stream-processor.js";
import type { SubscriptionRpcTarget } from "./stream-types.js";

export class Stream extends DurableObject<Env> {
  state: CoreStreamState;

  #subscriptions = new Set<LiveSubscription>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);

    // hydrate state from KV storage or use core processor initial state
    this.state =
      this.ctx.storage.kv.get<CoreStreamState>("state") ??
      coreStreamProcessorContract.stateSchema.parse(coreStreamProcessorContract.initialState);

    // If this stream has zero events, it means it was just created.
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

    // Startup should not wait for a persistent storage write before connecting
    // outbound WebSocket consumers. The sync write path is for later benchmarks.
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
    // StreamRpcTarget is the capnweb "main object" for the stream's side of the connection.
    // The peer on the other side of the connection receives an RPC stub to it, on which it can call
    // any methods that StreamRpcTarget has.
    // And (for the moment), StreamRpcTarget is just a very thin wrapper around this durable object.
    // Think of this line as `return newWorkersRpcResponse(request, this);`
    return newWorkersRpcResponse(request, new StreamRpcTarget(this));
  }

  /**
   * Convenience RPC for appending one event.
   *
   * Uses `appendBatch()`, so it supports the same durability options.
   * Cloudflare docs: https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#put
   */
  async append(args: {
    event: StreamEventInput;
    durability?: {
      closeOutputGate?: boolean;
      waitForStorageSync?: boolean;
    };
  }) {
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
    durability?: {
      closeOutputGate?: boolean;
      waitForStorageSync?: boolean;
    };
  }): Promise<StreamEvent[]> {
    const closeOutputGate = args.durability?.closeOutputGate ?? false;
    const waitForStorageSync = args.durability?.waitForStorageSync ?? true;
    if (closeOutputGate && !waitForStorageSync) {
      throw new Error("closeOutputGate requires waitForStorageSync for public appends");
    }

    const batch = this.#beforeAppend({ events: args.events });
    if (batch.newEvents.length === 0) return batch.events;

    if (closeOutputGate) {
      this.#persistAppendSync(batch);
      await this.#awaitSimulatedStorageSyncDelay();
    } else {
      const storageWrite = this.ctx.storage.put(this.#storageWritesForAppend(batch), {
        allowUnconfirmed: true,
        noCache: true,
      });
      this.state = batch.newState;

      await this.#awaitSimulatedStorageSyncDelay();

      if (waitForStorageSync) {
        await storageWrite;
        await this.ctx.storage.sync();
      }
    }

    await this.#afterAppend(batch);
    return batch.events;
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
        const existingOffset = this.ctx.storage.kv.get<number>(
          `idempotency:${input.idempotencyKey}`,
        );
        if (existingOffset !== undefined) {
          const existing = this.ctx.storage.kv.get<StreamEvent>(`event:${existingOffset}`);
          if (existing === undefined) {
            throw new Error(`idempotency index points at missing event ${existingOffset}`);
          }
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

  async #awaitSimulatedStorageSyncDelay(): Promise<void> {
    if (this.state.config.simulatedStorageSyncDelayMs === null) return;
    await new Promise((resolve) =>
      setTimeout(resolve, this.state.config.simulatedStorageSyncDelayMs ?? 0),
    );
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
    for (const subscription of this.#subscriptions) {
      this.#deliverBatch(subscription, batch.newEvents);
    }
    for (const [subscriptionKey, configuredSubscription] of Object.entries(
      this.state.subscriptionsByKey,
    )) {
      if (
        [...this.#subscriptions].some(
          (subscription) =>
            subscription.direction === "outbound" &&
            subscription.subscriptionKey === subscriptionKey,
        )
      ) {
        continue;
      }

      const event = configuredSubscription.latestConfiguredEvent;

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
      const subscriptionRpcTarget = newWebSocketRpcSession<StreamProcessorRunnerRpc>(webSocket);
      const request = await subscriptionRpcTarget.initOutboundSubscription({
        streamRpcTarget: new StreamRpcTarget(this),
        subscriptionConfiguredEvent: event,
        streamSnapshot: this.state,
      });
      const subscription = {
        direction: "outbound" as const,
        subscriptionKey,
        subscriptionRpcTarget,
      };
      this.#subscriptions.add(subscription);
      void this.#streamStoredEventsToSubscription(subscription, request.afterOffset ?? -1);

      const disposeSubscription = () => {
        this.#subscriptions.delete(subscription);
        subscription.subscriptionRpcTarget[Symbol.dispose]();
      };
      subscriptionRpcTarget.onRpcBroken(disposeSubscription);
    }
  }

  /**
   * Attaches a caller-provided subscriber target to this stream and starts replaying events.
   * This is called by the subscriber on a StreamRpcTarget.
   * The subscriber passes in their subscriptionRpcTarget, which is an RpcTarget on which we can
   * call the consumeEvents method.
   **/
  initInboundSubscription(args: {
    subscriptionRpcTarget: RpcStub<SubscriptionRpcTarget>;
    afterOffset?: number;
  }): void {
    const subscription = {
      direction: "inbound" as const,
      subscriptionRpcTarget: args.subscriptionRpcTarget.dup(),
    };
    this.#subscriptions.add(subscription);
    void this.#streamStoredEventsToSubscription(subscription, args.afterOffset ?? -1);
  }

  /** Aborts this Durable Object incarnation so tests can observe restart behavior. */
  kill(args?: { reason?: string }): never {
    const reason = args?.reason ?? "kill requested";
    this.ctx.abort(reason);
    throw new Error("This point should never be reached; abort should kill the DO.");
  }

  /** Cheap RPC for measuring unrelated egress while append waits on storage.sync(). */
  ping() {
    return { ok: true as const, at: Date.now() };
  }

  /** Returns durable core state plus runtime-only connection state for experiments. */
  debug() {
    return {
      state: this.state,
      runtime: {
        subscriptions: [...this.#subscriptions].map((subscription) => ({
          direction: subscription.direction,
          subscriptionKey: subscription.subscriptionKey,
        })),
      },
    };
  }

  async #streamStoredEventsToSubscription(subscription: LiveSubscription, afterOffset: number) {
    const maxOffset = this.state.maxOffset;
    let batch: StreamEvent[] = [];

    for (let offset = afterOffset + 1; offset <= maxOffset; offset++) {
      const event = this.ctx.storage.kv.get<StreamEvent>(`event:${offset}`);
      if (event !== undefined) batch.push(event);

      if (batch.length === 100) {
        this.#deliverBatch(subscription, batch);
        batch = [];
        await Promise.resolve();
      }
    }

    if (batch.length > 0) this.#deliverBatch(subscription, batch);
  }

  #deliverBatch(subscription: LiveSubscription, events: StreamEvent[]) {
    const result = subscription.subscriptionRpcTarget.consumeEvents({ events });
    result[Symbol.dispose]();
  }
}

export const StreamRpcTarget = makeRpcTargetClass(Stream);

/** A live capnweb subscription edge from this stream to a batch consumer. */
type LiveSubscription = {
  direction: "inbound" | "outbound";
  subscriptionKey?: string;
  subscriptionRpcTarget: RpcStub<SubscriptionRpcTarget>;
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
