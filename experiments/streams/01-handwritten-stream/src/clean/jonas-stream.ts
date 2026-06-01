import {
  newWebSocketRpcSession,
  newWorkersRpcResponse,
  type RpcStub,
  type RpcTarget,
} from "capnweb";
import { DurableObject } from "cloudflare:workers";
import {
  StreamEventInput as StreamEventInputSchema,
  type StreamEvent,
  type StreamEventInput,
} from "@cf-experiments/shared/event";
import { makeRpcTargetClass, type RpcMethods } from "@cf-experiments/shared/rpc-target";
import {
  coreStreamProcessorContract,
  reduceCoreStreamState,
  type CoreStreamState,
} from "./core-stream-processor.js";
import type { StreamProcessorRpc } from "./stream-processor.js";

/** The subscriber-side capability JonasStream calls whenever events are ready. */
export type SubscriberRpcTarget = RpcTarget & {
  consumeEvents(args: { events: StreamEvent[] }): unknown;
};

/**
 * Returned by either side of the subscription handshake.
 *
 * `afterOffset` is optional. Omitting it means "start before offset 0", so the
 * stream replays from the beginning.
 */
export type SubscriptionRequest = {
  subscriberRpcTarget: SubscriberRpcTarget;
  afterOffset?: number;
};

type CaptainWebSubscriberRpcTarget = RpcStub<SubscriberRpcTarget>;

/** A live CaptainWeb edge from a stream to something consuming event batches. */
type CaptainWebSubscription = {
  direction: "inbound" | "outbound";
  subscriptionKey?: string;
  subscriber: CaptainWebSubscriberRpcTarget;
  webSocket?: WebSocket;
  streamProcessor?: RpcStub<StreamProcessorRpc>;
};

const STORAGE_REPLAY_BATCH_SIZE = 100;

export class JonasStream extends DurableObject<Env> {
  readonly incarnationId = crypto.randomUUID();
  state: CoreStreamState;

  #simulatedStorageSyncDelayMs: number | null = null;
  #subscriptions = new Set<CaptainWebSubscription>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);

    // The worker names Jonas stream objects as "namespace:stream/path".
    // `namespace` is useful once we have multiple stream families sharing this class.
    if (this.ctx.id.name === undefined) throw new Error("JonasStream must be addressed by name");
    const splitAt = this.ctx.id.name.indexOf(":");
    const streamNamespace = splitAt === -1 ? "jonas" : this.ctx.id.name.slice(0, splitAt);
    const streamPath = splitAt === -1 ? this.ctx.id.name : this.ctx.id.name.slice(splitAt + 1);
    this.state =
      this.ctx.storage.kv.get<CoreStreamState>("coreState") ??
      coreStreamProcessorContract.stateSchema.parse(coreStreamProcessorContract.initialState);

    if (this.state.eventCount === 0) {
      const event = {
        type: "events.iterate.com/stream/created",
        payload: { streamNamespace, streamPath },
        offset: this.state.maxOffset + 1,
        createdAt: new Date().toISOString(),
      };
      this.state = reduceCoreStreamState({ state: this.state, event });
      this.ctx.storage.transactionSync(() => {
        this.ctx.storage.kv.put(`event:${event.offset}`, event);
        this.ctx.storage.kv.put("maxOffset", event.offset);
        this.ctx.storage.kv.put("coreState", this.state);
      });
    }

    const event = {
      type: "events.iterate.com/stream/woken",
      payload: { incarnationId: this.incarnationId },
      offset: this.state.maxOffset + 1,
      createdAt: new Date().toISOString(),
    };
    this.state = reduceCoreStreamState({ state: this.state, event });
    this.ctx.storage.transactionSync(() => {
      this.ctx.storage.kv.put(`event:${event.offset}`, event);
      this.ctx.storage.kv.put("maxOffset", event.offset);
      this.ctx.storage.kv.put("coreState", this.state);
    });
  }

  /** Opens the CaptainWeb RPC API for this stream Durable Object. */
  async fetch(request: Request) {
    return newWorkersRpcResponse(request, new JonasStreamRpcTarget(this));
  }

  /** Appends one event, persists it, updates core reduced state, then pushes it to live subscribers. */
  async append(args: { event: StreamEventInput }): Promise<StreamEvent> {
    const input = StreamEventInputSchema.strict().parse(args.event);

    if (input.idempotencyKey !== undefined) {
      const existingOffset = this.ctx.storage.kv.get<number>(`idempotency:${input.idempotencyKey}`);
      if (existingOffset !== undefined) {
        const existing = this.ctx.storage.kv.get<StreamEvent>(`event:${existingOffset}`);
        if (existing !== undefined) return existing;
        throw new Error(`idempotency index points at missing event ${existingOffset}`);
      }
    }

    const offset = this.state.maxOffset + 1;
    if (input.offset !== undefined && input.offset !== offset) {
      throw new Error(`expected offset ${offset}, got ${input.offset}`);
    }

    const event = { ...input, offset, createdAt: new Date().toISOString() };
    this.state = reduceCoreStreamState({ state: this.state, event });

    const writes = {
      [`event:${event.offset}`]: event,
      maxOffset: event.offset,
      coreState: this.state,
    };
    if (input.idempotencyKey !== undefined) {
      writes[`idempotency:${input.idempotencyKey}`] = event.offset;
    }
    void this.ctx.storage.put(writes, { allowUnconfirmed: true, noCache: true });

    if (this.#simulatedStorageSyncDelayMs !== null) {
      await new Promise((resolve) => setTimeout(resolve, this.#simulatedStorageSyncDelayMs ?? 0));
    }
    await this.ctx.storage.sync();

    for (const subscription of this.#subscriptions) {
      this.#deliverBatch(subscription, [event]);
    }
    await this.#reconcileOutboundSubscriptions();
    return event;
  }

  /** Attaches a caller-provided subscriber target to this stream and starts replaying events. */
  initInboundSubscription(args: {
    subscriberRpcTarget: CaptainWebSubscriberRpcTarget;
    afterOffset?: number;
  }): void {
    const subscription = {
      direction: "inbound" as const,
      subscriber: args.subscriberRpcTarget.dup(),
    };
    this.#subscriptions.add(subscription);
    void this.#streamStoredEventsToSubscription(subscription, args.afterOffset ?? -1);
  }

  /** Adds a storage sync delay so experiments can isolate durability cost from RPC cost. */
  simulateStorageSyncDelay(delayMs: number | null): number | null {
    if (delayMs !== null && (!Number.isInteger(delayMs) || delayMs < 0)) {
      throw new Error("simulated storage sync delay must be null or a non-negative integer");
    }
    this.#simulatedStorageSyncDelayMs = delayMs;
    return delayMs;
  }

  /** Aborts this Durable Object incarnation so tests can observe restart behavior. */
  kill(args?: { reason?: string }): never {
    const reason = args?.reason ?? "kill requested";
    this.ctx.abort(reason);
    throw new Error("This point should never be reached; abort should kill the DO.");
  }

  /** Returns the largest allocated event offset without dumping the full reduced state. */
  getMaxOffset() {
    return this.state.maxOffset;
  }

  /** Returns durable core state plus runtime-only connection state for experiments. */
  debug() {
    return {
      reducedState: this.state,
      runtime: {
        subscriptions: [...this.#subscriptions].map((subscription) => ({
          direction: subscription.direction,
          subscriptionKey: subscription.subscriptionKey,
          hasWebSocket: subscription.webSocket !== undefined,
          hasStreamProcessor: subscription.streamProcessor !== undefined,
        })),
      },
    };
  }

  async #reconcileOutboundSubscriptions() {
    for (const [subscriptionKey, configuredSubscription] of Object.entries(
      this.state.subscriptionsByKey,
    )) {
      if (
        [...this.#subscriptions].some(
          (subscription) =>
            subscription.direction === "outbound" && subscription.subscriptionKey === subscriptionKey,
        )
      ) {
        continue;
      }

      const event = configuredSubscription.latestConfiguredEvent;
      if (
        event.payload.subscriber.type !== "built-in" ||
        event.payload.subscriber.transport !== "captainweb-websocket"
      ) {
        continue;
      }

      const processor = this.env.STREAM_PROCESSOR.getByName(
        `${this.state.streamNamespace}:${this.state.streamPath}:${subscriptionKey}`,
      );
      const response = await processor.fetch(
        new Request("https://stream-processor.local/", {
          headers: { Upgrade: "websocket" },
        }),
      );
      const webSocket = response.webSocket;
      if (webSocket === null) throw new Error("expected stream processor websocket");

      webSocket.accept();
      const streamProcessor = newWebSocketRpcSession<StreamProcessorRpc>(webSocket);
      const request = await streamProcessor.initOutboundSubscription({
        streamRpcTarget: new JonasStreamRpcTarget(this),
        subscriptionConfiguredEvent: event,
        streamSnapshot: this.state,
      });
      const subscription = {
        direction: "outbound" as const,
        subscriptionKey,
        subscriber: request.subscriberRpcTarget.dup(),
        webSocket,
        streamProcessor,
      };
      this.#subscriptions.add(subscription);
      void this.#streamStoredEventsToSubscription(subscription, request.afterOffset ?? -1);

      const disposeSubscription = () => {
        this.#subscriptions.delete(subscription);
        subscription.subscriber[Symbol.dispose]();
        streamProcessor[Symbol.dispose]();
      };
      webSocket.addEventListener("close", disposeSubscription);
      webSocket.addEventListener("error", disposeSubscription);
    }
  }

  async #streamStoredEventsToSubscription(
    subscription: CaptainWebSubscription,
    afterOffset: number,
  ) {
    const maxOffset = this.state.maxOffset;
    let batch: StreamEvent[] = [];

    for (let offset = afterOffset + 1; offset <= maxOffset; offset++) {
      const event = this.ctx.storage.kv.get<StreamEvent>(`event:${offset}`);
      if (event !== undefined) batch.push(event);

      if (batch.length === STORAGE_REPLAY_BATCH_SIZE) {
        this.#deliverBatch(subscription, batch);
        batch = [];
        await Promise.resolve();
      }
    }

    if (batch.length > 0) this.#deliverBatch(subscription, batch);
  }

  #deliverBatch(subscription: CaptainWebSubscription, events: StreamEvent[]) {
    const result = subscription.subscriber.consumeEvents({ events });
    result[Symbol.dispose]();
  }
}

export type JonasStreamRpc = RpcMethods<JonasStream, "fetch" | "initInboundSubscription"> & {
  initInboundSubscription(args: SubscriptionRequest): void;
};

export const JonasStreamRpcTarget = makeRpcTargetClass<JonasStreamRpc, JonasStream>(JonasStream, {
  exclude: ["fetch"],
});
