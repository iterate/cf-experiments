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
  initialCoreStreamState,
  reduceCoreStreamState,
  type CoreStreamState,
} from "./core-stream-processor.js";
import type { StreamProcessorRpc } from "./stream-processor.js";

const STORAGE_REPLAY_BATCH_SIZE = 100;

export type SubscriberRpcTarget = RpcTarget & {
  consumeEvents(args: { events: StreamEvent[] }): unknown;
};

export type SubscriptionRequest = {
  subscriberRpcTarget: SubscriberRpcTarget;
  afterOffset?: number;
};

type CapnWebSubscriberRpcTarget = RpcStub<SubscriberRpcTarget>;

type CaptainWebSubscription = {
  direction: "inbound" | "outbound";
  subscriptionKey?: string;
  subscriber: CapnWebSubscriberRpcTarget;
};

type CaptainWebOutboundSession = {
  subscriptionKey: string;
  webSocket: WebSocket;
  rpc: RpcStub<StreamProcessorRpc>;
  subscription?: CaptainWebSubscription;
};

export class JonasStream extends DurableObject<Env> {
  #incarnationId = crypto.randomUUID();
  #streamName: string;
  #simulatedStorageSyncDelayMs: number | null = null;
  #coreState: CoreStreamState | undefined;
  #subscriptions = new Set<CaptainWebSubscription>();
  #outboundSessions = new Map<string, CaptainWebOutboundSession>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);

    // Every JS object incarnation gets a fresh id, so debug output can distinguish
    // durable state from runtime state after eviction/restart.
    if (this.ctx.id.name === undefined) {
      throw new Error("JonasStream must be addressed by name");
    }
    // The worker routes /jonas/:path to a Durable Object named by that path.
    // That name is the stable stream identity used for processor object names too.
    this.#streamName = this.ctx.id.name;
  }

  async fetch(request: Request) {
    const transport = new URL(request.url).searchParams.get("transport") ?? "capnweb";
    if (transport !== "capnweb") return new Response("transport must be capnweb", { status: 400 });
    return newWorkersRpcResponse(request, new JonasStreamRpcTarget(this));
  }

  async append(args: { event: StreamEventInput }): Promise<StreamEvent> {
    const result = this.#writeAppend(StreamEventInputSchema.strict().parse(args.event));
    if (this.#simulatedStorageSyncDelayMs !== null) {
      await new Promise((resolve) => setTimeout(resolve, this.#simulatedStorageSyncDelayMs ?? 0));
    }
    await this.ctx.storage.sync();
    if (result.appended) {
      this.#broadcast([result.event]);
      await this.#reconcileOutboundSubscriptions();
    }
    return result.event;
  }

  initInboundSubscription(args: {
    subscriberRpcTarget: CapnWebSubscriberRpcTarget;
    afterOffset?: number;
  }): void {
    this.#registerSubscription({
      direction: "inbound",
      subscriber: args.subscriberRpcTarget,
      afterOffset: args.afterOffset,
    });
  }

  simulateStorageSyncDelay(delayMs: number | null): number | null {
    if (delayMs !== null && (!Number.isInteger(delayMs) || delayMs < 0)) {
      throw new Error("simulated storage sync delay must be null or a non-negative integer");
    }
    this.#simulatedStorageSyncDelayMs = delayMs;
    return delayMs;
  }

  kill(args?: { reason?: string }): never {
    const reason = args?.reason ?? "kill requested";
    this.ctx.abort(reason);
    throw new Error("This point should never be reached; abort should kill the DO.");
  }

  ping() {
    return { incarnationId: this.#incarnationId };
  }

  getMaxOffset() {
    return this.#readCoreState().maxOffset;
  }

  debug() {
    return {
      incarnationId: this.#incarnationId,
      streamName: this.#readStreamName(),
      reducedState: this.#readCoreState(),
      runtime: {
        subscriptions: [...this.#subscriptions].map((subscription) => ({
          direction: subscription.direction,
          subscriptionKey: subscription.subscriptionKey,
        })),
        outboundSessions: [...this.#outboundSessions.keys()],
      },
    };
  }

  #writeAppend(input: StreamEventInput) {
    if (input.idempotencyKey !== undefined) {
      const existingOffset = this.ctx.storage.kv.get<number>(`idempotency:${input.idempotencyKey}`);
      if (existingOffset !== undefined) {
        const existing = this.ctx.storage.kv.get<StreamEvent>(`event:${existingOffset}`);
        if (existing !== undefined) return { event: existing, appended: false };
        throw new Error(`idempotency index points at missing event ${existingOffset}`);
      }
    }

    const offset = this.#readCoreState().maxOffset + 1;
    if (input.offset !== undefined && input.offset !== offset) {
      throw new Error(`expected offset ${offset}, got ${input.offset}`);
    }

    const event = { ...input, offset, createdAt: new Date().toISOString() };
    const reducedState = reduceCoreStreamState({ state: this.#readCoreState(), event });
    this.#coreState = reducedState;

    const writes = {
      [`event:${event.offset}`]: event,
      maxOffset: event.offset,
      coreState: reducedState,
    };
    if (input.idempotencyKey !== undefined) {
      writes[`idempotency:${input.idempotencyKey}`] = event.offset;
    }
    void this.ctx.storage.put(writes, { allowUnconfirmed: true, noCache: true });
    return { event, appended: true };
  }

  async #reconcileOutboundSubscriptions() {
    const state = this.#readCoreState();
    for (const [subscriptionKey, configuredSubscription] of Object.entries(
      state.subscriptionsByKey,
    )) {
      if (this.#outboundSessions.has(subscriptionKey)) continue;
      const event = configuredSubscription.latestConfiguredEvent;
      if (
        event.payload.subscriber.type !== "built-in" ||
        event.payload.subscriber.transport !== "captainweb-websocket"
      ) {
        continue;
      }

      const processor = this.env.STREAM_PROCESSOR.getByName(
        `${this.#readStreamName()}:${subscriptionKey}`,
      );
      const response = await processor.fetch(
        new Request("https://stream-processor.local/?transport=capnweb", {
          headers: { Upgrade: "websocket" },
        }),
      );
      const webSocket = response.webSocket;
      if (webSocket === null) throw new Error("expected stream processor websocket");

      webSocket.accept();
      const rpc = newWebSocketRpcSession<StreamProcessorRpc>(webSocket);
      const outboundSession: CaptainWebOutboundSession = { subscriptionKey, webSocket, rpc };
      this.#outboundSessions.set(subscriptionKey, outboundSession);

      const request = await rpc.initOutboundSubscription({
        streamRpcTarget: new JonasStreamRpcTarget(this),
        subscriptionConfiguredEvent: event,
        streamSnapshot: state,
      });
      outboundSession.subscription = this.#registerSubscription({
        direction: "outbound",
        subscriptionKey,
        subscriber: request.subscriberRpcTarget,
        afterOffset: request.afterOffset,
      });
      const removeOutboundSession = () => {
        this.#outboundSessions.delete(subscriptionKey);
        rpc[Symbol.dispose]();
        if (outboundSession.subscription !== undefined) {
          this.#removeSubscription(outboundSession.subscription);
        }
      };
      webSocket.addEventListener("close", removeOutboundSession);
      webSocket.addEventListener("error", removeOutboundSession);
    }
  }

  #registerSubscription(args: {
    direction: "inbound" | "outbound";
    subscriptionKey?: string;
    subscriber: CapnWebSubscriberRpcTarget;
    afterOffset?: number;
  }): CaptainWebSubscription {
    const subscription = {
      direction: args.direction,
      subscriptionKey: args.subscriptionKey,
      subscriber: args.subscriber.dup(),
    };
    this.#subscriptions.add(subscription);
    void this.#streamStoredEventsToSubscription(subscription, args.afterOffset ?? -1).catch((error) => {
      console.error("JonasStream replay failed", error);
      this.#removeSubscription(subscription);
    });
    return subscription;
  }

  async #streamStoredEventsToSubscription(
    subscription: CaptainWebSubscription,
    afterOffset: number,
  ) {
    const maxOffset = this.#readCoreState().maxOffset;
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

  #broadcast(events: StreamEvent[]) {
    for (const subscription of this.#subscriptions) {
      this.#deliverBatch(subscription, events);
    }
  }

  #deliverBatch(subscription: CaptainWebSubscription, events: StreamEvent[]) {
    try {
      const result = subscription.subscriber.consumeEvents({ events });
      result[Symbol.dispose]();
    } catch (error) {
      console.error("Error sending CaptainWeb event batch", error);
      this.#removeSubscription(subscription);
    }
  }

  #removeSubscription(subscription: CaptainWebSubscription) {
    if (!this.#subscriptions.delete(subscription)) return;
    subscription.subscriber[Symbol.dispose]();
    if (subscription.direction === "outbound" && subscription.subscriptionKey !== undefined) {
      const outboundSession = this.#outboundSessions.get(subscription.subscriptionKey);
      if (outboundSession?.subscription === subscription) {
        this.#outboundSessions.delete(subscription.subscriptionKey);
        outboundSession.rpc[Symbol.dispose]();
        outboundSession.webSocket.close();
      }
    }
  }

  #readCoreState() {
    this.#coreState ??=
      this.ctx.storage.kv.get<CoreStreamState>("coreState") ??
      initialCoreStreamState(new Date().toISOString());
    return this.#coreState;
  }

  #readStreamName() {
    return this.#streamName;
  }
}

export type JonasStreamRpc = Omit<
  RpcMethods<JonasStream, "fetch">,
  "initInboundSubscription"
> & {
  initInboundSubscription(args: SubscriptionRequest): void;
};

export const JonasStreamRpcTarget = makeRpcTargetClass<JonasStreamRpc, JonasStream>(JonasStream, {
  exclude: ["fetch"],
});
