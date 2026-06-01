import { newWebSocketRpcSession, type RpcStub, type RpcTarget } from "capnweb";
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

type CapnWebSubscriberRpcTarget = Disposable & {
  dup(): CapnWebSubscriberRpcTarget;
  consumeEvents(args: { events: StreamEvent[] }): Disposable;
};

type CaptainWebSubscription = {
  direction: "inbound" | "outbound";
  subscriptionKey?: string;
  subscriber: CapnWebSubscriberRpcTarget;
  outboundSession?: {
    webSocket: WebSocket;
    rpc: RpcStub<StreamProcessorRpc>;
  };
};

export class JonasStream extends DurableObject<Env> {
  #incarnationId = crypto.randomUUID();
  #streamName: string | undefined;
  #simulatedStorageSyncDelayMs: number | null = null;
  #coreState: CoreStreamState | undefined;
  #subscriptions = new Set<CaptainWebSubscription>();

  async fetch(request: Request) {
    this.#streamName = new URL(request.url).pathname.slice("/jonas/".length) || "default";
    this.ctx.storage.kv.put("streamName", this.#streamName);

    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("WebSocket only", { status: 400 });
    }

    const transport = new URL(request.url).searchParams.get("transport") ?? "capnweb";
    if (transport !== "capnweb") {
      return new Response("transport must be capnweb", { status: 400 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    newWebSocketRpcSession(server, this.getCapability());
    return new Response(null, { status: 101, webSocket: client });
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
          hasOutboundSession: subscription.outboundSession !== undefined,
        })),
      },
    };
  }

  getCapability(_policy?: unknown) {
    return new JonasStreamRpcTarget(this);
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
      if (this.#hasOutboundSubscription(subscriptionKey)) continue;
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

      const request = await rpc.initOutboundSubscription({
        streamRpcTarget: this.getCapability(),
        subscriptionConfiguredEvent: event,
        streamSnapshot: state,
      });
      const subscription = this.#registerSubscription({
        direction: "outbound",
        subscriptionKey,
        subscriber: request.subscriberRpcTarget,
        afterOffset: request.afterOffset,
        outboundSession: { webSocket, rpc },
      });
      webSocket.addEventListener("close", () => this.#removeSubscription(subscription));
      webSocket.addEventListener("error", () => this.#removeSubscription(subscription));
    }
  }

  #registerSubscription(args: {
    direction: "inbound" | "outbound";
    subscriptionKey?: string;
    subscriber: CapnWebSubscriberRpcTarget;
    afterOffset?: number;
    outboundSession?: CaptainWebSubscription["outboundSession"];
  }): CaptainWebSubscription {
    const subscription = {
      direction: args.direction,
      subscriptionKey: args.subscriptionKey,
      subscriber: args.subscriber.dup(),
      outboundSession: args.outboundSession,
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
    subscription.outboundSession?.rpc[Symbol.dispose]();
    subscription.outboundSession?.webSocket.close();
  }

  #hasOutboundSubscription(subscriptionKey: string) {
    return [...this.#subscriptions].some(
      (subscription) =>
        subscription.direction === "outbound" && subscription.subscriptionKey === subscriptionKey,
    );
  }

  #readCoreState() {
    this.#coreState ??=
      this.ctx.storage.kv.get<CoreStreamState>("coreState") ??
      initialCoreStreamState(new Date().toISOString());
    return this.#coreState;
  }

  #readStreamName() {
    this.#streamName ??= this.ctx.storage.kv.get<string>("streamName");
    if (this.#streamName === undefined) throw new Error("missing stream name");
    return this.#streamName;
  }
}

export type JonasStreamRpc = Omit<
  RpcMethods<JonasStream, "fetch" | "getCapability">,
  "initInboundSubscription"
> & {
  initInboundSubscription(args: SubscriptionRequest): void;
};

export const JonasStreamRpcTarget = makeRpcTargetClass<JonasStreamRpc, JonasStream>(JonasStream, {
  exclude: ["fetch", "getCapability"],
});
