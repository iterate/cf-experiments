import { newWebSocketRpcSession, type RpcStub } from "capnweb";
import { DurableObject } from "cloudflare:workers";
import {
  StreamEventInput as StreamEventInputSchema,
  type StreamEvent,
  type StreamEventInput,
} from "@cf-experiments/shared/event";
import { makeRpcTargetClass } from "@cf-experiments/shared/rpc-target";
import {
  initialCoreStreamState,
  reduceCoreStreamState,
  type CoreStreamState,
} from "./core-stream-processor.js";
import type { StreamProcessorRpc } from "./stream-processor.js";

const STORAGE_REPLAY_BATCH_SIZE = 100;

export type SubscriberRpcTarget = {
  consumeEvents(args: { events: StreamEvent[] }): unknown;
};

export type SubscriptionRequest = {
  subscriberRpcTarget: SubscriberRpcTarget;
  afterOffset?: number;
};

type LiveSubscriber = {
  direction: "inbound" | "outbound";
  subscriptionKey?: string;
  target: SubscriberRpcTarget;
  disposeTarget: () => void;
};

type OutboundSession = {
  subscriptionKey: string;
  webSocket: WebSocket;
  rpc: RpcStub<StreamProcessorRpc>;
};

export class JonasStream extends DurableObject<Env> {
  #incarnationId = crypto.randomUUID();
  #streamName: string | undefined;
  #simulatedStorageSyncDelayMs: number | null = null;
  #coreState: CoreStreamState | undefined;
  #subscribers = new Set<LiveSubscriber>();
  #outboundSessions = new Map<string, OutboundSession>();

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

  initInboundSubscription(args: SubscriptionRequest): void {
    this.#registerSubscriber({
      direction: "inbound",
      target: args.subscriberRpcTarget,
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
        subscribers: [...this.#subscribers].map((subscriber) => ({
          direction: subscriber.direction,
          subscriptionKey: subscriber.subscriptionKey,
        })),
        outboundSessions: [...this.#outboundSessions.keys()],
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
    for (const [subscriptionKey, subscription] of Object.entries(state.subscriptionsByKey)) {
      if (this.#outboundSessions.has(subscriptionKey)) continue;
      const event = subscription.latestConfiguredEvent;
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
      this.#outboundSessions.set(subscriptionKey, { subscriptionKey, webSocket, rpc });
      webSocket.addEventListener("close", () => this.#outboundSessions.delete(subscriptionKey));
      webSocket.addEventListener("error", () => this.#outboundSessions.delete(subscriptionKey));

      const request = await rpc.initOutboundSubscription({
        streamRpcTarget: this.getCapability(),
        subscriptionConfiguredEvent: event,
        streamSnapshot: state,
      });
      this.#registerSubscriber({
        direction: "outbound",
        subscriptionKey,
        target: request.subscriberRpcTarget,
        afterOffset: request.afterOffset,
      });
    }
  }

  #registerSubscriber(args: {
    direction: "inbound" | "outbound";
    subscriptionKey?: string;
    target: SubscriberRpcTarget;
    afterOffset?: number;
  }) {
    const retainedTarget = retainSubscriberRpcTarget(args.target);
    const subscriber = {
      direction: args.direction,
      subscriptionKey: args.subscriptionKey,
      target: retainedTarget.target,
      disposeTarget: retainedTarget.dispose,
    };
    this.#subscribers.add(subscriber);
    void this.#streamStoredEventsToSubscriber(subscriber, args.afterOffset ?? -1).catch((error) => {
      console.error("JonasStream replay failed", error);
      this.#removeSubscriber(subscriber);
    });
  }

  async #streamStoredEventsToSubscriber(subscriber: LiveSubscriber, afterOffset: number) {
    const maxOffset = this.#readCoreState().maxOffset;
    let batch: StreamEvent[] = [];

    for (let offset = afterOffset + 1; offset <= maxOffset; offset++) {
      const event = this.ctx.storage.kv.get<StreamEvent>(`event:${offset}`);
      if (event !== undefined) batch.push(event);

      if (batch.length === STORAGE_REPLAY_BATCH_SIZE) {
        this.#deliverBatch(subscriber, batch);
        batch = [];
        await Promise.resolve();
      }
    }

    if (batch.length > 0) this.#deliverBatch(subscriber, batch);
  }

  #broadcast(events: StreamEvent[]) {
    for (const subscriber of this.#subscribers) {
      this.#deliverBatch(subscriber, events);
    }
  }

  #deliverBatch(subscriber: LiveSubscriber, events: StreamEvent[]) {
    try {
      const result = subscriber.target.consumeEvents({ events });
      if (isDisposable(result)) result[Symbol.dispose]();
    } catch (error) {
      console.error("Error sending CaptainWeb event batch", error);
      this.#removeSubscriber(subscriber);
    }
  }

  #removeSubscriber(subscriber: LiveSubscriber) {
    this.#subscribers.delete(subscriber);
    subscriber.disposeTarget();
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

function isDisposable(value: unknown): value is Disposable {
  return (
    ((typeof value === "object" && value !== null) || typeof value === "function") &&
    Symbol.dispose in value &&
    typeof value[Symbol.dispose] === "function"
  );
}

function retainSubscriberRpcTarget(target: SubscriberRpcTarget): {
  target: SubscriberRpcTarget;
  dispose: () => void;
} {
  if (!isDuplicableSubscriberRpcTarget(target)) {
    throw new Error("subscriberRpcTarget must be duplicable");
  }
  const retained = target.dup();
  return {
    target: retained,
    dispose: () => retained[Symbol.dispose](),
  };
}

function isDuplicableSubscriberRpcTarget(
  value: SubscriberRpcTarget,
): value is SubscriberRpcTarget & { dup(): SubscriberRpcTarget & Disposable } {
  return (
    ((typeof value === "object" && value !== null) || typeof value === "function") &&
    "dup" in value &&
    typeof value.dup === "function"
  );
}

export type JonasStreamRpc = Pick<
  JonasStream,
  | "append"
  | "initInboundSubscription"
  | "simulateStorageSyncDelay"
  | "kill"
  | "ping"
  | "getMaxOffset"
  | "debug"
>;

export const JonasStreamRpcTarget = makeRpcTargetClass<JonasStreamRpc, JonasStream>(JonasStream, {
  exclude: ["fetch", "getCapability"],
});
