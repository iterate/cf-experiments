import { newWebSocketRpcSession, type RpcStub } from "capnweb";
import { DurableObject } from "cloudflare:workers";
import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import {
  createSimpleStreamProcessorRunner,
  type SimpleStreamProcessor,
  type SimpleStreamProcessorSnapshot,
} from "@cf-experiments/shared/simple-stream-processor";
import { makeRpcTargetClass, type RpcMethods } from "@cf-experiments/shared/rpc-target";
import { echoProcessor } from "./demo-processors/echo-processor.js";
import type {
  CoreStreamState,
  SubscriptionConfiguredEvent,
} from "./core-stream-processor.js";
import type { JonasStreamRpc, SubscriberRpcTarget, SubscriptionRequest } from "./jonas-stream.js";

export type ProcessorSlug = "echo";
type ProcessorState = { seen: number };

export type StreamProcessorInitOutboundSubscriptionArgs = {
  streamRpcTarget: JonasStreamRpc;
  subscriptionConfiguredEvent: SubscriptionConfiguredEvent;
  streamSnapshot: CoreStreamState;
};

type CapnWebStreamRpcTarget = RpcStub<JonasStreamRpc>;

export class StreamProcessor extends DurableObject {
  #processor: SimpleStreamProcessor<ProcessorState, { env: Env }> | undefined;
  #streamRpcTarget: CapnWebStreamRpcTarget | undefined;

  async fetch(request: Request) {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("WebSocket only", { status: 400 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    newWebSocketRpcSession<StreamProcessorRpc>(server, new StreamProcessorRpcTarget(this));
    return new Response(null, { status: 101, webSocket: client });
  }

  initOutboundSubscription(args: {
    streamRpcTarget: CapnWebStreamRpcTarget;
    subscriptionConfiguredEvent: SubscriptionConfiguredEvent;
    streamSnapshot: CoreStreamState;
  }): SubscriptionRequest {
    const subscriber = args.subscriptionConfiguredEvent.payload.subscriber;
    if (subscriber.type !== "built-in") {
      throw new Error("StreamProcessor only supports built-in subscribers");
    }
    if (subscriber.transport !== "captainweb-websocket") {
      throw new Error("StreamProcessor only supports captainweb-websocket subscribers");
    }

    this.#streamRpcTarget?.[Symbol.dispose]();
    this.#streamRpcTarget = args.streamRpcTarget.dup();
    this.#setProcessorSlug(subscriber.processorSlug);
    return {
      subscriberRpcTarget: new StreamProcessorSubscriberRpcTarget(this),
      afterOffset: this.#snapshot()?.offset,
    };
  }

  initialize(args: { processorSlug: ProcessorSlug }) {
    this.#setProcessorSlug(args.processorSlug);
    return { processorSlug: args.processorSlug };
  }

  status() {
    return {
      processorSlug: this.ctx.storage.kv.get<ProcessorSlug>("processorSlug"),
      snapshot: this.#snapshot(),
    };
  }

  async consumeEvents(args: { events: StreamEvent[] }) {
    const runner = await this.#runner();
    for (const event of args.events) {
      await runner.processEvent(event);
    }
  }

  async #runner() {
    return await createSimpleStreamProcessorRunner({
      processor: this.#loadProcessor(),
      deps: { env: this.env },
      append: (event) => this.#append(event),
      appendAndWait: (event) => this.#appendAndWait(event),
      loadSnapshot: () => this.#snapshot(),
      saveSnapshot: (snapshot) => this.ctx.storage.kv.put("snapshot", snapshot),
      waitUntil: (promise) => this.ctx.waitUntil(promise),
    });
  }

  #setProcessorSlug(processorSlug: string) {
    if (processorSlug !== "echo") throw new Error(`Unknown stream processor slug: ${processorSlug}`);
    this.ctx.storage.kv.put("processorSlug", processorSlug);
    this.#processor = processorForSlug(processorSlug);
  }

  #loadProcessor() {
    if (this.#processor !== undefined) return this.#processor;
    const slug = this.ctx.storage.kv.get<ProcessorSlug>("processorSlug");
    if (slug === undefined) throw new Error("StreamProcessor must be initialized first");
    this.#processor = processorForSlug(slug);
    return this.#processor;
  }

  #snapshot() {
    return this.ctx.storage.kv.get<SimpleStreamProcessorSnapshot<ProcessorState>>("snapshot");
  }

  #append(event: StreamEventInput) {
    this.ctx.waitUntil(
      Promise.resolve()
        .then(() => this.#readStreamRpcTarget().append({ event }))
        .catch((error) => {
          console.error("StreamProcessor background append failed", error);
        }),
    );
  }

  #appendAndWait(event: StreamEventInput) {
    return this.#readStreamRpcTarget().append({ event });
  }

  #readStreamRpcTarget() {
    if (this.#streamRpcTarget === undefined) {
      throw new Error("StreamProcessor has not been attached to a stream");
    }
    return this.#streamRpcTarget;
  }
}

function processorForSlug(
  slug: ProcessorSlug,
): SimpleStreamProcessor<ProcessorState, { env: Env }> {
  if (slug === "echo") return echoProcessor;
  throw new Error(`Unknown stream processor slug: ${slug}`);
}

export type StreamProcessorRpc = Omit<
  Pick<RpcMethods<StreamProcessor, "fetch">, "initOutboundSubscription" | "initialize" | "status">,
  "initOutboundSubscription"
> & {
  initOutboundSubscription(args: StreamProcessorInitOutboundSubscriptionArgs): SubscriptionRequest;
};

export const StreamProcessorRpcTarget = makeRpcTargetClass<StreamProcessorRpc, StreamProcessor>(
  StreamProcessor,
  {
    exclude: ["fetch"],
  },
);

export const StreamProcessorSubscriberRpcTarget = makeRpcTargetClass<
  SubscriberRpcTarget,
  StreamProcessor
>(StreamProcessor, {
  exclude: ["fetch"],
});
