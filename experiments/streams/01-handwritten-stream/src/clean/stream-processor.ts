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
import type { CoreStreamState, SubscriptionConfiguredEvent } from "./core-stream-processor.js";
import type { StreamRpc, SubscriptionRpcTarget } from "./stream-types.js";

export type ProcessorSlug = "echo";
type ProcessorState = { seen: number };

export type StreamProcessorRunnerInitOutboundSubscriptionArgs = {
  streamRpcTarget: StreamRpc;
  subscriptionConfiguredEvent: SubscriptionConfiguredEvent;
  streamSnapshot: CoreStreamState;
};

type CapnWebStreamRpcTarget = RpcStub<StreamRpc>;

export class StreamProcessorRunner extends DurableObject {
  #processor: SimpleStreamProcessor<ProcessorState, { env: Env }> | undefined;
  #streamRpcTarget: CapnWebStreamRpcTarget | undefined;

  async fetch(request: Request) {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("WebSocket only", { status: 400 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    newWebSocketRpcSession<StreamProcessorRunnerRpc>(
      server,
      new StreamProcessorRunnerRpcTarget(this),
    );
    return new Response(null, { status: 101, webSocket: client });
  }

  initOutboundSubscription(args: {
    streamRpcTarget: CapnWebStreamRpcTarget;
    subscriptionConfiguredEvent: SubscriptionConfiguredEvent;
    streamSnapshot: CoreStreamState;
  }): { afterOffset?: number } {
    const subscriber = args.subscriptionConfiguredEvent.payload.subscriber;
    if (subscriber.type !== "built-in") {
      throw new Error("StreamProcessorRunner only supports built-in subscribers");
    }
    if (subscriber.transport !== "capnweb-websocket") {
      throw new Error("StreamProcessorRunner only supports capnweb-websocket subscribers");
    }

    this.#streamRpcTarget?.[Symbol.dispose]();
    this.#streamRpcTarget = args.streamRpcTarget.dup();
    this.#setProcessorSlug(subscriber.processorSlug);
    return {
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
    if (processorSlug !== "echo")
      throw new Error(`Unknown stream processor slug: ${processorSlug}`);
    this.ctx.storage.kv.put("processorSlug", processorSlug);
    this.#processor = processorForSlug(processorSlug);
  }

  #loadProcessor() {
    if (this.#processor !== undefined) return this.#processor;
    const slug = this.ctx.storage.kv.get<ProcessorSlug>("processorSlug");
    if (slug === undefined) throw new Error("StreamProcessorRunner must be initialized first");
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
          console.error("StreamProcessorRunner background append failed", error);
        }),
    );
  }

  #appendAndWait(event: StreamEventInput) {
    return this.#readStreamRpcTarget().append({ event });
  }

  #readStreamRpcTarget() {
    if (this.#streamRpcTarget === undefined) {
      throw new Error("StreamProcessorRunner has not been attached to a stream");
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

export type StreamProcessorRunnerRpc = SubscriptionRpcTarget &
  Omit<
    Pick<
      RpcMethods<StreamProcessorRunner, "fetch">,
      "initOutboundSubscription" | "initialize" | "status" | "consumeEvents"
    >,
    "initOutboundSubscription" | "consumeEvents"
  > & {
    initOutboundSubscription(args: StreamProcessorRunnerInitOutboundSubscriptionArgs): {
      afterOffset?: number;
    };
  };

export const StreamProcessorRunnerRpcTarget = makeRpcTargetClass<
  StreamProcessorRunnerRpc,
  StreamProcessorRunner
>(StreamProcessorRunner, {
  exclude: ["fetch"],
});
