import { newWebSocketRpcSession, type RpcStub } from "capnweb";
import { DurableObject } from "cloudflare:workers";
import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import {
  createSimpleStreamProcessorRunner,
  type SimpleStreamProcessor,
  type SimpleStreamProcessorSnapshot,
} from "@cf-experiments/shared/simple-stream-processor";
import { makeRpcTargetClass } from "@cf-experiments/shared/rpc-target";
import { echoProcessor } from "./demo-processors/echo-processor.js";
import type {
  CoreStreamState,
  SubscriptionConfiguredEvent,
} from "./core-stream-processor.js";
import type { StreamRpc } from "./stream-types.js";

export type ProcessorSlug = "echo";
type ProcessorState = { seen: number };

type CapnWebStreamRpcTarget = RpcStub<StreamRpc>;

export class StreamProcessorRunner extends DurableObject {
  #processor: SimpleStreamProcessor<ProcessorState, { env: Env }> | undefined;
  #streamRpcTarget: CapnWebStreamRpcTarget | undefined;

  /** Opens the CaptainWeb RPC API for this stream processor runner. */
  async fetch(request: Request) {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("WebSocket only", { status: 400 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    newWebSocketRpcSession<StreamProcessorRunnerRpc>(server, new StreamProcessorRunnerRpcTarget(this));
    return new Response(null, { status: 101, webSocket: client });
  }

  /**
   * Initializes an outbound subscription started by a stream Durable Object.
   * Returns the last processed offset so the stream can replay exactly the missing events.
   */
  initOutboundSubscription(args: {
    streamRpcTarget: CapnWebStreamRpcTarget;
    subscriptionConfiguredEvent: SubscriptionConfiguredEvent;
    streamSnapshot: CoreStreamState;
  }): { afterOffset?: number } {
    const subscriber = args.subscriptionConfiguredEvent.payload.subscriber;
    if (subscriber.type !== "built-in") {
      throw new Error("StreamProcessorRunner only supports built-in subscribers");
    }
    if (subscriber.transport !== "captainweb-websocket") {
      throw new Error("StreamProcessorRunner only supports captainweb-websocket subscribers");
    }

    this.#streamRpcTarget?.[Symbol.dispose]();
    this.#streamRpcTarget = args.streamRpcTarget.dup();
    this.#setProcessorSlug(subscriber.processorSlug);
    return {
      afterOffset: this.#snapshot()?.offset,
    };
  }

  /** Returns durable processor state for test fixtures and operator inspection. */
  status() {
    return {
      processorSlug: this.ctx.storage.kv.get<ProcessorSlug>("processorSlug"),
      snapshot: this.#snapshot(),
    };
  }

  /** Consumes a batch delivered by a stream subscription RPC target. */
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
    if (slug === undefined) throw new Error("StreamProcessorRunner must be initialized first");
    this.#processor = processorForSlug(slug);
    return this.#processor;
  }

  #snapshot() {
    return this.ctx.storage.kv.get<SimpleStreamProcessorSnapshot<ProcessorState>>("snapshot");
  }

  #append(event: StreamEventInput) {
    void this.#readStreamRpcTarget()
      .append({ event })
      .catch((error) => {
        console.error("StreamProcessorRunner background append failed", error);
      });
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

export const StreamProcessorRunnerRpcTarget = makeRpcTargetClass(StreamProcessorRunner, {
  exclude: ["fetch"],
});

export type StreamProcessorRunnerRpc = InstanceType<typeof StreamProcessorRunnerRpcTarget>;
