import { newWebSocketRpcSession } from "capnweb";
import { DurableObject } from "cloudflare:workers";
import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import {
  createSimpleStreamProcessorRunner,
  type SimpleStreamProcessor,
  type SimpleStreamProcessorSnapshot,
} from "@cf-experiments/shared/simple-stream-processor";
import { makeRpcTargetClass } from "@cf-experiments/shared/rpc-target";
import { echoProcessor } from "./demo-processors/echo-processor.js";
import { JonasStreamAckFrame, JonasStreamEventsFrame } from "./jonas-stream-types.js";

export type ProcessorSlug = "echo";
type ProcessorState = { seen: number };

export class StreamProcessor extends DurableObject {
  #processor: SimpleStreamProcessor<ProcessorState, { env: Env }> | undefined;
  #streamWebSocket: WebSocket | undefined;
  #appendWaiters = new Map<string, PromiseWithResolvers<StreamEvent>>();

  async fetch(request: Request) {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("WebSocket only", { status: 400 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    const transport = new URL(request.url).searchParams.get("transport") ?? "raw-ws";

    if (transport === "capnweb") {
      server.accept();
      newWebSocketRpcSession<StreamProcessorRpc>(server, new StreamProcessorRpcTarget(this));
      return new Response(null, { status: 101, webSocket: client });
    }

    if (transport !== "raw-ws") {
      return new Response("transport must be raw-ws or capnweb", { status: 400 });
    }

    this.ctx.acceptWebSocket(server);
    this.#streamWebSocket = server;
    server.send(JSON.stringify({ op: "subscribe", afterOffset: this.#snapshot()?.offset ?? 0 }));
    return new Response(null, { status: 101, webSocket: client });
  }

  initialize(args: { processorSlug: ProcessorSlug }) {
    this.ctx.storage.kv.put("processorSlug", args.processorSlug);
    this.#processor = processorForSlug(args.processorSlug);
    return { processorSlug: args.processorSlug };
  }

  status() {
    return {
      processorSlug: this.ctx.storage.kv.get<ProcessorSlug>("processorSlug"),
      snapshot: this.#snapshot(),
    };
  }

  async webSocketMessage(_webSocket: WebSocket, message: string | ArrayBuffer) {
    const text = typeof message === "string" ? message : new TextDecoder().decode(message);
    const frame: unknown = JSON.parse(text);

    if (isAppendAckFrame(frame)) {
      const waiter = this.#appendWaiters.get(frame.appendKey);
      if (waiter !== undefined) {
        this.#appendWaiters.delete(frame.appendKey);
        waiter.resolve(frame.event);
      }
      return;
    }

    const runner = await this.#runner();
    for (const event of JonasStreamEventsFrame.parse(frame).events) {
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
    this.#streamWebSocket?.send(JSON.stringify({ op: "append", event }));
  }

  #appendAndWait(event: StreamEventInput) {
    const appendKey = crypto.randomUUID();
    const waiter = Promise.withResolvers<StreamEvent>();
    this.#appendWaiters.set(appendKey, waiter);
    this.#streamWebSocket?.send(
      JSON.stringify({
        op: "append",
        event,
        requestAck: { key: appendKey },
      }),
    );
    return waiter.promise;
  }
}

function processorForSlug(
  slug: ProcessorSlug,
): SimpleStreamProcessor<ProcessorState, { env: Env }> {
  if (slug === "echo") return echoProcessor;
  throw new Error(`Unknown stream processor slug: ${slug}`);
}

function isAppendAckFrame(value: unknown): value is ReturnType<typeof JonasStreamAckFrame.parse> {
  try {
    JonasStreamAckFrame.parse(value);
    return true;
  } catch {
    return false;
  }
}

export type StreamProcessorRpc = Pick<StreamProcessor, "initialize" | "status">;

export const StreamProcessorRpcTarget = makeRpcTargetClass<StreamProcessorRpc, StreamProcessor>(
  StreamProcessor,
  {
    exclude: ["fetch", "webSocketMessage"],
  },
);
