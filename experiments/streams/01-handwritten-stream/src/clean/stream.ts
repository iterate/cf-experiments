import { newWebSocketRpcSession } from "capnweb";
import { RpcTarget } from "cloudflare:workers";
import {
  DurableIteratorObject,
  type PublishEventOptions,
} from "@orpc/experimental-durable-iterator/durable-object";
import {
  StreamEventInput as StreamEventInputSchema,
  type StreamEvent,
  type StreamEventInput,
} from "@cf-experiments/shared/event";
import { CLEAN_STREAM_ORPC_SIGNING_KEY, type CleanStreamRpc } from "./protocol.js";

const APPEND_EVENT_INPUT_SCHEMA = StreamEventInputSchema.strict();

type Transport = "capnweb" | "orpc" | "rawws";

type CapnwebSubscriber = {
  controller: ReadableStreamDefaultController<StreamEvent>;
  sessionSubscribers?: Set<CapnwebSubscriber>;
};

export class CleanStream extends DurableIteratorObject<StreamEvent, Env> {
  #app = new StreamApp();
  #capnwebSubscribers = new Set<CapnwebSubscriber>();
  #rawwsSubscribers = new Set<WebSocket>();
  #fanoutAttempts = {
    capnweb: 0,
    orpc: 0,
    rawws: 0,
  };

  constructor(ctx: DurableObjectState<{}>, env: Env) {
    super(ctx, env, {
      signingKey: CLEAN_STREAM_ORPC_SIGNING_KEY,
      resumeRetentionSeconds: Number.NaN,
    });
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const transport = readTransport(url);
    if (transport === null) {
      return new Response("transport must be capnweb, orpc, or rawws", { status: 400 });
    }

    if (url.searchParams.get("op") === "debug") {
      return Response.json(this.debug());
    }

    if (url.searchParams.get("op") === "append") {
      const body = await request.json();
      return Response.json(this.#appendFromTransport(transport, body));
    }

    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("This endpoint only accepts WebSocket requests.", { status: 400 });
    }

    if (transport === "capnweb") return this.#fetchCapnweb();
    if (transport === "rawws") return this.#fetchRawws();
    return super.fetch(request);
  }

  debug() {
    return {
      kind: "clean-stream",
      offset: this.#app.offset,
      subscribers: {
        capnweb: this.#capnwebSubscribers.size,
        orpc: this.ctx.getWebSockets().length,
        rawws: this.#rawwsSubscribers.size,
      },
      fanoutAttempts: this.#fanoutAttempts,
    };
  }

  publishEvent(payload: StreamEvent, options?: PublishEventOptions): void {
    super.publishEvent(payload, options);
  }

  append(args: { event: StreamEventInput }): StreamEvent {
    return this.#appendFromTransport("capnweb", args);
  }

  subscribeForSession(sessionSubscribers: Set<CapnwebSubscriber>): ReadableStream<StreamEvent> {
    let subscriber: CapnwebSubscriber | undefined;

    return new ReadableStream<StreamEvent>({
      start: (controller) => {
        subscriber = { controller, sessionSubscribers };
        this.#capnwebSubscribers.add(subscriber);
        sessionSubscribers.add(subscriber);
      },
      cancel: () => {
        if (subscriber !== undefined) this.#removeCapnwebSubscriber(subscriber);
      },
    });
  }

  releaseSessionSubscribers(sessionSubscribers: Set<CapnwebSubscriber>): void {
    for (const subscriber of sessionSubscribers) {
      this.#removeCapnwebSubscriber(subscriber);
    }
    sessionSubscribers.clear();
  }

  #appendFromTransport(transport: Transport, args: unknown): StreamEvent {
    if (args === null || typeof args !== "object" || !("event" in args)) {
      throw new Error("append args must be an object with event");
    }
    const parsedEvent = APPEND_EVENT_INPUT_SCHEMA.safeParse(args.event);
    if (!parsedEvent.success) {
      throw new Error("append event must be a valid StreamEventInput");
    }

    const event = this.#app.append(parsedEvent.data);
    this.#broadcast(event);
    return event;
  }

  #broadcast(event: StreamEvent): void {
    this.#fanoutAttempts.capnweb += this.#capnwebSubscribers.size;
    for (const subscriber of this.#capnwebSubscribers) {
      try {
        subscriber.controller.enqueue(event);
      } catch (error) {
        console.error("Error enqueuing clean Cap'n Web event", event, error);
        this.#removeCapnwebSubscriber(subscriber);
      }
    }

    this.#fanoutAttempts.rawws += this.#rawwsSubscribers.size;
    const rawMessage = JSON.stringify({ op: "event", event });
    for (const webSocket of this.#rawwsSubscribers) {
      try {
        webSocket.send(rawMessage);
      } catch (error) {
        console.error("Error sending clean rawws event", event, error);
        this.#rawwsSubscribers.delete(webSocket);
      }
    }

    this.#fanoutAttempts.orpc += this.ctx.getWebSockets().length;
    this.publishEvent(event);
  }

  #fetchCapnweb(): Response {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    newWebSocketRpcSession(server, new CleanStreamCapnwebTarget(this));
    return new Response(null, { status: 101, webSocket: client });
  }

  #fetchRawws(): Response {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    server.addEventListener("message", (message) => {
      this.#handleRawwsMessage(server, parseFrame(message.data));
    });
    server.addEventListener("close", () => {
      this.#rawwsSubscribers.delete(server);
    });
    server.addEventListener("error", () => {
      this.#rawwsSubscribers.delete(server);
    });

    return new Response(null, { status: 101, webSocket: client });
  }

  #handleRawwsMessage(webSocket: WebSocket, frame: unknown): void {
    if (!isRecord(frame) || typeof frame.op !== "string") {
      throw new Error("rawws frame must be an object with op");
    }

    if (frame.op === "subscribe") {
      this.#rawwsSubscribers.add(webSocket);
      webSocket.send(JSON.stringify({ op: "subscribed" }));
      return;
    }

    if (frame.op !== "append") {
      throw new Error("rawws op must be subscribe or append");
    }
    if (typeof frame.requestId !== "string") {
      throw new Error("rawws append requires string requestId");
    }

    const event = this.#appendFromTransport("rawws", { event: frame.event });
    webSocket.send(JSON.stringify({ op: "ack", requestId: frame.requestId, event }));
  }

  #removeCapnwebSubscriber(subscriber: CapnwebSubscriber): void {
    this.#capnwebSubscribers.delete(subscriber);
    subscriber.sessionSubscribers?.delete(subscriber);
  }
}

class StreamApp {
  #offset = 0;

  get offset() {
    return this.#offset;
  }

  append(input: StreamEventInput): StreamEvent {
    this.#offset += 1;
    return {
      ...input,
      offset: this.#offset,
      createdAt: new Date().toISOString(),
    };
  }
}

class CleanStreamCapnwebTarget extends RpcTarget {
  #stream: CleanStream;
  #subscribers = new Set<CapnwebSubscriber>();

  constructor(stream: CleanStream) {
    super();
    this.#stream = stream;
  }

  append(args: { event: StreamEventInput }): StreamEvent {
    return this.#stream.append(args);
  }

  subscribe(args?: unknown): ReadableStream<StreamEvent> {
    if (args !== undefined) throw new Error("subscribe does not accept arguments");
    return this.#stream.subscribeForSession(this.#subscribers);
  }

  debug() {
    return this.#stream.debug();
  }

  [Symbol.dispose](): void {
    this.#stream.releaseSessionSubscribers(this.#subscribers);
  }
}

function readTransport(url: URL): Transport | null {
  const raw = url.searchParams.get("transport");
  if (raw === "capnweb" || raw === "orpc" || raw === "rawws") return raw;
  return null;
}

function parseFrame(data: unknown): unknown {
  if (typeof data === "string") return JSON.parse(data);
  if (data instanceof ArrayBuffer) return JSON.parse(new TextDecoder().decode(data));
  if (ArrayBuffer.isView(data)) return JSON.parse(new TextDecoder().decode(data));
  throw new TypeError(`unexpected WebSocket frame data: ${String(data)}`);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
