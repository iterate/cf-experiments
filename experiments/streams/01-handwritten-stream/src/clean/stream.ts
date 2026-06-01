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
import {
  CLEAN_STREAM_ORPC_SIGNING_KEY,
  type CleanStreamEventSink,
  type CleanStreamRpc,
} from "./protocol.js";

const APPEND_EVENT_INPUT_SCHEMA = StreamEventInputSchema.strict();

type Transport = "capnweb" | "capnweb-oneway" | "orpc" | "rawws";

type CapnwebSubscriber = {
  controller: ReadableStreamDefaultController<StreamEvent>;
  sessionSubscribers?: Set<CapnwebSubscriber>;
};

type CapnwebOneWaySubscriber = {
  sink: CleanStreamEventSink;
  disposeSink: () => void;
  sessionSubscribers?: Set<CapnwebOneWaySubscriber>;
};

export class CleanStream extends DurableIteratorObject<StreamEvent, Env> {
  #app = new StreamApp();
  #capnwebSubscribers = new Set<CapnwebSubscriber>();
  #capnwebOneWaySubscribers = new Set<CapnwebOneWaySubscriber>();
  #rawwsSubscribers = new Set<WebSocket>();
  #fanoutAttempts = {
    capnweb: 0,
    capnwebOneWay: 0,
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
      return new Response("transport must be capnweb, capnweb-oneway, orpc, or rawws", {
        status: 400,
      });
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

    if (transport === "capnweb" || transport === "capnweb-oneway") return this.#fetchCapnweb();
    if (transport === "rawws") return this.#fetchRawws();
    return super.fetch(request);
  }

  debug() {
    return {
      kind: "clean-stream",
      offset: this.#app.offset,
      subscribers: {
        capnweb: this.#capnwebSubscribers.size,
        capnwebOneWay: this.#capnwebOneWaySubscribers.size,
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

  /**
   * Cap'n Web exposes the same clean app contract as the other transports, but
   * capnweb@0.8.0 returned ReadableStreams are encoded as pipe writes:
   *
   *   in  ["stream",["pipeline",1,["write"],[event]]]
   *   out ["resolve",2,["undefined"]]
   *
   * That outbound frame is write completion, not an app-level subscriber ack.
   * It is still per-chunk return traffic, which is why this clean surface keeps
   * `transport=rawws` as the baseline for one-way fan-out after subscribe.
   */
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

  subscribeOneWayForSession(
    sink: CleanStreamEventSink,
    sessionSubscribers: Set<CapnwebOneWaySubscriber>,
  ): void {
    const retainedSink = retainEventSink(sink);
    const subscriber = {
      sink: retainedSink.sink,
      disposeSink: retainedSink.dispose,
      sessionSubscribers,
    };
    this.#capnwebOneWaySubscribers.add(subscriber);
    sessionSubscribers.add(subscriber);
  }

  releaseOneWaySessionSubscribers(sessionSubscribers: Set<CapnwebOneWaySubscriber>): void {
    for (const subscriber of sessionSubscribers) {
      this.#removeCapnwebOneWaySubscriber(subscriber);
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

    this.#fanoutAttempts.capnwebOneWay += this.#capnwebOneWaySubscribers.size;
    for (const subscriber of this.#capnwebOneWaySubscribers) {
      try {
        const result = subscriber.sink.event(event);
        /**
         * This is the one-way Cap'n Web probe. A normal Cap'n Web method call
         * sends `push`; the caller only asks for a result if it sends `pull`.
         * We deliberately never await the returned RpcPromise, then dispose it
         * so the client can release the ignored result without sending a
         * subscriber-originated `resolve`.
         */
        if (isDisposable(result)) result[Symbol.dispose]();
      } catch (error) {
        console.error("Error sending clean Cap'n Web one-way event", event, error);
        this.#removeCapnwebOneWaySubscriber(subscriber);
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

  #removeCapnwebOneWaySubscriber(subscriber: CapnwebOneWaySubscriber): void {
    this.#capnwebOneWaySubscribers.delete(subscriber);
    subscriber.sessionSubscribers?.delete(subscriber);
    subscriber.disposeSink();
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
  #oneWaySubscribers = new Set<CapnwebOneWaySubscriber>();

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

  subscribeOneWay(sink: CleanStreamEventSink, args?: unknown): void {
    if (args !== undefined) throw new Error("subscribeOneWay does not accept arguments");
    this.#stream.subscribeOneWayForSession(sink, this.#oneWaySubscribers);
  }

  debug() {
    return this.#stream.debug();
  }

  [Symbol.dispose](): void {
    this.#stream.releaseSessionSubscribers(this.#subscribers);
    this.#stream.releaseOneWaySessionSubscribers(this.#oneWaySubscribers);
  }
}

function readTransport(url: URL): Transport | null {
  const raw = url.searchParams.get("transport");
  if (raw === "capnweb" || raw === "capnweb-oneway" || raw === "orpc" || raw === "rawws") {
    return raw;
  }
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

function isDisposable(value: unknown): value is Disposable {
  return (
    isObjectLike(value) && Symbol.dispose in value && typeof value[Symbol.dispose] === "function"
  );
}

function retainEventSink(sink: CleanStreamEventSink): {
  sink: CleanStreamEventSink;
  dispose: () => void;
} {
  if (!isDuplicableEventSink(sink)) throw new Error("one-way event sink must be duplicable");
  const retained = sink.dup();
  return {
    sink: retained,
    dispose: () => retained[Symbol.dispose](),
  };
}

function isDuplicableEventSink(value: CleanStreamEventSink): value is CleanStreamEventSink & {
  dup(): CleanStreamEventSink & Disposable;
} {
  return isObjectLike(value) && "dup" in value && typeof value.dup === "function";
}

function isObjectLike(value: unknown): value is object {
  return (typeof value === "object" && value !== null) || typeof value === "function";
}
