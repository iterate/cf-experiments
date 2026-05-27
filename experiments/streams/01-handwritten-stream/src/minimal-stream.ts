import { DurableObject } from "cloudflare:workers";
import {
  StreamEventInput as StreamEventInputSchema,
  type StreamEvent,
} from "@cf-experiments/shared/event";

const APPEND_EVENT_INPUT_SCHEMA = StreamEventInputSchema.strict();

export class MinimalStream extends DurableObject {
  #offset = 0;
  #subscribers = new Set<WebSocket>();
  #fanoutAttempts = 0;

  async fetch(request: Request) {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("This endpoint only accepts WebSocket requests.", { status: 400 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();

    server.addEventListener("message", (message) => {
      this.#handleMessage(server, parseFrame(message.data));
    });
    server.addEventListener("close", () => {
      this.#subscribers.delete(server);
    });
    server.addEventListener("error", () => {
      this.#subscribers.delete(server);
    });

    return new Response(null, { status: 101, webSocket: client });
  }

  debug() {
    return {
      kind: "minimal-ws",
      offset: this.#offset,
      subscribers: this.#subscribers.size,
      fanoutAttempts: this.#fanoutAttempts,
    };
  }

  #handleMessage(webSocket: WebSocket, frame: unknown) {
    if (!isRecord(frame) || typeof frame.op !== "string") {
      throw new Error("minimal stream frame must be an object with op");
    }

    if (frame.op === "subscribe") {
      /**
       * This baseline is intentionally a tiny one-way subscriber protocol:
       * after this initial client-originated subscribe frame, a pure subscriber
       * should only receive `event` frames. There is no subscriber callback,
       * per-event ack, return value, or backchannel flow-control message for
       * the stream DO to wait on.
       */
      this.#subscribers.add(webSocket);
      webSocket.send(JSON.stringify({ op: "subscribed" }));
      return;
    }

    if (frame.op !== "append") {
      throw new Error("minimal stream op must be subscribe or append");
    }
    if (typeof frame.requestId !== "string") {
      throw new Error("minimal stream append requires string requestId");
    }
    const parsedEvent = APPEND_EVENT_INPUT_SCHEMA.safeParse(frame.event);
    if (!parsedEvent.success) {
      throw new Error("minimal stream append event must be a valid StreamEventInput");
    }

    this.#offset += 1;
    const event: StreamEvent = {
      ...parsedEvent.data,
      offset: this.#offset,
      createdAt: new Date().toISOString(),
    };
    this.#broadcast(event);
    webSocket.send(JSON.stringify({ op: "ack", requestId: frame.requestId, event }));
  }

  #broadcast(event: StreamEvent) {
    const message = JSON.stringify({ op: "event", event });
    this.#fanoutAttempts += this.#subscribers.size;
    for (const subscriber of this.#subscribers) {
      try {
        subscriber.send(message);
      } catch (error) {
        console.error("Error sending minimal stream event", event, error);
        this.#subscribers.delete(subscriber);
      }
    }
  }
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
