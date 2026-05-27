import { DurableObject } from "cloudflare:workers";
import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import { JonasStreamInboundFrame } from "./jonas-stream-types.js";

const MAX_UNCONFIRMED_WRITES = 100;

/**
 * Uses async `ctx.storage.put()` rather than sync `ctx.storage.kv.put()` so appends can opt into
 * `allowUnconfirmed`: subscribers can see accepted events immediately, while `storage.sync()`
 * checkpoints periodically bound the unconfirmed write window. Both APIs store KV data in the
 * SQLite-backed `__cf_kv` table; the async API is the one with the output-gate escape hatch.
 */
export class JonasStream extends DurableObject {
  #outboundWebSockets = new Set<{ metadata: { isSubscribed: boolean }; webSocket: WebSocket }>();
  #unconfirmedWrites = 0;
  #maxOffset: number | undefined;

  get websocketConnections() {
    return [
      ...this.ctx.getWebSockets().map((webSocket) => {
        const metadata = webSocket.deserializeAttachment();
        if (metadata === null) throw new Error("missing inbound WebSocket attachment");
        return {
          metadata,
          webSocket,
        };
      }),
      ...this.#outboundWebSockets,
    ];
  }

  async fetch(request: Request) {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("WebSocket only", { status: 400 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({
      isSubscribed: false,
    });
    return new Response(null, { status: 101, webSocket: client });
  }

  async connectOutboundWebSocket(url: string) {
    const response = await fetch(url, { headers: { Upgrade: "websocket" } });
    const webSocket = response.webSocket;
    if (webSocket === null) throw new Error("expected outbound websocket");

    webSocket.accept();

    const outboundWebSocket = {
      metadata: {
        isSubscribed: false,
      },
      webSocket,
    };
    this.#outboundWebSockets.add(outboundWebSocket);

    webSocket.addEventListener("message", (message) => {
      this.#handleWebSocketMessage(webSocket, outboundWebSocket.metadata, message.data);
    });
    webSocket.addEventListener("close", () => this.#outboundWebSockets.delete(outboundWebSocket));
    webSocket.addEventListener("error", () => this.#outboundWebSockets.delete(outboundWebSocket));
  }

  webSocketMessage(webSocket: WebSocket, message: string | ArrayBuffer) {
    const metadata = webSocket.deserializeAttachment();
    if (metadata === null) throw new Error("missing inbound WebSocket attachment");
    if (this.#handleWebSocketMessage(webSocket, metadata, message)) {
      webSocket.serializeAttachment(metadata);
    }
  }

  #handleWebSocketMessage(
    webSocket: WebSocket,
    metadata: { isSubscribed: boolean },
    data: string | ArrayBuffer,
  ) {
    const text = typeof data === "string" ? data : new TextDecoder().decode(data);
    const frame = JonasStreamInboundFrame.parse(JSON.parse(text));
    if (frame.op === "start") {
      // `start` is the live-subscribe handshake. Inbound hibernatable WebSockets
      // cannot be tracked in an in-memory subscriber set, so this flag replaces
      // Set membership and survives hibernation via the socket attachment.
      metadata.isSubscribed = true;
      return true;
    }

    const result = this.#append(frame.event);
    if (result.appended) this.#broadcast(result.event);
    if (frame.requestAck !== undefined) {
      webSocket.send(
        JSON.stringify({
          op: "append-ack",
          appendKey: frame.requestAck.key,
          event: result.event,
        }),
      );
    }
    return false;
  }

  #append(input: StreamEventInput) {
    if (input.idempotencyKey !== undefined) {
      const existingOffset = this.ctx.storage.kv.get<number>(`idempotency:${input.idempotencyKey}`);
      if (existingOffset !== undefined) {
        const existing = this.ctx.storage.kv.get<StreamEvent>(`event:${existingOffset}`);
        if (existing !== undefined) return { event: existing, appended: false };
        throw new Error(`idempotency index points at missing event ${existingOffset}`);
      }
    }

    const offset = this.#readMaxOffset() + 1;
    if (input.offset !== undefined && input.offset !== offset) {
      throw new Error(`expected offset ${offset}, got ${input.offset}`);
    }

    const event = { ...input, offset, createdAt: new Date().toISOString() };
    const writes = {
      [`event:${event.offset}`]: event,
      maxOffset: event.offset,
    };
    if (input.idempotencyKey !== undefined) {
      writes[`idempotency:${input.idempotencyKey}`] = event.offset;
    }
    this.#maxOffset = offset;
    void this.ctx.storage.put(writes, { allowUnconfirmed: true, noCache: true });
    this.#checkpointIfNeeded();
    return { event, appended: true };
  }

  #broadcast(event: StreamEvent) {
    const message = JSON.stringify({ op: "event", event });
    for (const connection of this.websocketConnections) {
      if (!connection.metadata.isSubscribed) continue;
      try {
        connection.webSocket.send(message);
      } catch {
        connection.webSocket.close();
      }
    }
  }

  #checkpointIfNeeded() {
    this.#unconfirmedWrites += 1;
    if (this.#unconfirmedWrites >= MAX_UNCONFIRMED_WRITES) {
      void this.ctx.storage.sync();
      this.#unconfirmedWrites = 0;
    }
  }

  #readMaxOffset() {
    this.#maxOffset ??= this.ctx.storage.kv.get<number>("maxOffset") ?? 0;
    return this.#maxOffset;
  }
}
