import { newWebSocketRpcSession } from "capnweb";
import { DurableObject } from "cloudflare:workers";
import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import { makeRpcTargetClass } from "@cf-experiments/shared/rpc-target";
import { JonasStreamInboundFrame } from "./jonas-stream-types.js";

/**
 * Always writes with `allowUnconfirmed: true` so storage writes do not put unrelated Durable Object
 * egress behind the platform output gate. `append()` rebuilds the caller-facing durability contract
 * explicitly: wait for `storage.sync()`, then broadcast and return the committed event.
 */
export class JonasStream extends DurableObject {
  #incarnationId = crypto.randomUUID();
  #outboundWebSockets = new Set<{ metadata: { isSubscribed: boolean }; webSocket: WebSocket }>();
  #maxOffset: number | undefined;
  #simulatedStorageSyncDelayMs: number | null = null;

  get websocketConnections() {
    return [
      ...this.#outboundWebSockets,
      ...this.ctx.getWebSockets().map((webSocket) => {
        const metadata = webSocket.deserializeAttachment();
        if (metadata === null) throw new Error("missing inbound WebSocket attachment");
        return {
          metadata,
          webSocket,
        };
      }),
    ];
  }

  async fetch(request: Request) {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("WebSocket only", { status: 400 });
    }

    const transport = new URL(request.url).searchParams.get("transport") ?? "raw-ws";
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    if (transport === "raw-ws") {
      this.ctx.acceptWebSocket(server);
      server.serializeAttachment({
        isSubscribed: false,
      });
      return new Response(null, { status: 101, webSocket: client });
    }

    if (transport !== "capnweb") {
      return new Response("transport must be raw-ws or capnweb", { status: 400 });
    }

    server.accept();
    newWebSocketRpcSession<JonasStreamRpc>(server, new JonasStreamRpcTarget(this));
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
      void this.#handleWebSocketMessage(webSocket, outboundWebSocket.metadata, message.data).catch(
        (error) => {
          console.error("JonasStream outbound WebSocket message failed", error);
          webSocket.close();
        },
      );
    });
    webSocket.addEventListener("close", () => this.#outboundWebSockets.delete(outboundWebSocket));
    webSocket.addEventListener("error", () => this.#outboundWebSockets.delete(outboundWebSocket));
  }

  // Called by cloudflare workers for any inbound messages to hibernatable websockets.
  async webSocketMessage(webSocket: WebSocket, message: string | ArrayBuffer) {
    const metadata = webSocket.deserializeAttachment();
    if (metadata === null) throw new Error("missing inbound WebSocket attachment");
    if (await this.#handleWebSocketMessage(webSocket, metadata, message)) {
      webSocket.serializeAttachment(metadata);
    }
  }

  async append(args: { event: StreamEventInput }): Promise<StreamEvent> {
    const result = this.#writeAppend(args.event);
    if (this.#simulatedStorageSyncDelayMs !== null) {
      await new Promise((resolve) => setTimeout(resolve, this.#simulatedStorageSyncDelayMs ?? 0));
    }
    await this.ctx.storage.sync();
    if (result.appended) this.#broadcast(result.event);
    return result.event;
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

  // Application logic for our websocket protocol
  async #handleWebSocketMessage(
    webSocket: WebSocket,
    metadata: { isSubscribed: boolean },
    data: string | ArrayBuffer,
  ): Promise<boolean> {
    const text = typeof data === "string" ? data : new TextDecoder().decode(data);
    const frame = JonasStreamInboundFrame.parse(JSON.parse(text));
    if (frame.op === "start") {
      // `start` is the live-subscribe handshake. Inbound hibernatable WebSockets
      // cannot be tracked in an in-memory subscriber set, so this flag replaces
      // Set membership and survives hibernation via the socket attachment.
      metadata.isSubscribed = true;
      return true;
    }

    const event = await this.append({ event: frame.event });
    if (frame.requestAck !== undefined) {
      webSocket.send(
        JSON.stringify({
          op: "append-ack",
          appendKey: frame.requestAck.key,
          event,
        }),
      );
    }
    return false;
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

  #readMaxOffset() {
    this.#maxOffset ??= this.ctx.storage.kv.get<number>("maxOffset") ?? 0;
    return this.#maxOffset;
  }
}

export type JonasStreamRpc = Pick<
  JonasStream,
  "append" | "simulateStorageSyncDelay" | "kill" | "ping" | "connectOutboundWebSocket"
>;

export const JonasStreamRpcTarget = makeRpcTargetClass<JonasStreamRpc, JonasStream>(JonasStream, {
  exclude: ["fetch", "webSocketMessage", "websocketConnections"],
});
