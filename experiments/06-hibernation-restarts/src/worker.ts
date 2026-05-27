import { DurableObject } from "cloudflare:workers";

const leaseMs = 1_000;

export class HibernationRestartProbe extends DurableObject<Env> {
  #incarnationId = crypto.randomUUID();
  #chunks: Uint8Array[] = [];
  #heldBytes = 0;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);

    // This is the whole experiment. Cloudflare can answer this message without
    // waking a hibernated Durable Object. We include a short lease so clients can
    // tell when the auto-response is stale and must be renewed by a real message.
    this.ctx.setWebSocketAutoResponse(
      new WebSocketRequestResponsePair(
        "ping",
        JSON.stringify({
          op: "auto-pong",
          incarnationId: this.#incarnationId,
          expiresAt: Date.now() + leaseMs,
        }),
      ),
    );
  }

  kill(): never {
    this.ctx.abort("kill requested");
    throw new Error("unreachable");
  }

  allocate(args: { bytes: number }) {
    while (this.#heldBytes < args.bytes) {
      const size = Math.min(1024 * 1024, args.bytes - this.#heldBytes);
      const chunk = new Uint8Array(size);
      chunk.fill(0xa5);
      this.#chunks.push(chunk);
      this.#heldBytes += size;
    }
    return { incarnationId: this.#incarnationId };
  }

  fetch(request: Request): Response {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 400 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    return new Response(null, { status: 101, webSocket: client });
  }

  webSocketMessage(webSocket: WebSocket, message: string | ArrayBuffer): void {
    const frame = JSON.parse(typeof message === "string" ? message : new TextDecoder().decode(message));
    if (frame.op !== "app-ping") throw new Error("expected app-ping");
    webSocket.send(JSON.stringify({ op: "app-pong", incarnationId: this.#incarnationId }));
  }

}

interface Env {
  PROBE: DurableObjectNamespace<HibernationRestartProbe>;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const stub = env.PROBE.getByName(url.searchParams.get("name") ?? "default");

    if (url.pathname === "/ws") return stub.fetch(request);
    if (url.pathname === "/kill" && request.method === "POST") return stub.kill();

    if (url.pathname === "/allocate" && request.method === "POST") {
      return Response.json(await stub.allocate({ bytes: Number(url.searchParams.get("bytes")) }));
    }

    return new Response("GET /ws, POST /kill, POST /allocate");
  },
} satisfies ExportedHandler<Env>;
