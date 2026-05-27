import { DurableObject } from "cloudflare:workers";

type ProbeMessage =
  | { op: "ping"; id: string }
  | {
      op: "pong";
      id: string;
      incarnationId: string;
      sockets: number;
    };

export class HibernationRestartProbe extends DurableObject {
  #incarnationId = crypto.randomUUID();
  #chunks: Uint8Array[] = [];

  ping() {
    return {
      incarnationId: this.#incarnationId,
      sockets: this.ctx.getWebSockets().length,
      heldBytes: this.#heldBytes(),
    };
  }

  allocate(args: { bytes: number }) {
    const chunkSize = 1024 * 1024;
    let allocated = 0;
    while (allocated < args.bytes) {
      const size = Math.min(chunkSize, args.bytes - allocated);
      const chunk = new Uint8Array(size);
      chunk.fill(0xa5);
      this.#chunks.push(chunk);
      allocated += size;
    }
    return this.ping();
  }

  kill(args?: { reason?: string }): never {
    this.ctx.abort(args?.reason ?? "kill requested");
    throw new Error("unreachable");
  }

  async fetch(request: Request): Promise<Response> {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("WebSocket only", { status: 400 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ connectedAt: Date.now() });
    return new Response(null, { status: 101, webSocket: client });
  }

  webSocketMessage(webSocket: WebSocket, message: string | ArrayBuffer): void {
    const text = typeof message === "string" ? message : new TextDecoder().decode(message);
    const frame = JSON.parse(text) as ProbeMessage;
    if (frame.op !== "ping") throw new Error("expected ping");
    webSocket.send(
      JSON.stringify({
        op: "pong",
        id: frame.id,
        incarnationId: this.#incarnationId,
        sockets: this.ctx.getWebSockets().length,
      } satisfies ProbeMessage),
    );
  }

  #heldBytes() {
    let heldBytes = 0;
    for (const chunk of this.#chunks) heldBytes += chunk.byteLength;
    return heldBytes;
  }
}

interface Env {
  PROBE: DurableObjectNamespace<HibernationRestartProbe>;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const name = url.searchParams.get("name") ?? "default";
    const stub = env.PROBE.getByName(name);

    if (url.pathname === "/ws") return stub.fetch(request);

    if (url.pathname === "/ping") return Response.json(await stub.ping());

    if (url.pathname === "/kill" && request.method === "POST") {
      await stub.kill({ reason: url.searchParams.get("reason") ?? undefined });
      return new Response("kill returned", { status: 500 });
    }

    if (url.pathname === "/allocate" && request.method === "POST") {
      const bytes = Number(url.searchParams.get("bytes"));
      if (!Number.isInteger(bytes) || bytes <= 0) {
        return new Response("bytes must be a positive integer", { status: 400 });
      }
      return Response.json(await stub.allocate({ bytes }));
    }

    return new Response(
      [
        "Hibernation restart probe",
        "",
        "GET /ping?name=...",
        "POST /kill?name=...",
        "POST /allocate?name=...&bytes=...",
        "GET /ws?name=... with Upgrade: websocket",
      ].join("\n"),
      { headers: { "content-type": "text/plain; charset=utf-8" } },
    );
  },
} satisfies ExportedHandler<Env>;

