import {
  newHttpBatchRpcResponse,
  newWebSocketRpcSession,
  RpcTarget as CapnwebRpcTarget,
} from "capnweb";
import { DurableObject, RpcTarget as CfRpcTarget } from "cloudflare:workers";
import {
  countStreamEvents,
  initStreamEventsTable,
  type StreamEventInput,
  writeEvent,
} from "@cf-experiments/shared/event";

class SuperSimpleStreamCfRpcTarget extends CfRpcTarget {
  constructor(private readonly stream: SuperSimpleStream) {
    super();
  }

  append(event: StreamEventInput) {
    return this.stream.append(event);
  }

  appendBatch(events: StreamEventInput[]) {
    return this.stream.appendBatch(events);
  }

  count() {
    return this.stream.count();
  }
}

class SuperSimpleStreamCapnwebRpcTarget extends CapnwebRpcTarget {
  constructor(private readonly stream: SuperSimpleStream) {
    super();
  }

  append(event: StreamEventInput) {
    return this.stream.append(event);
  }

  appendBatch(events: StreamEventInput[]) {
    return this.stream.appendBatch(events);
  }

  count() {
    return this.stream.count();
  }
}

export class SuperSimpleStream extends DurableObject {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    initStreamEventsTable({ sql: this.ctx.storage.sql });
  }

  append(event: StreamEventInput) {
    return writeEvent({ sql: this.ctx.storage.sql, input: event });
  }

  appendBatch(events: StreamEventInput[]) {
    return events.map((event) => this.append(event));
  }

  count() {
    return { sqlite: countStreamEvents({ sql: this.ctx.storage.sql }) };
  }

  getCfRpcTarget() {
    return new SuperSimpleStreamCfRpcTarget(this);
  }

  getCapability(_policy?: unknown) {
    // could in future construct RpcTarget with narrowed capabilities
    return this;
  }

  getCapnwebRpcTarget() {
    return new SuperSimpleStreamCapnwebRpcTarget(this);
  }

  async fetch(request: Request) {
    const url = new URL(request.url);
    if (url.pathname === "/_capnweb" || url.pathname === "/_capnweb-cf-target") {
      const main =
        url.pathname === "/_capnweb-cf-target" ? this.getCfRpcTarget() : this.getCapnwebRpcTarget();
      if (request.method === "POST") {
        const response = await newHttpBatchRpcResponse(request, main);
        response.headers.set("Access-Control-Allow-Origin", "*");
        return response;
      }
      if (request.headers.get("Upgrade")?.toLowerCase() === "websocket") {
        const pair = new WebSocketPair();
        const [client, server] = Object.values(pair);
        server.accept();
        newWebSocketRpcSession(server, main);
        return new Response(null, { status: 101, webSocket: client });
      }
      return new Response("This endpoint only accepts POST or WebSocket requests.", {
        status: 400,
      });
    }

    if (url.searchParams.has("count")) return Response.json(this.count());

    if (request.headers.get("Upgrade")?.toLowerCase() === "websocket") {
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      this.ctx.acceptWebSocket(server);
      return new Response(null, { status: 101, webSocket: client });
    }

    return new Response("Expected WebSocket upgrade or ?count", { status: 426 });
  }

  webSocketMessage(_ws: WebSocket, message: string | ArrayBuffer) {
    try {
      const raw = typeof message === "string" ? message : new TextDecoder().decode(message);
      const parsed = JSON.parse(raw);
      if (parsed?.op === "append" && typeof parsed.event?.type === "string") {
        this.append(parsed.event);
      }
      if (parsed?.op === "appendBatch" && Array.isArray(parsed.events)) {
        this.appendBatch(parsed.events);
      }
    } catch {
      // fire-and-forget clients do not read error frames
    }
  }
}
