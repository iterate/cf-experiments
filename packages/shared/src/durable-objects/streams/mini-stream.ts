import { DurableObject } from "cloudflare:workers";
import type { StreamEventInput } from "../../event.js";

export type MiniStreamEnv = {
  METRICS: AnalyticsEngineDataset;
  ENV_NAME: string;
};

/**
 * Append sink with no SQL. Event log via `ctx.storage.put` only.
 *
 * `storage=kv` uses `{ allowUnconfirmed: true }` so RPC/WebSocket replies are not
 * held until disk confirmation.
 */
export class MiniStream extends DurableObject<MiniStreamEnv> {
  private memoryAppendCount = 0;
  private kvAppendCount = 0;

  constructor(ctx: DurableObjectState, env: MiniStreamEnv) {
    super(ctx, env);
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  get path(): string {
    const name = this.ctx.id.name;
    if (name === undefined) {
      throw new Error("MiniStream must be addressed by name.");
    }
    return name;
  }

  append(args: { event: StreamEventInput; storage?: "memory" | "kv" }): { offset: number } {
    if (typeof args.event.type !== "string") {
      throw new Error("event.type must be a string.");
    }

    const storage = args.storage ?? "kv";

    if (storage === "memory") {
      this.memoryAppendCount += 1;
      recordAppendMetric({ env: this.env, streamPath: this.path, storage: "memory" });
      return { offset: this.memoryAppendCount };
    }

    const offset = this.kvAppendCount + 1;
    this.kvAppendCount = offset;
    const key = eventKey(offset);
    const value = serializeStoredEvent({ offset, event: args.event });

    void this.ctx.storage.put(key, value, { allowUnconfirmed: true });

    recordAppendMetric({ env: this.env, streamPath: this.path, storage: "kv" });
    return { offset };
  }

  appendBatch(args: { events: StreamEventInput[]; storage?: "memory" | "kv" }): {
    offsets: number[];
  } {
    const offsets: number[] = [];
    for (const event of args.events) {
      offsets.push(this.append({ event, storage: args.storage }).offset);
    }
    return { offsets };
  }

  count(): { memory: number; kv: number } {
    return { memory: this.memoryAppendCount, kv: this.kvAppendCount };
  }

  async sync(): Promise<void> {
    await this.ctx.storage.sync();
  }

  kill(args?: { reason?: string }): never {
    this.ctx.abort(args?.reason ?? "kill requested");
    throw new Error("unreachable");
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (url.searchParams.has("count")) {
      if (url.searchParams.has("sync")) {
        await this.sync();
      }
      return Response.json(this.count());
    }

    if (request.headers.get("Upgrade")?.toLowerCase() === "websocket") {
      if (request.method !== "GET") {
        return new Response("WebSocket connections must use GET", { status: 400 });
      }

      const storage = parseStorageMode(url.searchParams.get("storage"));
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      this.ctx.acceptWebSocket(server);
      server.serializeAttachment({ storage });
      return new Response(null, { status: 101, webSocket: client });
    }

    return new Response("Expected WebSocket upgrade or ?count", { status: 426 });
  }

  webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): void {
    try {
      const attachment = ws.deserializeAttachment() as { storage?: unknown } | undefined;
      const storage = parseStorageMode(
        typeof attachment?.storage === "string" ? attachment.storage : null,
      );

      const raw = typeof message === "string" ? message : new TextDecoder().decode(message);
      const parsed = JSON.parse(raw) as {
        op?: unknown;
        event?: unknown;
        events?: unknown;
      };

      if (parsed.op === "append" && parsed.event && typeof parsed.event === "object") {
        this.append({ event: parsed.event as StreamEventInput, storage });
        return;
      }

      if (parsed.op === "appendBatch" && Array.isArray(parsed.events)) {
        for (const event of parsed.events) {
          if (
            !event ||
            typeof event !== "object" ||
            typeof (event as StreamEventInput).type !== "string"
          ) {
            continue;
          }
          this.append({ event: event as StreamEventInput, storage });
        }
      }
    } catch {
      // Fire-and-forget clients do not read error frames.
    }
  }

  webSocketClose(ws: WebSocket, code: number, reason: string): void {
    ws.close(code, reason);
  }
}

function eventKey(offset: number): string {
  return `event:${offset}`;
}

function serializeStoredEvent(args: { offset: number; event: StreamEventInput }): string {
  return JSON.stringify({
    offset: args.offset,
    type: args.event.type,
    payload: args.event.payload,
    metadata: args.event.metadata,
    source: args.event.source,
    idempotencyKey: args.event.idempotencyKey,
  });
}

function parseStorageMode(value: string | null): "memory" | "kv" {
  if (value === "memory") return "memory";
  return "kv";
}

function recordAppendMetric(args: {
  env: MiniStreamEnv;
  streamPath: string;
  storage: "memory" | "kv";
}) {
  args.env.METRICS.writeDataPoint({
    indexes: [args.streamPath],
    blobs: [args.streamPath, "append", args.env.ENV_NAME, args.storage],
    doubles: [],
  });
}
