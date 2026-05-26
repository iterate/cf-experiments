import { DurableObject } from "cloudflare:workers";
import {
  countStreamEvents,
  initStreamEventsTable,
  type StreamEventInput,
  writeEvent,
} from "@cf-experiments/shared/event";

/** What we measure — isolates schema / indexing / per-write query overhead. */
export type WriteVariant =
  | "shared" // @cf-experiments/shared writeEvent: max(offset), json_valid, multi-column + idempotency index
  | "autoinc" // single raw_event column, sqlite autoincrement — no max(offset) per write
  | "blob" // single opaque text column, no json_valid
  | "tiny"; // minimal fixed string per row

export type WriteLoopResult = {
  mode: "in-do-loop";
  variant: WriteVariant;
  messages: number;
  payloadBytes: number;
  committed: number;
  serverCount: number;
  bytesWritten: number;
};

const VARIANT_SCHEMA: Record<WriteVariant, string> = {
  shared: "", // initStreamEventsTable
  autoinc: `
    create table if not exists bench_autoinc (
      offset integer primary key autoincrement,
      raw_event text not null check (json_valid(raw_event))
    )
  `,
  blob: `
    create table if not exists bench_blob (
      id integer primary key autoincrement,
      data text not null
    )
  `,
  tiny: `
    create table if not exists bench_tiny (
      id integer primary key autoincrement,
      v text not null
    )
  `,
};

/** Minimal SQLite append sink — measures in-DO write ceiling. */
export class WriteSink extends DurableObject {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    initStreamEventsTable({ sql: this.ctx.storage.sql });
  }

  writeLoop(args: { messages?: number; payloadBytes?: number; variant?: WriteVariant }) {
    const messages = args.messages ?? 10_000;
    const payloadBytes = args.payloadBytes ?? 256;
    const variant = args.variant ?? "shared";
    initVariantTable(this.ctx.storage.sql, variant);

    for (let n = 1; n <= messages; n++) {
      writeOne(this.ctx.storage.sql, variant, n, payloadBytes);
    }

    const serverCount = countVariant(this.ctx.storage.sql, variant);
    return {
      mode: "in-do-loop",
      variant,
      messages,
      payloadBytes,
      committed: messages,
      serverCount,
      bytesWritten: estimateBytesWritten({ variant, messages, payloadBytes }),
    } satisfies WriteLoopResult;
  }

  append(event: StreamEventInput) {
    return writeEvent({ sql: this.ctx.storage.sql, input: event });
  }

  count() {
    return { sqlite: countStreamEvents({ sql: this.ctx.storage.sql }) };
  }

  async fetch(request: Request) {
    const url = new URL(request.url);
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
    } catch {
      // fire-and-forget
    }
  }
}

function initVariantTable(sql: DurableObjectStorage["sql"], variant: WriteVariant) {
  if (variant === "shared") {
    initStreamEventsTable({ sql });
    return;
  }
  sql.exec(VARIANT_SCHEMA[variant]);
}

function writeOne(
  sql: DurableObjectStorage["sql"],
  variant: WriteVariant,
  n: number,
  payloadBytes: number,
) {
  switch (variant) {
    case "shared":
      writeEvent({ sql, input: buildSharedEvent(n, payloadBytes) });
      return;
    case "autoinc":
      sql.exec("insert into bench_autoinc (raw_event) values (?)", buildRawEventJson(n, payloadBytes));
      return;
    case "blob":
      sql.exec("insert into bench_blob (data) values (?)", buildBlob(n, payloadBytes));
      return;
    case "tiny":
      sql.exec("insert into bench_tiny (v) values (?)", "x");
      return;
  }
}

function countVariant(sql: DurableObjectStorage["sql"], variant: WriteVariant) {
  switch (variant) {
    case "shared":
      return countStreamEvents({ sql });
    case "autoinc":
      return sql.exec<{ c: number }>("select count(*) as c from bench_autoinc").one().c;
    case "blob":
      return sql.exec<{ c: number }>("select count(*) as c from bench_blob").one().c;
    case "tiny":
      return sql.exec<{ c: number }>("select count(*) as c from bench_tiny").one().c;
  }
}

function buildSharedEvent(n: number, payloadBytes: number): StreamEventInput {
  if (payloadBytes <= 0) return { type: "bench", payload: { n } };
  return { type: "bench", payload: { n, pad: "x".repeat(payloadBytes) } };
}

function buildRawEventJson(n: number, payloadBytes: number) {
  const payload =
    payloadBytes <= 0 ? { n } : { n, pad: "x".repeat(payloadBytes) };
  return JSON.stringify({
    type: "bench",
    payload,
    offset: n,
    createdAt: new Date().toISOString(),
  });
}

function buildBlob(n: number, payloadBytes: number) {
  if (payloadBytes <= 0) return String(n);
  return `${n}:${"x".repeat(payloadBytes)}`;
}

function estimateBytesWritten(args: {
  variant: WriteVariant;
  messages: number;
  payloadBytes: number;
}) {
  switch (args.variant) {
    case "tiny":
      return args.messages;
    case "blob":
      return args.messages * Math.max(1 + args.payloadBytes, 1);
    default:
      return args.messages * (80 + Math.max(args.payloadBytes, 0));
  }
}

function parseWriteVariant(raw: string | null): WriteVariant {
  if (raw === "autoinc" || raw === "blob" || raw === "tiny" || raw === "shared") return raw;
  return "shared";
}

export { parseWriteVariant };
