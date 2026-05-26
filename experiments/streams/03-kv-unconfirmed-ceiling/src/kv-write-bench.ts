import { DurableObject } from "cloudflare:workers";
import {
  countStreamEvents,
  countStreamEventsFromKv,
  initStreamEventsTable,
  type StreamEventInput,
  writeEvent,
  writeEventFromKv,
} from "@cf-experiments/shared/event";

export type WriteMode = "sql" | "kv-gated" | "kv-unconfirmed";

export type WriteLoopResult = {
  mode: WriteMode;
  messages: number;
  payloadBytes: number;
  committed: number;
  metaCount: number;
  loopMs: number;
  syncMs: number;
  flushCount: number;
  synced: boolean;
  verified: boolean;
};

export type FlushResult = {
  syncMs: number;
  metaCount: number;
};

export type AppendResult = WriteLoopResult & { startOffset: number };

/** Compare SQL vs KV gated vs KV allowUnconfirmed; optional manual flush control. */
export class KvWriteBench extends DurableObject {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    initStreamEventsTable({ sql: this.ctx.storage.sql });
  }

  ping() {
    return { ok: true as const, metaCount: readMetaCount(this.ctx.storage) };
  }

  count(args?: { mode?: WriteMode }) {
    const mode = args?.mode ?? "kv-unconfirmed";
    return { metaCount: countForMode(this.ctx.storage, mode), mode };
  }

  /** Append without syncing unless `sync`, `flushEvery`, or a later `/flush` call. */
  async writeLoop(args: {
    messages?: number;
    payloadBytes?: number;
    mode?: WriteMode;
    sync?: boolean;
    flushEvery?: number;
  }) {
    return this.appendBatch({
      messages: args.messages ?? 10_000,
      payloadBytes: args.payloadBytes ?? 4800,
      mode: args.mode ?? "kv-unconfirmed",
      sync: args.sync ?? false,
      flushEvery: args.flushEvery ?? 0,
      startOffset: readMetaCount(this.ctx.storage) + 1,
    });
  }

  async appendBatch(args: {
    messages: number;
    payloadBytes: number;
    mode: WriteMode;
    sync?: boolean;
    flushEvery?: number;
    startOffset?: number;
  }) {
    const startOffset = args.startOffset ?? readMetaCount(this.ctx.storage) + 1;
    const pad = args.payloadBytes > 0 ? "x".repeat(args.payloadBytes) : undefined;
    let flushCount = 0;
    let syncMs = 0;

    const loopStartedAt = performance.now();
    for (let i = 0; i < args.messages; i++) {
      const n = startOffset + i;
      const input: StreamEventInput =
        pad === undefined ? { type: "bench", payload: { n } } : { type: "bench", payload: { n, pad } };
      writeOne({ storage: this.ctx.storage, mode: args.mode, input });

      if (args.flushEvery !== undefined && args.flushEvery > 0 && (i + 1) % args.flushEvery === 0) {
        syncMs += await flushStorage(this.ctx.storage);
        flushCount += 1;
      }
    }
    const loopMs = performance.now() - loopStartedAt;

    if (args.sync) {
      syncMs += await flushStorage(this.ctx.storage);
      flushCount += 1;
    }

    const metaCount = countForMode(this.ctx.storage, args.mode);
    const expected = startOffset + args.messages - 1;

    return {
      mode: args.mode,
      messages: args.messages,
      payloadBytes: args.payloadBytes,
      committed: args.messages,
      metaCount,
      loopMs,
      syncMs,
      flushCount,
      synced: args.sync === true || flushCount > 0,
      verified: metaCount === expected,
      startOffset,
    } satisfies AppendResult;
  }

  /** Explicit durability boundary — caller controls when writes hit disk. */
  async flush() {
    const syncMs = await flushStorage(this.ctx.storage);
    const count = readMetaCount(this.ctx.storage);
    return { syncMs, metaCount: count } satisfies FlushResult;
  }

  /** One invocation: write until `maxMessages` or an in-DO exception (OOM may reset the object). */
  async writePressure(args: {
    maxMessages?: number;
    payloadBytes?: number;
    mode?: WriteMode;
    flushEvery?: number;
  }) {
    const maxMessages = args.maxMessages ?? 1_000_000;
    const payloadBytes = args.payloadBytes ?? 4800;
    const mode = args.mode ?? "kv-unconfirmed";
    const flushEvery = args.flushEvery ?? 0;
    const pad = payloadBytes > 0 ? "x".repeat(payloadBytes) : undefined;

    let written = 0;
    let flushCount = 0;
    let syncMs = 0;
    let error: string | undefined;

    const loopStartedAt = performance.now();
    try {
      for (let n = 1; n <= maxMessages; n++) {
        const input: StreamEventInput =
          pad === undefined ? { type: "bench", payload: { n } } : { type: "bench", payload: { n, pad } };
        writeOne({ storage: this.ctx.storage, mode, input });
        written = n;

        if (flushEvery > 0 && n % flushEvery === 0) {
          syncMs += await flushStorage(this.ctx.storage);
          flushCount += 1;
        }
      }
    } catch (cause) {
      error = cause instanceof Error ? cause.message : String(cause);
    }
    const loopMs = performance.now() - loopStartedAt;
    const meta = readMetaCount(this.ctx.storage);

    return {
      mode,
      maxMessages,
      payloadBytes,
      written,
      metaCount: meta,
      loopMs,
      syncMs,
      flushCount,
      synced: flushCount > 0,
      error,
      verified: error === undefined && meta === written,
    };
  }
}

function writeOne(args: {
  storage: DurableObjectStorage;
  mode: WriteMode;
  input: StreamEventInput;
}) {
  switch (args.mode) {
    case "sql":
      writeEvent({ sql: args.storage.sql, input: args.input });
      return;
    case "kv-gated":
      writeEventFromKv({ storage: args.storage, input: args.input, allowUnconfirmedWrites: false });
      return;
    case "kv-unconfirmed":
      writeEventFromKv({ storage: args.storage, input: args.input, allowUnconfirmedWrites: true });
      return;
  }
}

function countForMode(storage: DurableObjectStorage, mode: WriteMode) {
  if (mode === "sql") return countStreamEvents({ sql: storage.sql });
  return countStreamEventsFromKv({ kv: storage.kv });
}

function readMetaCount(storage: DurableObjectStorage) {
  return countStreamEventsFromKv({ kv: storage.kv });
}

async function flushStorage(storage: DurableObjectStorage) {
  const startedAt = performance.now();
  await storage.sync();
  return performance.now() - startedAt;
}

export function parseWriteMode(raw: string | null): WriteMode {
  if (raw === "kv-gated" || raw === "kv-unconfirmed" || raw === "sql") return raw;
  return "kv-unconfirmed";
}

export function parseBool(raw: string | null, fallback: boolean) {
  if (raw === "1" || raw === "true" || raw === "yes") return true;
  if (raw === "0" || raw === "false" || raw === "no") return false;
  return fallback;
}
