import type { StreamEvent } from "@cf-experiments/shared/event";

export type SqlValue = string | number | bigint | Uint8Array | number[] | null;

export type StreamEventRow = {
  local_index: number;
  offset: number;
  type: string;
  idempotency_key: string | null;
  created_at: string;
  inserted_at: string;
  raw_json: string;
};

export type StreamDatabaseInfo = {
  databaseSizeBytes: number;
  storageType: "opfs";
  persisted: boolean;
  crossOriginIsolated: boolean;
};

export type SqliteQueryStatus = "pending" | "ok" | "error";

export type SqliteQuerySnapshot<T> = {
  data: T[];
  status: SqliteQueryStatus;
  error: Error | undefined;
};

export type SqliteQueryHandle = {
  getSnapshot(): SqliteQuerySnapshot<Record<string, SqlValue>>;
  subscribe(listener: () => void): () => void;
};

type StreamDbChange =
  | { kind: "append"; minOffset: number; maxOffset: number }
  | { kind: "clear" };

type RegisteredQuery = {
  sql: string;
  params: SqlValue[];
  snapshot: SqliteQuerySnapshot<Record<string, SqlValue>>;
  started: boolean;
  gcTimer: ReturnType<typeof setTimeout> | undefined;
  readonly listeners: Set<() => void>;
  readonly handle: SqliteQueryHandle;
};

const PENDING: SqliteQuerySnapshot<never> = { data: [], status: "pending", error: undefined };
const BROWSER_DB_SCHEMA_VERSION = 2;

export class StreamBrowserDatabase implements Disposable {
  readonly databasePath: string;
  readonly downloadFilename: string;
  readonly #worker: Worker;
  readonly #channel: BroadcastChannel;
  readonly #ready: Promise<void>;
  #nextRequestId = 1;
  #disposed = false;
  #infoRefresh: Promise<StreamDatabaseInfo> | undefined;
  readonly #pending = new Map<number, { resolve: (value: unknown) => void; reject: (error: Error) => void }>();
  readonly #queries = new Map<string, RegisteredQuery>();
  readonly #changeListeners = new Set<(change: StreamDbChange) => void>();

  constructor(readonly streamPath: string) {
    this.databasePath = databasePathForStreamPath(streamPath);
    this.downloadFilename = downloadFilenameForStreamPath(streamPath);
    this.#worker = new Worker(new URL("./stream-db.worker.ts", import.meta.url), {
      type: "module",
    });
    this.#worker.onmessage = (event: MessageEvent<{ id: number; ok: boolean; result?: unknown; error?: string }>) => {
      const { id, ok, result, error } = event.data;
      const pending = this.#pending.get(id);
      if (pending === undefined) return;
      this.#pending.delete(id);
      if (ok) pending.resolve(result);
      else pending.reject(new Error(error ?? "stream db worker error"));
    };
    this.#channel = new BroadcastChannel(`stream-db:${encodeURIComponent(streamPath)}`);
    this.#channel.onmessage = (event: MessageEvent<StreamDbChange>) => this.#onChange(event.data);
    this.#ready = this.#call("init", { databasePath: this.databasePath }).then(() => this.#initSchema());
  }

  #assertOpen() {
    if (this.#disposed) throw new Error("stream browser database is disposed");
  }

  #call(op: string, args: Record<string, unknown>): Promise<unknown> {
    this.#assertOpen();
    const id = this.#nextRequestId++;
    return new Promise((resolve, reject) => {
      this.#pending.set(id, { resolve, reject });
      this.#worker.postMessage({ id, op, ...args });
    });
  }

  async exec(sql: string, params: SqlValue[] = []): Promise<Record<string, SqlValue>[]> {
    await this.#ready;
    return await this.#execReady(sql, params);
  }

  async #execReady(sql: string, params: SqlValue[] = []): Promise<Record<string, SqlValue>[]> {
    const rows = await this.#call("exec", { sql, params });
    if (!Array.isArray(rows)) throw new Error("stream db worker returned non-array exec result");
    return rows.filter(isSqlRow);
  }

  async #initSchema(): Promise<void> {
    const [schemaVersion] = await this.#execReady(`PRAGMA user_version`);
    if (Number(schemaVersion?.user_version ?? 0) !== BROWSER_DB_SCHEMA_VERSION) {
      await this.#call("batch", {
        transaction: true,
        statements: [
          { sql: `DROP TRIGGER IF EXISTS events_before_insert` },
          { sql: `DROP TABLE IF EXISTS events` },
          { sql: `PRAGMA user_version = ${BROWSER_DB_SCHEMA_VERSION}` },
        ],
      });
    }

    await this.#call("batch", {
      transaction: true,
      statements: [
        {
          sql: `
            -- Browser-owned append log mirror. raw_jsonb is the source of truth:
            -- SQLite derives the queryable event fields from it, so future JSON-field
            -- indexes can use the same payload without duplicating text JSON.
            --
            -- local_index is deliberately separate from offset. Today it is offset - 1,
            -- because server offsets are one-based and TanStack Virtual indexes are
            -- zero-based. Keeping a separate local list position gives us room to age
            -- server events out later while still rendering a dense local list.
            CREATE TABLE IF NOT EXISTS events (
              local_index INTEGER PRIMARY KEY,
              raw_jsonb BLOB NOT NULL,
              offset INTEGER GENERATED ALWAYS AS (json_extract(raw_jsonb, '$.offset')) STORED NOT NULL UNIQUE,
              type TEXT GENERATED ALWAYS AS (json_extract(raw_jsonb, '$.type')) STORED NOT NULL,
              idempotency_key TEXT GENERATED ALWAYS AS (json_extract(raw_jsonb, '$.idempotencyKey')) STORED,
              created_at TEXT GENERATED ALWAYS AS (json_extract(raw_jsonb, '$.createdAt')) STORED NOT NULL,
              inserted_at TEXT NOT NULL DEFAULT (datetime('now')),
              CHECK (local_index = offset - 1)
            )
          `,
        },
        {
          sql: `
            -- This trigger is the browser mirror's append invariant:
            -- 1. Identical replay is accepted and ignored, preserving inserted_at as
            --    "first stored locally".
            -- 2. Same offset with different JSON is a conflicting duplicate.
            -- 3. New rows must append continuously, so a missed offset fails loudly.
            CREATE TRIGGER IF NOT EXISTS events_before_insert
            BEFORE INSERT ON events
            BEGIN
              SELECT CASE
                WHEN EXISTS (
                  SELECT 1
                  FROM events
                  WHERE offset = NEW.offset
                    AND json(raw_jsonb) = json(NEW.raw_jsonb)
                ) THEN RAISE(IGNORE)
                WHEN EXISTS (
                  SELECT 1
                  FROM events
                  WHERE offset = NEW.offset
                ) THEN RAISE(ABORT, 'stream browser mirror replay changed an existing offset')
                WHEN NEW.offset != COALESCE((SELECT MAX(offset) + 1 FROM events), 1)
                  THEN RAISE(ABORT, 'stream browser mirror offsets must append continuously')
              END;
            END
          `,
        },
      ],
    });
  }

  async maxOffset(): Promise<number> {
    const [row] = await this.exec(
      `SELECT MAX(offset) AS max_offset FROM events`,
    );
    return Number(row?.max_offset ?? -1);
  }

  async insertEventBatch(args: { events: StreamEvent[] }): Promise<void> {
    if (args.events.length === 0) return;
    const statements = args.events.map((event) => ({
      sql: `INSERT INTO events (local_index, raw_jsonb) VALUES (?, jsonb(?))`,
      params: [event.offset - 1, JSON.stringify(event)] satisfies SqlValue[],
    }));
    await this.#ready;
    await this.#call("batch", { statements, transaction: true });
    const offsets = args.events.map((event) => event.offset);
    this.#publishChange({
      kind: "append",
      minOffset: Math.min(...offsets),
      maxOffset: Math.max(...offsets),
    });
  }

  async info(): Promise<StreamDatabaseInfo> {
    this.#infoRefresh ??= (async () => {
      try {
        const persisted = (await navigator.storage?.persisted?.()) ?? false;
        const [size] = await this.exec(
          `SELECT page_count * page_size AS bytes
           FROM pragma_page_count(), pragma_page_size()`,
        );
        return {
          databaseSizeBytes: Number(size?.bytes ?? 0),
          storageType: "opfs",
          persisted,
          crossOriginIsolated: globalThis.crossOriginIsolated,
        };
      } finally {
        this.#infoRefresh = undefined;
      }
    })();
    return this.#infoRefresh;
  }

  async download() {
    await this.#ready;
    const buffer = await this.#call("export", {});
    if (!(buffer instanceof ArrayBuffer)) {
      throw new Error("stream db worker returned non-ArrayBuffer export result");
    }
    const url = URL.createObjectURL(new Blob([buffer], { type: "application/x-sqlite3" }));
    const link = document.createElement("a");
    link.href = url;
    link.download = this.downloadFilename;
    link.click();
    URL.revokeObjectURL(url);
  }

  async clear() {
    await this.exec(`DELETE FROM events`);
    this.#publishChange({ kind: "clear" });
  }

  async compact() {
    await this.exec(`VACUUM`);
  }

  query(
    sql: string,
    params: SqlValue[],
  ): SqliteQueryHandle {
    const key = `${sql}\0${JSON.stringify(params)}`;
    const existing = this.#queries.get(key);
    if (existing !== undefined) return existing.handle;

    const entry: RegisteredQuery = {
      sql,
      params,
      snapshot: PENDING,
      started: false,
      gcTimer: undefined,
      listeners: new Set(),
      handle: {
        getSnapshot: () => entry.snapshot,
        subscribe: (listener) => {
          entry.listeners.add(listener);
          if (entry.gcTimer !== undefined) {
            clearTimeout(entry.gcTimer);
            entry.gcTimer = undefined;
          }
          if (!entry.started) {
            entry.started = true;
            void this.#runQuery(entry);
          }
          return () => {
            entry.listeners.delete(listener);
            if (entry.listeners.size > 0) return;
            entry.gcTimer = setTimeout(() => {
              if (entry.listeners.size === 0) this.#queries.delete(key);
            }, 0);
          };
        },
      },
    };
    this.#queries.set(key, entry);
    return entry.handle;
  }

  onChange(listener: (change: StreamDbChange) => void) {
    this.#changeListeners.add(listener);
    return () => void this.#changeListeners.delete(listener);
  }

  async #runQuery(entry: RegisteredQuery): Promise<void> {
    try {
      const data = await this.exec(entry.sql, entry.params);
      entry.snapshot = { data, status: "ok", error: undefined };
    } catch (error) {
      entry.snapshot = {
        ...entry.snapshot,
        status: "error",
        error: error instanceof Error ? error : new Error(String(error)),
      };
    }
    for (const listener of entry.listeners) listener();
  }

  #publishChange(change: StreamDbChange) {
    this.#channel.postMessage(change);
    this.#onChange(change);
  }

  #onChange(change: StreamDbChange) {
    this.#infoRefresh = undefined;
    for (const entry of this.#queries.values()) void this.#runQuery(entry);
    for (const listener of this.#changeListeners) listener(change);
  }

  dispose() {
    if (this.#disposed) return;
    this.#disposed = true;
    for (const pending of this.#pending.values()) {
      pending.reject(new Error("stream browser database disposed"));
    }
    this.#pending.clear();
    this.#queries.clear();
    this.#changeListeners.clear();
    this.#channel.close();
    this.#worker.terminate();
  }

  [Symbol.dispose]() {
    this.dispose();
  }
}

function databasePathForStreamPath(streamPath: string) {
  return `${databaseSlugForStreamPath(streamPath)}.sqlite3`;
}

function downloadFilenameForStreamPath(streamPath: string) {
  return `${databaseSlugForStreamPath(streamPath)}.sqlite3`;
}

function databaseSlugForStreamPath(streamPath: string) {
  const segments = streamPath.split("/").filter(Boolean).map(encodeURIComponent);
  const hint = segments.at(-1) ?? "root";
  return `stream-${fnv1a32(streamPath).toString(16).padStart(8, "0")}-${hint.slice(0, 24)}`;
}

function fnv1a32(value: string) {
  let hash = 0x811c9dc5;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash;
}

function isSqlRow(value: unknown): value is Record<string, SqlValue> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  return Object.values(value).every(isSqlValue);
}

function isSqlValue(value: unknown): value is SqlValue {
  return (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "bigint" ||
    value instanceof Uint8Array ||
    (Array.isArray(value) && value.every((item) => typeof item === "number"))
  );
}
