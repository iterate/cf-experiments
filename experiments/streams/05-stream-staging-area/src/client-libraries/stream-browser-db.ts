import type { StreamEvent } from "@cf-experiments/shared/event";

// Per-tab SQLite mirror of an append-only stream, backed by wa-sqlite's OPFSCoopSyncVFS
// (one connection per tab in a dedicated worker — see stream-db.worker.ts). On top of it
// sits a tiny reactive-query layer that exploits the append-only workload: a query
// declares the offset range its result depends on, and a write announces the offset
// range it appended, so we re-run only the queries an append can actually change. A
// query over a fixed historical window (every row already below the append) is provably
// unaffected and never re-runs. Cross-tab freshness rides a BroadcastChannel: the one
// writer tab announces each committed batch and every tab re-runs its affected queries
// against its own local connection.

export type SqlValue = string | number | bigint | Uint8Array | null;

export type StreamEventRow = {
  virtual_index: number;
  offset: number;
  type: string;
  idempotency_key: string | null;
  created_at: string;
  raw_json: string;
};

export type StreamEventMeta = {
  event_count: number;
};

export type StreamDatabaseInfo = {
  databaseSizeBytes: number;
  storageType: "opfs";
  persisted: boolean;
  crossOriginIsolated: boolean;
};

export type StreamDatabaseWriteMode = "batch" | "row";

// What a reactive query's result depends on. `tail` = the head of the log (counts and
// the live window): re-runs on every append. `range` = a fixed window bounded above by
// `untilVirtualIndex`: append-only immutability means rows below it never change, so it
// re-runs only on a clear, never on append.
export type StreamQueryScope = { type: "tail" } | { type: "range"; untilVirtualIndex: number };

type StreamDbChange =
  | { kind: "append"; minOffset: number; maxOffset: number; eventCount: number }
  | { kind: "clear"; clearVersion: number };

export type ReactiveQueryStatus = "pending" | "ok" | "error";

export type ReactiveQuerySnapshot<T> = {
  data: T[];
  status: ReactiveQueryStatus;
  error: Error | undefined;
};

export type ReactiveQueryHandle<T> = {
  getSnapshot(): ReactiveQuerySnapshot<T>;
  subscribe(listener: () => void): () => void;
};

/** The row count — all TanStack Virtual needs — plus a load-status flag for the UI. */
export type EventCountSnapshot = {
  count: number;
  status: ReactiveQueryStatus;
  error: Error | undefined;
};

type RegisteredQuery = {
  sql: string;
  params: SqlValue[];
  scope: StreamQueryScope;
  snapshot: ReactiveQuerySnapshot<Record<string, SqlValue>>;
  started: boolean; // first subscribe kicks the initial run, so reactiveQuery() stays pure in render
  readonly listeners: Set<() => void>;
  // Bound once per entry so useSyncExternalStore sees stable identities (no resubscribe loop).
  readonly handle: ReactiveQueryHandle<Record<string, SqlValue>>;
};

const PENDING: ReactiveQuerySnapshot<never> = { data: [], status: "pending", error: undefined };

const streamDatabases = new Map<string, StreamBrowserDatabase>();

export function getStreamBrowserDatabase(streamPath: string) {
  const existing = streamDatabases.get(streamPath);
  if (existing !== undefined) return existing;
  const streamDatabase = new StreamBrowserDatabase(streamPath);
  streamDatabases.set(streamPath, streamDatabase);
  return streamDatabase;
}

export class StreamBrowserDatabase {
  readonly databasePath: string;
  readonly downloadFilename: string;
  readonly #worker: Worker;
  readonly #channel: BroadcastChannel;
  readonly #ready: Promise<void>;
  #nextRequestId = 1;
  readonly #pending = new Map<number, { resolve: (value: unknown) => void; reject: (error: Error) => void }>();
  readonly #queries = new Map<string, RegisteredQuery>();
  #infoRefresh: Promise<StreamDatabaseInfo> | undefined;

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

  #call(op: string, args: Record<string, unknown>): Promise<unknown> {
    const id = this.#nextRequestId++;
    return new Promise((resolve, reject) => {
      this.#pending.set(id, { resolve, reject });
      this.#worker.postMessage({ id, op, ...args });
    });
  }

  async #exec<T extends Record<string, SqlValue>>(sql: string, params: SqlValue[] = []): Promise<T[]> {
    await this.#ready;
    return (await this.#call("exec", { sql, params })) as T[];
  }

  async #initSchema(): Promise<void> {
    // CREATE IF NOT EXISTS is idempotent and safe to run from every tab's connection.
    // One transaction = ONE cooperative file-lock cycle for all DDL (six separate
    // autocommits otherwise cost six lock acquire/release round-trips on first open).
    await this.#call("batch", {
      transaction: true,
      statements: [
        {
          sql: `CREATE TABLE IF NOT EXISTS events (
            virtual_index INTEGER PRIMARY KEY,
            offset INTEGER NOT NULL,
            type TEXT NOT NULL,
            idempotency_key TEXT,
            created_at TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            UNIQUE (offset)
          )`,
        },
        {
          sql: `CREATE TABLE IF NOT EXISTS stream_meta (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            event_count INTEGER NOT NULL DEFAULT 0
          )`,
        },
        { sql: `INSERT INTO stream_meta (id, event_count) VALUES (1, 0) ON CONFLICT(id) DO NOTHING` },
        // Processor-owned durable snapshots, keyed by slug — the SAME shape as the Stream
        // DO's `processor_state` table (src/stream.ts) and the StreamProcessorRunner DO's
        // snapshot. Today the browser hosts one projector that writes `events`; this table
        // lets it host several *reducing* processors next, each persisting its reduced state
        // here under its own slug. The symmetry is deliberate: every processor runtime
        // (browser tab, Stream DO, runner DO) folds the same event log into the same SQLite
        // snapshot shape — these environments may eventually converge onto shared code.
        {
          sql: `CREATE TABLE IF NOT EXISTS processor_state (
            processor_slug TEXT PRIMARY KEY,
            state TEXT NOT NULL
          )`,
        },
        {
          sql: `CREATE TRIGGER IF NOT EXISTS events_after_insert AFTER INSERT ON events
            BEGIN UPDATE stream_meta SET event_count = event_count + 1 WHERE id = 1; END`,
        },
        {
          sql: `CREATE TRIGGER IF NOT EXISTS events_after_delete AFTER DELETE ON events
            BEGIN UPDATE stream_meta SET event_count = event_count - 1 WHERE id = 1; END`,
        },
      ],
    });
  }

  // --- Coalescing writer (writer tab only): per-event afterAppend stays per-event while
  // still writing one SQLite transaction per delivered batch. `write()` is fire-and-forget;
  // it buffers and flushes on a microtask, then notifies listeners and announces the
  // committed offset range to every tab.
  #writeMode: StreamDatabaseWriteMode = "batch";
  #pendingWrites: StreamEvent[] = [];
  #flushScheduled = false;
  readonly #insertedListeners = new Set<(rows: StreamEventRow[]) => void>();
  readonly #writeErrorListeners = new Set<(error: unknown) => void>();

  setWriteMode(writeMode: StreamDatabaseWriteMode) {
    this.#writeMode = writeMode;
  }

  onInserted(listener: (rows: StreamEventRow[]) => void) {
    this.#insertedListeners.add(listener);
    return () => void this.#insertedListeners.delete(listener);
  }

  onWriteError(listener: (error: unknown) => void) {
    this.#writeErrorListeners.add(listener);
    return () => void this.#writeErrorListeners.delete(listener);
  }

  write(event: StreamEvent) {
    this.#pendingWrites.push(event);
    // Coalesce a batch of per-event writes into one flush on the next microtask.
    if (this.#flushScheduled) return;
    this.#flushScheduled = true;
    void Promise.resolve().then(() => this.#flushPendingWrites());
  }

  clearPendingWrites() {
    this.#pendingWrites = [];
  }

  async #flushPendingWrites() {
    this.#flushScheduled = false;
    const events = this.#pendingWrites;
    this.#pendingWrites = [];
    if (events.length === 0) return;
    try {
      const rows = await this.insertEventBatch({ events, writeMode: this.#writeMode });
      if (rows.length > 0) for (const listener of this.#insertedListeners) listener(rows);
    } catch (error) {
      for (const listener of this.#writeErrorListeners) listener(error);
    }
  }

  /** Resume cursor: the side-effect target is its own checkpoint. */
  async maxOffset(): Promise<number> {
    const [row] = await this.#exec<{ max_offset: number | null }>(
      `SELECT MAX(offset) AS max_offset FROM events`,
    );
    return row?.max_offset ?? -1;
  }

  async insertEventBatch(args: {
    events: StreamEvent[];
    writeMode: StreamDatabaseWriteMode;
  }): Promise<StreamEventRow[]> {
    const { events } = args;
    if (events.length === 0) return [];
    const [{ event_count: eventCountBefore }] = await this.#exec<StreamEventMeta>(
      `SELECT event_count FROM stream_meta WHERE id = 1`,
    );
    const insert = (event: StreamEvent) => ({
      sql: `INSERT OR IGNORE INTO events (offset, type, idempotency_key, created_at, raw_json)
            VALUES (?, ?, ?, ?, ?)`,
      params: [
        event.offset,
        event.type,
        event.idempotencyKey ?? null,
        event.createdAt,
        JSON.stringify(event, null, 2),
      ] as SqlValue[],
    });
    await this.#ready;
    // writeMode "row" = one autocommit statement each; "batch" = one transaction.
    await this.#call("batch", {
      statements: events.map(insert),
      transaction: args.writeMode === "batch",
    });
    const rows = await this.#exec<StreamEventRow>(
      `SELECT virtual_index, offset, type, idempotency_key, created_at, raw_json
       FROM events WHERE virtual_index > ? ORDER BY virtual_index ASC`,
      [eventCountBefore],
    );
    if (rows.length > 0) {
      this.#publishChange({
        kind: "append",
        minOffset: rows[0]!.offset,
        maxOffset: rows.at(-1)!.offset,
        eventCount: eventCountBefore + rows.length,
      });
    }
    return rows;
  }

  async info(): Promise<StreamDatabaseInfo> {
    this.#infoRefresh ??= (async () => {
      try {
        // Pure read of the current grant — does NOT request persistence (that's persist()).
        const persisted = (await navigator.storage?.persisted?.()) ?? false;
        const [size] = await this.#exec<{ bytes: number }>(
          `SELECT page_count * page_size AS bytes
           FROM pragma_page_count(), pragma_page_size()`,
        );
        return {
          databaseSizeBytes: size?.bytes ?? 0,
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
    const buffer = (await this.#call("export", {})) as ArrayBuffer;
    const url = URL.createObjectURL(new Blob([buffer], { type: "application/x-sqlite3" }));
    const link = document.createElement("a");
    link.href = url;
    link.download = this.downloadFilename;
    link.click();
    URL.revokeObjectURL(url);
  }

  async clear() {
    this.clearPendingWrites();
    await this.#exec(`DELETE FROM events`);
    this.#publishChange({ kind: "clear", clearVersion: Date.now() });
  }

  async compact() {
    await this.#exec(`VACUUM`);
  }

  // --- Reactive query layer ------------------------------------------------------------

  /**
   * Registers a reactive query. The returned handle is `useSyncExternalStore`-shaped:
   * `getSnapshot()` returns a referentially-stable result that only changes on re-run,
   * and `subscribe()` re-runs the query when an append/clear can affect its `scope`.
   */
  reactiveQuery<T extends Record<string, SqlValue>>(
    sql: string,
    params: SqlValue[],
    scope: StreamQueryScope,
  ): ReactiveQueryHandle<T> {
    const key = `${sql} ${JSON.stringify(params)}`;
    const existing = this.#queries.get(key);
    if (existing !== undefined) {
      existing.scope = scope; // a scrolled window flips tail<->range; keep it current
      return existing.handle as ReactiveQueryHandle<T>;
    }
    const entry: RegisteredQuery = {
      sql,
      params,
      scope,
      snapshot: PENDING,
      started: false,
      listeners: new Set(),
      handle: {
        getSnapshot: () => entry.snapshot,
        subscribe: (listener) => {
          entry.listeners.add(listener);
          // Defer the first run to subscribe (React calls it after commit) so the
          // reactiveQuery() call in render has no async side effect.
          if (!entry.started) {
            entry.started = true;
            void this.#runQuery(entry);
          }
          return () => {
            entry.listeners.delete(listener);
            if (entry.listeners.size === 0) this.#queries.delete(key);
          };
        },
      },
    };
    this.#queries.set(key, entry);
    return entry.handle as ReactiveQueryHandle<T>;
  }

  async #runQuery(entry: RegisteredQuery): Promise<void> {
    try {
      const data = await this.#exec(entry.sql, entry.params);
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

  // --- Event count (special-cased) -----------------------------------------------------
  // TanStack Virtual only needs the row COUNT, and the writer already computes it for every
  // committed batch. So instead of re-running a COUNT query on each append, we keep the
  // count in memory: read it once, then advance it straight from the append/clear change
  // notifications. O(1), always current, zero per-append SQL — and it doubles as the
  // "db ready" signal for the UI.
  #count: EventCountSnapshot = { count: 0, status: "pending", error: undefined };
  #countStarted = false;
  readonly #countListeners = new Set<() => void>();
  readonly #countHandle: { getSnapshot: () => EventCountSnapshot; subscribe: (l: () => void) => () => void } = {
    getSnapshot: () => this.#count,
    subscribe: (listener) => {
      this.#countListeners.add(listener);
      if (!this.#countStarted) {
        this.#countStarted = true;
        void this.#loadCount();
      }
      return () => void this.#countListeners.delete(listener);
    },
  };

  /** useSyncExternalStore handle for the live event count (stable identity). */
  eventCount() {
    return this.#countHandle;
  }

  async #loadCount() {
    try {
      const [row] = await this.#exec<StreamEventMeta>(
        `SELECT event_count FROM stream_meta WHERE id = 1`,
      );
      this.#setCount(row?.event_count ?? 0, "ok");
    } catch (error) {
      this.#count = {
        ...this.#count,
        status: "error",
        error: error instanceof Error ? error : new Error(String(error)),
      };
      for (const listener of this.#countListeners) listener();
    }
  }

  #setCount(count: number, status: ReactiveQueryStatus) {
    if (this.#count.count === count && this.#count.status === status) return;
    this.#count = { count, status, error: undefined };
    for (const listener of this.#countListeners) listener();
  }

  #publishChange(change: StreamDbChange) {
    this.#channel.postMessage(change);
    this.#onChange(change);
  }

  #onChange(change: StreamDbChange) {
    // The change carries the authoritative new count, so update it without touching SQLite.
    this.#setCount(change.kind === "append" ? change.eventCount : 0, "ok");
    for (const entry of this.#queries.values()) {
      const dirty =
        change.kind === "clear"
          ? true
          : entry.scope.type === "tail"; // a fixed `range` window is immutable under append-only
      if (dirty) void this.#runQuery(entry);
    }
    this.#infoRefresh = undefined;
  }
}

function databasePathForStreamPath(streamPath: string) {
  const segments = streamPath.split("/").filter(Boolean).map(encodeURIComponent);
  if (segments.length === 0) return "/streams/_db.sqlite3";
  return `/streams/${segments.join("/")}/_db.sqlite3`;
}

function downloadFilenameForStreamPath(streamPath: string) {
  const segments = streamPath.split("/").filter(Boolean).map(encodeURIComponent);
  return `streams${segments.map((segment) => `__${segment}`).join("")}__db.sqlite3`;
}
