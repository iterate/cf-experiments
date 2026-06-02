import { SQLocal, type DatabaseInfo } from "sqlocal";
import type { StreamEvent } from "@cf-experiments/shared/event";

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

export type StreamDatabaseInfo = DatabaseInfo & {
  crossOriginIsolated: boolean;
};

export type StreamDatabaseWriteMode = "batch" | "row";

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
  readonly sqlocal: SQLocal;
  readonly downloadFilename: string;
  #infoRefresh: Promise<StreamDatabaseInfo> | undefined;

  constructor(readonly streamPath: string) {
    this.databasePath = databasePathForStreamPath(streamPath);
    this.downloadFilename = downloadFilenameForStreamPath(streamPath);
    this.sqlocal = new SQLocal({
      databasePath: this.databasePath,
      reactive: true,
      onInit: (sql) => [
        sql`
          CREATE TABLE IF NOT EXISTS events (
            virtual_index INTEGER PRIMARY KEY,
            offset INTEGER NOT NULL,
            type TEXT NOT NULL,
            idempotency_key TEXT,
            created_at TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            UNIQUE (offset)
          )
        `,
        sql`
          CREATE TABLE IF NOT EXISTS stream_meta (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            event_count INTEGER NOT NULL DEFAULT 0
          )
        `,
        // SQLocal runs onInit for every tab/worker that opens this OPFS file.
        // Keep it schema-only; the triggers below maintain this row after that.
        sql`
          INSERT INTO stream_meta (id, event_count)
          VALUES (1, 0)
          ON CONFLICT(id) DO NOTHING
        `,
        sql`
          CREATE TRIGGER IF NOT EXISTS events_after_insert
          AFTER INSERT ON events
          BEGIN
            UPDATE stream_meta
            SET event_count = event_count + 1
            WHERE id = 1;
          END
        `,
        sql`
          CREATE TRIGGER IF NOT EXISTS events_after_delete
          AFTER DELETE ON events
          BEGIN
            UPDATE stream_meta
            SET event_count = event_count - 1
            WHERE id = 1;
          END
        `,
      ],
    });
  }

  // --- Coalescing writer: the db-layer batching that lets a per-event processor
  // afterAppend stay per-event while still writing one SQLite transaction per
  // delivered batch. `write()` is fire-and-forget; it buffers and flushes on a
  // microtask, then notifies listeners with the inserted rows (for the UI) or the
  // error (so the caller can reconnect).
  #writeMode: StreamDatabaseWriteMode = "batch";
  #pendingWrites: StreamEvent[] = [];
  #flushing: Promise<void> | undefined;
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
    this.#flushing ??= Promise.resolve().then(() => this.#flushPendingWrites());
  }

  clearPendingWrites() {
    this.#pendingWrites = [];
  }

  async #flushPendingWrites() {
    this.#flushing = undefined;
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
    const [row] = await this.sqlocal.sql<{ max_offset: number | null }>`
      SELECT MAX(offset) AS max_offset FROM events
    `;
    return row?.max_offset ?? -1;
  }

  async insertEventBatch(args: {
    events: StreamEvent[];
    writeMode: StreamDatabaseWriteMode;
  }): Promise<StreamEventRow[]> {
    const { events } = args;
    if (events.length === 0) return [];
    const [{ event_count: eventCountBefore }] = await this.sqlocal.sql<StreamEventMeta>`
      SELECT event_count
      FROM stream_meta
      WHERE id = 1
    `;

    if (args.writeMode === "row") {
      for (const event of events) {
        await this.sqlocal.sql`
          INSERT OR IGNORE INTO events (
            offset,
            type,
            idempotency_key,
            created_at,
            raw_json
          )
          VALUES (
            ${event.offset},
            ${event.type},
            ${event.idempotencyKey ?? null},
            ${event.createdAt},
            ${JSON.stringify(event, null, 2)}
          )
        `;
      }
      return this.#eventsAfterVirtualIndex(eventCountBefore);
    }

    await this.sqlocal.transaction(async (tx) => {
      await tx.batch((sql) =>
        events.map((event) => sql`
          INSERT OR IGNORE INTO events (
            offset,
            type,
            idempotency_key,
            created_at,
            raw_json
          )
          VALUES (
            ${event.offset},
            ${event.type},
            ${event.idempotencyKey ?? null},
            ${event.createdAt},
            ${JSON.stringify(event, null, 2)}
          )
        `),
      );
    });
    return this.#eventsAfterVirtualIndex(eventCountBefore);
  }

  async info(): Promise<StreamDatabaseInfo> {
    if (this.#infoRefresh === undefined) {
      this.#infoRefresh = (async () => {
        await navigator.storage?.persist?.();
        try {
          return {
            ...(await this.sqlocal.getDatabaseInfo()),
            crossOriginIsolated: globalThis.crossOriginIsolated,
          };
        } finally {
          this.#infoRefresh = undefined;
        }
      })();
    }
    return this.#infoRefresh;
  }

  async #eventsAfterVirtualIndex(virtualIndex: number) {
    return this.sqlocal.sql<StreamEventRow>`
      SELECT virtual_index, offset, type, idempotency_key, created_at, raw_json
      FROM events
      WHERE virtual_index > ${virtualIndex}
      ORDER BY virtual_index ASC
    `;
  }

  async download() {
    const file = await this.sqlocal.getDatabaseFile();
    const url = URL.createObjectURL(file);
    const link = document.createElement("a");
    link.href = url;
    link.download = this.downloadFilename;
    link.click();
    URL.revokeObjectURL(url);
  }

  async clear() {
    this.clearPendingWrites();
    await this.sqlocal.sql`DELETE FROM events`;
  }

  async compact() {
    await this.sqlocal.sql`VACUUM`;
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
