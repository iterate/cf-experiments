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

export type StreamDatabaseInfo = DatabaseInfo & {
  crossOriginIsolated: boolean;
};

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

  constructor(readonly streamPath: string) {
    this.databasePath = databasePathForStreamPath(streamPath);
    this.downloadFilename = downloadFilenameForStreamPath(streamPath);
    this.sqlocal = new SQLocal({
      databasePath: this.databasePath,
      reactive: true,
      onInit: (sql) => [
        sql`
          CREATE TABLE IF NOT EXISTS events (
            virtual_index INTEGER PRIMARY KEY AUTOINCREMENT,
            offset INTEGER NOT NULL,
            type TEXT NOT NULL,
            idempotency_key TEXT,
            created_at TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            UNIQUE (offset)
          )
        `,
      ],
    });
  }

  async insertEventBatch(events: StreamEvent[]) {
    if (events.length === 0) return;

    await this.sqlocal.transaction(async (tx) => {
      await tx.batch((sql) =>
        events.map((event) => sql`
          INSERT OR IGNORE INTO events (
            offset,
            type,
            idempotency_key,
            created_at,
            raw_json
          ) VALUES (
            ${event.offset},
            ${event.type},
            ${event.idempotencyKey ?? null},
            ${event.createdAt},
            ${JSON.stringify(event, null, 2)}
          )
        `),
      );
    });
  }

  async info(): Promise<StreamDatabaseInfo> {
    return {
      ...(await this.sqlocal.getDatabaseInfo()),
      crossOriginIsolated: globalThis.crossOriginIsolated,
    };
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
    await this.sqlocal.deleteDatabaseFile();
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
