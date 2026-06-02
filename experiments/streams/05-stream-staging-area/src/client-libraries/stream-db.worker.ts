/// <reference lib="webworker" />
// Per-tab dedicated worker that owns ONE wa-sqlite connection to the stream's OPFS
// database, using the OPFSCoopSyncVFS VFS. That VFS needs no SharedArrayBuffer, no
// COOP/COEP, and no async-proxy worker (the things that deadlocked SQLocal's default
// "opfs" VFS in production builds); it cooperatively shares the OPFS file across the
// per-tab connections of every open tab, so each tab reads locally and only ONE tab
// (elected via Web Locks on the main thread) writes.
//
// This worker is intentionally generic: it speaks `exec` / `batch` / `export`. All the
// stream-specific schema and the reactive-query logic live on the main thread in
// stream-browser-db.ts, so this file never needs to change as the schema evolves.
import SQLiteESMFactory from "@journeyapps/wa-sqlite/dist/wa-sqlite.mjs";
import wasmUrl from "@journeyapps/wa-sqlite/dist/wa-sqlite.wasm?url";
import { Factory } from "@journeyapps/wa-sqlite";
import {
  SQLITE_OPEN_CREATE,
  SQLITE_OPEN_READWRITE,
  SQLITE_ROW,
} from "@journeyapps/wa-sqlite/src/sqlite-constants.js";
import { OPFSCoopSyncVFS } from "@journeyapps/wa-sqlite/src/examples/OPFSCoopSyncVFS.js";

type Sqlite3 = ReturnType<typeof Factory>;
// Matches wa-sqlite's SQLiteCompatibleType (blobs surface as Uint8Array or number[]). Our
// schema only stores text/integer, but the row() return type can be any of these.
type SqlValue = string | number | bigint | Uint8Array | number[] | null;
type Statement = { sql: string; params?: SqlValue[] };
type Request =
  | { id: number; op: "init"; databasePath: string }
  | { id: number; op: "exec"; sql: string; params?: SqlValue[] }
  | { id: number; op: "batch"; statements: Statement[]; transaction: boolean }
  | { id: number; op: "export" };

let sqlite3: Sqlite3 | undefined;
let db: number | undefined;
let databasePath = "";
const VFS_NAME = "stream-opfs-coop";

async function open(path: string): Promise<void> {
  const module = await SQLiteESMFactory({ locateFile: () => wasmUrl });
  sqlite3 = Factory(module);
  const vfs = await OPFSCoopSyncVFS.create(VFS_NAME, module);
  // makeDefault:false — register under a name and pass it to open_v2, so we never touch
  // the built-in "opfs"/"memory" VFS registration.
  sqlite3.vfs_register(vfs, false);
  databasePath = path;
  db = await sqlite3.open_v2(path, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, VFS_NAME);
  // Busy timeout lets cooperative-lock contention with other tabs' connections resolve
  // inside SQLite instead of surfacing SQLITE_BUSY to every read.
  await sqlite3.exec(db, "PRAGMA busy_timeout = 5000;");
}

/** Runs one statement, collecting any result rows as plain objects. */
async function exec(sql: string, params?: SqlValue[]): Promise<Record<string, SqlValue>[]> {
  if (sqlite3 === undefined || db === undefined) throw new Error("db not initialised");
  const rows: Record<string, SqlValue>[] = [];
  for await (const stmt of sqlite3.statements(db, sql)) {
    if (params !== undefined && params.length > 0) sqlite3.bind_collection(stmt, params);
    const columns = sqlite3.column_names(stmt);
    while ((await sqlite3.step(stmt)) === SQLITE_ROW) {
      const values = sqlite3.row(stmt);
      const row: Record<string, SqlValue> = {};
      columns.forEach((name, i) => (row[name] = values[i] ?? null));
      rows.push(row);
    }
  }
  return rows;
}

async function batch(statements: Statement[], transaction: boolean): Promise<void> {
  if (sqlite3 === undefined || db === undefined) throw new Error("db not initialised");
  if (transaction) await sqlite3.exec(db, "BEGIN IMMEDIATE;");
  try {
    for (const statement of statements) await exec(statement.sql, statement.params);
    if (transaction) await sqlite3.exec(db, "COMMIT;");
  } catch (error) {
    if (transaction) await sqlite3.exec(db, "ROLLBACK;");
    throw error;
  }
}

async function exportFile(): Promise<ArrayBuffer> {
  // OPFSCoopSyncVFS stores a real, transparent SQLite file in OPFS; read it back raw.
  const root = await navigator.storage.getDirectory();
  const segments = databasePath.split("/").filter(Boolean);
  let dir = root;
  for (const segment of segments.slice(0, -1)) dir = await dir.getDirectoryHandle(segment);
  const handle = await dir.getFileHandle(segments.at(-1) ?? databasePath);
  return (await handle.getFile()).arrayBuffer();
}

async function handle(request: Request): Promise<unknown> {
  switch (request.op) {
    case "init":
      await open(request.databasePath);
      return undefined;
    case "exec":
      return exec(request.sql, request.params);
    case "batch":
      return batch(request.statements, request.transaction);
    case "export":
      return exportFile();
  }
}

self.onmessage = (event: MessageEvent<Request>) => {
  const request = event.data;
  handle(request).then(
    (result) => {
      // ArrayBuffer results are transferred to avoid a copy on the way back.
      const transfer = result instanceof ArrayBuffer ? [result] : [];
      self.postMessage({ id: request.id, ok: true, result }, { transfer });
    },
    (error: unknown) => {
      self.postMessage({
        id: request.id,
        ok: false,
        error: String((error as { message?: string } | undefined)?.message ?? error),
      });
    },
  );
};
