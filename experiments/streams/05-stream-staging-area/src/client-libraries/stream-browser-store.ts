import type { RpcPromise } from "capnweb";
import { z } from "zod";
import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import { defineProcessorContract } from "@cf-experiments/shared/stream-processors";
import {
  createProcessorRunner,
  implementProcessor,
  ProcessorSink,
  streamPortFromRpc,
} from "../stream-processor.js";
import { acquireWriterRole, type WriterRole } from "./stream-leader.js";
import { withStream, type StreamBrowserConnectionStatus } from "./stream-browser.js";
import {
  getStreamBrowserDatabase,
  type StreamBrowserDatabase,
  type StreamDatabaseInfo,
  type StreamDatabaseWriteMode,
} from "./stream-browser-db.js";

export type StreamBrowserSnapshot = {
  connectionStatus:
    | StreamBrowserConnectionStatus
    | "reconnecting"
    | "subscribing"
    | "subscribed";
  subscriptionStatus: "idle" | "electing" | "leader" | "follower";
  clearVersion: number;
  connectionError: string | undefined;
  /** Events this tab's hosted processor has received from the stream (in-memory, not SQLite). */
  receivedEventCount: number;
  databaseInfo: StreamDatabaseInfo | undefined;
};

export type StreamBrowserStore = Disposable & {
  appendBatch(args: { events: StreamEventInput[] }): RpcPromise<StreamEvent[]>;
  clearLocalDatabase(): Promise<void>;
  kill(): RpcPromise<void>;
  getSnapshot(): StreamBrowserSnapshot;
  getServerSnapshot(): StreamBrowserSnapshot;
  subscribe(listener: () => void): () => void;
};

// The browser is just another host for the SAME processor model: it projects
// every stream event into a local SQLite mirror. afterAppend is per-event and
// fire-and-forget; the db layer (`write`) coalesces into one transaction per
// delivered batch, so the batch/row write-mode optimization is preserved.
const sqliteProjectorContract = defineProcessorContract({
  slug: "browser.sqlite-projector",
  version: "0.1.0",
  description: "Projects every stream event into the local SQLite mirror.",
  stateSchema: z.object({}),
  initialState: {},
  events: {},
  consumes: ["*"],
  emits: [],
});

const sqliteProjector = implementProcessor(
  sqliteProjectorContract,
  (deps: { db: StreamBrowserDatabase }) => ({
    afterAppend({ event }) {
      deps.db.write(event);
    },
  }),
);

/** Creates a lazy browser stream store for React's `useSyncExternalStore`. */
export function createStreamBrowserStore(args: {
  streamPath: string;
  sqliteWriteMode: StreamDatabaseWriteMode;
  onDispose?: () => void;
}): StreamBrowserStore {
  let stream: ReturnType<typeof withStream> | undefined;
  let subscriptionHandle: { unsubscribe(): void } | undefined;
  let writerRole: WriterRole | undefined;
  let connectTimer: ReturnType<typeof setTimeout> | undefined;
  let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  let databaseInfoTimer: ReturnType<typeof setTimeout> | undefined;
  const listeners = new Set<() => void>();
  let disposed = false;
  let receivedEventCount = 0;
  const streamDatabase = getStreamBrowserDatabase(args.streamPath);
  const browserSubscriberStorageKey = "stream-browser-subscriber-id";
  const browserSubscriberId =
    localStorage.getItem(browserSubscriberStorageKey) ?? crypto.randomUUID();
  localStorage.setItem(browserSubscriberStorageKey, browserSubscriberId);
  streamDatabase.setWriteMode(args.sqliteWriteMode);
  let snapshot: StreamBrowserSnapshot = {
    clearVersion: 0,
    connectionStatus: "connecting",
    connectionError: undefined,
    receivedEventCount: 0,
    databaseInfo: undefined,
    subscriptionStatus: "idle",
  };

  function emitSnapshot() {
    for (const listener of listeners) listener();
  }

  function refreshDatabaseInfo() {
    void streamDatabase.info().then((databaseInfo) => {
      if (disposed) return;
      snapshot = { ...snapshot, databaseInfo };
      emitSnapshot();
    }).catch((error: unknown) => {
      if (disposed) return;
      const message = String((error as { message?: string } | undefined)?.message ?? error);
      snapshot = { ...snapshot, connectionError: "local database error: " + message };
      emitSnapshot();
    });
  }

  function refreshDatabaseInfoSoon() {
    if (disposed || databaseInfoTimer !== undefined) return;
    databaseInfoTimer = setTimeout(() => {
      databaseInfoTimer = undefined;
      refreshDatabaseInfo();
    }, 1_000);
  }

  // Each committed write nudges a (debounced) db-size refresh. The rows themselves reach
  // the UI through reactive queries (db.reactiveQuery), not this store — so there is no
  // in-memory row cache to maintain here, and writer/reader tabs render identically.
  const offInserted = streamDatabase.onInserted(() => {
    if (disposed) return;
    refreshDatabaseInfoSoon();
  });
  const offWriteError = streamDatabase.onWriteError((error) => {
    // The subscription is intentionally independent of local SQLite health: a transient
    // OPFS write hiccup (e.g. cooperative-lock contention with another tab's connection)
    // must NOT make this tab resign the writer lock, or two tabs would thrash leadership.
    // Events are still being received; the projector will catch up on the next batch.
    console.error("Browser stream SQLite write failed", error);
  });

  function reconnectAfter(connectionError: string) {
    if (disposed || reconnectTimer !== undefined) return;
    snapshot = { ...snapshot, connectionError, connectionStatus: "reconnecting" };
    emitSnapshot();
    reconnectTimer = setTimeout(() => {
      reconnectTimer = undefined;
      connect();
    }, 1_000);
  }

  function reconnectNow() {
    if (connectTimer !== undefined) {
      clearTimeout(connectTimer);
      connectTimer = undefined;
    }
    if (reconnectTimer !== undefined) {
      clearTimeout(reconnectTimer);
      reconnectTimer = undefined;
    }
    connect();
  }

  function connect() {
    if (stream !== undefined || disposed) return;

    const streamUrl = new URL(
      `/stream/${encodeURIComponent(args.streamPath)}`,
      window.location.href,
    );
    const subscriptionKey = `browser:${browserSubscriberId}`;

    const connection = withStream({
      url: streamUrl,
      onConnectionStatusChange(connectionStatus, connectionError) {
        if (disposed) return;
        if (connectionStatus === "closed" || connectionStatus === "error") {
          stopSubscriptionElection();
          subscriptionHandle = undefined;
          stream = undefined;
          reconnectAfter(connectionError ?? connectionStatus);
          return;
        }
        snapshot = {
          ...snapshot,
          connectionError: connectionStatus === "connected" ? undefined : snapshot.connectionError,
          connectionStatus,
        };
        emitSnapshot();
      },
    });
    stream = connection;
    startSubscriptionElection({ connection, subscriptionKey });
  }

  function startSubscriptionElection(election: {
    connection: ReturnType<typeof withStream>;
    subscriptionKey: string;
  }) {
    // Host the projector on the shared runner; the runner IS the subscription sink.
    // Resume from the local SQLite mirror's max offset (the events table is the durable
    // cursor) so a reconnect replays ONLY new events — not the whole stream. Without this,
    // every reconnect (e.g. after "kill stream") re-subscribes at offset -1 and the DO
    // replays all N events before the next one arrives, a delay that grows with stream size.
    // On any DB read failure we fall back to undefined → offset -1 (full replay), so a
    // slow/unavailable local DB still can't block receiving events.
    const runner = createProcessorRunner({
      processor: sqliteProjector,
      deps: { db: streamDatabase },
      storage: {
        load: async () => {
          try {
            return { state: {}, offset: await streamDatabase.maxOffset() };
          } catch {
            return undefined;
          }
        },
        save: () => {},
      },
      stream: streamPortFromRpc(election.connection.rpc),
    });
    const sink = new ProcessorSink((batch) => {
      receivedEventCount += batch.events.length;
      (globalThis as unknown as { __receivedEventCount?: number }).__receivedEventCount = receivedEventCount;
      snapshot = { ...snapshot, receivedEventCount };
      emitSnapshot();
      return runner.processEventBatch(batch);
    });

    // Web Locks elect exactly one writer tab. Until this tab wins the lock it is a reader
    // (it still reads the shared OPFS db reactively, but does NOT subscribe or write). When
    // it wins, it subscribes and hosts the projector; if it later closes, the lock releases
    // and another tab takes over automatically.
    snapshot = { ...snapshot, subscriptionStatus: "electing" };
    emitSnapshot();

    const followerTimeout = setTimeout(() => {
      if (!disposed && subscriptionHandle === undefined) {
        snapshot = { ...snapshot, subscriptionStatus: "follower" };
        emitSnapshot();
      }
    }, 250);

    writerRole = acquireWriterRole(args.streamPath);
    void writerRole.whenWriter
      .then(() => {
        clearTimeout(followerTimeout);
        if (disposed || stream !== election.connection) return undefined;
        snapshot = { ...snapshot, subscriptionStatus: "leader" };
        emitSnapshot();
        return runner.afterOffset();
      })
      .then((afterOffset) => {
        if (afterOffset === undefined || disposed || stream !== election.connection) return undefined;
        return election.connection.rpc.subscribe({
          subscriptionKey: election.subscriptionKey,
          sink,
          afterOffset,
        });
      })
      .then((handle) => {
        if (handle === undefined) return;
        if (disposed) {
          handle.unsubscribe();
          return;
        }
        subscriptionHandle = handle;
        snapshot = { ...snapshot, connectionError: undefined, connectionStatus: "subscribed" };
        emitSnapshot();
      })
      .catch((error: unknown) => {
        clearTimeout(followerTimeout);
        if (disposed) return;
        stopSubscriptionElection();
        stream?.[Symbol.dispose]();
        stream = undefined;
        reconnectAfter(`subscribe failed: ${String(error)}`);
      });
  }

  function stopSubscriptionElection() {
    subscriptionHandle?.unsubscribe();
    subscriptionHandle = undefined;
    writerRole?.release();
    writerRole = undefined;
    snapshot = { ...snapshot, subscriptionStatus: "idle" };
    if (!disposed) emitSnapshot();
  }

  function teardown() {
    if (connectTimer !== undefined) {
      clearTimeout(connectTimer);
      connectTimer = undefined;
    }
    if (reconnectTimer !== undefined) {
      clearTimeout(reconnectTimer);
      reconnectTimer = undefined;
    }
    if (databaseInfoTimer !== undefined) {
      clearTimeout(databaseInfoTimer);
      databaseInfoTimer = undefined;
    }
    stopSubscriptionElection();
    stream?.[Symbol.dispose]();
    stream = undefined;
    offInserted();
    offWriteError();
    args.onDispose?.();
  }

  return {
    appendBatch(appendArgs) {
      reconnectNow();
      if (stream === undefined) throw new Error("stream connection is disposed");
      return stream.rpc.appendBatch(appendArgs);
    },
    async clearLocalDatabase() {
      stopSubscriptionElection();
      stream?.[Symbol.dispose]();
      stream = undefined;
      streamDatabase.clearPendingWrites();
      snapshot = {
        ...snapshot,
        clearVersion: snapshot.clearVersion + 1,
        databaseInfo: undefined,
      };
      emitSnapshot();
      await streamDatabase.clear();
      await streamDatabase.compact();
      snapshot = { ...snapshot, databaseInfo: undefined };
      emitSnapshot();
      refreshDatabaseInfo();
      reconnectNow();
    },
    kill() {
      reconnectNow();
      if (stream === undefined) throw new Error("stream connection is disposed");
      return stream.rpc.kill();
    },
    getSnapshot() {
      return snapshot;
    },
    getServerSnapshot() {
      return snapshot;
    },
    subscribe(listener) {
      listeners.add(listener);
      if (listeners.size === 1) {
        disposed = false;
        snapshot = { ...snapshot, connectionStatus: "subscribing" };
        emitSnapshot();
        refreshDatabaseInfo();
        connectTimer = setTimeout(() => {
          connectTimer = undefined;
          connect();
        }, 0);
      }
      return () => {
        listeners.delete(listener);
        if (listeners.size === 0 && !disposed) {
          disposed = true;
          teardown();
        }
      };
    },
    [Symbol.dispose]() {
      listeners.clear();
      if (!disposed) {
        disposed = true;
        teardown();
      }
    },
  };
}
