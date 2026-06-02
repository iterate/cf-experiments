import type { RpcPromise } from "capnweb";
import { BroadcastChannel, createLeaderElection } from "broadcast-channel";
import { z } from "zod";
import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import { defineProcessorContract } from "@cf-experiments/shared/stream-processors";
import {
  createProcessorRunner,
  implementProcessor,
  ProcessorSink,
  streamPortFromRpc,
} from "../stream-processor.js";
import { withStream, type StreamBrowserConnectionStatus } from "./stream-browser.js";
import {
  getStreamBrowserDatabase,
  type StreamBrowserDatabase,
  type StreamEventRow,
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
  databaseInfo: StreamDatabaseInfo | undefined;
  recentRowsByVirtualIndex: ReadonlyMap<number, StreamEventRow>;
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
  let leaderChannel: BroadcastChannel<unknown> | undefined;
  let leaderElector: ReturnType<typeof createLeaderElection> | undefined;
  let connectTimer: ReturnType<typeof setTimeout> | undefined;
  let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  let databaseInfoTimer: ReturnType<typeof setTimeout> | undefined;
  const listeners = new Set<() => void>();
  let disposed = false;
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
    databaseInfo: undefined,
    recentRowsByVirtualIndex: new Map(),
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
    });
  }

  function refreshDatabaseInfoSoon() {
    if (disposed || databaseInfoTimer !== undefined) return;
    databaseInfoTimer = setTimeout(() => {
      databaseInfoTimer = undefined;
      refreshDatabaseInfo();
    }, 1_000);
  }

  // The projector's SQLite writes land here (for the UI) and surface errors (to reconnect).
  const offInserted = streamDatabase.onInserted((rows) => {
    if (disposed) return;
    const recentRowsByVirtualIndex = new Map(snapshot.recentRowsByVirtualIndex);
    for (const row of rows) recentRowsByVirtualIndex.set(row.virtual_index - 1, row);
    while (recentRowsByVirtualIndex.size > 10_000) {
      const oldestKey = recentRowsByVirtualIndex.keys().next().value;
      if (oldestKey === undefined) break;
      recentRowsByVirtualIndex.delete(oldestKey);
    }
    snapshot = { ...snapshot, recentRowsByVirtualIndex };
    emitSnapshot();
    refreshDatabaseInfoSoon();
  });
  const offWriteError = streamDatabase.onWriteError((error) => {
    console.error("Browser stream SQLite write failed", error);
    stopSubscriptionElection();
    stream?.[Symbol.dispose]();
    stream = undefined;
    reconnectAfter(String(error));
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
    const runner = createProcessorRunner({
      processor: sqliteProjector,
      deps: { db: streamDatabase },
      storage: {
        load: async () => ({ state: {}, offset: await streamDatabase.maxOffset() }),
        save: () => {}, // SQLite is its own checkpoint (MAX(offset))
      },
      stream: streamPortFromRpc(election.connection.rpc),
    });
    const sink = new ProcessorSink((batch) =>
      runner.processEventBatch(batch),
    );

    leaderChannel = new BroadcastChannel(`stream-subscription:${encodeURIComponent(args.streamPath)}`);
    leaderElector = createLeaderElection(leaderChannel);
    leaderElector.onduplicate = () => {
      console.error("Duplicate browser stream subscription leader detected", args.streamPath);
      stopSubscriptionElection();
    };

    snapshot = { ...snapshot, subscriptionStatus: "electing" };
    emitSnapshot();

    const leadershipTimeout = setTimeout(() => {
      if (!disposed && leaderElector !== undefined && subscriptionHandle === undefined) {
        snapshot = { ...snapshot, subscriptionStatus: "follower" };
        emitSnapshot();
      }
    }, 250);

    void leaderElector
      .awaitLeadership()
      .then(() => {
        clearTimeout(leadershipTimeout);
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
        clearTimeout(leadershipTimeout);
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
    void leaderElector?.die();
    void leaderChannel?.close();
    leaderElector = undefined;
    leaderChannel = undefined;
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
        recentRowsByVirtualIndex: new Map(),
      };
      emitSnapshot();
      await streamDatabase.clear();
      await streamDatabase.compact();
      snapshot = {
        ...snapshot,
        databaseInfo: undefined,
        recentRowsByVirtualIndex: new Map(),
      };
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
