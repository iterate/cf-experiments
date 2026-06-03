import type { RpcPromise, RpcStub } from "capnweb";
import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import { connectStream, type StreamBrowserConnectionStatus } from "./connect.js";
import { rawEventsBrowserProcessor } from "../processors/raw-events-browser/implementation.js";
import { createProcessorRunner } from "../processor-runner.js";
import type { StreamRpc } from "../types.js";
import { createStreamSubscription, type StreamSubscription } from "../subscription.js";
import { acquireWriterRole, type WriterRole } from "./stream-leader.js";
import {
  BROWSER_DB_SCHEMA_VERSION,
  StreamBrowserDatabase,
  type StreamDatabaseInfo,
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
  receivedEventCount: number;
  databaseInfo: StreamDatabaseInfo | undefined;
};

export type StreamBrowserStore = Disposable & {
  readonly streamDatabase: StreamBrowserDatabase;
  appendBatch(args: { events: StreamEventInput[] }): RpcPromise<StreamEvent[]>;
  clearLocalDatabase(): Promise<void>;
  kill(): RpcPromise<void>;
  reset(): RpcPromise<void>;
  getSnapshot(): StreamBrowserSnapshot;
  getServerSnapshot(): StreamBrowserSnapshot;
  subscribe(listener: () => void): () => void;
};

export function createStreamBrowserStore(args: {
  streamPath: string;
  onDispose?: () => void;
}): StreamBrowserStore {
  const streamDatabase = new StreamBrowserDatabase(args.streamPath);
  const listeners = new Set<() => void>();
  let stream: Awaited<ReturnType<typeof connectStream>> | undefined;
  let subscriptionHandle: { unsubscribe(): void } | undefined;
  let subscription: StreamSubscription | undefined;
  let processing: AsyncDisposable | undefined;
  let writerRole: WriterRole | undefined;
  let connectTimer: ReturnType<typeof setTimeout> | undefined;
  let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  let databaseInfoTimer: ReturnType<typeof setTimeout> | undefined;
  let disposeTimer: ReturnType<typeof setTimeout> | undefined;
  let disposed = false;
  let started = false;
  let receivedEventCount = 0;
  const browserSubscriberStorageKey = "stream-browser-subscriber-id";
  const browserSubscriberId =
    localStorage.getItem(browserSubscriberStorageKey) ?? crypto.randomUUID();
  localStorage.setItem(browserSubscriberStorageKey, browserSubscriberId);
  let snapshot: StreamBrowserSnapshot = {
    clearVersion: 0,
    connectionStatus: "connecting",
    connectionError: undefined,
    receivedEventCount: 0,
    databaseInfo: undefined,
    subscriptionStatus: "idle",
  };

  const offDatabaseChange = streamDatabase.onChange(() => {
    if (disposed) return;
    refreshDatabaseInfoSoon();
  });

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
      console.error(`[stream-browser ${args.streamPath}] local database info refresh failed`, error);
      snapshot = { ...snapshot, connectionError: "local database error: " + errorMessage(error) };
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

  async function discardLocalMirror() {
    await streamDatabase.clear();
    await streamDatabase.compact();
    receivedEventCount = 0;
    snapshot = {
      ...snapshot,
      clearVersion: snapshot.clearVersion + 1,
      databaseInfo: undefined,
      receivedEventCount: 0,
    };
    emitSnapshot();
    refreshDatabaseInfo();
  }

  async function reconcileLocalMirrorWithServer(rpc: RpcStub<StreamRpc>) {
    const localSummary = await streamDatabase.eventSummary();
    if (!localSummary.isContinuous) {
      console.warn(
        `[stream-browser ${args.streamPath}] Local SQLite mirror has non-continuous offsets; discarding local database.`,
        { streamPath: args.streamPath, localSummary },
      );
      await discardLocalMirror();
      return;
    }

    const localMaxOffset = localSummary.maxOffset ?? -1;
    if (localMaxOffset < 0) return;

    const { state: serverState } = await rpc.runtimeState();
    const serverMaxOffset = serverState.maxOffset;
    if (serverMaxOffset >= localMaxOffset) return;

    console.warn(
      `[stream-browser ${args.streamPath}] Server stream has fewer events than the local SQLite mirror; discarding local database.`,
      { streamPath: args.streamPath, serverMaxOffset, localMaxOffset },
    );
    await discardLocalMirror();
  }

  function connect() {
    if (stream !== undefined || disposed) return;

    const streamUrl = new URL(
      `/stream/${encodeURIComponent(args.streamPath)}`,
      window.location.href,
    );
    const subscriptionKey = `browser:${browserSubscriberId}`;

    void connectStream({
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
    }).then((connection) => {
      if (disposed) {
        void connection[Symbol.asyncDispose]();
        return;
      }
      stream = connection;
      startSubscriptionElection({ connection, subscriptionKey });
    }).catch((error: unknown) => {
      if (disposed) return;
      reconnectAfter(`connect failed: ${errorMessage(error)}`);
    });
  }

  function startSubscriptionElection(election: {
    connection: Awaited<ReturnType<typeof connectStream>>;
    subscriptionKey: string;
  }) {
    snapshot = { ...snapshot, subscriptionStatus: "electing" };
    emitSnapshot();

    const followerTimeout = setTimeout(() => {
      if (!disposed && subscriptionHandle === undefined) {
        snapshot = { ...snapshot, subscriptionStatus: "follower" };
        emitSnapshot();
      }
    }, 250);

    // Version the Web Lock by browser DB schema. Without this, an old deployed tab
    // can keep the old writer lock after a new tab migrates/drops the shared OPFS
    // table, leaving the new tab as a follower with an empty DB and no replay.
    writerRole = acquireWriterRole({
      compatibilityVersion: `browser-db-v${BROWSER_DB_SCHEMA_VERSION}`,
      streamPath: args.streamPath,
    });
    void writerRole.whenWriter
      .then(async () => {
        clearTimeout(followerTimeout);
        if (disposed || stream !== election.connection) return undefined;
        snapshot = { ...snapshot, subscriptionStatus: "leader" };
        emitSnapshot();
        await reconcileLocalMirrorWithServer(election.connection.stream);
        const localSummary = await streamDatabase.eventSummary();
        const localMaxOffset = localSummary.maxOffset ?? undefined;
        const replayAfterOffset = localMaxOffset ?? 0;
        let flushFrame: number | undefined;
        let pendingEvents: StreamEvent[] = [];
        let tail = Promise.resolve();
        const flushSoon = () => {
          if (flushFrame !== undefined) return;
          // The server may deliver several batches inside one browser frame. Buffer
          // them into one SQLite transaction so the local mirror remains high-throughput
          // and TanStack Virtual sees one large append instead of same-turn count churn.
          flushFrame = requestAnimationFrame(() => {
            flushFrame = undefined;
            const events = pendingEvents;
            pendingEvents = [];
            if (events.length === 0) return;
            // Never let failed SQLite writes disappear into the fire-and-forget
            // subscription path. A broken local mirror must be visible in both console
            // and React state, otherwise followers can show an infinite empty spinner.
            tail = tail
              .then(() => streamDatabase.insertEventBatch({ events }), () =>
                streamDatabase.insertEventBatch({ events }),
              )
              .then(() => onStored(events))
              .catch((error: unknown) => {
                onMirrorWriteError(error);
              });
            if (pendingEvents.length > 0) flushSoon();
          });
        };
        const processorRunner = createProcessorRunner({
          processor: rawEventsBrowserProcessor,
          deps: {
            storeEvent(event) {
              pendingEvents.push(event);
              flushSoon();
            },
          },
          storage: {
            load: () =>
              localMaxOffset === undefined ? undefined : { state: {}, offset: localMaxOffset },
            save: () => {},
          },
          stream: election.connection.stream,
        });
        const streamSubscription = createStreamSubscription({
          subscriptionKey: election.subscriptionKey,
        });
        subscription = streamSubscription;
        processing = processorRunner.run({ subscription: streamSubscription });
        return {
          replayAfterOffset,
          sink: streamSubscription.sink,
        };
      })
      .then((subscription) => {
        if (subscription === undefined || disposed || stream !== election.connection) return undefined;
        return election.connection.stream.subscribe({
          subscriptionKey: election.subscriptionKey,
          sink: subscription.sink,
          replayAfterOffset: subscription.replayAfterOffset,
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
        console.error(`[stream-browser ${args.streamPath}] subscribe failed`, error);
        stopSubscriptionElection();
        void stream?.[Symbol.asyncDispose]();
        stream = undefined;
        reconnectAfter(`subscribe failed: ${errorMessage(error)}`);
      });
  }

  function onStored(events: StreamEvent[]) {
    receivedEventCount += events.length;
    snapshot = { ...snapshot, receivedEventCount };
    emitSnapshot();
  }

  function onMirrorWriteError(error: unknown) {
    if (disposed) return;
    console.error(`[stream-browser ${args.streamPath}] local mirror write failed`, error);
    snapshot = {
      ...snapshot,
      connectionError: `local mirror write failed: ${errorMessage(error)}`,
    };
    emitSnapshot();
  }

  function stopSubscriptionElection() {
    subscriptionHandle?.unsubscribe();
    subscriptionHandle = undefined;
    void processing?.[Symbol.asyncDispose]();
    processing = undefined;
    void subscription?.[Symbol.asyncDispose]();
    subscription = undefined;
    writerRole?.release();
    writerRole = undefined;
    snapshot = { ...snapshot, subscriptionStatus: "idle" };
    if (!disposed) emitSnapshot();
  }

  function start() {
    if (started || disposed) return;
    started = true;
    snapshot = { ...snapshot, connectionStatus: "subscribing" };
    emitSnapshot();
    refreshDatabaseInfo();
    connectTimer = setTimeout(() => {
      connectTimer = undefined;
      connect();
    }, 0);
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
    void stream?.[Symbol.asyncDispose]();
    stream = undefined;
    offDatabaseChange();
    streamDatabase.dispose();
    args.onDispose?.();
  }

  function dispose() {
    listeners.clear();
    if (disposed) return;
    if (disposeTimer !== undefined) {
      clearTimeout(disposeTimer);
      disposeTimer = undefined;
    }
    disposed = true;
    teardown();
  }

  return {
    streamDatabase,
    appendBatch(appendArgs) {
      reconnectNow();
      if (stream === undefined) throw new Error("stream connection is disposed");
      return stream.stream.appendBatch(appendArgs);
    },
    async clearLocalDatabase() {
      stopSubscriptionElection();
      void stream?.[Symbol.asyncDispose]();
      stream = undefined;
      await discardLocalMirror();
      reconnectNow();
    },
    kill() {
      reconnectNow();
      if (stream === undefined) throw new Error("stream connection is disposed");
      return stream.stream.kill();
    },
    reset() {
      reconnectNow();
      if (stream === undefined) throw new Error("stream connection is disposed");
      return stream.stream.reset();
    },
    getSnapshot() {
      return snapshot;
    },
    getServerSnapshot() {
      return snapshot;
    },
    subscribe(listener) {
      if (disposeTimer !== undefined) {
        clearTimeout(disposeTimer);
        disposeTimer = undefined;
      }
      listeners.add(listener);
      start();
      return () => {
        listeners.delete(listener);
        if (listeners.size === 0 && !disposed) {
          disposeTimer = setTimeout(() => {
            disposeTimer = undefined;
            if (listeners.size === 0) dispose();
          }, 0);
        }
      };
    },
    [Symbol.dispose]() {
      dispose();
    },
  };
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}
