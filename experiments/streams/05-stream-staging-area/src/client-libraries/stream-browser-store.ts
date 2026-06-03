import { RpcTarget, type RpcPromise, type RpcStub } from "capnweb";
import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import type { SubscriptionSink, StreamRpc } from "../stream-types.js";
import { acquireWriterRole, type WriterRole } from "./stream-leader.js";
import { withStream, type StreamBrowserConnectionStatus } from "./stream-browser.js";
import {
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
  let stream: ReturnType<typeof withStream> | undefined;
  let subscriptionHandle: { unsubscribe(): void } | undefined;
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
    const localMaxOffset = await streamDatabase.maxOffset();
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
      .then(async () => {
        clearTimeout(followerTimeout);
        if (disposed || stream !== election.connection) return undefined;
        snapshot = { ...snapshot, subscriptionStatus: "leader" };
        emitSnapshot();
        await reconcileLocalMirrorWithServer(election.connection.rpc);
        const afterOffset = await streamDatabase.maxOffset();
        return { afterOffset, sink: new BrowserMirrorSink({ streamDatabase, onStored }) };
      })
      .then((subscription) => {
        if (subscription === undefined || disposed || stream !== election.connection) return undefined;
        return election.connection.rpc.subscribe({
          subscriptionKey: election.subscriptionKey,
          sink: subscription.sink,
          afterOffset: subscription.afterOffset,
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
        reconnectAfter(`subscribe failed: ${errorMessage(error)}`);
      });
  }

  function onStored(events: StreamEvent[]) {
    receivedEventCount += events.length;
    snapshot = { ...snapshot, receivedEventCount };
    emitSnapshot();
  }

  function stopSubscriptionElection() {
    subscriptionHandle?.unsubscribe();
    subscriptionHandle = undefined;
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
    stream?.[Symbol.dispose]();
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
      return stream.rpc.appendBatch(appendArgs);
    },
    async clearLocalDatabase() {
      stopSubscriptionElection();
      stream?.[Symbol.dispose]();
      stream = undefined;
      await discardLocalMirror();
      reconnectNow();
    },
    kill() {
      reconnectNow();
      if (stream === undefined) throw new Error("stream connection is disposed");
      return stream.rpc.kill();
    },
    reset() {
      reconnectNow();
      if (stream === undefined) throw new Error("stream connection is disposed");
      return stream.rpc.reset();
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

class BrowserMirrorSink extends RpcTarget implements SubscriptionSink {
  #tail = Promise.resolve();
  readonly #streamDatabase: StreamBrowserDatabase;
  readonly #onStored: (events: StreamEvent[]) => void;

  constructor(args: {
    streamDatabase: StreamBrowserDatabase;
    onStored(events: StreamEvent[]): void;
  }) {
    super();
    this.#streamDatabase = args.streamDatabase;
    this.#onStored = args.onStored;
  }

  processEventBatch(args: { events: StreamEvent[] }): undefined {
    this.#tail = this.#tail.then(() => this.#store(args.events), () => this.#store(args.events));
  }

  async #store(events: StreamEvent[]) {
    await this.#streamDatabase.insertEventBatch({ events });
    this.#onStored(events);
  }
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}
