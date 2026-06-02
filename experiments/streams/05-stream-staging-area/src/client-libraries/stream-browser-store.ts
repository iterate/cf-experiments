import { RpcTarget, type RpcPromise } from "capnweb";
import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import { withStream, type StreamBrowserConnectionStatus } from "./stream-browser.js";
import {
  getStreamBrowserDatabase,
  type StreamDatabaseInfo,
  type StreamDatabaseWriteMode,
} from "./stream-browser-db.js";
import type { SubscriptionSink } from "../stream-types.js";

export type StreamBrowserSnapshot = {
  connectionStatus: StreamBrowserConnectionStatus | "subscribing" | "subscribed";
  databaseInfo: StreamDatabaseInfo | undefined;
};

export type StreamBrowserStore = Disposable & {
  appendBatch(args: { events: StreamEventInput[] }): RpcPromise<StreamEvent[]>;
  kill(): RpcPromise<void>;
  getSnapshot(): StreamBrowserSnapshot;
  getServerSnapshot(): StreamBrowserSnapshot;
  subscribe(listener: () => void): () => void;
};

/** Creates a lazy browser stream store for React's `useSyncExternalStore`. */
export function createStreamBrowserStore(args: {
  streamPath: string;
  sqliteWriteMode: StreamDatabaseWriteMode;
  onDispose?: () => void;
}): StreamBrowserStore {
  let stream: ReturnType<typeof withStream> | undefined;
  let subscriptionHandle: { unsubscribe(): void } | undefined;
  let connectTimer: ReturnType<typeof setTimeout> | undefined;
  const listeners = new Set<() => void>();
  let disposed = false;
  let writeQueue = Promise.resolve();
  let writeFailed = false;
  const streamDatabase = getStreamBrowserDatabase(args.streamPath);
  let snapshot: StreamBrowserSnapshot = {
    connectionStatus: "connecting",
    databaseInfo: undefined,
  };

  function emitSnapshot() {
    for (const listener of listeners) listener();
  }

  function connect() {
    if (stream !== undefined || disposed) return;

    const streamUrl = new URL(
      `/stream/${encodeURIComponent(args.streamPath)}`,
      window.location.href,
    );
    const subscriptionKey = `browser:${crypto.randomUUID()}`;
    const sink = new BrowserSubscriptionSink((events) => {
      if (writeFailed) return;
      writeQueue = writeQueue
        .then(async () => {
          await streamDatabase.insertEventBatch({
            events,
            writeMode: args.sqliteWriteMode,
          });
          if (disposed) return;
          snapshot = {
            ...snapshot,
            databaseInfo: await streamDatabase.info(),
          };
          emitSnapshot();
        })
        .catch((error: unknown) => {
          console.error("Browser stream SQLite write failed", error);
          writeFailed = true;
          subscriptionHandle?.unsubscribe();
          stream?.[Symbol.dispose]();
          stream = undefined;
          snapshot = { ...snapshot, connectionStatus: "error" };
          emitSnapshot();
        });
    });

    stream = withStream({
      url: streamUrl,
      onConnectionStatusChange(connectionStatus) {
        if (disposed) return;
        if (connectionStatus === "closed" || connectionStatus === "error") {
          subscriptionHandle = undefined;
          stream = undefined;
        }
        snapshot = {
          ...snapshot,
          connectionStatus,
        };
        emitSnapshot();
      },
    });

    void stream.rpc
      .subscribe({ subscriptionKey, sink })
      .then((handle) => {
        if (disposed) {
          handle.unsubscribe();
          return;
        }
        subscriptionHandle = handle;
        snapshot = { ...snapshot, connectionStatus: "subscribed" };
        emitSnapshot();
      })
      .catch(() => {
        if (disposed) return;
        snapshot = { ...snapshot, connectionStatus: "error" };
        emitSnapshot();
      });
  }

  return {
    appendBatch(appendArgs) {
      if (connectTimer !== undefined) {
        clearTimeout(connectTimer);
        connectTimer = undefined;
      }
      connect();
      if (stream === undefined) throw new Error("stream connection is disposed");
      return stream.rpc.appendBatch(appendArgs);
    },
    kill() {
      if (connectTimer !== undefined) {
        clearTimeout(connectTimer);
        connectTimer = undefined;
      }
      connect();
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
        writeFailed = false;
        snapshot = { ...snapshot, connectionStatus: "subscribing" };
        emitSnapshot();

        void streamDatabase.info().then((databaseInfo) => {
          if (disposed) return;
          snapshot = { ...snapshot, databaseInfo };
          emitSnapshot();
        });

        connectTimer = setTimeout(() => {
          connectTimer = undefined;
          connect();
        }, 0);
      }

      return () => {
        listeners.delete(listener);
        if (listeners.size === 0) {
          if (!disposed) {
            disposed = true;
            if (connectTimer !== undefined) {
              clearTimeout(connectTimer);
              connectTimer = undefined;
            }
            subscriptionHandle?.unsubscribe();
            subscriptionHandle = undefined;
            stream?.[Symbol.dispose]();
            stream = undefined;
            args.onDispose?.();
          }
        }
      };
    },
    [Symbol.dispose]() {
      listeners.clear();
      if (!disposed) {
        disposed = true;
        if (connectTimer !== undefined) {
          clearTimeout(connectTimer);
          connectTimer = undefined;
        }
        subscriptionHandle?.unsubscribe();
        subscriptionHandle = undefined;
        stream?.[Symbol.dispose]();
        stream = undefined;
        args.onDispose?.();
      }
    },
  };
}

class BrowserSubscriptionSink extends RpcTarget implements SubscriptionSink {
  readonly #processEventBatch: (events: StreamEvent[]) => void;

  constructor(processEventBatch: (events: StreamEvent[]) => void) {
    super();
    this.#processEventBatch = processEventBatch;
  }

  processEventBatch(args: { events: StreamEvent[] }): undefined {
    // Do not await SQLite here. The stream deliberately sends event batches one-way;
    // awaiting would add subscriber-originated ack traffic to the hot path.
    this.#processEventBatch(args.events);
  }
}
