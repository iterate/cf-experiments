import { RpcTarget } from "capnweb";
import type { StreamEvent } from "@cf-experiments/shared/event";
import { withStream, type StreamBrowserConnectionStatus } from "./stream-browser.js";
import type { SubscriptionRpcTarget } from "../stream-types.js";

export type StreamBrowserSnapshot = {
  connectionStatus: StreamBrowserConnectionStatus | "subscribing" | "subscribed";
  events: StreamEvent[];
};

export type StreamBrowserStore = Disposable & {
  getSnapshot(): StreamBrowserSnapshot;
  getServerSnapshot(): StreamBrowserSnapshot;
  subscribe(listener: () => void): () => void;
};

/** Creates a lazy browser stream store for React's `useSyncExternalStore`. */
export function createStreamBrowserStore(args: {
  streamPath: string;
  onDispose?: () => void;
}): StreamBrowserStore {
  let stream: ReturnType<typeof withStream> | undefined;
  const listeners = new Set<() => void>();
  let disposed = false;
  let snapshot: StreamBrowserSnapshot = {
    connectionStatus: "connecting",
    events: [],
  };

  return {
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
        const streamUrl = new URL(
          `/stream/${encodeURIComponent(args.streamPath)}`,
          window.location.href,
        );
        const subscriptionRpcTarget = new BrowserSubscriptionRpcTarget((events) => {
          snapshot = {
            ...snapshot,
            events: [...snapshot.events, ...events],
          };
          for (const listener of listeners) listener();
        });

        stream = withStream({
          url: streamUrl,
          onConnectionStatusChange(connectionStatus) {
            snapshot = {
              ...snapshot,
              connectionStatus,
            };
            for (const listener of listeners) listener();
          },
        });

        snapshot = { ...snapshot, connectionStatus: "subscribing" };
        for (const listener of listeners) listener();

        void stream.rpc
          .initInboundSubscription({ subscriptionRpcTarget })
          .then(() => {
            snapshot = { ...snapshot, connectionStatus: "subscribed" };
            for (const listener of listeners) listener();
          })
          .catch(() => {
            snapshot = { ...snapshot, connectionStatus: "error" };
            for (const listener of listeners) listener();
          });
      }

      return () => {
        listeners.delete(listener);
        if (listeners.size === 0) {
          if (!disposed) {
            disposed = true;
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
        stream?.[Symbol.dispose]();
        stream = undefined;
        args.onDispose?.();
      }
    },
  };
}

class BrowserSubscriptionRpcTarget extends RpcTarget implements SubscriptionRpcTarget {
  readonly #consumeEvents: (events: StreamEvent[]) => void;

  constructor(consumeEvents: (events: StreamEvent[]) => void) {
    super();
    this.#consumeEvents = consumeEvents;
  }

  consumeEvents(args: { events: StreamEvent[] }): undefined {
    this.#consumeEvents(args.events);
  }
}
