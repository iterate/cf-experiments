import { RpcTarget, type RpcStub } from "capnweb";
import type { StreamEvent } from "@cf-experiments/shared/event";
import type { StreamRpc, SubscriptionSink } from "../stream-types.js";

export async function withStreamSubscription(args: {
  connection: { rpc: RpcStub<StreamRpc> };
  subscriptionKey: string;
  afterOffset?: number;
  processEventBatch?: (events: StreamEvent[]) => void;
}): Promise<
  AsyncDisposable &
    AsyncIterable<StreamEvent> & {
      waitForEvent<T extends StreamEvent>(args: {
        predicate: (event: StreamEvent) => event is T;
        timeoutMs?: number;
      }): Promise<T>;
    }
> {
  const inbox = messageInbox<StreamEvent>();
  const waiters = new Set<{
    predicate(event: StreamEvent): boolean;
    resolve(event: StreamEvent): void;
    reject(error: unknown): void;
    timeout: ReturnType<typeof setTimeout>;
  }>();
  let handle: { unsubscribe(): void } | undefined;
  const sink = new ClientSubscriptionSink((events) => {
    try {
      args.processEventBatch?.(events);
    } catch (error) {
      inbox.error(error);
      for (const waiter of waiters) waiter.reject(error);
      handle?.unsubscribe();
      return;
    }

    for (const event of events) {
      inbox.push(event);
      // Deleting the current element during Set iteration is safe in JS.
      for (const waiter of waiters) {
        if (!waiter.predicate(event)) continue;
        clearTimeout(waiter.timeout);
        waiters.delete(waiter);
        waiter.resolve(event);
      }
    }
  });

  handle = await args.connection.rpc.subscribe({
    subscriptionKey: args.subscriptionKey,
    sink,
    afterOffset: args.afterOffset,
  });

  const subscription = {
    waitForEvent<T extends StreamEvent>(waitArgs: {
      predicate: (event: StreamEvent) => event is T;
      timeoutMs?: number;
    }) {
      const timeoutMs = waitArgs.timeoutMs ?? 4_000;
      return new Promise<T>((resolve, reject) => {
        const waiter = {
          predicate: waitArgs.predicate as (event: StreamEvent) => boolean,
          resolve: resolve as (event: StreamEvent) => void,
          reject,
          timeout: setTimeout(() => {
            waiters.delete(waiter);
            reject(new Error("Timed out waiting for stream event."));
          }, timeoutMs),
        };
        waiters.add(waiter);
      });
    },
    [Symbol.asyncIterator]() {
      return inbox;
    },
    async [Symbol.asyncDispose]() {
      for (const waiter of waiters) {
        clearTimeout(waiter.timeout);
        waiter.reject(new Error("Stream subscription disposed."));
      }
      waiters.clear();
      inbox.close();
      handle?.unsubscribe();
    },
  };

  return subscription;
}

class ClientSubscriptionSink extends RpcTarget implements SubscriptionSink {
  readonly #processEventBatch: (events: StreamEvent[]) => void;

  constructor(processEventBatch: (events: StreamEvent[]) => void) {
    super();
    this.#processEventBatch = processEventBatch;
  }

  processEventBatch(args: { events: StreamEvent[] }): undefined {
    this.#processEventBatch(args.events);
  }
}

function messageInbox<T>(): AsyncIterableIterator<T> & {
  push(value: T): void;
  close(): void;
  error(error: unknown): void;
} {
  const messages: T[] = [];
  const waiters: PromiseWithResolvers<IteratorResult<T>>[] = [];
  let closed = false;
  let thrown: unknown;
  const inbox = {
    push(value: T) {
      const waiter = waiters.shift();
      if (waiter === undefined) {
        messages.push(value);
      } else {
        waiter.resolve({ done: false, value });
      }
    },
    close() {
      closed = true;
      for (const waiter of waiters.splice(0)) waiter.resolve({ done: true, value: undefined });
    },
    error(error: unknown) {
      thrown = error;
      for (const waiter of waiters.splice(0)) waiter.reject(error);
    },
    next() {
      const value = messages.shift();
      if (value !== undefined) return Promise.resolve({ done: false as const, value });
      if (thrown !== undefined) return Promise.reject(thrown);
      if (closed) return Promise.resolve({ done: true as const, value: undefined });
      const waiter = Promise.withResolvers<IteratorResult<T>>();
      waiters.push(waiter);
      return waiter.promise;
    },
    [Symbol.asyncIterator]() {
      return inbox;
    },
  };
  return inbox;
}
