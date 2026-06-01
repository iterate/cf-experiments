import { newWebSocketRpcSession, type RpcStub } from "capnweb";
import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import {
  createSimpleStreamProcessorRunner,
  type SimpleStreamProcessor,
  type SimpleStreamProcessorSnapshot,
} from "@cf-experiments/shared/simple-stream-processor";
import type { JonasStreamRpc } from "./jonas-stream.js";
import type { StreamProcessorRpc } from "./stream-processor.js";
import { JonasStreamAckFrame, JonasStreamEventsFrame } from "./jonas-stream-types.js";

const defaultWorkerUrl = process.env.WORKER_URL ?? "http://localhost:8787";

type FetchEndpoint = (request: Request) => Promise<Response>;
type PendingAppend = PromiseWithResolvers<StreamEvent>;

export type JonasStreamMessage = {
  direction: "out" | "in";
  data: string;
};

export type JonasStreamRawClient = AsyncDisposable & {
  wsMessages: JonasStreamMessage[];
  append(event: StreamEventInput): void;
  appendAndWaitForResponse(
    event: StreamEventInput,
    options?: { key?: string },
  ): Promise<StreamEvent>;
  stream(): AsyncIterableIterator<StreamEvent>;
};

export type JonasStreamCapnwebClient = AsyncDisposable & {
  capnweb: RpcStub<JonasStreamRpc>;
};

export type StreamProcessorCapnwebClient = AsyncDisposable & {
  capnweb: RpcStub<StreamProcessorRpc>;
};

export type JonasStreamFixture = AsyncDisposable & {
  raw: JonasStreamRawClient;
  capnweb: RpcStub<JonasStreamRpc>;
  append(args: { event: StreamEventInput }): Promise<StreamEvent>;
  stream(): AsyncIterableIterator<StreamEvent>;
  withProcessor<State, Deps = undefined>(
    processor: SimpleStreamProcessor<State, Deps>,
    options?: { deps?: Deps },
  ): Promise<JonasStreamProcessorFixture<State>>;
};

export type JonasStreamProcessorFixture<State> = AsyncDisposable & {
  snapshot(): SimpleStreamProcessorSnapshot<State>;
  done: Promise<SimpleStreamProcessorSnapshot<State>>;
};

export type JonasStreamEndpoint = {
  path?: string;
  workerUrl?: string;
  url?: string | URL;
  fetch?: FetchEndpoint;
};

export async function withStreamRaw(endpoint: JonasStreamEndpoint): Promise<JonasStreamRawClient> {
  const wsMessages: JonasStreamMessage[] = [];
  const events = messageInbox<StreamEvent>();
  const pendingAppends = new Map<string, PendingAppend>();
  const webSocket = await openWebSocket(endpoint, "raw-ws");
  const fail = (error: Error) => {
    events.error(error);
    for (const pending of pendingAppends.values()) pending.reject(error);
    pendingAppends.clear();
  };

  webSocket.addEventListener("message", (message) => {
    const data = frameText(message.data);
    wsMessages.push({ direction: "in", data });
    const frame: unknown = JSON.parse(data);
    if (isRecord(frame) && frame.op === "append-ack") {
      const ack = JonasStreamAckFrame.parse(frame);
      const pending = pendingAppends.get(ack.appendKey);
      if (pending !== undefined) {
        pendingAppends.delete(ack.appendKey);
        pending.resolve(ack.event);
      }
      return;
    }
    for (const event of JonasStreamEventsFrame.parse(frame).events) {
      events.push(event);
    }
  });
  webSocket.addEventListener(
    "close",
    () => {
      events.close();
      fail(new Error("WebSocket closed"));
    },
    { once: true },
  );
  webSocket.addEventListener("error", () => fail(new Error("WebSocket failed")), { once: true });

  return {
    wsMessages,
    append(event) {
      send(webSocket, wsMessages, { op: "append", event });
    },
    appendAndWaitForResponse(event, { key = crypto.randomUUID() } = {}) {
      const ack = Promise.withResolvers<StreamEvent>();
      pendingAppends.set(key, ack);
      send(webSocket, wsMessages, { op: "append", event, requestAck: { key } });
      return ack.promise;
    },
    stream() {
      send(webSocket, wsMessages, { op: "subscribe" });
      return events;
    },
    async [Symbol.asyncDispose]() {
      await closeWebSocket(webSocket);
    },
  };
}

export async function withStreamCapnweb(
  endpoint: JonasStreamEndpoint,
): Promise<JonasStreamCapnwebClient> {
  const webSocket = await openWebSocket(endpoint, "capnweb");
  const capnweb = newWebSocketRpcSession<JonasStreamRpc>(webSocket);

  return {
    capnweb,
    async [Symbol.asyncDispose]() {
      capnweb[Symbol.dispose]();
      await closeWebSocket(webSocket);
    },
  };
}

export async function withStreamProcessorCapnweb(
  endpoint: JonasStreamEndpoint,
): Promise<StreamProcessorCapnwebClient> {
  const webSocket = await openWebSocket(
    {
      ...endpoint,
      url: streamProcessorUrl(endpoint),
    },
    "capnweb",
  );
  const capnweb = newWebSocketRpcSession<StreamProcessorRpc>(webSocket);

  return {
    capnweb,
    async [Symbol.asyncDispose]() {
      capnweb[Symbol.dispose]();
      await closeWebSocket(webSocket);
    },
  };
}

export async function withStream(endpoint: JonasStreamEndpoint): Promise<JonasStreamFixture> {
  const raw = await withStreamRaw(endpoint);
  const capnwebClient = await withStreamCapnweb(endpoint);

  return {
    raw,
    capnweb: capnwebClient.capnweb,
    append(args) {
      return capnwebClient.capnweb.append(args);
    },
    stream() {
      return raw.stream();
    },
    async withProcessor(processor, options) {
      const processorRaw = await withStreamRaw(endpoint);
      const abort = new AbortController();
      const runner = await createSimpleStreamProcessorRunner({
        processor,
        deps: options?.deps,
        append: (event) => processorRaw.append(event),
        appendAndWait: (event) => processorRaw.appendAndWaitForResponse(event),
        signal: abort.signal,
      });
      const done = runner.run(processorRaw.stream()).finally(async () => {
        await processorRaw[Symbol.asyncDispose]();
      });

      return {
        snapshot: runner.snapshot,
        done,
        async [Symbol.asyncDispose]() {
          abort.abort();
          await processorRaw[Symbol.asyncDispose]();
        },
      };
    },
    async [Symbol.asyncDispose]() {
      await raw[Symbol.asyncDispose]();
      await capnwebClient[Symbol.asyncDispose]();
    },
  };
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

function send(webSocket: WebSocket, wsMessages: JonasStreamMessage[], frame: unknown) {
  const data = JSON.stringify(frame);
  wsMessages.push({ direction: "out", data });
  webSocket.send(data);
}

async function openWebSocket(endpoint: JonasStreamEndpoint, transport: "raw-ws" | "capnweb") {
  const url = streamUrl(endpoint, transport);
  if (endpoint.fetch !== undefined) {
    const response = await endpoint.fetch(new Request(url, { headers: { Upgrade: "websocket" } }));
    const webSocket = response.webSocket;
    if (webSocket === null) throw new Error("endpoint did not return a WebSocket");
    webSocket.accept();
    return webSocket;
  }

  const webSocket = new WebSocket(toWebSocketUrl(url));
  await waitForOpen(webSocket);
  return webSocket;
}

function streamUrl(endpoint: JonasStreamEndpoint, transport: "raw-ws" | "capnweb") {
  const url =
    endpoint.url === undefined
      ? new URL(endpoint.workerUrl ?? defaultWorkerUrl)
      : new URL(endpoint.url);
  if (endpoint.url === undefined) url.pathname = `/jonas/${endpoint.path ?? "default"}`;
  if (transport !== "capnweb") url.searchParams.set("transport", transport);
  return url;
}

function streamProcessorUrl(endpoint: JonasStreamEndpoint) {
  const url =
    endpoint.url === undefined
      ? new URL(endpoint.workerUrl ?? defaultWorkerUrl)
      : new URL(endpoint.url);
  if (endpoint.url === undefined) url.pathname = `/stream-processor/${endpoint.path ?? "default"}`;
  return url;
}

function toWebSocketUrl(url: URL): string {
  url = new URL(url);
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";
  return url.toString();
}

function waitForOpen(webSocket: WebSocket): Promise<void> {
  if (webSocket.readyState === WebSocket.OPEN) return Promise.resolve();
  return new Promise((resolve, reject) => {
    webSocket.addEventListener("open", () => resolve(), { once: true });
    webSocket.addEventListener("error", () => reject(new Error("WebSocket failed")), {
      once: true,
    });
  });
}

function closeWebSocket(webSocket: WebSocket): Promise<void> {
  if (webSocket.readyState === WebSocket.CLOSED) return Promise.resolve();
  return new Promise((resolve) => {
    webSocket.addEventListener("close", () => resolve(), { once: true });
    webSocket.close();
    setTimeout(resolve, 100);
  });
}

function frameText(data: unknown): string {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (ArrayBuffer.isView(data)) return new TextDecoder().decode(data);
  throw new TypeError(`unexpected WebSocket frame data: ${String(data)}`);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
