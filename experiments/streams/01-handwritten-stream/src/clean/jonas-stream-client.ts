import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import { JonasStreamAckFrame, JonasStreamEventFrame } from "./jonas-stream-types.js";

const defaultWorkerUrl = process.env.WORKER_URL ?? "http://localhost:8787";

type FetchEndpoint = (request: Request) => Promise<Response>;
type PendingAppend = PromiseWithResolvers<StreamEvent>;

export type JonasStreamMessage = {
  direction: "out" | "in";
  data: string;
};

export type JonasStreamClient = AsyncDisposable & {
  wsMessages: JonasStreamMessage[];
  append(event: StreamEventInput): void;
  appendAndWaitForResponse(
    event: StreamEventInput,
    options?: { key?: string },
  ): Promise<StreamEvent>;
  stream(): AsyncIterableIterator<StreamEvent>;
};

export type JonasStreamEndpoint = {
  path?: string;
  workerUrl?: string;
  url?: string | URL;
  fetch?: FetchEndpoint;
};

export async function withStream(endpoint: JonasStreamEndpoint): Promise<JonasStreamClient> {
  const wsMessages: JonasStreamMessage[] = [];
  const events = messageInbox<StreamEvent>();
  const pendingAppends = new Map<string, PendingAppend>();
  const webSocket = await openWebSocket(endpoint);
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
    events.push(JonasStreamEventFrame.parse(frame).event);
  });
  webSocket.addEventListener("close", () => {
    events.close();
    fail(new Error("WebSocket closed"));
  }, { once: true });
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
      send(webSocket, wsMessages, { op: "start" });
      return events;
    },
    async [Symbol.asyncDispose]() {
      await closeWebSocket(webSocket);
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

async function openWebSocket(endpoint: JonasStreamEndpoint) {
  const url = streamUrl(endpoint);
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

function streamUrl(endpoint: JonasStreamEndpoint) {
  if (endpoint.url !== undefined) return new URL(endpoint.url);
  const url = new URL(endpoint.workerUrl ?? defaultWorkerUrl);
  url.pathname = `/jonas/${endpoint.path ?? "default"}`;
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
