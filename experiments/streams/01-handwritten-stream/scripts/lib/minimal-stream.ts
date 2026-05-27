/**
 * Test fixture for the minimal JSON-over-WebSocket baseline stream.
 *
 *   await using subscriber = await withMinimalStream({ path: "my-stream" });
 *   await subscriber.subscribe();
 */

import {
  StreamEvent as StreamEventSchema,
  type StreamEvent,
  type StreamEventInput,
} from "@cf-experiments/shared/event";

const defaultWorkerUrl = process.env.WORKER_URL ?? "http://localhost:8787";

export type MinimalWsMessage = {
  direction: "out" | "in";
  data: string;
  tMs: number;
};

export type MinimalStreamFixture = AsyncDisposable & {
  wsMessages: MinimalWsMessage[];
  send(message: unknown): void;
  subscribe(): Promise<void>;
  append(args: { event: StreamEventInput; requestId?: string }): Promise<StreamEvent>;
  read(timeoutMs?: number): Promise<unknown>;
  readEvent(timeoutMs?: number): Promise<StreamEvent>;
};

export async function withMinimalStream({
  path,
  workerUrl = defaultWorkerUrl,
}: {
  path: string;
  workerUrl?: string;
}): Promise<MinimalStreamFixture> {
  const startedAt = performance.now();
  const wsMessages: MinimalWsMessage[] = [];
  const webSocket = newRecordingWebSocket(toWebSocketUrl(workerUrl, path), wsMessages, startedAt);
  await waitForWebSocketOpen(webSocket);

  const messages: unknown[] = [];
  const waiters: ((message: unknown) => void)[] = [];
  webSocket.addEventListener("message", (event) => {
    const message = JSON.parse(describeWebSocketFrameData(event.data));
    const waiter = waiters.shift();
    if (waiter === undefined) {
      messages.push(message);
    } else {
      waiter(message);
    }
  });

  const fixture: MinimalStreamFixture = {
    wsMessages,
    send(message) {
      webSocket.send(JSON.stringify(message));
    },
    async subscribe() {
      fixture.send({ op: "subscribe" });
      await expectOp(fixture.read(), "subscribed");
    },
    async append({ event, requestId = crypto.randomUUID() }) {
      fixture.send({ op: "append", requestId, event });
      const ack = await expectOp(fixture.read(), "ack");
      if (!isRecord(ack) || ack.event === undefined) {
        throw new Error("minimal stream ack missing event");
      }
      return StreamEventSchema.parse(ack.event);
    },
    read(timeoutMs = 1_000) {
      const message = messages.shift();
      if (message !== undefined) return Promise.resolve(message);
      return withTimeout(
        new Promise((resolve) => {
          waiters.push(resolve);
        }),
        timeoutMs,
      );
    },
    async readEvent(timeoutMs = 1_000) {
      const message = await expectOp(fixture.read(timeoutMs), "event");
      if (!isRecord(message) || message.event === undefined) {
        throw new Error("minimal stream event frame missing event");
      }
      return StreamEventSchema.parse(message.event);
    },
    async [Symbol.asyncDispose]() {
      await closeWebSocket(webSocket);
    },
  };

  return fixture;
}

async function expectOp(promise: Promise<unknown>, op: string) {
  const message = await promise;
  if (!isRecord(message) || message.op !== op) {
    throw new Error(`expected minimal stream op ${op}, got ${JSON.stringify(message)}`);
  }
  return message;
}

function newRecordingWebSocket(
  url: string,
  wsMessages: MinimalWsMessage[],
  startedAt: number,
) {
  const webSocket = new WebSocket(url);
  const send = webSocket.send.bind(webSocket);

  webSocket.send = ((data: Parameters<WebSocket["send"]>[0]) => {
    wsMessages.push({
      direction: "out",
      data: describeWebSocketFrameData(data),
      tMs: performance.now() - startedAt,
    });
    return send(data);
  }) as WebSocket["send"];

  webSocket.addEventListener("message", (event) => {
    wsMessages.push({
      direction: "in",
      data: describeWebSocketFrameData(event.data),
      tMs: performance.now() - startedAt,
    });
  });

  return webSocket;
}

function describeWebSocketFrameData(data: unknown) {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (ArrayBuffer.isView(data)) return new TextDecoder().decode(data);
  throw new TypeError(`unexpected WebSocket frame data: ${String(data)}`);
}

function toWebSocketUrl(raw: string, path: string) {
  const url = new URL(raw);
  url.pathname = `/minimal/${path}`;
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";
  return url.toString();
}

function waitForWebSocketOpen(webSocket: WebSocket) {
  if (webSocket.readyState === WebSocket.OPEN) return Promise.resolve();
  return new Promise<void>((resolve, reject) => {
    webSocket.addEventListener("open", () => resolve(), { once: true });
    webSocket.addEventListener(
      "error",
      () => reject(new Error("WebSocket connection failed")),
      { once: true },
    );
  });
}

function closeWebSocket(webSocket: WebSocket) {
  if (webSocket.readyState === WebSocket.CLOSED) return Promise.resolve();
  return new Promise<void>((resolve) => {
    webSocket.addEventListener("close", () => resolve(), { once: true });
    webSocket.close();
  });
}

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  return Promise.race([
    promise,
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error(`timed out after ${ms}ms`)), ms),
    ),
  ]);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
