import { createORPCClient, type Client } from "@orpc/client";
import { RPCLink as ORPCWebSocketLink } from "@orpc/client/websocket";
import { signDurableIteratorToken } from "@orpc/experimental-durable-iterator";
import { newWebSocketRpcSession, type RpcStub } from "capnweb";
import {
  StreamEvent as StreamEventSchema,
  type StreamEvent,
  type StreamEventInput,
} from "@cf-experiments/shared/event";
import { CLEAN_STREAM_ORPC_SIGNING_KEY, type CleanStreamRpc } from "./protocol.js";

export type CleanStreamTransport = "capnweb" | "orpc" | "rawws";

export type CleanStreamEndpoint =
  | {
      url: string | URL;
      fetch?: undefined;
    }
  | {
      fetch: (request: Request) => Promise<Response>;
      url?: string | URL;
    };

export type CleanStreamClient = AsyncDisposable & {
  transport: CleanStreamTransport;
  append(event: StreamEventInput): Promise<StreamEvent>;
  subscribe(): Promise<CleanStreamSubscription>;
};

export type CleanStreamSubscription = AsyncDisposable & {
  read(timeoutMs?: number): Promise<StreamEvent>;
};

type OrpcDurableIteratorClient = {
  subscribe: Client<Record<never, never>, undefined, AsyncIterator<StreamEvent>, unknown>;
};

export async function connectCleanStream(args: {
  transport: CleanStreamTransport;
  endpoint: CleanStreamEndpoint;
}): Promise<CleanStreamClient> {
  if (args.transport === "capnweb") return connectCleanCapnwebStream(args.endpoint);
  if (args.transport === "orpc") return connectCleanOrpcStream(args.endpoint);
  return connectCleanRawwsStream(args.endpoint);
}

export async function connectCleanCapnwebStream(
  endpoint: CleanStreamEndpoint,
): Promise<CleanStreamClient> {
  const webSocket = await openWebSocket(endpoint, "capnweb");
  const rpc = newWebSocketRpcSession<CleanStreamRpc>(webSocket);
  return {
    transport: "capnweb",
    append(event) {
      return Promise.resolve(rpc.append({ event }));
    },
    async subscribe() {
      const readable = await rpc.subscribe();
      const reader = (readable as unknown as ReadableStream<StreamEvent>).getReader();
      return {
        async read(timeoutMs = 1_000) {
          const result = await withTimeout(reader.read(), timeoutMs);
          if (result.done) throw new Error("capnweb stream ended before an event arrived");
          return result.value;
        },
        async [Symbol.asyncDispose]() {
          await reader.cancel();
          reader.releaseLock();
        },
      };
    },
    async [Symbol.asyncDispose]() {
      rpc[Symbol.dispose]();
      await closeWebSocket(webSocket);
    },
  };
}

export async function connectCleanRawwsStream(
  endpoint: CleanStreamEndpoint,
): Promise<CleanStreamClient> {
  const webSocket = await openWebSocket(endpoint, "rawws");
  const inbox = new MessageInbox();
  webSocket.addEventListener("message", (event) => {
    inbox.push(JSON.parse(describeWebSocketFrameData(event.data)));
  });

  return {
    transport: "rawws",
    async append(event) {
      const requestId = crypto.randomUUID();
      webSocket.send(JSON.stringify({ op: "append", requestId, event }));
      while (true) {
        const message = await inbox.read(1_000);
        if (isRecord(message) && message.op === "ack" && message.requestId === requestId) {
          return StreamEventSchema.parse(message.event);
        }
      }
    },
    async subscribe() {
      webSocket.send(JSON.stringify({ op: "subscribe" }));
      await waitForRawOp(inbox, "subscribed", 1_000);
      return {
        async read(timeoutMs = 1_000) {
          const message = await waitForRawOp(inbox, "event", timeoutMs);
          return readEventFrame(message);
        },
        async [Symbol.asyncDispose]() {},
      };
    },
    async [Symbol.asyncDispose]() {
      await closeWebSocket(webSocket);
    },
  };
}

export async function connectCleanOrpcStream(
  endpoint: CleanStreamEndpoint,
): Promise<CleanStreamClient> {
  const webSocket = await openWebSocket(endpoint, "orpc", await orpcTokenParams());
  const client = createORPCClient<OrpcDurableIteratorClient>(
    new ORPCWebSocketLink({ websocket: webSocket }),
  );
  return {
    transport: "orpc",
    async append(event) {
      const url = endpointUrl(endpoint);
      url.searchParams.set("transport", "orpc");
      url.searchParams.set("op", "append");
      const response = await endpointFetch(endpoint)(
        new Request(url, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ event }),
        }),
      );
      if (!response.ok) throw new Error(`orpc append failed: ${await response.text()}`);
      return StreamEventSchema.parse(await response.json());
    },
    async subscribe() {
      const iterator = await client.subscribe();
      return {
        async read(timeoutMs = 1_000) {
          const result = await withTimeout(iterator.next(), timeoutMs);
          if (result.done) throw new Error("orpc iterator ended before an event arrived");
          return result.value;
        },
        async [Symbol.asyncDispose]() {
          await withTimeout(Promise.resolve(iterator.return?.()).then(() => undefined), 100).catch(
            () => undefined,
          );
        },
      };
    },
    async [Symbol.asyncDispose]() {
      await closeWebSocket(webSocket);
    },
  };
}

async function openWebSocket(
  endpoint: CleanStreamEndpoint,
  transport: CleanStreamTransport,
  extraParams: Record<string, string> = {},
): Promise<WebSocket> {
  const url = endpointUrl(endpoint);
  url.searchParams.set("transport", transport);
  for (const [key, value] of Object.entries(extraParams)) {
    url.searchParams.set(key, value);
  }

  if (endpoint.fetch !== undefined) {
    const response = await endpoint.fetch(new Request(url, { headers: { Upgrade: "websocket" } }));
    const webSocket = response.webSocket;
    if (webSocket === null) throw new Error(`${transport} endpoint did not return a WebSocket`);
    webSocket.accept();
    return webSocket;
  }

  const webSocket = new WebSocket(toWebSocketUrl(url));
  await waitForWebSocketOpen(webSocket);
  return webSocket;
}

function endpointUrl(endpoint: CleanStreamEndpoint): URL {
  return new URL(endpoint.url ?? "https://clean-stream.internal/");
}

function endpointFetch(endpoint: CleanStreamEndpoint): (request: Request) => Promise<Response> {
  if (endpoint.fetch !== undefined) return endpoint.fetch;
  return fetch;
}

async function orpcTokenParams(): Promise<Record<string, string>> {
  const nowInSeconds = Math.floor(Date.now() / 1_000);
  return {
    id: crypto.randomUUID(),
    token: await signDurableIteratorToken(CLEAN_STREAM_ORPC_SIGNING_KEY, {
      chn: "clean-stream",
      iat: nowInSeconds,
      exp: nowInSeconds + 60 * 60,
    }),
  };
}

async function waitForRawOp(inbox: MessageInbox, op: string, timeoutMs: number) {
  while (true) {
    const message = await inbox.read(timeoutMs);
    if (isRecord(message) && message.op === op) return message;
  }
}

function readEventFrame(message: unknown): StreamEvent {
  if (!isRecord(message)) throw new Error(`expected event frame, got ${JSON.stringify(message)}`);
  return StreamEventSchema.parse(message.event);
}

class MessageInbox {
  #messages: unknown[] = [];
  #waiters: ((message: unknown) => void)[] = [];

  push(message: unknown) {
    const waiter = this.#waiters.shift();
    if (waiter === undefined) {
      this.#messages.push(message);
    } else {
      waiter(message);
    }
  }

  read(timeoutMs: number): Promise<unknown> {
    const message = this.#messages.shift();
    if (message !== undefined) return Promise.resolve(message);
    return withTimeout(
      new Promise((resolve) => {
        this.#waiters.push(resolve);
      }),
      timeoutMs,
    );
  }
}

function toWebSocketUrl(url: URL): string {
  const webSocketUrl = new URL(url);
  if (webSocketUrl.protocol === "http:") webSocketUrl.protocol = "ws:";
  if (webSocketUrl.protocol === "https:") webSocketUrl.protocol = "wss:";
  return webSocketUrl.toString();
}

function waitForWebSocketOpen(webSocket: WebSocket): Promise<void> {
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

function closeWebSocket(webSocket: WebSocket): Promise<void> {
  if (webSocket.readyState === WebSocket.CLOSED) return Promise.resolve();
  return new Promise<void>((resolve) => {
    let resolved = false;
    const finish = () => {
      if (resolved) return;
      resolved = true;
      resolve();
    };
    webSocket.addEventListener("close", finish, { once: true });
    webSocket.close();
    setTimeout(finish, 100);
  });
}

function describeWebSocketFrameData(data: unknown): string {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (ArrayBuffer.isView(data)) return new TextDecoder().decode(data);
  throw new TypeError(`unexpected WebSocket frame data: ${String(data)}`);
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
