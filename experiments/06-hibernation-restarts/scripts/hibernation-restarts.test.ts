import { describe, expect, it } from "vitest";

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";
const hibernationWaitMs = Number(process.env.HIBERNATION_WAIT_MS ?? 15_000);
const oomBytes = Number(process.env.OOM_BYTES ?? 256 * 1024 * 1024);
const deployedIt = workerUrl.includes("localhost") ? it.skip : it;
const oomIt = process.env.RUN_OOM_PROBE === "true" ? deployedIt : it.skip;

type PingResult = {
  incarnationId: string;
  sockets: number;
  heldBytes: number;
};

type SocketOutcome =
  | { kind: "responded"; incarnationId: string }
  | { kind: "closed" }
  | { kind: "errored"; error: unknown }
  | { kind: "stale" };

describe("hibernation restart probe", () => {
  deployedIt("keeps one hibernatable websocket connected across idle hibernation", async () => {
    const name = `hibernate-${crypto.randomUUID()}`;

    await using socket = await openProbeSocket(name);
    const before = await ping(name);

    await delay(hibernationWaitMs);

    const pong = await socket.ping();
    expect(pong.incarnationId).not.toBe(before.incarnationId);

    const after = await ping(name);
    expect(after.incarnationId).toBe(pong.incarnationId);
    expect(after.sockets).toBe(1);
  }, 30_000);

  deployedIt("does not keep the old hibernatable websocket in fan-out after ctx.abort", async () => {
    const name = `abort-${crypto.randomUUID()}`;

    await using socket = await openProbeSocket(name);
    const before = await socket.ping();

    await expect(kill(name)).rejects.toThrow();

    const oldSocket = await observeOldSocket(socket, 1_000);
    expect(oldSocket.kind).not.toBe("responded");

    await using fresh = await openProbeSocket(name);
    const after = await fresh.ping();
    expect(after.incarnationId).not.toBe(before.incarnationId);
  });

  oomIt("does not keep the old hibernatable websocket in fan-out after OOM", async () => {
    const name = `oom-${crypto.randomUUID()}`;

    await using socket = await openProbeSocket(name);
    const before = await socket.ping();

    await expect(allocate(name, oomBytes)).rejects.toThrow();

    const oldSocket = await observeOldSocket(socket, 1_000);
    expect(oldSocket.kind).not.toBe("responded");

    await using fresh = await openProbeSocket(name);
    const after = await fresh.ping();
    expect(after.incarnationId).not.toBe(before.incarnationId);
  }, 60_000);
});

async function ping(name: string): Promise<PingResult> {
  const response = await fetch(url("/ping", name));
  if (!response.ok) throw new Error(`ping failed: ${response.status} ${await response.text()}`);
  return (await response.json()) as PingResult;
}

async function kill(name: string): Promise<void> {
  const response = await fetch(url("/kill", name), { method: "POST" });
  if (!response.ok) throw new Error(`kill failed: ${response.status} ${await response.text()}`);
}

async function allocate(name: string, bytes: number): Promise<void> {
  const requestUrl = url("/allocate", name);
  requestUrl.searchParams.set("bytes", String(bytes));
  const response = await fetch(requestUrl, { method: "POST" });
  if (!response.ok) {
    throw new Error(`allocate failed: ${response.status} ${await response.text()}`);
  }
}

async function openProbeSocket(name: string) {
  const webSocket = new WebSocket(toWebSocketUrl(url("/ws", name)));
  const inbox = messageInbox<unknown>();
  let closed = false;
  let errored: unknown;

  webSocket.addEventListener("message", (message) => inbox.push(JSON.parse(String(message.data))));
  webSocket.addEventListener("close", () => {
    closed = true;
    inbox.close();
  });
  webSocket.addEventListener("error", (error) => {
    errored = error;
    inbox.error(error);
  });
  await waitForOpen(webSocket);

  return {
    async ping(timeoutMs = 2_000) {
      const id = crypto.randomUUID();
      webSocket.send(JSON.stringify({ op: "ping", id }));
      while (true) {
        const result = await withTimeout(inbox.next(), timeoutMs);
        if (result.done) throw new Error("WebSocket closed");
        const message = result.value;
        if (isRecord(message) && message.op === "pong" && message.id === id) {
          if (typeof message.incarnationId !== "string") throw new Error("missing incarnationId");
          return {
            incarnationId: message.incarnationId,
            sockets: Number(message.sockets),
          };
        }
      }
    },
    get closed() {
      return closed;
    },
    get errored() {
      return errored;
    },
    async [Symbol.asyncDispose]() {
      if (webSocket.readyState !== WebSocket.CLOSED) webSocket.close();
      await delay(20);
    },
  };
}

async function observeOldSocket(
  socket: Awaited<ReturnType<typeof openProbeSocket>>,
  timeoutMs: number,
): Promise<SocketOutcome> {
  if (socket.closed) return { kind: "closed" };
  if (socket.errored !== undefined) return { kind: "errored", error: socket.errored };
  try {
    const response = await socket.ping(timeoutMs);
    return { kind: "responded", incarnationId: response.incarnationId };
  } catch (error) {
    if (socket.closed) return { kind: "closed" };
    if (socket.errored !== undefined) return { kind: "errored", error: socket.errored };
    if (error instanceof Error && error.message.includes("timed out")) return { kind: "stale" };
    return { kind: "errored", error };
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
      if (waiter === undefined) messages.push(value);
      else waiter.resolve({ done: false, value });
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

function url(path: string, name: string): URL {
  const requestUrl = new URL(path, workerUrl);
  requestUrl.searchParams.set("name", name);
  return requestUrl;
}

function toWebSocketUrl(requestUrl: URL): string {
  const webSocketUrl = new URL(requestUrl);
  if (webSocketUrl.protocol === "http:") webSocketUrl.protocol = "ws:";
  if (webSocketUrl.protocol === "https:") webSocketUrl.protocol = "wss:";
  return webSocketUrl.toString();
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

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  return Promise.race([
    promise,
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error(`timed out after ${ms}ms`)), ms),
    ),
  ]);
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
