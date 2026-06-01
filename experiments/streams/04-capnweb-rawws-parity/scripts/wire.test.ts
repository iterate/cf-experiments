import { newWebSocketRpcSession, RpcTarget } from "capnweb";
import { describe, expect, it } from "vitest";

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";

const DISPOSE = Symbol.for("dispose");

type EventInput = {
  frameId: string;
  payload: string;
};

type StreamEvent = EventInput & {
  offset: number;
  createdAt: string;
};

type StreamRpc = {
  append(args: { event: EventInput }): StreamEvent;
  subscribeEvent(args?: unknown): void;
  subscribeProcessEvents(args?: unknown): void;
  subscribeBatch(args?: { batchMs?: number }): void;
  subscribeProcessEventsBatch(args?: { batchMs?: number }): void;
};

type WsMessage = {
  direction: "in" | "out";
  data: string;
};

describe("capnweb rawws parity wire shape", () => {
  it("capnweb-event sends event callbacks without subscriber-originated frames", async () => {
    const path = `wire-event-${crypto.randomUUID()}`;
    const subscriber = await connectCapnweb(path);
    await subscriber.rpc.subscribeEvent();
    const afterSubscribe = subscriber.frames.length;

    const publisher = await connectCapnweb(path);
    const appended = await Promise.all([
      publisher.rpc.append({ event: { frameId: "p0-f1", payload: "x" } }),
      publisher.rpc.append({ event: { frameId: "p0-f2", payload: "x" } }),
    ]);
    await waitFor(() => subscriber.delivered.length === 2, 1_000);

    expect(subscriber.delivered).toEqual(appended);
    expect(outboundFramesAfter(subscriber.frames, afterSubscribe)).toEqual([]);
    expect(pushMethodNamesAfter(subscriber.frames, afterSubscribe)).toEqual([
      "afterAppend",
      "afterAppend",
    ]);

    await publisher.dispose();
    await subscriber.dispose();
  });

  it("capnweb-batch batches callbacks and still has no subscriber-originated frames", async () => {
    const path = `wire-batch-${crypto.randomUUID()}`;
    const subscriber = await connectCapnweb(path);
    await subscriber.rpc.subscribeBatch({ batchMs: 100 });
    const afterSubscribe = subscriber.frames.length;

    const publisher = await connectCapnweb(path);
    const appended = await Promise.all([
      publisher.rpc.append({ event: { frameId: "p0-f1", payload: "x" } }),
      publisher.rpc.append({ event: { frameId: "p0-f2", payload: "x" } }),
    ]);
    await waitFor(() => subscriber.delivered.length === 2, 1_000);
    await sleep(25);

    expect(subscriber.delivered).toEqual(appended);
    expect(outboundFramesAfter(subscriber.frames, afterSubscribe)).toEqual([]);
    expect(pushMethodNamesAfter(subscriber.frames, afterSubscribe)).toEqual(["afterAppendBatch"]);

    await publisher.dispose();
    await subscriber.dispose();
  });

  it("capnweb-process-events sends array callbacks per event without subscriber-originated frames", async () => {
    const path = `wire-process-events-${crypto.randomUUID()}`;
    const subscriber = await connectCapnweb(path);
    await subscriber.rpc.subscribeProcessEvents();
    const afterSubscribe = subscriber.frames.length;

    const publisher = await connectCapnweb(path);
    const appended = await Promise.all([
      publisher.rpc.append({ event: { frameId: "p0-f1", payload: "x" } }),
      publisher.rpc.append({ event: { frameId: "p0-f2", payload: "x" } }),
    ]);
    await waitFor(() => subscriber.delivered.length === 2, 1_000);

    expect(subscriber.delivered).toEqual(appended);
    expect(outboundFramesAfter(subscriber.frames, afterSubscribe)).toEqual([]);
    expect(pushMethodNamesAfter(subscriber.frames, afterSubscribe)).toEqual([
      "processEvents",
      "processEvents",
    ]);
    expect(pushEventCountsAfter(subscriber.frames, afterSubscribe)).toEqual([1, 1]);

    await publisher.dispose();
    await subscriber.dispose();
  });

  it("capnweb-process-events-batch sends batched processEvents callbacks without subscriber-originated frames", async () => {
    const path = `wire-process-events-batch-${crypto.randomUUID()}`;
    const subscriber = await connectCapnweb(path);
    await subscriber.rpc.subscribeProcessEventsBatch({ batchMs: 100 });
    const afterSubscribe = subscriber.frames.length;

    const publisher = await connectCapnweb(path);
    const appended = await Promise.all([
      publisher.rpc.append({ event: { frameId: "p0-f1", payload: "x" } }),
      publisher.rpc.append({ event: { frameId: "p0-f2", payload: "x" } }),
    ]);
    await waitFor(() => subscriber.delivered.length === 2, 1_000);
    await sleep(25);

    expect(subscriber.delivered).toEqual(appended);
    expect(outboundFramesAfter(subscriber.frames, afterSubscribe)).toEqual([]);
    expect(pushMethodNamesAfter(subscriber.frames, afterSubscribe)).toEqual(["processEvents"]);
    expect(pushEventCountsAfter(subscriber.frames, afterSubscribe)).toEqual([2]);

    await publisher.dispose();
    await subscriber.dispose();
  });
});

class ClientMain extends RpcTarget {
  delivered: StreamEvent[] = [];

  afterAppend(args: { event: StreamEvent }): undefined {
    this.delivered.push(args.event);
  }

  afterAppendBatch(args: { events: StreamEvent[] }): undefined {
    this.delivered.push(...args.events);
  }

  processEvents(args: { events: StreamEvent[] }): undefined {
    this.delivered.push(...args.events);
  }
}

async function connectCapnweb(path: string) {
  const frames: WsMessage[] = [];
  const clientMain = new ClientMain();
  const webSocket = recordingWebSocket(webSocketUrl(path), frames);
  await waitForOpen(webSocket);
  const rpc = newWebSocketRpcSession<StreamRpc>(webSocket, clientMain);
  return {
    frames,
    rpc,
    get delivered() {
      return clientMain.delivered;
    },
    async dispose() {
      disposeIfPresent(clientMain);
      disposeIfPresent(rpc);
      await closeWebSocket(webSocket);
    },
  };
}

function webSocketUrl(path: string): string {
  const url = new URL(workerUrl);
  url.pathname = `/${path}`;
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";
  return url.toString();
}

function recordingWebSocket(url: string, frames: WsMessage[]) {
  const webSocket = new WebSocket(url);
  const send = webSocket.send.bind(webSocket);
  webSocket.send = (data: Parameters<WebSocket["send"]>[0]) => {
    frames.push({ direction: "out", data: frameData(data) });
    return send(data);
  };
  webSocket.addEventListener("message", (event) => {
    frames.push({ direction: "in", data: frameData(event.data) });
  });
  return webSocket;
}

function outboundFramesAfter(frames: WsMessage[], afterFrame: number) {
  return frames
    .slice(afterFrame)
    .filter((frame) => frame.direction === "out")
    .map((frame) => JSON.parse(frame.data));
}

function pushMethodNamesAfter(frames: WsMessage[], afterFrame: number) {
  return frames
    .slice(afterFrame)
    .filter((frame) => frame.direction === "in")
    .map((frame) => JSON.parse(frame.data))
    .filter((frame) => Array.isArray(frame) && frame[0] === "push")
    .map((frame) => frame[1][2][0]);
}

function pushEventCountsAfter(frames: WsMessage[], afterFrame: number) {
  return frames
    .slice(afterFrame)
    .filter((frame) => frame.direction === "in")
    .map((frame) => JSON.parse(frame.data))
    .filter((frame) => Array.isArray(frame) && frame[0] === "push")
    .map((frame) => {
      const args = frame[1][3];
      return args[0].events[0].length;
    });
}

function waitForOpen(webSocket: WebSocket): Promise<void> {
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
    webSocket.addEventListener("close", () => resolve(), { once: true });
    webSocket.close();
    setTimeout(resolve, 100);
  });
}

async function waitFor(predicate: () => boolean, timeoutMs: number): Promise<void> {
  const startedAt = performance.now();
  while (performance.now() - startedAt < timeoutMs) {
    if (predicate()) return;
    await sleep(10);
  }
  throw new Error(`timed out after ${timeoutMs}ms`);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function frameData(data: unknown): string {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (ArrayBuffer.isView(data)) return new TextDecoder().decode(data);
  throw new TypeError(`unexpected frame data: ${String(data)}`);
}

function disposeIfPresent(value: unknown): void {
  if ((typeof value !== "object" || value === null) && typeof value !== "function") return;
  const dispose = Reflect.get(value, DISPOSE);
  if (typeof dispose === "function") Reflect.apply(dispose, value, []);
}
