import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import { newWebSocketRpcSession, RpcTarget } from "capnweb";
import { describe, expect, it } from "vitest";
import {
  connectCleanCapnwebOneWayStream,
  connectCleanCapnwebStream,
  connectCleanOrpcStream,
  connectCleanRawwsStream,
  connectCleanStream,
  type CleanStreamClient,
  type CleanStreamEndpoint,
  type CleanStreamTransport,
} from "../src/clean/client.js";
import type { CleanStreamEventSink, CleanStreamRpc } from "../src/clean/protocol.js";

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";
const transports = [
  { transport: "capnweb", connect: connectCleanCapnwebStream },
  { transport: "capnweb-oneway", connect: connectCleanCapnwebOneWayStream },
  { transport: "orpc", connect: connectCleanOrpcStream },
  { transport: "rawws", connect: connectCleanRawwsStream },
] as const satisfies readonly {
  transport: CleanStreamTransport;
  connect: (endpoint: CleanStreamEndpoint) => Promise<CleanStreamClient>;
}[];

type SmokeResult = {
  transport: CleanStreamTransport;
  matched: boolean;
  appended: unknown;
  delivered: unknown;
};

type WsMessage = {
  direction: "in" | "out";
  data: string;
};

function cleanStreamUrl(path: string) {
  return new URL(`/clean/${path}`, workerUrl);
}

function cleanStreamWebSocketUrl(path: string, transport: CleanStreamTransport) {
  const url = cleanStreamUrl(path);
  url.searchParams.set("transport", transport);
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";
  return url;
}

describe("clean stream transport comparison", () => {
  for (const { transport, connect } of transports) {
    it(`${transport} client appends and subscribes through the URL endpoint`, async () => {
      const path = `clean-${transport}-${crypto.randomUUID()}`;
      const event: StreamEventInput = {
        type: "test.clean-stream.url-client",
        payload: { transport, path },
      };

      await using subscriber = await connect({ url: cleanStreamUrl(path) });
      await using publisher = await connect({ url: cleanStreamUrl(path) });
      await using subscription = await subscriber.subscribe();

      const appended = await publisher.append(event);
      const delivered = await subscription.read();

      expect(appended).toMatchObject({
        type: event.type,
        payload: event.payload,
        offset: 1,
        createdAt: expect.any(String),
      });
      expect(delivered).toEqual(appended);
    });

    it(`${transport} client works from a Durable Object with fetch input`, async () => {
      const path = `clean-fetch-${transport}-${crypto.randomUUID()}`;
      const url = new URL("/clean-client-smoke", workerUrl);
      url.searchParams.set("stream", path);
      url.searchParams.set("transport", transport);

      const response = await fetch(url);
      expect(response.ok).toBe(true);
      const result = (await response.json()) as SmokeResult;

      expect(result).toMatchObject({
        transport,
        matched: true,
        appended: {
          type: "test.clean-stream.fetch-client",
          offset: 1,
          createdAt: expect.any(String),
        },
      });
      expect(result.delivered).toEqual(result.appended);
    });
  }

  it("generic clean client dispatches to the selected transport", async () => {
    const path = `clean-generic-${crypto.randomUUID()}`;
    const event: StreamEventInput = {
      type: "test.clean-stream.generic-client",
      payload: { path },
    };

    await using subscriber = await connectCleanStream({
      transport: "rawws",
      endpoint: { url: cleanStreamUrl(path) },
    });
    await using publisher = await connectCleanStream({
      transport: "rawws",
      endpoint: { url: cleanStreamUrl(path) },
    });
    await using subscription = await subscriber.subscribe();

    const appended = await publisher.append(event);
    expect(await subscription.read()).toEqual(appended);
  });

  it("requires an explicit clean stream transport", async () => {
    const path = `clean-${crypto.randomUUID()}`;
    const response = await fetch(cleanStreamUrl(path));

    expect(response.status).toBe(400);
    expect(await response.text()).toContain("transport must be capnweb");
  });

  it("capnweb-oneway subscriber originates no websocket frames per event", async () => {
    const path = `clean-capnweb-oneway-frames-${crypto.randomUUID()}`;
    const events: StreamEventInput[] = Array.from({ length: 2 }, (_, i) => ({
      type: "test.clean-stream.capnweb-oneway-frames",
      payload: { n: i + 1 },
    }));
    const delivered: StreamEvent[] = [];

    const subscriberWebSocket = newRecordingWebSocket(
      cleanStreamWebSocketUrl(path, "capnweb-oneway"),
    );
    await waitForRecordedWebSocketOpen(subscriberWebSocket);
    const subscriberRpc = newWebSocketRpcSession<CleanStreamRpc>(subscriberWebSocket);
    const sink = new TestEventSink((event) => {
      delivered.push(event);
    });

    await subscriberRpc.subscribeOneWay(sink);
    const framesAfterSubscribe = subscriberWebSocket.wsMessages.length;

    await using publisher = await connectCleanCapnwebStream({ url: cleanStreamUrl(path) });
    const appended = await Promise.all(events.map((event) => publisher.append(event)));
    await waitFor(() => delivered.length === events.length, 1_000);

    expect(delivered).toEqual(appended);
    expect(outboundFramesAfter(subscriberWebSocket.wsMessages, framesAfterSubscribe)).toEqual([]);
    const inboundFrames = inboundFramesAfter(subscriberWebSocket.wsMessages, framesAfterSubscribe);
    const inboundPushFrames = inboundFrames.filter((frame) => frame.data[0] === "push");
    expect(inboundFrames.every((frame) => frame.data[0] === "push" || frame.data[0] === "release"))
      .toBe(true);
    expect(inboundPushFrames).toMatchObject([
      {
        direction: "in",
        data: [
          "push",
          [
            "pipeline",
            expect.any(Number),
            ["event"],
            [
              {
                type: events[0]!.type,
                payload: events[0]!.payload,
                offset: 1,
                createdAt: expect.any(String),
              },
            ],
          ],
        ],
      },
      {
        direction: "in",
        data: [
          "push",
          [
            "pipeline",
            expect.any(Number),
            ["event"],
            [
              {
                type: events[1]!.type,
                payload: events[1]!.payload,
                offset: 2,
                createdAt: expect.any(String),
              },
            ],
          ],
        ],
      },
    ]);

    sink[Symbol.dispose]();
    subscriberRpc[Symbol.dispose]();
    await closeRecordedWebSocket(subscriberWebSocket);
  });
});

class TestEventSink extends RpcTarget implements CleanStreamEventSink {
  #onEvent: (event: StreamEvent) => void;
  #disposed = false;

  constructor(onEvent: (event: StreamEvent) => void) {
    super();
    this.#onEvent = onEvent;
  }

  event(event: StreamEvent): undefined {
    if (!this.#disposed) this.#onEvent(event);
  }

  [Symbol.dispose](): void {
    this.#disposed = true;
  }
}

function newRecordingWebSocket(url: URL): WebSocket & { wsMessages: WsMessage[] } {
  const wsMessages: WsMessage[] = [];
  const webSocket = new WebSocket(url.toString());
  const send = webSocket.send.bind(webSocket);
  webSocket.send = (data: Parameters<WebSocket["send"]>[0]) => {
    wsMessages.push({ direction: "out", data: describeFrameData(data) });
    return send(data);
  };
  webSocket.addEventListener("message", (event) => {
    wsMessages.push({ direction: "in", data: describeFrameData(event.data) });
  });
  return Object.assign(webSocket, { wsMessages });
}

function outboundFramesAfter(wsMessages: WsMessage[], afterFrameIndex: number) {
  return wsMessages
    .slice(afterFrameIndex)
    .filter((frame) => frame.direction === "out")
    .map((frame) => JSON.parse(frame.data));
}

function inboundFramesAfter(wsMessages: WsMessage[], afterFrameIndex: number) {
  return wsMessages
    .slice(afterFrameIndex)
    .filter((frame) => frame.direction === "in")
    .map((frame) => ({ ...frame, data: JSON.parse(frame.data) }));
}

function waitForRecordedWebSocketOpen(webSocket: WebSocket): Promise<void> {
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

function closeRecordedWebSocket(webSocket: WebSocket): Promise<void> {
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
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`timed out after ${timeoutMs}ms`);
}

function describeFrameData(data: unknown): string {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (ArrayBuffer.isView(data)) return new TextDecoder().decode(data);
  throw new TypeError(`unexpected WebSocket frame data: ${String(data)}`);
}
