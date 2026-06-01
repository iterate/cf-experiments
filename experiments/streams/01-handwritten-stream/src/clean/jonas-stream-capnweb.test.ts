import { newWebSocketRpcSession, RpcTarget, type RpcStub } from "capnweb";
import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import { describe, expect, it } from "vitest";
import type { JonasStreamRpc, SubscriberRpcTarget } from "./jonas-stream.js";
import { withStreamProcessorCapnweb } from "./jonas-stream-client.js";

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";
const e2eIt = process.env.JONAS_STREAM_E2E === "true" ? it : it.skip;

type WsMessage = {
  direction: "out" | "in";
  data: string;
};

class TestSubscriberRpcTarget extends RpcTarget implements SubscriberRpcTarget {
  readonly batches: StreamEvent[][] = [];

  consumeEvents(args: { events: StreamEvent[] }): undefined {
    this.batches.push(args.events);
  }
}

describe("jonas stream CaptainWeb protocol", () => {
  e2eIt("appends zero-based events over CaptainWeb", async () => {
    const path = `jonas-capnweb-append-${crypto.randomUUID()}`;
    await using stream = await connectJonasStream(path);

    const appended = await stream.rpc.append({
      event: {
        type: "test.jonas.capnweb-append",
        payload: { path },
      },
    });

    expect(appended).toMatchObject({
      type: "test.jonas.capnweb-append",
      payload: { path },
      offset: 0,
      createdAt: expect.any(String),
    });
    expect(await stream.rpc.getMaxOffset()).toBe(0);
  });

  e2eIt("replays history and then delivers live batches to inbound subscribers", async () => {
    const path = `jonas-capnweb-replay-${crypto.randomUUID()}`;
    await using stream = await connectJonasStream(path);

    const first = await stream.rpc.append({
      event: {
        type: "test.jonas.capnweb-replay",
        payload: { n: 1 },
      },
    });

    const subscriberTarget = new TestSubscriberRpcTarget();
    await stream.rpc.initInboundSubscription({ subscriberRpcTarget: subscriberTarget });
    await waitFor(() => subscriberTarget.batches.length === 1, 1_000);

    const second = await stream.rpc.append({
      event: {
        type: "test.jonas.capnweb-replay",
        payload: { n: 2 },
      },
    });
    await waitFor(() => subscriberTarget.batches.length === 2, 1_000);

    expect(subscriberTarget.batches).toEqual([[first], [second]]);
  });

  e2eIt("runs a built-in outbound processor from subscription-configured", async () => {
    const path = `jonas-capnweb-processor-${crypto.randomUUID()}`;
    const subscriptionKey = "echo";
    await using stream = await connectJonasStream(path);
    await using processor = await withStreamProcessorCapnweb({
      path: `${path}:${subscriptionKey}`,
      workerUrl,
    });

    const configured = await stream.rpc.append({
      event: {
        type: "events.iterate.com/stream/subscription-configured",
        idempotencyKey: `subscription:${subscriptionKey}`,
        payload: {
          subscriptionKey,
          subscriber: {
            type: "built-in",
            transport: "captainweb-websocket",
            processorSlug: "echo",
          },
        },
      },
    });

    await waitFor(async () => {
      const status = await processor.capnweb.status();
      return (
        status.processorSlug === "echo" &&
        status.snapshot?.offset === configured.offset &&
        status.snapshot.state.seen === 0
      );
    }, 1_000);

    await stream.rpc.append({
      event: {
        type: "test.processor.input",
        payload: { path },
      },
    });

    await waitFor(async () => {
      const status = await processor.capnweb.status();
      return status.snapshot?.state.seen === 1 && status.snapshot.offset >= 2;
    }, 1_000);
  });

  e2eIt("delivers event batches without subscriber-originated return traffic", async () => {
    const path = `jonas-capnweb-wire-${crypto.randomUUID()}`;
    const subscriberTarget = new TestSubscriberRpcTarget();

    await using subscriber = await connectJonasStream(path);
    await subscriber.rpc.initInboundSubscription({
      subscriberRpcTarget: subscriberTarget,
    });
    const afterSubscribe = subscriber.wsMessages.length;

    await using publisher = await connectJonasStream(path);
    const input: StreamEventInput = {
      type: "test.jonas.capnweb-wire",
      payload: { path },
    };
    const appended = await publisher.rpc.append({ event: input });
    await waitFor(() => subscriberTarget.batches.length === 1, 1_000);

    expect(appended).toMatchObject({
      type: input.type,
      payload: input.payload,
      offset: 0,
      createdAt: expect.any(String),
    });
    expect(subscriberTarget.batches).toEqual([[appended]]);
    expect(outboundFrames(subscriber.wsMessages, afterSubscribe)).toEqual([]);

    const inbound = parsedFrames(subscriber.wsMessages)
      .slice(afterSubscribe)
      .filter((frame) => frame.direction === "in");
    expect(inbound.every((frame) => isPushOrReleaseFrame(frame.data))).toBe(true);
    expect(inbound.filter((frame) => isPushFrame(frame.data))).toMatchObject([
      {
        direction: "in",
        data: [
          "push",
          [
            "pipeline",
            expect.any(Number),
            ["consumeEvents"],
            [
              {
                events: [
                  [
                    {
                      type: input.type,
                      payload: input.payload,
                      offset: 0,
                      createdAt: expect.any(String),
                    },
                  ],
                ],
              },
            ],
          ],
        ],
      },
    ]);
  });
});

async function connectJonasStream(path: string): Promise<
  AsyncDisposable & {
    rpc: RpcStub<JonasStreamRpc>;
    wsMessages: WsMessage[];
  }
> {
  const wsMessages: WsMessage[] = [];
  const webSocket = newRecordingWebSocket(toJonasWebSocketUrl(path), wsMessages);
  await waitForWebSocketOpen(webSocket);
  const rpc = newWebSocketRpcSession<JonasStreamRpc>(webSocket);

  return {
    rpc,
    wsMessages,
    async [Symbol.asyncDispose]() {
      rpc[Symbol.dispose]();
      await closeWebSocket(webSocket);
    },
  };
}

function newRecordingWebSocket(url: string, wsMessages: WsMessage[]) {
  const webSocket = new WebSocket(url);
  const send = webSocket.send.bind(webSocket);

  webSocket.send = ((data: Parameters<WebSocket["send"]>[0]) => {
    wsMessages.push({ direction: "out", data: describeWebSocketFrameData(data) });
    return send(data);
  }) as WebSocket["send"];

  webSocket.addEventListener("message", (event) => {
    wsMessages.push({ direction: "in", data: describeWebSocketFrameData(event.data) });
  });

  return webSocket;
}

function toJonasWebSocketUrl(path: string) {
  const url = new URL(workerUrl);
  url.pathname = `/jonas/${path}`;
  url.searchParams.set("transport", "capnweb");
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";
  return url.toString();
}

function parsedFrames(messages: WsMessage[]) {
  return messages.map((frame) => ({
    direction: frame.direction,
    data: JSON.parse(frame.data) as unknown,
  }));
}

function outboundFrames(messages: WsMessage[], afterFrameIndex: number) {
  return parsedFrames(messages)
    .slice(afterFrameIndex)
    .filter((frame) => frame.direction === "out")
    .map((frame) => frame.data);
}

function isPushOrReleaseFrame(value: unknown) {
  return isPushFrame(value) || (Array.isArray(value) && value[0] === "release");
}

function isPushFrame(value: unknown) {
  return Array.isArray(value) && value[0] === "push";
}

function describeWebSocketFrameData(data: unknown) {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (ArrayBuffer.isView(data)) return new TextDecoder().decode(data);
  throw new TypeError(`unexpected WebSocket frame data: ${String(data)}`);
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

async function waitFor(assertion: () => boolean | Promise<boolean>, timeoutMs: number) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (await assertion()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`timed out after ${timeoutMs}ms`);
}
