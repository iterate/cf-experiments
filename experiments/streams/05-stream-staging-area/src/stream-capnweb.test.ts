import { newWebSocketRpcSession, RpcTarget, type RpcStub } from "capnweb";
import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import { describe, expect, it } from "vitest";
import type { StreamRpc, SubscriptionSink } from "./stream-types.js";
import { connectStreamProcessorRunnerFromNode } from "./client-libraries/stream-node-worker.js";
import { withStream } from "./client-libraries/stream-browser.js";

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";
const e2eIt = process.env.STREAM_STAGING_E2E === "true" ? it : it.skip;

type WsMessage = {
  direction: "out" | "in";
  data: string;
};

class TestSubscriptionSink extends RpcTarget implements SubscriptionSink {
  readonly batches: StreamEvent[][] = [];

  processEventBatch(args: { events: StreamEvent[] }): undefined {
    this.batches.push(args.events);
  }
}

describe("stream capnweb protocol", () => {
  e2eIt("browser client appends events by stream URL", async () => {
    const path = `stream-browser-client-${crypto.randomUUID()}`;
    await using stream = await withStream({ url: toStreamWebSocketUrl(path) });

    const appended = await stream.rpc.append({
      event: {
        type: "test.stream.browser-client",
        payload: { path },
      },
    });

    expect(appended).toMatchObject({
      type: "test.stream.browser-client",
      payload: { path },
      offset: 2,
      createdAt: expect.any(String),
    });
  });

  e2eIt("appends events after the stream-created event over capnweb", async () => {
    const path = `stream-capnweb-append-${crypto.randomUUID()}`;
    await using stream = await connectStream(path);

    const appended = await stream.rpc.append({
      event: {
        type: "test.stream.capnweb-append",
        payload: { path },
      },
    });

    expect(appended).toMatchObject({
      type: "test.stream.capnweb-append",
      payload: { path },
      offset: 2,
      createdAt: expect.any(String),
    });
  });

  e2eIt("appendBatch returns events in input order including idempotency hits", async () => {
    const path = `stream-capnweb-batch-${crypto.randomUUID()}`;
    await using stream = await connectStream(path);

    const existing = await stream.rpc.append({
      event: {
        type: "test.stream.capnweb-batch-existing",
        idempotencyKey: "batch-existing",
        payload: { path },
      },
    });
    await expect(stream.rpc.getEvent({ idempotencyKey: "batch-existing" })).resolves.toEqual(
      existing,
    );
    const batch = await stream.rpc.appendBatch({
      events: [
        {
          type: "test.stream.capnweb-batch-new",
          payload: { n: 1 },
        },
        {
          type: "test.stream.capnweb-batch-existing",
          idempotencyKey: "batch-existing",
          payload: { path },
        },
        {
          type: "test.stream.capnweb-batch-new",
          payload: { n: 2 },
        },
      ],
    });

    expect(batch).toMatchObject([
      {
        type: "test.stream.capnweb-batch-new",
        offset: 3,
        payload: { n: 1 },
      },
      existing,
      {
        type: "test.stream.capnweb-batch-new",
        offset: 4,
        payload: { n: 2 },
      },
    ]);
  });

  e2eIt("can append through the public output-gated storage path", async () => {
    const path = `stream-capnweb-sync-append-${crypto.randomUUID()}`;
    await using stream = await connectStream(path);

    const appended = await stream.rpc.append({
      event: {
        type: "test.stream.capnweb-sync-append",
        payload: { path },
      },
      durability: {
        closeOutputGate: true,
      },
    });

    expect(appended).toMatchObject({
      type: "test.stream.capnweb-sync-append",
      payload: { path },
      offset: 2,
      createdAt: expect.any(String),
    });
  });

  e2eIt("replays history and then delivers live batches to inbound subscribers", async () => {
    const path = `stream-capnweb-replay-${crypto.randomUUID()}`;
    await using stream = await connectStream(path);

    const first = await stream.rpc.append({
      event: {
        type: "test.stream.capnweb-replay",
        payload: { n: 1 },
      },
    });

    const sink = new TestSubscriptionSink();
    await stream.rpc.subscribe({ subscriptionKey: "replay", sink });
    await waitFor(() => sink.batches.length === 1, 1_000);

    const second = await stream.rpc.append({
      event: {
        type: "test.stream.capnweb-replay",
        payload: { n: 2 },
      },
    });
    await waitFor(() => sink.batches.length === 2, 1_000);

    expect(sink.batches).toEqual([
      [
        expect.objectContaining({
          type: "events.iterate.com/stream/created",
          offset: 0,
          payload: {
            namespace: "stream",
            path,
          },
        }),
        expect.objectContaining({
          type: "events.iterate.com/stream/woken",
          offset: 1,
          payload: {
            incarnationId: expect.any(String),
          },
        }),
        first,
      ],
      [second],
    ]);
  });

  e2eIt("runs a built-in outbound processor from subscription-configured", async () => {
    const path = `stream-capnweb-processor-${crypto.randomUUID()}`;
    const subscriptionKey = "echo";
    await using stream = await connectStream(path);
    await using processor = await connectStreamProcessorRunnerFromNode({
      path: `stream:${path}:${subscriptionKey}`,
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
            transport: "capnweb-websocket",
            processorSlug: "echo",
          },
        },
      },
    });

    await waitFor(async () => {
      const status = await processor.rpc.runtimeState();
      return (
        status.processorSlug === "echo" &&
        status.snapshot?.offset === configured.offset &&
        status.snapshot?.state.seen === 0
      );
    }, 1_000);

    await stream.rpc.append({
      event: {
        type: "test.processor.input",
        payload: { path },
      },
    });

    await waitFor(async () => {
      const status = await processor.rpc.runtimeState();
      return status.snapshot?.state.seen === 1 && status.snapshot.offset >= 4;
    }, 1_000);
  });

  e2eIt("delivers event batches without subscriber-originated return traffic", async () => {
    const path = `stream-capnweb-wire-${crypto.randomUUID()}`;
    const sink = new TestSubscriptionSink();

    await using subscriber = await connectStream(path);
    await subscriber.rpc.subscribe({
      subscriptionKey: "wire",
      sink,
      afterOffset: 1,
    });
    const afterSubscribe = subscriber.wsMessages.length;

    await using publisher = await connectStream(path);
    const input: StreamEventInput = {
      type: "test.stream.capnweb-wire",
      payload: { path },
    };
    const appended = await publisher.rpc.append({ event: input });
    await waitFor(() => sink.batches.length === 1, 1_000);

    expect(appended).toMatchObject({
      type: input.type,
      payload: input.payload,
      offset: 2,
      createdAt: expect.any(String),
    });
    expect(sink.batches).toEqual([[appended]]);
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
            ["processEventBatch"],
            [
              {
                events: [
                  [
                    {
                      type: input.type,
                      payload: input.payload,
                      offset: 2,
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

async function connectStream(path: string): Promise<
  AsyncDisposable & {
    rpc: RpcStub<StreamRpc>;
    wsMessages: WsMessage[];
  }
> {
  const wsMessages: WsMessage[] = [];
  const webSocket = newRecordingWebSocket(toStreamWebSocketUrl(path), wsMessages);
  await waitForWebSocketOpen(webSocket);
  const rpc = newWebSocketRpcSession<StreamRpc>(webSocket);

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

function toStreamWebSocketUrl(path: string) {
  const url = new URL(workerUrl);
  url.pathname = `/stream/${path}`;
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
    webSocket.addEventListener("error", () => reject(new Error("WebSocket connection failed")), {
      once: true,
    });
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
