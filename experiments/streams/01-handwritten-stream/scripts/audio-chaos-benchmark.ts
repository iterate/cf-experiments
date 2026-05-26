#!/usr/bin/env node
/**
 * Measure Cap'n Web stream fan-out under audio-like payloads.
 *
 * Defaults model Grok Voice's documented default PCM setting: 24 kHz Linear16
 * audio, base64 encoded in realtime chunks. A 20 ms mono PCM16 frame is 960 raw
 * bytes and about 1280 base64 characters.
 * https://docs.x.ai/developers/model-capabilities/audio/voice-agent
 *
 *   node scripts/audio-chaos-benchmark.ts http://localhost:8787
 *   node scripts/audio-chaos-benchmark.ts https://01-handwritten-stream.iterate-dev-preview.workers.dev \
 *     --publishers 10 --subscribers 36 --frames-per-publisher 50 --pace-ms 20 --durability best-effort
 */

import { newWebSocketRpcSession, type RpcStub } from "capnweb";
import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import type { StreamRpc } from "../src/stream.js";

type Args = Record<string, string | undefined>;

type Fixture = {
  rpc: RpcStub<StreamRpc>;
  webSocket: WebSocket;
  wsMessages: { direction: "out" | "in"; data: string }[];
  dispose(): Promise<void>;
};

type FrameRecord = {
  sentAtMs: number;
  firstDeliveryMs?: number;
  allDeliveryMs?: number;
  deliveries: number;
};

type AppendDurabilityMode = "confirmed" | "best-effort" | "checkpointed";

const args = parseArgs(process.argv.slice(2));
const workerUrl = (args.url ?? process.env.WORKER_URL ?? "http://localhost:8787").replace(/\/$/, "");
const streamPath = args.stream ?? `audio-chaos-${crypto.randomUUID().slice(0, 8)}`;
const publishers = positiveInt(args.publishers ?? "10", "publishers");
const subscribers = positiveInt(args.subscribers ?? "36", "subscribers");
const slowSubscribers = nonNegativeInt(args["slow-subscribers"] ?? "0", "slow-subscribers");
const framesPerPublisher = positiveInt(args["frames-per-publisher"] ?? "50", "frames-per-publisher");
const frameMs = positiveInt(args["frame-ms"] ?? "20", "frame-ms");
const paceMs = nonNegativeInt(args["pace-ms"] ?? "0", "pace-ms");
const sampleRate = positiveInt(args["sample-rate"] ?? "24000", "sample-rate");
const channels = positiveInt(args.channels ?? "1", "channels");
const bytesPerSample = positiveInt(args["bytes-per-sample"] ?? "2", "bytes-per-sample");
const timeoutMs = positiveInt(args["timeout-ms"] ?? "30000", "timeout-ms");
const durability = parseDurability(args.durability ?? "best-effort");
const checkpointEveryUnconfirmedAppends = positiveInt(
  args["checkpoint-every"] ?? "100",
  "checkpoint-every",
);
const totalEvents = publishers * framesPerPublisher;
const rawFrameBytes = Math.ceil((sampleRate * frameMs * channels * bytesPerSample) / 1000);
const audio = Buffer.alloc(rawFrameBytes, 0x7f).toString("base64");
const runId = args["run-id"] ?? crypto.randomUUID();

const frames = new Map<string, FrameRecord>();
const fixtures: Fixture[] = [];

try {
  const echo = await measureSameSessionEcho();
  const result = await runFanoutBenchmark();
  console.log(JSON.stringify({ ...result, sameSessionEchoMs: echo }, null, 2));
} catch (error) {
  console.error(error);
  process.exitCode = 1;
} finally {
  await Promise.allSettled(fixtures.splice(0).reverse().map((fixture) => fixture.dispose()));
}

process.exit(process.exitCode ?? 0);

async function measureSameSessionEcho() {
  const path = `${streamPath}-echo`;
  const fixture = await connect(path);
  fixtures.push(fixture);
  const readable = await fixture.rpc.stream();
  // @ts-expect-error capnweb@0.8.0 types only model ReadableStream<Uint8Array>
  const reader = (readable as ReadableStream<StreamEvent>).getReader();
  const read = reader.read() as Promise<ReadableStreamReadResult<StreamEvent>>;
  const startedAt = performance.now();
  await fixture.rpc.append({
    event: buildAudioEvent({ publisher: "echo", frame: 1, frameId: "echo-1" }),
    durability: appendDurability(),
  });
  const delivered = await withTimeout(read, timeoutMs);
  if (delivered.done) throw new Error("same-session echo stream ended");
  reader.releaseLock();
  return performance.now() - startedAt;
}

async function runFanoutBenchmark() {
  const activeSubscribers = await Promise.all(
    Array.from({ length: subscribers }, async (_, i) => {
      const fixture = await connect(streamPath);
      fixtures.push(fixture);
      const readable = await fixture.rpc.stream();
      const subscribedFrameCount = fixture.wsMessages.length;
      // @ts-expect-error capnweb@0.8.0 types only model ReadableStream<Uint8Array>
      const reader = (readable as ReadableStream<StreamEvent>).getReader();
      return { fixture, reader, index: i, subscribedFrameCount };
    }),
  );

  const passiveSubscribers = await Promise.all(
    Array.from({ length: slowSubscribers }, async () => {
      const fixture = await connect(streamPath);
      fixtures.push(fixture);
      await fixture.rpc.stream();
      return fixture;
    }),
  );

  const readers = activeSubscribers.map((subscriber) => collectSubscriber(subscriber));

  const publisherFixtures = await Promise.all(
    Array.from({ length: publishers }, async () => {
      const fixture = await connect(streamPath);
      fixtures.push(fixture);
      return fixture;
    }),
  );
  const selfEchoPublisher = publisherFixtures[0]!;
  const selfEchoReadable = await selfEchoPublisher.rpc.stream();
  const selfEchoSubscribedFrameCount = selfEchoPublisher.wsMessages.length;
  // @ts-expect-error capnweb@0.8.0 types only model ReadableStream<Uint8Array>
  const selfEchoReader = (selfEchoReadable as ReadableStream<StreamEvent>).getReader();
  const selfEchoLatencies: number[] = [];
  const selfEcho = collectSelfEcho({ reader: selfEchoReader, latencies: selfEchoLatencies });

  const publishStartedAt = performance.now();
  for (let frame = 1; frame <= framesPerPublisher; frame += 1) {
    for (let publisher = 0; publisher < publishers; publisher += 1) {
      const frameId = `p${publisher}-f${frame}`;
      frames.set(frameId, { sentAtMs: performance.now(), deliveries: 0 });
      fireAndForgetAppend(
        publisherFixtures[publisher]!.rpc,
        buildAudioEvent({ publisher: String(publisher), frame, frameId }),
      );
    }
    if (paceMs > 0) {
      const nextFrameAt = publishStartedAt + frame * paceMs;
      await sleep(Math.max(0, nextFrameAt - performance.now()));
    }
  }

  await Promise.all([...readers, selfEcho]);
  const finishedAt = performance.now();
  const debug = await publisherFixtures[0]!.rpc.debug();

  for (const subscriber of activeSubscribers) subscriber.reader.releaseLock();
  selfEchoReader.releaseLock();

  return {
    type: "audio-chaos-benchmark-result",
    workerUrl,
    streamPath,
    runId,
    publishers,
    subscribers,
    slowSubscribers,
    framesPerPublisher,
    totalEvents,
    durability,
    checkpointEveryUnconfirmedAppends:
      durability === "checkpointed" ? checkpointEveryUnconfirmedAppends : undefined,
    audio: {
      frameMs,
      paceMs,
      sampleRate,
      channels,
      bytesPerSample,
      rawFrameBytes,
      base64Chars: audio.length,
    },
    elapsedMs: finishedAt - publishStartedAt,
    eventsPerSecond: totalEvents / ((finishedAt - publishStartedAt) / 1000),
    firstSubscriberLatencyMs: summarize(
      Array.from(frames.values(), (frame) => frame.firstDeliveryMs! - frame.sentAtMs),
    ),
    allSubscribersLatencyMs: summarize(
      Array.from(frames.values(), (frame) => frame.allDeliveryMs! - frame.sentAtMs),
    ),
    publisherSelfEchoLatencyMs: summarize(selfEchoLatencies),
    publisherSelfEchoOutboundPullPushFrames: countOutboundPullPushFrames(
      selfEchoPublisher.wsMessages,
      selfEchoSubscribedFrameCount,
    ),
    readerOutboundPullPushFrames: summarize(
      activeSubscribers.map(({ fixture, subscribedFrameCount }) =>
        countOutboundPullPushFrames(fixture.wsMessages, subscribedFrameCount),
      ),
    ),
    publisherOutboundPullPushFrames: summarize(
      publisherFixtures.map(({ wsMessages }) => countOutboundPullPushFrames(wsMessages)),
    ),
    passiveSubscribers: passiveSubscribers.length,
    serverDebug: debug,
  };
}

async function collectSubscriber(args: {
  reader: ReadableStreamDefaultReader<StreamEvent>;
  index: number;
}) {
  for (let delivered = 0; delivered < totalEvents; delivered += 1) {
    const result = await withTimeout(
      args.reader.read() as Promise<ReadableStreamReadResult<StreamEvent>>,
      timeoutMs,
    );
    if (result.done) throw new Error(`subscriber ${args.index} ended early`);
    const frameId = readFrameId(result.value);
    const frame = frames.get(frameId);
    if (frame === undefined) throw new Error(`subscriber ${args.index} saw unknown frame ${frameId}`);

    const deliveredAt = performance.now();
    frame.deliveries += 1;
    frame.firstDeliveryMs = Math.min(frame.firstDeliveryMs ?? deliveredAt, deliveredAt);
    if (frame.deliveries === subscribers) frame.allDeliveryMs = deliveredAt;
  }
}

async function collectSelfEcho(args: {
  reader: ReadableStreamDefaultReader<StreamEvent>;
  latencies: number[];
}) {
  for (let delivered = 0; delivered < totalEvents; delivered += 1) {
    const result = await withTimeout(
      args.reader.read() as Promise<ReadableStreamReadResult<StreamEvent>>,
      timeoutMs,
    );
    if (result.done) throw new Error("publisher self-echo stream ended early");
    const frameId = readFrameId(result.value);
    if (!frameId.startsWith("p0-")) continue;
    const frame = frames.get(frameId);
    if (frame === undefined) throw new Error(`publisher saw unknown self frame ${frameId}`);
    args.latencies.push(performance.now() - frame.sentAtMs);
  }
}

function fireAndForgetAppend(rpc: RpcStub<StreamRpc>, event: StreamEventInput) {
  const append = rpc.append({ event, durability: appendDurability() }) as unknown as {
    [Symbol.dispose](): void;
  };
  append[Symbol.dispose]();
}

function appendDurability() {
  if (durability === "checkpointed") {
    return { mode: durability, checkpointEveryUnconfirmedAppends };
  }
  return durability;
}

function buildAudioEvent(args: { publisher: string; frame: number; frameId: string }): StreamEventInput {
  return {
    type: "benchmark.audio-frame",
    payload: {
      runId,
      frameId: args.frameId,
      publisher: args.publisher,
      frame: args.frame,
      codec: "pcm16-base64",
      sampleRate,
      frameMs,
      audio,
    },
    metadata: { runId },
  };
}

function readFrameId(event: StreamEvent) {
  if (
    event.payload === null ||
    typeof event.payload !== "object" ||
    !("frameId" in event.payload) ||
    typeof event.payload.frameId !== "string"
  ) {
    throw new Error(`event ${event.offset} did not contain a frameId`);
  }
  return event.payload.frameId;
}

async function connect(path: string): Promise<Fixture> {
  const webSocket = new WebSocket(toWebSocketUrl(workerUrl, path));
  const wsMessages: Fixture["wsMessages"] = [];
  const send = webSocket.send.bind(webSocket);
  webSocket.send = ((data: Parameters<WebSocket["send"]>[0]) => {
    wsMessages.push({ direction: "out", data: describeFrameData(data) });
    return send(data);
  }) as WebSocket["send"];
  webSocket.addEventListener("message", (event) => {
    wsMessages.push({ direction: "in", data: describeFrameData(event.data) });
  });
  await waitForOpen(webSocket);
  const rpc = newWebSocketRpcSession<StreamRpc>(webSocket);
  return {
    rpc,
    webSocket,
    wsMessages,
    async dispose() {
      rpc[Symbol.dispose]();
      await closeWebSocket(webSocket);
    },
  };
}

function countOutboundPullPushFrames(wsMessages: Fixture["wsMessages"], afterFrame = 0) {
  return wsMessages
    .slice(afterFrame)
    .filter((frame) => frame.direction === "out")
    .map((frame) => JSON.parse(frame.data) as unknown[])
    .filter((data) => data[0] === "pull" || data[0] === "push").length;
}

function summarize(values: number[]) {
  const sorted = [...values].sort((a, b) => a - b);
  return {
    min: sorted[0] ?? 0,
    p50: percentile(sorted, 0.5),
    p95: percentile(sorted, 0.95),
    p99: percentile(sorted, 0.99),
    max: sorted.at(-1) ?? 0,
    avg: sorted.reduce((sum, value) => sum + value, 0) / Math.max(sorted.length, 1),
  };
}

function percentile(sorted: number[], p: number) {
  if (sorted.length === 0) return 0;
  return sorted[Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * p))]!;
}

function toWebSocketUrl(raw: string, path: string) {
  const url = new URL(raw);
  url.pathname = `/${path}`;
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";
  return url.toString();
}

function waitForOpen(webSocket: WebSocket) {
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
    const timeout = setTimeout(() => resolve(), 1_000);
    webSocket.addEventListener(
      "close",
      () => {
        clearTimeout(timeout);
        resolve();
      },
      { once: true },
    );
    webSocket.close();
  });
}

function describeFrameData(data: unknown) {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (ArrayBuffer.isView(data)) return new TextDecoder().decode(data);
  throw new TypeError(`unexpected WebSocket frame data: ${String(data)}`);
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  return Promise.race([
    promise,
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error(`timed out after ${timeoutMs}ms`)), timeoutMs),
    ),
  ]);
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function positiveInt(raw: string, name: string) {
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) throw new Error(`${name} must be a positive integer`);
  return value;
}

function nonNegativeInt(raw: string, name: string) {
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }
  return value;
}

function parseDurability(raw: string): AppendDurabilityMode {
  if (raw !== "confirmed" && raw !== "best-effort" && raw !== "checkpointed") {
    throw new Error("durability must be confirmed, best-effort, or checkpointed");
  }
  return raw;
}

function parseArgs(argv: string[]) {
  const parsed: Args = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i]!;
    if (!arg.startsWith("--")) {
      parsed.url = arg;
      continue;
    }
    const key = arg.slice(2);
    const next = argv[i + 1];
    if (next && !next.startsWith("--")) {
      parsed[key] = next;
      i += 1;
    } else {
      parsed[key] = "true";
    }
  }
  return parsed;
}
