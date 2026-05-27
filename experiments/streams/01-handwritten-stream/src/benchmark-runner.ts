import { DurableObject } from "cloudflare:workers";
import { newWebSocketRpcSession, type RpcStub } from "capnweb";
import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import type { StreamRpc } from "./stream.js";

export type BenchmarkMode = "rpc-serial" | "rpc-batch" | "rpc-pipelined";
type AppendDurabilityMode = "confirmed" | "best-effort" | "checkpointed";
type StreamKind = "durable" | "volatile";
type AppendDurability =
  | AppendDurabilityMode
  | {
      mode: AppendDurabilityMode;
      checkpointEveryUnconfirmedAppends?: number;
    };

export type RunBenchmarkArgs = {
  stream: string;
  mode?: BenchmarkMode;
  messages?: number;
  payloadBytes?: number;
  batchSize?: number;
  runId?: string;
};

export type BenchmarkResult = {
  runner: string;
  stream: string;
  mode: BenchmarkMode;
  messages: number;
  payloadBytes: number;
  committed: number;
  elapsedMs: number;
  eventsPerSecond: number;
  serverMaxOffset: number;
  runId: string;
  dispatchMs?: number;
};

export type RunAudioChaosArgs = {
  stream?: string;
  streamKind?: StreamKind;
  runId?: string;
  publishers?: number;
  subscribers?: number;
  slowSubscribers?: number;
  framesPerPublisher?: number;
  frameMs?: number;
  paceMs?: number;
  sampleRate?: number;
  channels?: number;
  bytesPerSample?: number;
  timeoutMs?: number;
  durability?: AppendDurabilityMode;
  checkpointEveryUnconfirmedAppends?: number;
  measureAppendAck?: boolean;
  measureSelfEcho?: boolean;
};

type AudioChaosConfig = {
  stream: string;
  streamKind: StreamKind;
  runId: string;
  publishers: number;
  subscribers: number;
  slowSubscribers: number;
  framesPerPublisher: number;
  frameMs: number;
  paceMs: number;
  sampleRate: number;
  channels: number;
  bytesPerSample: number;
  timeoutMs: number;
  durability: AppendDurabilityMode;
  checkpointEveryUnconfirmedAppends: number;
  measureAppendAck: boolean;
  measureSelfEcho: boolean;
};

type AudioSample = {
  frameId: string;
  latencyMs: number;
};

type AudioSubscriberResult = {
  runner: string;
  subscriber: string;
  received: number;
  samples: AudioSample[];
  latencyMs: Summary;
};

type AudioPublisherResult = {
  runner: string;
  publisher: number;
  sent: number;
  elapsedMs: number;
  appendAckLatencyMs: Summary;
  selfEchoLatencyMs: Summary;
  appendStartToSelfEchoLatencyMs: Summary;
  ackToSelfEchoLatencyMs: Summary;
};

export type AudioChaosResult = {
  type: "audio-chaos-benchmark-result";
  runner: string;
  streamPath: string;
  streamKind: StreamKind;
  runId: string;
  publishers: number;
  subscribers: number;
  slowSubscribers: number;
  framesPerPublisher: number;
  totalEvents: number;
  durability: AppendDurabilityMode;
  measureAppendAck: boolean;
  measureSelfEcho: boolean;
  checkpointEveryUnconfirmedAppends?: number;
  audio: {
    frameMs: number;
    paceMs: number;
    sampleRate: number;
    channels: number;
    bytesPerSample: number;
    rawFrameBytes: number;
    base64Chars: number;
  };
  elapsedMs: number;
  eventsPerSecond: number;
  framesFullyDelivered: number;
  framesMissingFullDelivery: number;
  minFrameDeliveries: number;
  maxFrameDeliveries: number;
  subscriberCreatedAtLatencyMs: Summary;
  firstSubscriberCreatedAtLatencyMs: Summary;
  allSubscribersCreatedAtLatencyMs: Summary;
  publisherSelfEchoCreatedAtLatencyMs: Summary;
  publisherAppendStartToSelfEchoLatencyMs: Summary;
  publisherAppendAckLatencyMs: Summary;
  publisherAckToSelfEchoLatencyMs: Summary;
  subscriberResults: Pick<AudioSubscriberResult, "subscriber" | "received" | "latencyMs">[];
  publisherResults: Pick<
    AudioPublisherResult,
    | "publisher"
    | "sent"
    | "elapsedMs"
    | "appendAckLatencyMs"
    | "selfEchoLatencyMs"
    | "appendStartToSelfEchoLatencyMs"
    | "ackToSelfEchoLatencyMs"
  >[];
  serverDebug: unknown;
};

type Summary = {
  count: number;
  min: number;
  p50: number;
  p95: number;
  p99: number;
  max: number;
  avg: number;
};

type StreamRpcFixture = {
  rpc: RpcStub<StreamRpc>;
  streamKind: StreamKind;
  webSocket: WebSocket;
  dispose(): void;
};

export class BenchmarkRunner extends DurableObject {
  async runBenchmark(args: RunBenchmarkArgs): Promise<BenchmarkResult> {
    const stream = args.stream;
    const mode = args.mode ?? "rpc-serial";
    const messages = args.messages ?? 1_000;
    const payloadBytes = args.payloadBytes ?? 256;
    const batchSize = args.batchSize ?? 100;
    const runId = args.runId ?? crypto.randomUUID();
    const stub = this.env.STREAM.getByName(stream);

    const startedAt = Date.now();
    let committed = 0;
    let dispatchMs: number | undefined;

    if (mode === "rpc-serial") {
      for (let n = 1; n <= messages; n++) {
        await stub.append({ event: buildEvent(n, runId, payloadBytes) });
        committed += 1;
      }
    } else if (mode === "rpc-pipelined") {
      const pending: Promise<unknown>[] = [];
      for (let n = 1; n <= messages; n++) {
        pending.push(stub.append({ event: buildEvent(n, runId, payloadBytes) }));
      }
      dispatchMs = Date.now() - startedAt;
      await Promise.all(pending);
      committed = messages;
    } else {
      for (let offset = 0; offset < messages; offset += batchSize) {
        const count = Math.min(batchSize, messages - offset);
        const events = Array.from({ length: count }, (_, i) =>
          buildEvent(offset + i + 1, runId, payloadBytes),
        );
        await stub.appendBatch({ events });
        committed += count;
      }
    }

    const elapsedMs = Date.now() - startedAt;
    const serverMaxOffset = await stub.maxOffset();

    return {
      runner: this.ctx.id.name ?? this.ctx.id.toString(),
      stream,
      mode,
      messages,
      payloadBytes,
      committed,
      elapsedMs,
      eventsPerSecond: committed / (elapsedMs / 1_000),
      serverMaxOffset,
      runId,
      ...(dispatchMs !== undefined ? { dispatchMs } : {}),
    };
  }

  async runAudioChaos(args: RunAudioChaosArgs): Promise<AudioChaosResult> {
    const config = normalizeAudioChaosConfig(args);
    const totalEvents = config.publishers * config.framesPerPublisher;
    const audio = makeAudioFixture(config);
    const stream = this.env.STREAM.getByName(config.stream);
    if (config.streamKind === "durable") {
      await stream.patchSettings({
        defaultAppendDurabilityMode: config.durability,
        checkpointEveryUnconfirmedAppends: config.checkpointEveryUnconfirmedAppends,
      });
    }

    /**
     * This benchmark deliberately runs publisher/subscriber clients in separate
     * BenchmarkRunner Durable Objects, then connects them to the Stream DO over
     * the same Cap'n Web WebSocket endpoint external clients use. That keeps
     * laptop WiFi out of the timing path without switching to native DO RPC,
     * whose typed `ReadableStream` support is byte-stream-oriented rather than
     * our pass-by-value `StreamEvent` chunks.
     */
    const activeSubscribers = Array.from({ length: config.subscribers }, (_, i) =>
      this.env.BENCHMARK_RUNNER.getByName(`${config.runId}:subscriber:${i}`).runAudioSubscriber({
        ...config,
        subscriber: `active-${i}`,
      }),
    );

    const passiveSubscribers = Array.from({ length: config.slowSubscribers }, (_, i) =>
      this.env.BENCHMARK_RUNNER.getByName(`${config.runId}:passive:${i}`).runAudioPassiveSubscriber(
        {
          ...config,
          subscriber: `passive-${i}`,
          holdMs: config.framesPerPublisher * Math.max(config.paceMs, 1) + 3_000,
        },
      ),
    );

    if (config.streamKind === "volatile") {
      const expectedSubscribers = config.subscribers + config.slowSubscribers;
      const attachDeadline = Date.now() + 5_000;
      while (Date.now() < attachDeadline) {
        const debug = await stream.debug();
        if (debug.volatileSubscribers.length >= expectedSubscribers) break;
        await sleep(25);
      }
      const debug = await stream.debug();
      if (debug.volatileSubscribers.length < expectedSubscribers) {
        throw new Error(
          `expected ${expectedSubscribers} volatile subscribers, saw ${debug.volatileSubscribers.length}`,
        );
      }
    } else {
      // Durable streams can replay missed history, but this still avoids measuring connection setup.
      await sleep(250);
    }

    const publishStartedAt = Date.now();
    const publishers = Array.from({ length: config.publishers }, (_, publisher) =>
      this.env.BENCHMARK_RUNNER.getByName(`${config.runId}:publisher:${publisher}`).runAudioPublisher(
        {
          ...config,
          audio,
          publisher,
          selfEcho: config.measureSelfEcho && publisher === 0,
        },
      ),
    );

    const [subscriberResults, publisherResults] = await Promise.all([
      Promise.all(activeSubscribers),
      Promise.all(publishers),
    ]);
    await Promise.allSettled(passiveSubscribers);

    const elapsedMs = Date.now() - publishStartedAt;
    const frameLatencies = new Map<string, number[]>();
    for (const subscriber of subscriberResults) {
      for (const sample of subscriber.samples) {
        const latencies = frameLatencies.get(sample.frameId) ?? [];
        latencies.push(sample.latencyMs);
        frameLatencies.set(sample.frameId, latencies);
      }
    }

    const subscriberLatencies = subscriberResults.flatMap((subscriber) =>
      subscriber.samples.map((sample) => sample.latencyMs),
    );
    const frameDeliveryCounts: number[] = [];
    const firstSubscriberLatencies: number[] = [];
    const allSubscriberLatencies: number[] = [];
    for (let publisher = 0; publisher < config.publishers; publisher += 1) {
      for (let frame = 1; frame <= config.framesPerPublisher; frame += 1) {
        const latencies = frameLatencies.get(`p${publisher}-f${frame}`) ?? [];
        frameDeliveryCounts.push(latencies.length);
        if (latencies.length > 0) firstSubscriberLatencies.push(Math.min(...latencies));
        if (config.subscribers > 0 && latencies.length === config.subscribers) {
          allSubscriberLatencies.push(Math.max(...latencies));
        }
      }
    }
    const selfEchoPublisher = publisherResults.find((publisher) => publisher.publisher === 0);
    const serverDebug = await stream.debug();

    return {
      type: "audio-chaos-benchmark-result",
      runner: this.ctx.id.name ?? this.ctx.id.toString(),
      streamPath: config.stream,
      streamKind: config.streamKind,
      runId: config.runId,
      publishers: config.publishers,
      subscribers: config.subscribers,
      slowSubscribers: config.slowSubscribers,
      framesPerPublisher: config.framesPerPublisher,
      totalEvents,
      durability: config.durability,
      measureAppendAck: config.measureAppendAck,
      measureSelfEcho: config.measureSelfEcho,
      ...(config.durability === "checkpointed"
        ? { checkpointEveryUnconfirmedAppends: config.checkpointEveryUnconfirmedAppends }
        : {}),
      audio: {
        frameMs: config.frameMs,
        paceMs: config.paceMs,
        sampleRate: config.sampleRate,
        channels: config.channels,
        bytesPerSample: config.bytesPerSample,
        rawFrameBytes: audio.rawFrameBytes,
        base64Chars: audio.base64.length,
      },
      elapsedMs,
      eventsPerSecond: totalEvents / (elapsedMs / 1_000),
      framesFullyDelivered:
        config.subscribers === 0
          ? 0
          : frameDeliveryCounts.filter((count) => count === config.subscribers).length,
      framesMissingFullDelivery:
        config.subscribers === 0
          ? 0
          : frameDeliveryCounts.filter((count) => count !== config.subscribers).length,
      minFrameDeliveries: Math.min(...frameDeliveryCounts),
      maxFrameDeliveries: Math.max(...frameDeliveryCounts),
      subscriberCreatedAtLatencyMs: summarize(subscriberLatencies),
      firstSubscriberCreatedAtLatencyMs: summarize(firstSubscriberLatencies),
      allSubscribersCreatedAtLatencyMs: summarize(allSubscriberLatencies),
      publisherSelfEchoCreatedAtLatencyMs: selfEchoPublisher?.selfEchoLatencyMs ?? summarize([]),
      publisherAppendStartToSelfEchoLatencyMs:
        selfEchoPublisher?.appendStartToSelfEchoLatencyMs ?? summarize([]),
      publisherAppendAckLatencyMs: selfEchoPublisher?.appendAckLatencyMs ?? summarize([]),
      publisherAckToSelfEchoLatencyMs: selfEchoPublisher?.ackToSelfEchoLatencyMs ?? summarize([]),
      subscriberResults: subscriberResults.map(({ subscriber, received, latencyMs }) => ({
        subscriber,
        received,
        latencyMs,
      })),
      publisherResults: publisherResults.map(
        ({
          publisher,
          sent,
          elapsedMs: publisherElapsedMs,
          appendAckLatencyMs,
          selfEchoLatencyMs,
          appendStartToSelfEchoLatencyMs,
          ackToSelfEchoLatencyMs,
        }) => ({
          publisher,
          sent,
          elapsedMs: publisherElapsedMs,
          appendAckLatencyMs,
          selfEchoLatencyMs,
          appendStartToSelfEchoLatencyMs,
          ackToSelfEchoLatencyMs,
        }),
      ),
      serverDebug,
    };
  }

  async runAudioSubscriber(
    args: AudioChaosConfig & { subscriber: string },
  ): Promise<AudioSubscriberResult> {
    const fixture = await connectStreamRpc(this.env, args.stream, args.streamKind);
    const reader = await objectStreamReader(fixture.rpc, fixture.streamKind);
    const samples: AudioSample[] = [];
    try {
      for (let received = 0; received < args.publishers * args.framesPerPublisher; received += 1) {
        const result = await withTimeout(reader.read(), args.timeoutMs);
        if (result.done) throw new Error(`${args.subscriber} stream ended early`);
        samples.push({
          frameId: readFrameId(result.value),
          latencyMs: Math.max(0, Date.now() - Date.parse(result.value.createdAt)),
        });
      }
    } finally {
      reader.releaseLock();
      fixture.dispose();
    }

    return {
      runner: this.ctx.id.name ?? this.ctx.id.toString(),
      subscriber: args.subscriber,
      received: samples.length,
      samples,
      latencyMs: summarize(samples.map((sample) => sample.latencyMs)),
    };
  }

  async runAudioPassiveSubscriber(args: AudioChaosConfig & { subscriber: string; holdMs: number }) {
    const fixture = await connectStreamRpc(this.env, args.stream, args.streamKind);
    if (fixture.streamKind === "volatile") {
      await fixture.rpc.streamVolatile();
    } else {
      await fixture.rpc.stream();
    }
    try {
      await sleep(args.holdMs);
      return {
        runner: this.ctx.id.name ?? this.ctx.id.toString(),
        subscriber: args.subscriber,
        heldMs: args.holdMs,
      };
    } finally {
      fixture.dispose();
    }
  }

  async runAudioPublisher(
    args: AudioChaosConfig & {
      audio: AudioFixture;
      publisher: number;
      selfEcho: boolean;
    },
  ): Promise<AudioPublisherResult> {
    const fixture = await connectStreamRpc(this.env, args.stream, args.streamKind);
    const appendPromises: Promise<StreamEvent>[] = [];
    const appendStartedAtByFrame = new Map<string, number>();
    const ackLatencyByFrame = new Map<string, number>();
    const ackAtByFrame = new Map<string, number>();
    const selfEchoLatencyByFrame = new Map<string, number>();
    const appendStartToSelfEchoLatencyByFrame = new Map<string, number>();
    const selfEchoAtByFrame = new Map<string, number>();
    let markSelfEchoReady: () => void = () => {};
    const selfEchoReady = args.selfEcho
      ? new Promise<void>((resolve) => {
          markSelfEchoReady = resolve;
        })
      : Promise.resolve();
    const selfEcho =
      args.selfEcho
        ? this.collectAudioSelfEcho({
            ...args,
            rpc: fixture.rpc,
            appendStartedAtByFrame,
            selfEchoLatencyByFrame,
            appendStartToSelfEchoLatencyByFrame,
            selfEchoAtByFrame,
            markReady: markSelfEchoReady,
          })
        : Promise.resolve();

    try {
      await selfEchoReady;
      const startedAt = Date.now();
      for (let frame = 1; frame <= args.framesPerPublisher; frame += 1) {
        const frameId = `p${args.publisher}-f${frame}`;
        const appendStartedAt = Date.now();
        appendStartedAtByFrame.set(frameId, appendStartedAt);
        const event = buildAudioEvent({
          config: args,
          audio: args.audio,
          publisher: String(args.publisher),
          frame,
          frameId,
        });
        const append =
          fixture.streamKind === "volatile"
            ? fixture.rpc.appendVolatile({ event })
            : fixture.rpc.append({
                event,
                durability: appendDurability(args),
              });
        appendPromises.push(
          append.then((event) => {
            if (args.measureAppendAck && args.publisher === 0) {
              const ackAt = Date.now();
              ackLatencyByFrame.set(frameId, ackAt - appendStartedAt);
              ackAtByFrame.set(frameId, ackAt);
            }
            return event;
          }),
        );
        if (args.paceMs > 0) {
          const nextFrameAt = startedAt + frame * args.paceMs;
          await sleep(Math.max(0, nextFrameAt - Date.now()));
        }
      }

      await Promise.all(appendPromises);
      await selfEcho;
      const elapsedMs = Date.now() - startedAt;
      const ackToSelfEchoLatencyMs =
        args.measureAppendAck && args.publisher === 0
          ? Array.from(ackAtByFrame, ([frameId, ackAt]) => {
              const selfEchoAt = selfEchoAtByFrame.get(frameId);
              return selfEchoAt === undefined ? 0 : selfEchoAt - ackAt;
            }).filter((latency) => latency >= 0)
          : [];

      return {
        runner: this.ctx.id.name ?? this.ctx.id.toString(),
        publisher: args.publisher,
        sent: args.framesPerPublisher,
        elapsedMs,
        appendAckLatencyMs: summarize(Array.from(ackLatencyByFrame.values())),
        selfEchoLatencyMs: summarize(Array.from(selfEchoLatencyByFrame.values())),
        appendStartToSelfEchoLatencyMs: summarize(
          Array.from(appendStartToSelfEchoLatencyByFrame.values()),
        ),
        ackToSelfEchoLatencyMs: summarize(ackToSelfEchoLatencyMs),
      };
    } finally {
      fixture.dispose();
    }
  }

  private async collectAudioSelfEcho(args: AudioChaosConfig & {
    rpc: RpcStub<StreamRpc>;
    appendStartedAtByFrame: Map<string, number>;
    selfEchoLatencyByFrame: Map<string, number>;
    appendStartToSelfEchoLatencyByFrame: Map<string, number>;
    selfEchoAtByFrame: Map<string, number>;
    markReady: () => void;
    publisher: number;
  }) {
    const reader = await objectStreamReader(args.rpc, args.streamKind);
    args.markReady();
    try {
      let ownFramesDelivered = 0;
      while (ownFramesDelivered < args.framesPerPublisher) {
        const result = await withTimeout(reader.read(), args.timeoutMs);
        if (result.done) throw new Error("publisher self-echo stream ended early");
        const frameId = readFrameId(result.value);
        if (frameId.startsWith(`p${args.publisher}-`)) {
          ownFramesDelivered += 1;
          const selfEchoAt = Date.now();
          const appendStartedAt = args.appendStartedAtByFrame.get(frameId);
          args.selfEchoAtByFrame.set(frameId, selfEchoAt);
          args.selfEchoLatencyByFrame.set(
            frameId,
            Math.max(0, selfEchoAt - Date.parse(result.value.createdAt)),
          );
          if (appendStartedAt !== undefined) {
            args.appendStartToSelfEchoLatencyByFrame.set(frameId, selfEchoAt - appendStartedAt);
          }
        }
      }
    } finally {
      reader.releaseLock();
    }
  }
}

function buildEvent(n: number, runId: string, payloadBytes: number): StreamEventInput {
  return {
    type: "benchmark.append",
    payload: { n, runId, pad: "x".repeat(Math.max(0, payloadBytes)) },
    metadata: { runId },
  };
}

type AudioFixture = {
  rawFrameBytes: number;
  base64: string;
};

function normalizeAudioChaosConfig(args: RunAudioChaosArgs): AudioChaosConfig {
  return {
    stream: args.stream ?? `audio-chaos-${crypto.randomUUID().slice(0, 8)}`,
    streamKind: args.streamKind ?? "durable",
    runId: args.runId ?? crypto.randomUUID(),
    publishers: args.publishers ?? 10,
    subscribers: args.subscribers ?? 36,
    slowSubscribers: args.slowSubscribers ?? 0,
    framesPerPublisher: args.framesPerPublisher ?? 50,
    frameMs: args.frameMs ?? 20,
    paceMs: args.paceMs ?? 0,
    sampleRate: args.sampleRate ?? 24_000,
    channels: args.channels ?? 1,
    bytesPerSample: args.bytesPerSample ?? 2,
    timeoutMs: args.timeoutMs ?? 30_000,
    durability: args.durability ?? "best-effort",
    checkpointEveryUnconfirmedAppends: args.checkpointEveryUnconfirmedAppends ?? 100,
    measureAppendAck: args.measureAppendAck ?? false,
    measureSelfEcho: args.measureSelfEcho ?? true,
  };
}

function makeAudioFixture(config: AudioChaosConfig): AudioFixture {
  const rawFrameBytes = Math.ceil(
    (config.sampleRate * config.frameMs * config.channels * config.bytesPerSample) / 1_000,
  );
  return {
    rawFrameBytes,
    base64: btoa(String.fromCharCode(...new Uint8Array(rawFrameBytes).fill(0x7f))),
  };
}

function appendDurability(config: AudioChaosConfig): AppendDurability {
  if (config.durability === "checkpointed") {
    return {
      mode: config.durability,
      checkpointEveryUnconfirmedAppends: config.checkpointEveryUnconfirmedAppends,
    };
  }
  return config.durability;
}

function buildAudioEvent(args: {
  config: AudioChaosConfig;
  audio: AudioFixture;
  publisher: string;
  frame: number;
  frameId: string;
}): StreamEventInput {
  return {
    type: "benchmark.audio-frame",
    payload: {
      runId: args.config.runId,
      frameId: args.frameId,
      publisher: args.publisher,
      frame: args.frame,
      codec: "pcm16-base64",
      sampleRate: args.config.sampleRate,
      frameMs: args.config.frameMs,
      audio: args.audio.base64,
    },
    metadata: { runId: args.config.runId },
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

async function connectStreamRpc(
  env: Env,
  stream: string,
  streamKind: StreamKind,
): Promise<StreamRpcFixture> {
  const response = await env.STREAM.getByName(stream).fetch("https://stream.internal/", {
    headers: { Upgrade: "websocket" },
  });
  const webSocket = response.webSocket;
  if (webSocket === null) throw new Error("stream DO did not return a WebSocket");
  webSocket.accept();
  const rpc = newWebSocketRpcSession<StreamRpc>(webSocket);
  return {
    rpc,
    streamKind,
    webSocket,
    dispose() {
      rpc[Symbol.dispose]();
      webSocket.close();
    },
  };
}

async function objectStreamReader(rpc: RpcStub<StreamRpc>, streamKind: StreamKind) {
  const readable = streamKind === "volatile" ? await rpc.streamVolatile() : await rpc.stream();
  // capnweb@0.8.0's TS surface only models ReadableStream<Uint8Array>, but this
  // experiment intentionally sends pass-by-value StreamEvent objects over Cap'n Web.
  return (readable as unknown as ReadableStream<StreamEvent>).getReader();
}

function summarize(values: number[]): Summary {
  const sorted = [...values].sort((a, b) => a - b);
  return {
    count: sorted.length,
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
