import { RpcTarget, newWebSocketRpcSession, type RpcStub } from "capnweb";
import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import type { StreamRpc, SubscriptionRpcTarget } from "./stream-types.js";

/** How append persistence interacts with the DO output gate. */
export type CleanDurabilityMode = "best-effort" | "confirmed-sync" | "output-gated";

export type CleanDurabilityOptions = {
  closeOutputGate: boolean;
  waitForStorageSync: boolean;
};

export function cleanDurabilityOptions(mode: CleanDurabilityMode): CleanDurabilityOptions {
  if (mode === "best-effort") return { closeOutputGate: false, waitForStorageSync: false };
  if (mode === "confirmed-sync") return { closeOutputGate: false, waitForStorageSync: true };
  return { closeOutputGate: true, waitForStorageSync: true };
}

export type RunCleanAppendThroughputArgs = {
  stream?: string;
  runId?: string;
  mode?: CleanDurabilityMode;
  messages?: number;
  payloadBytes?: number;
  simulatedStorageSyncDelayMs?: number;
  pipelined?: boolean;
};

export type CleanAppendThroughputResult = {
  type: "clean-append-throughput-result";
  streamPath: string;
  runId: string;
  durability: CleanDurabilityMode;
  messages: number;
  payloadBytes: number;
  simulatedStorageSyncDelayMs: number;
  pipelined: boolean;
  committed: number;
  elapsedMs: number;
  eventsPerSecond: number;
  appendAckLatencyMs: Summary;
  serverMaxOffset: number;
};

export type RunCleanAudioChaosArgs = {
  stream?: string;
  runId?: string;
  durability?: CleanDurabilityMode;
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
  simulatedStorageSyncDelayMs?: number;
  measureAppendAck?: boolean;
  measureSelfEcho?: boolean;
};

export type CleanAudioChaosResult = {
  type: "clean-audio-chaos-result";
  streamPath: string;
  runId: string;
  durability: CleanDurabilityMode;
  simulatedStorageSyncDelayMs: number;
  publishers: number;
  subscribers: number;
  slowSubscribers: number;
  framesPerPublisher: number;
  totalEvents: number;
  measureAppendAck: boolean;
  measureSelfEcho: boolean;
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
  subscriberResults: { subscriber: string; received: number; latencyMs: Summary }[];
  publisherResults: {
    publisher: number;
    sent: number;
    elapsedMs: number;
    appendAckLatencyMs: Summary;
    selfEchoLatencyMs: Summary;
    appendStartToSelfEchoLatencyMs: Summary;
    ackToSelfEchoLatencyMs: Summary;
  }[];
  serverDebug: unknown;
};

export type RunCleanEgressContentionArgs = {
  stream?: string;
  runId?: string;
  durability?: CleanDurabilityMode;
  simulatedStorageSyncDelayMs?: number;
  samples?: number;
};

export type CleanEgressContentionResult = {
  type: "clean-egress-contention-result";
  streamPath: string;
  runId: string;
  durability: CleanDurabilityMode;
  simulatedStorageSyncDelayMs: number;
  samples: number;
  appendAckLatencyMs: Summary;
  pingDuringAppendLatencyMs: Summary;
};

export type RunCleanUnconfirmedSweepArgs = {
  runId?: string;
  simulatedStorageSyncDelayMs?: number;
  appendMessages?: number;
  appendPayloadBytes?: number;
  audioPublishers?: number;
  audioSubscribers?: number;
  audioFramesPerPublisher?: number;
  audioPaceMs?: number;
};

export type CleanUnconfirmedSweepResult = {
  type: "clean-unconfirmed-sweep-result";
  runId: string;
  simulatedStorageSyncDelayMs: number;
  appendThroughput: CleanAppendThroughputResult[];
  egressContention: CleanEgressContentionResult[];
  audioChaos: CleanAudioChaosResult[];
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

type AudioFixture = {
  rawFrameBytes: number;
  base64: string;
};

type AudioChaosConfig = {
  stream: string;
  runId: string;
  durability: CleanDurabilityMode;
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
  simulatedStorageSyncDelayMs: number;
  measureAppendAck: boolean;
  measureSelfEcho: boolean;
};

type CleanStreamFixture = AsyncDisposable & {
  rpc: RpcStub<StreamRpc>;
  webSocket: WebSocket;
  dispose(): void;
};

const CLEAN_DURABILITY_MODES: CleanDurabilityMode[] = [
  "best-effort",
  "confirmed-sync",
  "output-gated",
];

export async function runCleanAppendThroughput(
  env: Env,
  args: RunCleanAppendThroughputArgs,
): Promise<CleanAppendThroughputResult> {
  const runId = args.runId ?? crypto.randomUUID();
  const streamPath = args.stream ?? streamPathForRun(`clean-append-${runId}`);
  const durability = args.mode ?? "best-effort";
  const messages = args.messages ?? 1_000;
  const payloadBytes = args.payloadBytes ?? 4_800;
  const simulatedStorageSyncDelayMs = args.simulatedStorageSyncDelayMs ?? 0;
  const pipelined = args.pipelined ?? false;
  const stub = cleanStreamStub(env, streamPath);

  await configureStream(stub, simulatedStorageSyncDelayMs);

  const appendAckLatencies: number[] = [];
  const startedAt = Date.now();
  let committed = 0;

  if (pipelined) {
    const pending: Promise<StreamEvent>[] = [];
    const dispatchStartedAt = Date.now();
    for (let n = 1; n <= messages; n += 1) {
      pending.push(
        timedAppend(stub, buildBenchEvent(n, runId, payloadBytes), durability, appendAckLatencies),
      );
    }
    void dispatchStartedAt;
    await Promise.all(pending);
    committed = messages;
  } else {
    for (let n = 1; n <= messages; n += 1) {
      await timedAppend(stub, buildBenchEvent(n, runId, payloadBytes), durability, appendAckLatencies);
      committed += 1;
    }
  }

  const elapsedMs = Date.now() - startedAt;
  const debug = await stub.debug();

  return {
    type: "clean-append-throughput-result",
    streamPath,
    runId,
    durability,
    messages,
    payloadBytes,
    simulatedStorageSyncDelayMs,
    pipelined,
    committed,
    elapsedMs,
    eventsPerSecond: committed / (elapsedMs / 1_000),
    appendAckLatencyMs: summarize(appendAckLatencies),
    serverMaxOffset: debug.state.maxOffset,
  };
}

export async function runCleanEgressContention(
  env: Env,
  args: RunCleanEgressContentionArgs,
): Promise<CleanEgressContentionResult> {
  const runId = args.runId ?? crypto.randomUUID();
  const streamPath = args.stream ?? streamPathForRun(`clean-egress-${runId}`);
  const durability = args.durability ?? "confirmed-sync";
  const simulatedStorageSyncDelayMs = args.simulatedStorageSyncDelayMs ?? 200;
  const samples = args.samples ?? 20;
  const stub = cleanStreamStub(env, streamPath);

  await configureStream(stub, simulatedStorageSyncDelayMs);
  await using probe = await connectCleanStreamRpc(env, streamPath);

  const appendAckLatencies: number[] = [];
  const pingLatencies: number[] = [];

  for (let i = 0; i < samples; i += 1) {
    const appendStartedAt = Date.now();
    const appendPromise = stub.append({
      event: buildBenchEvent(i + 1, runId, 256),
      durability: cleanDurabilityOptions(durability),
    });
    const pingStartedAt = Date.now();
    await probe.rpc.ping();
    pingLatencies.push(Date.now() - pingStartedAt);
    await appendPromise;
    appendAckLatencies.push(Date.now() - appendStartedAt);
  }

  return {
    type: "clean-egress-contention-result",
    streamPath,
    runId,
    durability,
    simulatedStorageSyncDelayMs,
    samples,
    appendAckLatencyMs: summarize(appendAckLatencies),
    pingDuringAppendLatencyMs: summarize(pingLatencies),
  };
}

export async function runCleanAudioChaos(
  env: Env,
  args: RunCleanAudioChaosArgs,
): Promise<CleanAudioChaosResult> {
  const config = normalizeCleanAudioChaosConfig(args);
  const totalEvents = config.publishers * config.framesPerPublisher;
  const audio = makeAudioFixture(config);
  const stub = cleanStreamStub(env, config.stream);

  await configureStream(stub, config.simulatedStorageSyncDelayMs);

  const activeSubscribers = Array.from({ length: config.subscribers }, (_, i) =>
    env.BENCHMARK_RUNNER.getByName(`${config.runId}:clean-subscriber:${i}`).runCleanAudioSubscriber({
      ...config,
      subscriber: `active-${i}`,
    }),
  );

  const passiveSubscribers = Array.from({ length: config.slowSubscribers }, (_, i) =>
    env.BENCHMARK_RUNNER.getByName(`${config.runId}:clean-passive:${i}`).runCleanAudioPassiveSubscriber(
      {
        ...config,
        subscriber: `passive-${i}`,
        holdMs: config.framesPerPublisher * Math.max(config.paceMs, 1) + 3_000,
      },
    ),
  );

  await sleep(250);

  const publishStartedAt = Date.now();
  const publishers = Array.from({ length: config.publishers }, (_, publisher) =>
    env.BENCHMARK_RUNNER.getByName(`${config.runId}:clean-publisher:${publisher}`).runCleanAudioPublisher(
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
  const serverDebug = await stub.debug();

  return {
    type: "clean-audio-chaos-result",
    streamPath: config.stream,
    runId: config.runId,
    durability: config.durability,
    simulatedStorageSyncDelayMs: config.simulatedStorageSyncDelayMs,
    publishers: config.publishers,
    subscribers: config.subscribers,
    slowSubscribers: config.slowSubscribers,
    framesPerPublisher: config.framesPerPublisher,
    totalEvents,
    measureAppendAck: config.measureAppendAck,
    measureSelfEcho: config.measureSelfEcho,
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
    minFrameDeliveries: frameDeliveryCounts.length === 0 ? 0 : Math.min(...frameDeliveryCounts),
    maxFrameDeliveries: frameDeliveryCounts.length === 0 ? 0 : Math.max(...frameDeliveryCounts),
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

export async function runCleanUnconfirmedSweep(
  env: Env,
  args: RunCleanUnconfirmedSweepArgs,
): Promise<CleanUnconfirmedSweepResult> {
  const runId = args.runId ?? crypto.randomUUID();
  const simulatedStorageSyncDelayMs = args.simulatedStorageSyncDelayMs ?? 0;
  const appendMessages = args.appendMessages ?? 1_000;
  const appendPayloadBytes = args.appendPayloadBytes ?? 4_800;

  const appendThroughput: CleanAppendThroughputResult[] = [];
  for (const mode of CLEAN_DURABILITY_MODES) {
    appendThroughput.push(
      await runCleanAppendThroughput(env, {
        runId: `${runId}:append:${mode}`,
        mode,
        messages: appendMessages,
        payloadBytes: appendPayloadBytes,
        simulatedStorageSyncDelayMs,
      }),
    );
  }

  const egressContention: CleanEgressContentionResult[] = [];
  for (const durability of CLEAN_DURABILITY_MODES) {
    egressContention.push(
      await runCleanEgressContention(env, {
        runId: `${runId}:egress:${durability}`,
        durability,
        simulatedStorageSyncDelayMs: Math.max(simulatedStorageSyncDelayMs, 200),
      }),
    );
  }

  const audioChaos: CleanAudioChaosResult[] = [];
  for (const durability of CLEAN_DURABILITY_MODES) {
    audioChaos.push(
      await runCleanAudioChaos(env, {
        runId: `${runId}:audio:${durability}`,
        durability,
        publishers: args.audioPublishers ?? 10,
        subscribers: args.audioSubscribers ?? 36,
        framesPerPublisher: args.audioFramesPerPublisher ?? 50,
        paceMs: args.audioPaceMs ?? 20,
        simulatedStorageSyncDelayMs,
        measureAppendAck: true,
        measureSelfEcho: true,
      }),
    );
  }

  return {
    type: "clean-unconfirmed-sweep-result",
    runId,
    simulatedStorageSyncDelayMs,
    appendThroughput,
    egressContention,
    audioChaos,
  };
}

export type CleanAudioSubscriberResult = {
  subscriber: string;
  received: number;
  samples: { frameId: string; latencyMs: number }[];
  latencyMs: Summary;
};

export async function runCleanAudioSubscriberOnRunner(
  env: Env,
  args: AudioChaosConfig & { subscriber: string },
): Promise<CleanAudioSubscriberResult> {
  await using fixture = await connectCleanStreamRpc(env, args.stream);
  const subscription = new AudioSubscriptionTarget();
  await fixture.rpc.initInboundSubscription({
    subscriptionRpcTarget: subscription,
    afterOffset: 1,
  });
  await sleep(250);

  const samples: { frameId: string; latencyMs: number }[] = [];
  for (let received = 0; received < args.publishers * args.framesPerPublisher; received += 1) {
    const event = await subscription.read(args.timeoutMs);
    samples.push({
      frameId: readFrameId(event),
      latencyMs: Math.max(0, Date.now() - Date.parse(event.createdAt)),
    });
  }

  return {
    subscriber: args.subscriber,
    received: samples.length,
    samples,
    latencyMs: summarize(samples.map((sample) => sample.latencyMs)),
  };
}

export async function runCleanAudioPassiveSubscriberOnRunner(
  env: Env,
  args: AudioChaosConfig & { subscriber: string; holdMs: number },
) {
  await using fixture = await connectCleanStreamRpc(env, args.stream);
  const subscription = new AudioSubscriptionTarget();
  await fixture.rpc.initInboundSubscription({ subscriptionRpcTarget: subscription });
  try {
    await sleep(args.holdMs);
    return { subscriber: args.subscriber, heldMs: args.holdMs };
  } finally {
    fixture.dispose();
  }
}

export type CleanAudioPublisherResult = {
  publisher: number;
  sent: number;
  elapsedMs: number;
  appendAckLatencyMs: Summary;
  selfEchoLatencyMs: Summary;
  appendStartToSelfEchoLatencyMs: Summary;
  ackToSelfEchoLatencyMs: Summary;
};

export async function runCleanAudioPublisherOnRunner(
  env: Env,
  args: AudioChaosConfig & {
    audio: AudioFixture;
    publisher: number;
    selfEcho: boolean;
  },
): Promise<CleanAudioPublisherResult> {
  await using fixture = await connectCleanStreamRpc(env, args.stream);
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

  const selfEchoDone = args.selfEcho
    ? (async () => {
        const subscription = new AudioSubscriptionTarget();
        await fixture.rpc.initInboundSubscription({
          subscriptionRpcTarget: subscription,
          afterOffset: 1,
        });
        await sleep(250);
        markSelfEchoReady();
        await collectSelfEcho({
          subscription,
          publisher: args.publisher,
          framesPerPublisher: args.framesPerPublisher,
          timeoutMs: args.timeoutMs,
          appendStartedAtByFrame,
          selfEchoLatencyByFrame,
          appendStartToSelfEchoLatencyByFrame,
          selfEchoAtByFrame,
        });
      })()
    : Promise.resolve();

  await selfEchoReady;
  const startedAt = Date.now();
  for (let frame = 1; frame <= args.framesPerPublisher; frame += 1) {
    const frameId = `p${args.publisher}-f${frame}`;
    const appendStartedAt = Date.now();
    appendStartedAtByFrame.set(frameId, appendStartedAt);
    const event = buildAudioEvent(args, frameId, frame);
    const append = fixture.rpc.append({
      event,
      durability: cleanDurabilityOptions(args.durability),
    });
    appendPromises.push(
      append.then((committed) => {
        if (args.measureAppendAck && args.publisher === 0) {
          const ackAt = Date.now();
          ackLatencyByFrame.set(frameId, ackAt - appendStartedAt);
          ackAtByFrame.set(frameId, ackAt);
        }
        return committed;
      }),
    );
    if (args.paceMs > 0) {
      const nextFrameAt = startedAt + frame * args.paceMs;
      await sleep(Math.max(0, nextFrameAt - Date.now()));
    }
  }

  await Promise.all(appendPromises);
  await selfEchoDone;
  const elapsedMs = Date.now() - startedAt;
  const ackToSelfEchoLatencyMs =
    args.measureAppendAck && args.publisher === 0
      ? Array.from(ackAtByFrame, ([frameId, ackAt]) => {
          const selfEchoAt = selfEchoAtByFrame.get(frameId);
          return selfEchoAt === undefined ? 0 : selfEchoAt - ackAt;
        }).filter((latency) => latency >= 0)
      : [];

  return {
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
}

class AudioSubscriptionTarget extends RpcTarget implements SubscriptionRpcTarget {
  readonly #events: StreamEvent[] = [];
  readonly #waiters: ((event: StreamEvent) => void)[] = [];

  consumeEvents(args: { events: StreamEvent[] }): undefined {
    for (const event of args.events) {
      if (event.type !== "benchmark.audio-frame") continue;
      const waiter = this.#waiters.shift();
      if (waiter === undefined) {
        this.#events.push(event);
      } else {
        waiter(event);
      }
    }
  }

  read(timeoutMs: number): Promise<StreamEvent> {
    const event = this.#events.shift();
    if (event !== undefined) return Promise.resolve(event);
    return withTimeout(
      new Promise((resolve) => {
        this.#waiters.push(resolve);
      }),
      timeoutMs,
    );
  }
}

async function collectSelfEcho(args: {
  subscription: AudioSubscriptionTarget;
  publisher: number;
  framesPerPublisher: number;
  timeoutMs: number;
  appendStartedAtByFrame: Map<string, number>;
  selfEchoLatencyByFrame: Map<string, number>;
  appendStartToSelfEchoLatencyByFrame: Map<string, number>;
  selfEchoAtByFrame: Map<string, number>;
}) {
  const prefix = `p${args.publisher}-f`;
  while (args.selfEchoLatencyByFrame.size < args.framesPerPublisher) {
    const event = await args.subscription.read(args.timeoutMs);
    const frameId = readFrameId(event);
    if (!frameId.startsWith(prefix)) continue;
    const deliveredAt = Date.now();
    args.selfEchoAtByFrame.set(frameId, deliveredAt);
    args.selfEchoLatencyByFrame.set(
      frameId,
      Math.max(0, deliveredAt - Date.parse(event.createdAt)),
    );
    const appendStartedAt = args.appendStartedAtByFrame.get(frameId);
    if (appendStartedAt !== undefined) {
      args.appendStartToSelfEchoLatencyByFrame.set(frameId, deliveredAt - appendStartedAt);
    }
  }
}

function streamPathForRun(prefix: string) {
  return `${prefix}-${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
}

function cleanStreamStub(env: Env, streamPath: string) {
  return env.STREAM.getByName(`stream:${streamPath}`);
}

async function configureStream(
  stub: ReturnType<typeof cleanStreamStub>,
  simulatedStorageSyncDelayMs: number,
) {
  if (simulatedStorageSyncDelayMs <= 0) return;
  await stub.append({
    event: {
      type: "events.iterate.com/stream/configured",
      payload: { config: { simulatedStorageSyncDelayMs } },
    },
    durability: cleanDurabilityOptions("confirmed-sync"),
  });
}

async function timedAppend(
  stub: ReturnType<typeof cleanStreamStub>,
  event: StreamEventInput,
  durability: CleanDurabilityMode,
  appendAckLatencies: number[],
) {
  const startedAt = Date.now();
  const committed = await stub.append({
    event,
    durability: cleanDurabilityOptions(durability),
  });
  appendAckLatencies.push(Date.now() - startedAt);
  return committed;
}

async function connectCleanStreamRpc(env: Env, streamPath: string): Promise<CleanStreamFixture> {
  const response = await env.STREAM.getByName(`stream:${streamPath}`).fetch(
    "https://clean-stream.internal/",
    { headers: { Upgrade: "websocket" } },
  );
  const webSocket = response.webSocket;
  if (webSocket === null) throw new Error("clean stream DO did not return a WebSocket");
  webSocket.accept();
  const rpc = newWebSocketRpcSession<StreamRpc>(webSocket);
  return {
    rpc,
    webSocket,
    dispose() {
      rpc[Symbol.dispose]();
      webSocket.close();
    },
    async [Symbol.asyncDispose]() {
      rpc[Symbol.dispose]();
      webSocket.close();
    },
  };
}

function buildBenchEvent(n: number, runId: string, payloadBytes: number): StreamEventInput {
  return {
    type: "benchmark.append",
    payload: { n, runId, pad: "x".repeat(Math.max(0, payloadBytes)) },
    metadata: { runId },
  };
}

function buildAudioEvent(
  args: AudioChaosConfig & { audio: AudioFixture; publisher: number },
  frameId: string,
  frame: number,
): StreamEventInput {
  return {
    type: "benchmark.audio-frame",
    payload: {
      runId: args.runId,
      frameId,
      publisher: String(args.publisher),
      frame,
      codec: "pcm16-base64",
      sampleRate: args.sampleRate,
      frameMs: args.frameMs,
      audio: args.audio.base64,
    },
    metadata: { runId: args.runId },
  };
}

function normalizeCleanAudioChaosConfig(args: RunCleanAudioChaosArgs): AudioChaosConfig {
  const runId = args.runId ?? crypto.randomUUID();
  return {
    stream: args.stream ?? streamPathForRun(`clean-audio-${runId}`),
    runId,
    durability: args.durability ?? "best-effort",
    publishers: args.publishers ?? 10,
    subscribers: args.subscribers ?? 36,
    slowSubscribers: args.slowSubscribers ?? 0,
    framesPerPublisher: args.framesPerPublisher ?? 50,
    frameMs: args.frameMs ?? 20,
    paceMs: args.paceMs ?? 20,
    sampleRate: args.sampleRate ?? 24_000,
    channels: args.channels ?? 1,
    bytesPerSample: args.bytesPerSample ?? 2,
    timeoutMs: args.timeoutMs ?? 30_000,
    simulatedStorageSyncDelayMs: args.simulatedStorageSyncDelayMs ?? 0,
    measureAppendAck: args.measureAppendAck ?? true,
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
