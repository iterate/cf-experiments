import { newWebSocketRpcSession, RpcTarget, type RpcStub } from "capnweb";
import { DurableObject } from "cloudflare:workers";

const DISPOSE = Symbol.for("dispose");

type Mode =
  | "raw"
  | "capnweb-event"
  | "capnweb-process-events"
  | "capnweb-batch"
  | "capnweb-process-events-batch";

type EventInput = {
  frameId: string;
  payload: string;
};

type StreamEvent = EventInput & {
  offset: number;
  createdAt: string;
};

type ClientMain = {
  afterAppend(args: { event: StreamEvent }): unknown;
  afterAppendBatch(args: { events: StreamEvent[] }): unknown;
  processEvents(args: { events: StreamEvent[] }): unknown;
};

type RawAppend = {
  op: "append";
  requestId: string;
  event: EventInput;
};

type RawSubscribe = {
  op: "subscribe";
};

type CapnwebSubscriber = {
  mode: "event" | "process-events" | "batch" | "process-events-batch";
  client: RpcStub<ClientMain>;
  batchMs: number;
  subscribedAfterOffset: number;
};

type BenchmarkArgs = {
  mode?: Mode;
  stream?: string;
  runId?: string;
  publishers?: number;
  subscribers?: number;
  framesPerPublisher?: number;
  paceMs?: number;
  payloadBytes?: number;
  timeoutMs?: number;
  batchMs?: number;
};

type BenchmarkConfig = Required<BenchmarkArgs>;

type Sample = {
  frameId: string;
  latencyMs: number;
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

type SubscriberResult = {
  subscriber: number;
  received: number;
  samples: Sample[];
  latencyMs: Summary;
};

type PublisherResult = {
  publisher: number;
  sent: number;
  ackSamples: number[];
  appendAckLatencyMs: Summary;
};

type RawFixture = {
  send(frame: unknown): void;
  read(timeoutMs: number): Promise<unknown>;
  dispose(): void;
};

type CapnwebFixture = {
  rpc: RpcStub<StreamRpc>;
  inbox: EventInbox;
  dispose(): void;
};

type StreamRpc = {
  append(args: { event: EventInput }): StreamEvent;
  subscribeEvent(args?: unknown): void;
  subscribeProcessEvents(args?: unknown): void;
  subscribeBatch(args?: { batchMs?: number }): void;
  subscribeProcessEventsBatch(args?: { batchMs?: number }): void;
  debug(): unknown;
};

interface Env {
  STREAM: DurableObjectNamespace<Stream>;
  RUNNER: DurableObjectNamespace<Runner>;
}

export class Stream extends DurableObject {
  #offset = 0;
  #rawSubscribers = new Set<WebSocket>();
  #capnwebSubscribers = new Set<CapnwebSubscriber>();
  #batchPending: StreamEvent[] = [];
  #batchFlushScheduled = false;
  #fanout = {
    rawFrames: 0,
    capnwebEventCalls: 0,
    capnwebProcessEventsCalls: 0,
    capnwebBatchEvents: 0,
    capnwebBatchCalls: 0,
    capnwebProcessEventsBatchEvents: 0,
    capnwebProcessEventsBatchCalls: 0,
  };

  async fetch(request: Request): Promise<Response> {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("This endpoint only accepts WebSocket requests.", { status: 400 });
    }

    const url = new URL(request.url);
    if (url.searchParams.get("mode") === "raw") return this.#fetchRaw();
    return this.#fetchCapnweb();
  }

  append(args: { event: EventInput }): StreamEvent {
    const event = this.#commit(args.event);
    this.#broadcast(event);
    return event;
  }

  subscribeEventForSession(client: RpcStub<ClientMain>): void {
    this.#capnwebSubscribers.add({
      mode: "event",
      client,
      batchMs: 0,
      subscribedAfterOffset: this.#offset,
    });
  }

  subscribeBatchForSession(client: RpcStub<ClientMain>, batchMs: number): void {
    this.#capnwebSubscribers.add({
      mode: "batch",
      client,
      batchMs,
      subscribedAfterOffset: this.#offset,
    });
  }

  subscribeProcessEventsForSession(client: RpcStub<ClientMain>): void {
    this.#capnwebSubscribers.add({
      mode: "process-events",
      client,
      batchMs: 0,
      subscribedAfterOffset: this.#offset,
    });
  }

  subscribeProcessEventsBatchForSession(client: RpcStub<ClientMain>, batchMs: number): void {
    this.#capnwebSubscribers.add({
      mode: "process-events-batch",
      client,
      batchMs,
      subscribedAfterOffset: this.#offset,
    });
  }

  releaseClient(client: RpcStub<ClientMain>): void {
    for (const subscriber of this.#capnwebSubscribers) {
      if (subscriber.client === client) this.#capnwebSubscribers.delete(subscriber);
    }
  }

  debug() {
    return {
      offset: this.#offset,
      subscribers: {
        raw: this.#rawSubscribers.size,
        capnwebEvent: Array.from(this.#capnwebSubscribers).filter((sub) => sub.mode === "event")
          .length,
        capnwebProcessEvents: Array.from(this.#capnwebSubscribers).filter(
          (sub) => sub.mode === "process-events",
        ).length,
        capnwebBatch: Array.from(this.#capnwebSubscribers).filter((sub) => sub.mode === "batch")
          .length,
        capnwebProcessEventsBatch: Array.from(this.#capnwebSubscribers).filter(
          (sub) => sub.mode === "process-events-batch",
        ).length,
      },
      fanout: this.#fanout,
    };
  }

  #fetchRaw(): Response {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    server.addEventListener("message", (message) => this.#handleRawMessage(server, message.data));
    server.addEventListener("close", () => this.#rawSubscribers.delete(server));
    server.addEventListener("error", () => this.#rawSubscribers.delete(server));
    return new Response(null, { status: 101, webSocket: client });
  }

  #fetchCapnweb(): Response {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    const target = new StreamTarget(this);
    const clientMain = newWebSocketRpcSession<ClientMain>(server, target);
    target.setClientMain(clientMain);
    return new Response(null, { status: 101, webSocket: client });
  }

  #handleRawMessage(webSocket: WebSocket, data: unknown): void {
    const frame = parseFrame(data) as RawAppend | RawSubscribe;
    if (frame.op === "subscribe") {
      this.#rawSubscribers.add(webSocket);
      webSocket.send(JSON.stringify({ op: "subscribed" }));
      return;
    }

    if (frame.op !== "append") throw new Error("raw op must be subscribe or append");
    const event = this.#commit(frame.event);
    this.#broadcast(event);
    webSocket.send(JSON.stringify({ op: "ack", requestId: frame.requestId, event }));
  }

  #commit(input: EventInput): StreamEvent {
    this.#offset += 1;
    return {
      ...input,
      offset: this.#offset,
      createdAt: new Date().toISOString(),
    };
  }

  #broadcast(event: StreamEvent): void {
    const rawMessage = JSON.stringify({ op: "event", event });
    this.#fanout.rawFrames += this.#rawSubscribers.size;
    for (const subscriber of this.#rawSubscribers) {
      subscriber.send(rawMessage);
    }

    let hasBatchSubscribers = false;
    for (const subscriber of this.#capnwebSubscribers) {
      if (subscriber.mode === "event") {
        this.#fanout.capnwebEventCalls += 1;
        const result = subscriber.client.afterAppend({ event });
        disposeIfPresent(result);
        continue;
      }

      if (subscriber.mode === "process-events") {
        this.#fanout.capnwebProcessEventsCalls += 1;
        const result = subscriber.client.processEvents({ events: [event] });
        disposeIfPresent(result);
        continue;
      }

      hasBatchSubscribers = true;
    }
    if (hasBatchSubscribers) this.#queueBatchEvent(event);
  }

  #queueBatchEvent(event: StreamEvent): void {
    const batchSubscribers = this.#batchSubscribers();
    if (batchSubscribers.length === 0) return;
    for (const subscriber of batchSubscribers) {
      if (subscriber.mode === "batch") {
        this.#fanout.capnwebBatchEvents += 1;
      } else {
        this.#fanout.capnwebProcessEventsBatchEvents += 1;
      }
    }
    this.#batchPending.push(event);
    if (!this.#batchFlushScheduled) {
      this.#batchFlushScheduled = true;
      setTimeout(() => this.#flushBatch(), batchDelayMs(batchSubscribers));
    }
  }

  #flushBatch(): void {
    this.#batchFlushScheduled = false;
    const events = this.#batchPending.splice(0);
    if (events.length === 0) return;
    for (const subscriber of this.#batchSubscribers()) {
      const visibleEvents = visibleBatchEvents(events, subscriber.subscribedAfterOffset);
      if (visibleEvents.length === 0) continue;
      const result =
        subscriber.mode === "batch"
          ? subscriber.client.afterAppendBatch({ events: visibleEvents })
          : subscriber.client.processEvents({ events: visibleEvents });
      if (subscriber.mode === "batch") {
        this.#fanout.capnwebBatchCalls += 1;
      } else {
        this.#fanout.capnwebProcessEventsBatchCalls += 1;
      }
      disposeIfPresent(result);
    }
  }

  #batchSubscribers(): CapnwebSubscriber[] {
    return Array.from(this.#capnwebSubscribers).filter(
      (subscriber) => subscriber.mode === "batch" || subscriber.mode === "process-events-batch",
    );
  }
}

class StreamTarget extends RpcTarget implements StreamRpc {
  #clientMain: RpcStub<ClientMain> | undefined;

  constructor(private readonly stream: Stream) {
    super();
  }

  setClientMain(clientMain: RpcStub<ClientMain>): void {
    this.#clientMain = clientMain;
  }

  append(args: { event: EventInput }): StreamEvent {
    return this.stream.append(args);
  }

  subscribeEvent(args?: unknown): void {
    if (args !== undefined) throw new Error("subscribeEvent does not accept arguments");
    if (this.#clientMain === undefined) throw new Error("client main object is required");
    this.stream.subscribeEventForSession(this.#clientMain);
  }

  subscribeBatch(args?: { batchMs?: number }): void {
    if (this.#clientMain === undefined) throw new Error("client main object is required");
    this.stream.subscribeBatchForSession(this.#clientMain, args?.batchMs ?? 0);
  }

  subscribeProcessEvents(args?: unknown): void {
    if (args !== undefined) throw new Error("subscribeProcessEvents does not accept arguments");
    if (this.#clientMain === undefined) throw new Error("client main object is required");
    this.stream.subscribeProcessEventsForSession(this.#clientMain);
  }

  subscribeProcessEventsBatch(args?: { batchMs?: number }): void {
    if (this.#clientMain === undefined) throw new Error("client main object is required");
    this.stream.subscribeProcessEventsBatchForSession(this.#clientMain, args?.batchMs ?? 0);
  }

  debug(): unknown {
    return this.stream.debug();
  }

  [DISPOSE](): void {
    if (this.#clientMain !== undefined) this.stream.releaseClient(this.#clientMain);
  }
}

export class Runner extends DurableObject<Env> {
  async runBenchmark(args: BenchmarkArgs) {
    const config = normalize(args);
    const stream = this.env.STREAM.getByName(config.stream);
    const subscriberPromises = Array.from({ length: config.subscribers }, (_, subscriber) =>
      this.env.RUNNER.getByName(`${config.runId}:subscriber:${subscriber}`).runSubscriber({
        ...config,
        subscriber,
      }),
    );

    const attachDeadline = Date.now() + 5_000;
    while (Date.now() < attachDeadline) {
      const count = await subscriberCount(stream, config.mode);
      if (count >= config.subscribers) break;
      await sleep(25);
    }

    const startedAt = Date.now();
    const publisherPromises = Array.from({ length: config.publishers }, (_, publisher) =>
      this.env.RUNNER.getByName(`${config.runId}:publisher:${publisher}`).runPublisher({
        ...config,
        publisher,
      }),
    );
    const [subscriberResults, publisherResults] = await Promise.all([
      Promise.all(subscriberPromises),
      Promise.all(publisherPromises),
    ]);
    const elapsedMs = Date.now() - startedAt;

    const frameLatencies = new Map<string, number[]>();
    for (const subscriber of subscriberResults) {
      for (const sample of subscriber.samples) {
        const latencies = frameLatencies.get(sample.frameId) ?? [];
        latencies.push(sample.latencyMs);
        frameLatencies.set(sample.frameId, latencies);
      }
    }

    const allSubscribersLatencies: number[] = [];
    const firstSubscriberLatencies: number[] = [];
    const deliveryCounts: number[] = [];
    for (let publisher = 0; publisher < config.publishers; publisher += 1) {
      for (let frame = 1; frame <= config.framesPerPublisher; frame += 1) {
        const latencies = frameLatencies.get(`p${publisher}-f${frame}`) ?? [];
        deliveryCounts.push(latencies.length);
        if (latencies.length > 0) firstSubscriberLatencies.push(Math.min(...latencies));
        if (latencies.length === config.subscribers) {
          allSubscribersLatencies.push(Math.max(...latencies));
        }
      }
    }

    return {
      type: "capnweb-rawws-parity-result",
      mode: config.mode,
      stream: config.stream,
      runId: config.runId,
      publishers: config.publishers,
      subscribers: config.subscribers,
      framesPerPublisher: config.framesPerPublisher,
      batchMs: config.batchMs,
      totalEvents: config.publishers * config.framesPerPublisher,
      elapsedMs,
      framesFullyDelivered: deliveryCounts.filter((count) => count === config.subscribers).length,
      framesMissingFullDelivery: deliveryCounts.filter((count) => count !== config.subscribers)
        .length,
      minFrameDeliveries: Math.min(...deliveryCounts),
      maxFrameDeliveries: Math.max(...deliveryCounts),
      firstSubscriberCreatedAtLatencyMs: summarize(firstSubscriberLatencies),
      allSubscribersCreatedAtLatencyMs: summarize(allSubscribersLatencies),
      subscriberCreatedAtLatencyMs: summarize(
        subscriberResults.flatMap((subscriber) => subscriber.samples.map((sample) => sample.latencyMs)),
      ),
      appendAckLatencyMs: summarize(publisherResults.flatMap((publisher) => publisher.ackSamples)),
      publisherResults: publisherResults.map(({ publisher, sent, appendAckLatencyMs }) => ({
        publisher,
        sent,
        appendAckLatencyMs,
      })),
      subscriberResults: subscriberResults.map(({ subscriber, received, latencyMs }) => ({
        subscriber,
        received,
        latencyMs,
      })),
      serverDebug: await stream.debug(),
    };
  }

  async runSubscriber(
    args: BenchmarkConfig & { subscriber: number },
  ): Promise<SubscriberResult> {
    const samples: Sample[] = [];
    const total = args.publishers * args.framesPerPublisher;
    if (args.mode === "raw") {
      const fixture = await connectRaw(this.env, args.stream);
      fixture.send({ op: "subscribe" });
      await waitForRawOp(fixture, "subscribed", args.timeoutMs);
      try {
        for (let received = 0; received < total; received += 1) {
          const frame = await waitForRawOp(fixture, "event", args.timeoutMs);
          const event = readRawEvent(frame);
          samples.push(sample(event));
        }
      } finally {
        fixture.dispose();
      }
    } else {
      const fixture = await connectCapnweb(this.env, args.stream);
      if (args.mode === "capnweb-event") {
        await fixture.rpc.subscribeEvent();
      } else if (args.mode === "capnweb-process-events") {
        await fixture.rpc.subscribeProcessEvents();
      } else if (args.mode === "capnweb-process-events-batch") {
        await fixture.rpc.subscribeProcessEventsBatch({ batchMs: args.batchMs });
      } else {
        await fixture.rpc.subscribeBatch({ batchMs: args.batchMs });
      }
      try {
        for (let received = 0; received < total; received += 1) {
          samples.push(sample(await fixture.inbox.read(args.timeoutMs)));
        }
      } finally {
        fixture.dispose();
      }
    }

    return {
      subscriber: args.subscriber,
      received: samples.length,
      samples,
      latencyMs: summarize(samples.map((entry) => entry.latencyMs)),
    };
  }

  async runPublisher(args: BenchmarkConfig & { publisher: number }): Promise<PublisherResult> {
    const ackLatencies: number[] = [];
    if (args.mode === "raw") {
      const fixture = await connectRaw(this.env, args.stream);
      try {
        const startedAt = Date.now();
        for (let frame = 1; frame <= args.framesPerPublisher; frame += 1) {
          const frameId = `p${args.publisher}-f${frame}`;
          const appendStartedAt = Date.now();
          fixture.send({
            op: "append",
            requestId: frameId,
            event: buildEvent(args, args.publisher, frame),
          });
          await waitForRawOp(fixture, "ack", args.timeoutMs);
          ackLatencies.push(Date.now() - appendStartedAt);
          await pace(startedAt, frame, args.paceMs);
        }
      } finally {
        fixture.dispose();
      }
    } else {
      const fixture = await connectCapnweb(this.env, args.stream);
      try {
        const startedAt = Date.now();
        for (let frame = 1; frame <= args.framesPerPublisher; frame += 1) {
          const appendStartedAt = Date.now();
          await fixture.rpc.append({ event: buildEvent(args, args.publisher, frame) });
          ackLatencies.push(Date.now() - appendStartedAt);
          await pace(startedAt, frame, args.paceMs);
        }
      } finally {
        fixture.dispose();
      }
    }

    return {
      publisher: args.publisher,
      sent: args.framesPerPublisher,
      ackSamples: ackLatencies,
      appendAckLatencyMs: summarize(ackLatencies),
    };
  }
}

class ClientMainTarget extends RpcTarget implements ClientMain {
  #disposed = false;

  constructor(private readonly inbox: EventInbox) {
    super();
  }

  afterAppend(args: { event: StreamEvent }): undefined {
    if (!this.#disposed) this.inbox.push(args.event);
  }

  afterAppendBatch(args: { events: StreamEvent[] }): undefined {
    if (!this.#disposed) {
      for (const event of args.events) this.inbox.push(event);
    }
  }

  processEvents(args: { events: StreamEvent[] }): undefined {
    if (!this.#disposed) {
      for (const event of args.events) this.inbox.push(event);
    }
  }

  [DISPOSE](): void {
    this.#disposed = true;
  }
}

class EventInbox {
  #events: StreamEvent[] = [];
  #waiters: ((event: StreamEvent) => void)[] = [];

  push(event: StreamEvent): void {
    const waiter = this.#waiters.shift();
    if (waiter === undefined) {
      this.#events.push(event);
    } else {
      waiter(event);
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

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/benchmark") {
      const runId = url.searchParams.get("run-id") ?? crypto.randomUUID();
      return Response.json(
        await env.RUNNER.getByName(`${runId}:orchestrator`).runBenchmark({
          mode: modeParam(url),
          stream: url.searchParams.get("stream") ?? undefined,
          runId,
          publishers: positiveIntParam(url, "publishers"),
          subscribers: nonNegativeIntParam(url, "subscribers"),
          framesPerPublisher: positiveIntParam(url, "frames-per-publisher"),
          paceMs: nonNegativeIntParam(url, "pace-ms"),
          payloadBytes: nonNegativeIntParam(url, "payload-bytes"),
          timeoutMs: positiveIntParam(url, "timeout-ms"),
          batchMs: nonNegativeIntParam(url, "batch-ms"),
        }),
      );
    }

    const stream = url.pathname.slice(1) || "default";
    return env.STREAM.getByName(stream).fetch(request);
  },
} satisfies ExportedHandler<Env>;

async function connectRaw(env: Env, stream: string): Promise<RawFixture> {
  const url = new URL("https://stream.internal/");
  url.searchParams.set("mode", "raw");
  const response = await env.STREAM.getByName(stream).fetch(url, {
    headers: { Upgrade: "websocket" },
  });
  const webSocket = response.webSocket;
  if (webSocket === null) throw new Error("raw stream did not return a WebSocket");
  webSocket.accept();
  const inbox = new MessageInbox();
  webSocket.addEventListener("message", (event) => inbox.push(parseFrame(event.data)));
  return {
    send(frame) {
      webSocket.send(JSON.stringify(frame));
    },
    read(timeoutMs) {
      return inbox.read(timeoutMs);
    },
    dispose() {
      webSocket.close();
    },
  };
}

async function connectCapnweb(env: Env, stream: string): Promise<CapnwebFixture> {
  const inbox = new EventInbox();
  const clientMain = new ClientMainTarget(inbox);
  const response = await env.STREAM.getByName(stream).fetch("https://stream.internal/", {
    headers: { Upgrade: "websocket" },
  });
  const webSocket = response.webSocket;
  if (webSocket === null) throw new Error("Cap'n Web stream did not return a WebSocket");
  webSocket.accept();
  const rpc = newWebSocketRpcSession<StreamRpc>(webSocket, clientMain);
  return {
    rpc,
    inbox,
    dispose() {
      disposeIfPresent(clientMain);
      disposeIfPresent(rpc);
      webSocket.close();
    },
  };
}

class MessageInbox {
  #messages: unknown[] = [];
  #waiters: ((message: unknown) => void)[] = [];

  push(message: unknown): void {
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

async function subscriberCount(stream: DurableObjectStub<Stream>, mode: Mode): Promise<number> {
  const debug = (await stream.debug()) as ReturnType<Stream["debug"]>;
  if (mode === "raw") return debug.subscribers.raw;
  if (mode === "capnweb-event") return debug.subscribers.capnwebEvent;
  if (mode === "capnweb-process-events") return debug.subscribers.capnwebProcessEvents;
  if (mode === "capnweb-process-events-batch") return debug.subscribers.capnwebProcessEventsBatch;
  return debug.subscribers.capnwebBatch;
}

function normalize(args: BenchmarkArgs): BenchmarkConfig {
  const runId = args.runId ?? crypto.randomUUID();
  return {
    mode: args.mode ?? "raw",
    stream: args.stream ?? `parity-${runId.slice(0, 8)}`,
    runId,
    publishers: args.publishers ?? 10,
    subscribers: args.subscribers ?? 36,
    framesPerPublisher: args.framesPerPublisher ?? 50,
    paceMs: args.paceMs ?? 20,
    payloadBytes: args.payloadBytes ?? 1280,
    timeoutMs: args.timeoutMs ?? 30_000,
    batchMs: args.batchMs ?? 0,
  };
}

function buildEvent(config: BenchmarkConfig, publisher: number, frame: number): EventInput {
  const frameId = `p${publisher}-f${frame}`;
  return {
    frameId,
    payload: "x".repeat(config.payloadBytes),
  };
}

function sample(event: StreamEvent): Sample {
  return {
    frameId: event.frameId,
    latencyMs: Math.max(0, Date.now() - Date.parse(event.createdAt)),
  };
}

async function waitForRawOp(fixture: RawFixture, op: string, timeoutMs: number): Promise<unknown> {
  while (true) {
    const message = await fixture.read(timeoutMs);
    if (isRecord(message) && message.op === op) return message;
  }
}

function readRawEvent(message: unknown): StreamEvent {
  if (!isRecord(message) || !isRecord(message.event)) {
    throw new Error("raw frame did not contain an event");
  }
  return message.event as StreamEvent;
}

function parseFrame(data: unknown): unknown {
  if (typeof data === "string") return JSON.parse(data);
  if (data instanceof ArrayBuffer) return JSON.parse(new TextDecoder().decode(data));
  if (ArrayBuffer.isView(data)) return JSON.parse(new TextDecoder().decode(data));
  throw new TypeError(`unexpected frame data: ${String(data)}`);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function disposeIfPresent(value: unknown): void {
  if ((typeof value !== "object" || value === null) && typeof value !== "function") return;
  const dispose = Reflect.get(value, DISPOSE);
  if (typeof dispose === "function") Reflect.apply(dispose, value, []);
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

function percentile(sorted: number[], p: number): number {
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

async function pace(startedAt: number, frame: number, paceMs: number): Promise<void> {
  if (paceMs === 0) return;
  const nextFrameAt = startedAt + frame * paceMs;
  await sleep(Math.max(0, nextFrameAt - Date.now()));
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function modeParam(url: URL): Mode | undefined {
  const raw = url.searchParams.get("mode");
  if (raw === null) return undefined;
  if (
    raw === "raw" ||
    raw === "capnweb-event" ||
    raw === "capnweb-process-events" ||
    raw === "capnweb-batch" ||
    raw === "capnweb-process-events-batch"
  ) {
    return raw;
  }
  throw new Error(
    "mode must be raw, capnweb-event, capnweb-process-events, capnweb-batch, or capnweb-process-events-batch",
  );
}

function positiveIntParam(url: URL, name: string): number | undefined {
  const raw = url.searchParams.get(name);
  if (raw === null) return undefined;
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) throw new Error(`${name} must be a positive integer`);
  return value;
}

function nonNegativeIntParam(url: URL, name: string): number | undefined {
  const raw = url.searchParams.get(name);
  if (raw === null) return undefined;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }
  return value;
}

function batchDelayMs(subscribers: CapnwebSubscriber[]): number {
  return Math.min(...subscribers.map((subscriber) => subscriber.batchMs));
}

function visibleBatchEvents(events: StreamEvent[], subscribedAfterOffset: number): StreamEvent[] {
  if (events[0]!.offset > subscribedAfterOffset) return events;
  if (events.at(-1)!.offset <= subscribedAfterOffset) return [];
  return events.filter((event) => event.offset > subscribedAfterOffset);
}
