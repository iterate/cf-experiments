import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import { RpcTarget } from "capnweb";
import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";
import { withStream } from "./lib/with-stream.js";

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  return Promise.race([
    promise,
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error(`timed out after ${ms}ms`)), ms),
    ),
  ]);
}

function outboundPullPushFrames(
  wsMessages: { direction: string; data: string }[],
  afterFrameIndex: number,
) {
  return wsMessages
    .slice(afterFrameIndex)
    .filter((frame) => frame.direction === "out")
    .map((frame) => JSON.parse(frame.data))
    .filter((data) => data[0] === "pull" || data[0] === "push");
}

function outboundFrames(wsMessages: { direction: string; data: string }[], afterFrameIndex: number) {
  return wsMessages
    .slice(afterFrameIndex)
    .filter((frame) => frame.direction === "out")
    .map((frame) => JSON.parse(frame.data));
}

async function readEvents(
  streamReader: ReadableStreamDefaultReader<StreamEvent>,
  count: number,
  timeoutMs: number,
) {
  const results = await Promise.all(
    Array.from({ length: count }, () => withTimeout(streamReader.read(), timeoutMs)),
  );

  const events: StreamEvent[] = [];
  for (const result of results) {
    if (result.done) throw new Error("stream ended before expected event count");
    events.push(result.value);
  }
  return events;
}

function expectContiguousOffsets(events: StreamEvent[], count: number) {
  expect(events.map((event) => event.offset)).toEqual(
    Array.from({ length: count }, (_, i) => i + 1),
  );
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate: () => boolean, timeoutMs: number): Promise<void> {
  const startedAt = performance.now();
  while (performance.now() - startedAt < timeoutMs) {
    if (predicate()) return;
    await sleep(10);
  }
  throw new Error(`timed out after ${timeoutMs}ms`);
}

async function expectTimesOut(promise: Promise<unknown>, ms: number) {
  await expect(withTimeout(promise, ms)).rejects.toThrow(/timed out/);
}

async function expectReadTimesOut(read: Promise<ReadableStreamReadResult<StreamEvent>>, ms: number) {
  await expectTimesOut(read, ms);
}

class TestAfterAppendClientMain extends RpcTarget {
  #onEvent: (event: StreamEvent) => void;
  #disposed = false;

  constructor(onEvent: (event: StreamEvent) => void) {
    super();
    this.#onEvent = onEvent;
  }

  afterAppend(args: { event: StreamEvent }): undefined {
    if (!this.#disposed) this.#onEvent(args.event);
  }

  [Symbol.dispose](): void {
    this.#disposed = true;
  }
}

describe("handwritten stream capnweb", () => {
  it("rejects non-websocket requests at the stream durable object boundary", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const response = await fetch(new URL(`/${path}`, workerUrl));

    expect(response.status).toBe(400);
    expect(await response.text()).toBe("This endpoint only accepts WebSocket requests.");
  });

  it("append uses the allowUnconfirmed write fast path", async () => {
    const source = await readFile(
      decodeURIComponent(new URL("../src/stream.ts", import.meta.url).pathname),
      "utf8",
    );

    expect(source).toMatch(
      /writeEventFromKv\(\{\s*storage: this\.ctx\.storage,\s*input: event,\s*allowUnconfirmedWrites: true,\s*\}\)/,
    );
  });

  it("append returns committed event over capnweb", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const event: StreamEventInput = { type: "test.append", payload: { path } };

    await using fixture = await withStream({ path });

    const appended = await fixture.rpc.append({ event });

    expect(appended).toMatchObject({
      type: event.type,
      offset: 1,
      payload: event.payload,
      createdAt: expect.any(String),
    });
    expect(fixture.parsedWsMessages()).toMatchObject([
      { direction: "out", data: ["push", ["pipeline", 0, ["append"], [{ event }]]] },
      { direction: "out", data: ["pull", 1] },
      {
        direction: "in",
        data: [
          "resolve",
          1,
          {
            type: event.type,
            offset: expect.any(Number),
            createdAt: expect.any(String),
          },
        ],
      },
      { direction: "out", data: ["release", 1, expect.any(Number)] },
    ]);
    expect(await fixture.rpc.maxOffset()).toBe(1);
  });

  it("batched json volatile stream coalesces events into json-array chunks", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const events: StreamEventInput[] = Array.from({ length: 3 }, (_, i) => ({
      type: "test.batched-json-volatile",
      payload: { n: i + 1 },
    }));

    await using fixture = await withStream({ path });
    const readable = await fixture.rpc.streamBatchedJsonVolatile();
    const reader = (readable as unknown as ReadableStream<string>).getReader();

    const chunkPromise = withTimeout(reader.read(), 1_000);
    const appended = await Promise.all(
      events.map((event) => fixture.rpc.appendBatchedJsonVolatile({ event })),
    );
    const chunk = await chunkPromise;

    if (chunk.done) throw new Error("batched json volatile stream ended early");
    const delivered = JSON.parse(chunk.value) as StreamEvent[];
    expect(delivered).toEqual(appended);
    expect(delivered.map((event) => event.offset)).toEqual([1, 2, 3]);
    reader.releaseLock();
  });

  it("rejects malformed append events before idempotency or durability handling", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect(
      fixture.rpc.append({
        event: JSON.parse("null"),
        durability: JSON.parse("1"),
      }),
    ).rejects.toThrow(/append event must be a valid StreamEventInput/);

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("rejects non-string event types at the append envelope boundary", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect(
      fixture.rpc.append({
        event: JSON.parse('{"type":123}'),
        durability: JSON.parse('"not-a-mode"'),
      }),
    ).rejects.toThrow(/append event must be a valid StreamEventInput/);

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("rejects non-integer event offsets at the append envelope boundary", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect(
      fixture.rpc.append({
        event: JSON.parse('{"type":"test.append.fractional-offset","offset":1.5}'),
        durability: JSON.parse('"not-a-mode"'),
      }),
    ).rejects.toThrow(/append event must be a valid StreamEventInput/);

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("rejects non-positive event offsets at the append envelope boundary", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect(
      fixture.rpc.append({
        event: JSON.parse('{"type":"test.append.zero-offset","offset":0}'),
        durability: JSON.parse('"not-a-mode"'),
      }),
    ).rejects.toThrow(/append event must be a valid StreamEventInput/);

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("rejects non-string idempotency keys before idempotency lookup", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect(
      fixture.rpc.append({
        event: JSON.parse('{"type":"test.append.numeric-idempotency","idempotencyKey":123}'),
        durability: JSON.parse('"not-a-mode"'),
      }),
    ).rejects.toThrow(/append event must be a valid StreamEventInput/);

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("rejects scalar metadata at the append envelope boundary", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect(
      fixture.rpc.append({
        event: JSON.parse('{"type":"test.append.scalar-metadata","metadata":123}'),
        durability: JSON.parse('"not-a-mode"'),
      }),
    ).rejects.toThrow(/append event must be a valid StreamEventInput/);

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("rejects unknown top-level append event fields instead of dropping them", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect(
      fixture.rpc.append({
        event: JSON.parse('{"type":"test.append.extra-field","payload":{"ok":true},"extra":1}'),
      }),
    ).rejects.toThrow(/append event must be a valid StreamEventInput/);

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("preserves audio-shaped payload and metadata while rejecting only top-level event fields", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const audio = Buffer.alloc(960, 0x7f).toString("base64");
    const event: StreamEventInput = {
      type: "benchmark.audio-frame",
      payload: {
        runId: path,
        frameId: "p0-f1",
        publisher: "0",
        frame: 1,
        codec: "pcm16-base64",
        sampleRate: 24_000,
        frameMs: 20,
        audio,
        nested: { arbitrary: ["metadata", 1, true] },
      },
      metadata: {
        runId: path,
        nested: { keep: "this" },
      },
    };
    await using fixture = await withStream({ path });

    const appended = await fixture.rpc.append({ event, durability: "best-effort" });

    expect(appended).toMatchObject({
      type: event.type,
      payload: event.payload,
      metadata: event.metadata,
      offset: 1,
      createdAt: expect.any(String),
    });
    expect(await fixture.rpc.maxOffset()).toBe(1);
  });

  it("rejects unknown source envelope fields instead of dropping them", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect(
      fixture.rpc.append({
        event: JSON.parse(
          '{"type":"test.append.source-extra","source":{"processor":{"slug":"p","version":"1","extra":true}}}',
        ),
      }),
    ).rejects.toThrow(/append event must be a valid StreamEventInput/);

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("rejects malformed source processor fields at the append envelope boundary", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect(
      fixture.rpc.append({
        event: JSON.parse(
          '{"type":"test.append.source-processor-type","source":{"processor":{"slug":123,"version":"1"}}}',
        ),
        durability: JSON.parse('"not-a-mode"'),
      }),
    ).rejects.toThrow(/append event must be a valid StreamEventInput/);
    await expect(
      fixture.rpc.append({
        event: JSON.parse(
          '{"type":"test.append.source-processor-type","source":{"processor":{"slug":"p","version":1}}}',
        ),
        durability: JSON.parse('"not-a-mode"'),
      }),
    ).rejects.toThrow(/append event must be a valid StreamEventInput/);

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("rejects unknown source object fields instead of dropping them", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect(
      fixture.rpc.append({
        event: JSON.parse(
          '{"type":"test.append.source-object-extra","source":{"kind":"microphone"}}',
        ),
      }),
    ).rejects.toThrow(/append event must be a valid StreamEventInput/);

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("rejects malformed idempotent retries before reading the idempotency index", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const idempotencyKey = crypto.randomUUID();
    await using fixture = await withStream({ path });

    await fixture.rpc.append({
      event: { type: "test.append.malformed-idempotent-retry", idempotencyKey },
      durability: "best-effort",
    });

    await expect(
      fixture.rpc.append({
        event: {
          type: JSON.parse("123"),
          idempotencyKey,
        },
        durability: JSON.parse('"not-a-mode"'),
      }),
    ).rejects.toThrow(/append event must be a valid StreamEventInput/);

    expect(await fixture.rpc.maxOffset()).toBe(1);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 1,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("rejects malformed append args before reading event or durability", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect(fixture.rpc.append(JSON.parse("null"))).rejects.toThrow(
      /append args must be an object with event/,
    );
    await expect(fixture.rpc.append(JSON.parse("{}"))).rejects.toThrow(
      /append args must be an object with event/,
    );

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("rejects unknown append argument fields before allocating an offset", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect(
      fixture.rpc.append(
        JSON.parse('{"event":{"type":"test.append.unknown-arg"},"durabilty":"best-effort"}'),
      ),
    ).rejects.toThrow(/Unknown append argument field/);

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("avoids pulls for unobserved results and for .map() source arrays", async () => {
    const fireAndForgetName = `stream-${crypto.randomUUID()}`;
    const mapName = `stream-${crypto.randomUUID()}`;
    const events = [
      { type: "test.map", payload: { n: 1 } },
      { type: "test.map", payload: { n: 2 } },
    ];

    await using fireAndForget = await withStream({ path: fireAndForgetName });
    {
      using _append = fireAndForget.rpc.append({
        event: { type: "test.fire-and-forget", payload: { name: fireAndForgetName } },
      });
      await new Promise((resolve) => setTimeout(resolve, 0));
    }

    expect(fireAndForget.parsedWsMessages()).toMatchObject([
      {
        direction: "out",
        data: [
          "push",
          [
            "pipeline",
            0,
            ["append"],
            [{ event: { type: "test.fire-and-forget", payload: { name: fireAndForgetName } } }],
          ],
        ],
      },
      { direction: "out", data: ["release", 1, expect.any(Number)] },
    ]);
    expect(fireAndForget.wireAnalysis().resultWaits).toHaveLength(0);

    await using mapped = await withStream({ path: mapName });
    const appended = mapped.rpc.appendBatch({ events });
    const offsets = await appended.map((event) => event.offset);

    expect(offsets).toEqual([1, 2]);
    expect(mapped.parsedWsMessages()).toContainEqual({
      direction: "out",
      data: ["push", ["remap", 1, [], [], [["pipeline", 0, ["offset"]]]]],
    });
    expect(mapped.parsedWsMessages()).not.toContainEqual({
      direction: "out",
      data: ["pull", 1],
    });
    expect(mapped.parsedWsMessages()).toContainEqual({
      direction: "out",
      data: ["pull", 2],
    });
    expect(mapped.wireAnalysis().resultWaits).toHaveLength(1);
  });

  it(
    "receives live appends on a separate connection with no read-side outbound traffic",
    async () => {
      const path = `stream-${crypto.randomUUID()}`;
      const events: StreamEventInput[] = [
        { type: "test.stream", payload: { n: 1 } },
        { type: "test.stream", payload: { n: 2 } },
      ];

      await using reader = await withStream({ path });

      const readable = await reader.rpc.stream();
      // @ts-expect-error capnweb@0.8.0 types only model ReadableStream<Uint8Array>
      const streamReader = (readable as ReadableStream<StreamEvent>).getReader();
      const outboundAfterSubscribe = reader.wsMessages.length;

      const firstRead = streamReader.read() as Promise<ReadableStreamReadResult<StreamEvent>>;
      const secondRead = streamReader.read() as Promise<ReadableStreamReadResult<StreamEvent>>;

      await using writer = await withStream({ path });
      await writer.rpc.appendBatch({ events });

      const first = await withTimeout(firstRead, 500);
      expect(first.done).toBe(false);
      expect(first.value).toMatchObject({
        type: events[0].type,
        offset: 1,
        payload: events[0].payload,
        createdAt: expect.any(String),
      });

      const second = await withTimeout(secondRead, 500);
      expect(second.done).toBe(false);
      expect(second.value).toMatchObject({
        type: events[1].type,
        offset: 2,
        payload: events[1].payload,
        createdAt: expect.any(String),
      });

      const outboundWhileReading = outboundPullPushFrames(
        reader.wsMessages,
        outboundAfterSubscribe,
      );
      expect(outboundWhileReading).toEqual([]);

      streamReader.releaseLock();
    },
    2_000,
  );

  it("does not expose session-owned stream internals over capnweb", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect((fixture.rpc as any).streamForSession()).rejects.toThrow();

    expect(await fixture.rpc.debug()).toMatchObject({
      subscribers: [],
    });
  });

  it("rejects stream arguments instead of silently ignoring subscription options", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect((fixture.rpc as any).stream(JSON.parse('{"fromOffset":2}'))).rejects.toThrow(
      /stream does not accept arguments/,
    );

    expect(await fixture.rpc.debug()).toMatchObject({
      subscribers: [],
    });
  });

  it("pure subscribers do not originate per-event pull or push websocket traffic", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const events: StreamEventInput[] = Array.from({ length: 10 }, (_, i) => ({
      type: "test.pure-subscriber",
      payload: { n: i + 1 },
    }));

    await using subscriber = await withStream({ path });
    const readable = await subscriber.rpc.stream();
    const outboundAfterSubscribe = subscriber.wsMessages.length;
    // @ts-expect-error capnweb@0.8.0 types only model ReadableStream<Uint8Array>
    const reader = (readable as ReadableStream<StreamEvent>).getReader();

    await using writer = await withStream({ path });
    await writer.rpc.appendBatch({ events, durability: "best-effort" });

    const delivered = await readEvents(reader, events.length, 1_000);
    expect(delivered.map((event) => event.payload)).toEqual(events.map((event) => event.payload));

    expect(outboundPullPushFrames(subscriber.wsMessages, outboundAfterSubscribe)).toEqual([]);
    expect(subscriber.wireAnalysis().resultWaits).toHaveLength(1);

    reader.releaseLock();
  });

  it("documents the concrete Cap'n Web returned-stream pipe frames", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const events: StreamEventInput[] = Array.from({ length: 2 }, (_, i) => ({
      type: "test.capnweb.pipe-frame",
      payload: { n: i + 1 },
    }));

    await using subscriber = await withStream({ path });
    const readable = await subscriber.rpc.stream();
    const framesAfterSubscribe = subscriber.wsMessages.length;
    // @ts-expect-error capnweb@0.8.0 types only model ReadableStream<Uint8Array>
    const reader = (readable as ReadableStream<StreamEvent>).getReader();

    await using writer = await withStream({ path });
    await writer.rpc.appendBatch({ events, durability: "best-effort" });
    await readEvents(reader, events.length, 1_000);
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(subscriber.parsedWsMessages().slice(framesAfterSubscribe)).toMatchObject([
      {
        direction: "in",
        data: [
          "stream",
          [
            "pipeline",
            expect.any(Number),
            ["write"],
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
      { direction: "out", data: ["resolve", expect.any(Number), ["undefined"]] },
      {
        direction: "in",
        data: [
          "stream",
          [
            "pipeline",
            expect.any(Number),
            ["write"],
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
      { direction: "out", data: ["resolve", expect.any(Number), ["undefined"]] },
    ]);

    reader.releaseLock();
  });

  it("client-main afterAppend subscriber originates no websocket frames per event", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const events: StreamEventInput[] = Array.from({ length: 2 }, (_, i) => ({
      type: "test.after-append-client-main",
      payload: { n: i + 1 },
    }));
    const delivered: StreamEvent[] = [];
    const clientMain = new TestAfterAppendClientMain((event) => delivered.push(event));

    await using subscriber = await withStream({ path, localMain: clientMain });
    await subscriber.rpc.subscribeAfterAppendVolatile();
    const outboundAfterSubscribe = subscriber.wsMessages.length;

    await using writer = await withStream({ path });
    const appended = await Promise.all(
      events.map((event) => writer.rpc.appendVolatile({ event })),
    );
    await waitFor(() => delivered.length === events.length, 1_000);

    expect(delivered).toEqual(appended);
    expect(outboundFrames(subscriber.wsMessages, outboundAfterSubscribe)).toEqual([]);
    const inbound = subscriber
      .parsedWsMessages()
      .slice(outboundAfterSubscribe)
      .filter((frame) => frame.direction === "in");
    const pushFrames = inbound.filter((frame) => Array.isArray(frame.data) && frame.data[0] === "push");
    expect(inbound.every((frame) => Array.isArray(frame.data) && (frame.data[0] === "push" || frame.data[0] === "release"))).toBe(true);
    expect(pushFrames).toMatchObject([
      {
        direction: "in",
        data: [
          "push",
          [
            "pipeline",
            expect.any(Number),
            ["afterAppend"],
            [
              {
                event: {
                  type: events[0]!.type,
                  payload: events[0]!.payload,
                  offset: 1,
                  createdAt: expect.any(String),
                },
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
            ["afterAppend"],
            [
              {
                event: {
                  type: events[1]!.type,
                  payload: events[1]!.payload,
                  offset: 2,
                  createdAt: expect.any(String),
                },
              },
            ],
          ],
        ],
      },
    ]);

    clientMain[Symbol.dispose]();
  });

  it.fails(
    "pure subscribers would send no per-event websocket frames if Cap'n Web returned streams were wire-one-way",
    async () => {
      const path = `stream-${crypto.randomUUID()}`;
      const events: StreamEventInput[] = Array.from({ length: 10 }, (_, i) => ({
        type: "test.pure-subscriber",
        payload: { n: i + 1 },
      }));

      await using subscriber = await withStream({ path });
      const readable = await subscriber.rpc.stream();
      const outboundAfterSubscribe = subscriber.wsMessages.length;
      // @ts-expect-error capnweb@0.8.0 types only model ReadableStream<Uint8Array>
      const reader = (readable as ReadableStream<StreamEvent>).getReader();

      await using writer = await withStream({ path });
      await writer.rpc.appendBatch({ events, durability: "best-effort" });

      const delivered = await readEvents(reader, events.length, 1_000);
      expect(delivered.map((event) => event.payload)).toEqual(events.map((event) => event.payload));

      expect(outboundFrames(subscriber.wsMessages, outboundAfterSubscribe)).toEqual([]);

      reader.releaseLock();
    },
  );

  it(
    "delivers global offset order to multiple subscribers with no per-event RPC from readers or writers",
    async () => {
      const path = `stream-${crypto.randomUUID()}`;
      const eventsPerWriter = 3;
      const writerCount = 2;
      const totalEvents = eventsPerWriter * writerCount;
      const writer1Events: StreamEventInput[] = Array.from({ length: eventsPerWriter }, (_, i) => ({
        type: "test.multi",
        payload: { writer: 1, n: i + 1 },
      }));
      const writer2Events: StreamEventInput[] = Array.from({ length: eventsPerWriter }, (_, i) => ({
        type: "test.multi",
        payload: { writer: 2, n: i + 1 },
      }));

      await using reader1 = await withStream({ path });
      await using reader2 = await withStream({ path });

      const readable1 = await reader1.rpc.stream();
      const readable2 = await reader2.rpc.stream();
      // @ts-expect-error capnweb@0.8.0 types only model ReadableStream<Uint8Array>
      const streamReader1 = (readable1 as ReadableStream<StreamEvent>).getReader();
      // @ts-expect-error capnweb@0.8.0 types only model ReadableStream<Uint8Array>
      const streamReader2 = (readable2 as ReadableStream<StreamEvent>).getReader();

      const outboundAfterSubscribe1 = reader1.wsMessages.length;
      const outboundAfterSubscribe2 = reader2.wsMessages.length;

      const reads1 = Array.from({ length: totalEvents }, () =>
        streamReader1.read() as Promise<ReadableStreamReadResult<StreamEvent>>,
      );
      const reads2 = Array.from({ length: totalEvents }, () =>
        streamReader2.read() as Promise<ReadableStreamReadResult<StreamEvent>>,
      );

      await using writer1 = await withStream({ path });
      await using writer2 = await withStream({ path });

      {
        using _batch1 = writer1.rpc.appendBatch({ events: writer1Events });
        using _batch2 = writer2.rpc.appendBatch({ events: writer2Events });
        await new Promise((resolve) => setTimeout(resolve, 0));
      }

      const collectOffsets = async (
        reads: Promise<ReadableStreamReadResult<StreamEvent>>[],
      ) => {
        const results = await Promise.all(reads.map((read) => withTimeout(read, 1_000)));
        return results.map((result) => {
          expect(result.done).toBe(false);
          return result.value!.offset;
        });
      };

      const offsets1 = await collectOffsets(reads1);
      const offsets2 = await collectOffsets(reads2);

      expect(offsets1).toEqual(Array.from({ length: totalEvents }, (_, i) => i + 1));
      expect(offsets2).toEqual(Array.from({ length: totalEvents }, (_, i) => i + 1));

      expect(outboundPullPushFrames(reader1.wsMessages, outboundAfterSubscribe1)).toEqual([]);
      expect(outboundPullPushFrames(reader2.wsMessages, outboundAfterSubscribe2)).toEqual([]);

      // Writers fire-and-forget appendBatch: push + release only, no pulled results.
      expect(writer1.wireAnalysis().resultWaits).toHaveLength(0);
      expect(writer2.wireAnalysis().resultWaits).toHaveLength(0);

      streamReader1.releaseLock();
      streamReader2.releaseLock();
    },
    5_000,
  );

  it("delivers to an active subscriber while another subscriber does not read", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const burstSize = 50;
    const events: StreamEventInput[] = Array.from({ length: burstSize }, (_, i) => ({
      type: "test.slow-consumer",
      payload: { n: i + 1 },
    }));

    await using slow = await withStream({ path });
    await using fast = await withStream({ path });

    await slow.rpc.stream();
    const fastReadable = await fast.rpc.stream();
    // @ts-expect-error capnweb@0.8.0 types only model ReadableStream<Uint8Array>
    const fastReader = (fastReadable as ReadableStream<StreamEvent>).getReader();
    const outboundAfterSubscribe = fast.wsMessages.length;

    const reads = Array.from({ length: burstSize }, () =>
      fastReader.read() as Promise<ReadableStreamReadResult<StreamEvent>>,
    );

    await using writer = await withStream({ path });
    await writer.rpc.appendBatch({ events, durability: "best-effort" });

    const delivered = await Promise.all(reads.map((read) => withTimeout(read, 1_000)));
    expect(delivered.map((result) => result.value?.payload)).toEqual(
      events.map((event) => event.payload),
    );
    expect(outboundPullPushFrames(fast.wsMessages, outboundAfterSubscribe)).toEqual([]);

    const debug = await writer.rpc.debug();
    expect(debug.subscribers).toHaveLength(2);
    expect(debug.subscribers.every((subscriber) => subscriber.enqueuedEvents >= burstSize)).toBe(
      true,
    );

    fastReader.releaseLock();
  });

  it("replays committed history before switching to live appends", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const history: StreamEventInput[] = [
      { type: "test.replay", payload: { phase: "history", n: 1 } },
      { type: "test.replay", payload: { phase: "history", n: 2 } },
      { type: "test.replay", payload: { phase: "history", n: 3 } },
    ];
    const live: StreamEventInput = { type: "test.replay", payload: { phase: "live", n: 4 } };

    await using writer = await withStream({ path });
    await writer.rpc.appendBatch({ events: history });

    await using reader = await withStream({ path });
    const readable = await reader.rpc.stream();
    // @ts-expect-error capnweb@0.8.0 types only model ReadableStream<Uint8Array>
    const streamReader = (readable as ReadableStream<StreamEvent>).getReader();

    const replayed = await readEvents(streamReader, history.length, 500);
    expectContiguousOffsets(replayed, history.length);
    expect(replayed.map((event) => event.payload)).toEqual(history.map((event) => event.payload));

    await writer.rpc.append({ event: live });
    const [liveEvent] = await readEvents(streamReader, 1, 500);
    expect(liveEvent).toMatchObject({
      offset: 4,
      payload: live.payload,
    });

    streamReader.releaseLock();
  });

  it("accounts replayed events through the same subscriber enqueue path as live fan-out", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const history: StreamEventInput[] = [
      { type: "test.replay.accounting", payload: { n: 1 } },
      { type: "test.replay.accounting", payload: { n: 2 } },
      { type: "test.replay.accounting", payload: { n: 3 } },
    ];

    await using writer = await withStream({ path });
    await writer.rpc.appendBatch({ events: history, durability: "best-effort" });
    await writer.rpc.sync();

    await using reader = await withStream({ path });
    const readable = await reader.rpc.stream();
    // @ts-expect-error capnweb@0.8.0 types only model ReadableStream<Uint8Array>
    const streamReader = (readable as ReadableStream<StreamEvent>).getReader();

    expect(await reader.rpc.debug()).toMatchObject({
      subscribers: [{ enqueuedEvents: history.length }],
    });

    const replayed = await readEvents(streamReader, history.length, 500);
    expect(replayed.map((event) => event.payload)).toEqual(history.map((event) => event.payload));

    streamReader.releaseLock();
  });

  it("fails replay loudly when committed history has a missing event key", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const history: StreamEventInput[] = [
      { type: "test.replay.gap", payload: { n: 1 } },
      { type: "test.replay.gap", payload: { n: 2 } },
    ];

    await using fixture = await withStream({ path });
    await fixture.rpc.appendBatch({ events: history });
    await fixture.rpc.debugDeleteEventForReplay({ offset: 1 });

    const readable = await fixture.rpc.stream();
    // @ts-expect-error capnweb@0.8.0 types only model ReadableStream<Uint8Array>
    const streamReader = (readable as ReadableStream<StreamEvent>).getReader();

    await expect(withTimeout(streamReader.read(), 500)).rejects.toThrow(
      /Missing stream event at offset 1 while replaying through 2/,
    );
  });

  it("removes replay subscribers when committed history is corrupt", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await fixture.rpc.appendBatch({
      events: [
        { type: "test.replay.gap-cleanup", payload: { n: 1 } },
        { type: "test.replay.gap-cleanup", payload: { n: 2 } },
      ],
    });
    await fixture.rpc.debugDeleteEventForReplay({ offset: 1 });

    const readable = await fixture.rpc.stream();
    // @ts-expect-error capnweb@0.8.0 types only model ReadableStream<Uint8Array>
    const streamReader = (readable as ReadableStream<StreamEvent>).getReader();
    await expect(withTimeout(streamReader.read(), 500)).rejects.toThrow(
      /Missing stream event at offset 1 while replaying through 2/,
    );

    expect(await fixture.rpc.debug()).toMatchObject({
      subscribers: [],
    });
  });

  it("removes cancelled subscribers from live fan-out", async () => {
    const path = `stream-${crypto.randomUUID()}`;

    {
      await using reader = await withStream({ path });
      const readable = await reader.rpc.stream();

      expect(await reader.rpc.debug()).toMatchObject({
        subscribers: [{ enqueuedEvents: 0 }],
      });
    }

    await using probe = await withStream({ path });
    expect(await probe.rpc.debug()).toMatchObject({
      subscribers: [],
    });

    await using writer = await withStream({ path });
    await writer.rpc.append({
      event: { type: "test.stream.cancel", payload: { shouldFanOut: false } },
    });

    expect(await probe.rpc.debug()).toMatchObject({
      subscribers: [],
    });
  });

  it("documents that capnweb reader cancel does not release the server subscriber", async () => {
    const path = `stream-${crypto.randomUUID()}`;

    {
      await using fixture = await withStream({ path });

      const readable = await fixture.rpc.stream();
      // @ts-expect-error capnweb@0.8.0 types only model ReadableStream<Uint8Array>
      const streamReader = (readable as ReadableStream<StreamEvent>).getReader();
      expect(await fixture.rpc.debug()).toMatchObject({
        subscribers: [{ enqueuedEvents: 0 }],
      });

      await fixture.rpc.append({
        event: { type: "test.stream.remote-cancel-before-cancel" },
        durability: "best-effort",
      });
      await withTimeout(streamReader.read(), 500);
      await streamReader.cancel("client no longer wants this stream");
      await sleep(100);

      expect(await fixture.rpc.debug()).toMatchObject({
        subscribers: [{ enqueuedEvents: 1 }],
      });

      await fixture.rpc.append({
        event: { type: "test.stream.remote-cancel-after-cancel" },
        durability: "best-effort",
      });
      expect(await fixture.rpc.debug()).toMatchObject({
        subscribers: [],
      });
    }

    await using probe = await withStream({ path });
    expect(await probe.rpc.debug()).toMatchObject({
      subscribers: [],
    });
  });

  it("removes every stream opened by a disposed capnweb session", async () => {
    const path = `stream-${crypto.randomUUID()}`;

    {
      await using reader = await withStream({ path });
      await reader.rpc.stream();
      await reader.rpc.stream();

      expect(await reader.rpc.debug()).toMatchObject({
        subscribers: [{ enqueuedEvents: 0 }, { enqueuedEvents: 0 }],
      });
    }

    await using probe = await withStream({ path });
    expect(await probe.rpc.debug()).toMatchObject({
      subscribers: [],
    });
  });

  it("removes locally cancelled streams from live fan-out", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    const result = await fixture.rpc.debugOpenAndCancelLocalStream();

    expect(result.beforeCancel).toMatchObject({
      subscribers: [{ enqueuedEvents: 0 }],
    });
    expect(result.afterCancel).toMatchObject({
      subscribers: [],
    });

    await fixture.rpc.append({
      event: { type: "test.stream.local-cancel", payload: { shouldFanOut: false } },
    });
    expect(await fixture.rpc.debug()).toMatchObject({
      subscribers: [],
    });
  });

  it("removes subscribers whose stream controller rejects enqueue", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    expect(await fixture.rpc.debugInstallErroredLocalSubscriber()).toMatchObject({
      subscribers: [{ enqueuedEvents: 0 }],
    });

    await fixture.rpc.append({
      event: { type: "test.stream.enqueue-error", payload: { shouldRemoveSubscriber: true } },
    });

    expect(await fixture.rpc.debug()).toMatchObject({
      subscribers: [],
    });
  });

  it("continues fan-out to later subscribers after removing a broken subscriber", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    expect(await fixture.rpc.debugInstallErroredLocalSubscriber()).toMatchObject({
      subscribers: [{ enqueuedEvents: 0 }],
    });

    const readable = await fixture.rpc.stream();
    // @ts-expect-error capnweb@0.8.0 types only model ReadableStream<Uint8Array>
    const streamReader = (readable as ReadableStream<StreamEvent>).getReader();
    expect(await fixture.rpc.debug()).toMatchObject({
      subscribers: [{ enqueuedEvents: 0 }, { enqueuedEvents: 0 }],
    });

    await fixture.rpc.append({
      event: { type: "test.stream.enqueue-error-isolated", payload: { shouldReachLater: true } },
      durability: "best-effort",
    });

    const delivered = await withTimeout(streamReader.read(), 500);
    expect(delivered.done).toBe(false);
    expect(delivered.value).toMatchObject({
      type: "test.stream.enqueue-error-isolated",
      payload: { shouldReachLater: true },
    });
    expect(await fixture.rpc.debug()).toMatchObject({
      subscribers: [{ enqueuedEvents: 1 }],
    });

    streamReader.releaseLock();
  });

  it("idempotent append returns the original event and emits once to live subscribers", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const idempotencyKey = crypto.randomUUID();

    await using reader = await withStream({ path });
    const readable = await reader.rpc.stream();
    // @ts-expect-error capnweb@0.8.0 types only model ReadableStream<Uint8Array>
    const streamReader = (readable as ReadableStream<StreamEvent>).getReader();

    await using writer = await withStream({ path });
    const firstAppend = await writer.rpc.append({
      event: { type: "test.idempotency", idempotencyKey, payload: { attempt: 1 } },
    });
    const retryAppend = await writer.rpc.append({
      event: { type: "test.idempotency", idempotencyKey, payload: { attempt: 2 } },
    });

    expect(retryAppend).toEqual(firstAppend);
    expect(await writer.rpc.maxOffset()).toBe(1);

    const [delivered] = await readEvents(streamReader, 1, 500);
    expect(delivered).toEqual(firstAppend);

    const duplicateRead = streamReader.read();
    await expect(withTimeout(duplicateRead, 100)).rejects.toThrow(/timed out/);
    await streamReader.cancel("done checking for duplicate idempotent delivery");
    await duplicateRead.catch(() => undefined);
  });

  it("rejects offset precondition failures without advancing the stream", async () => {
    const path = `stream-${crypto.randomUUID()}`;

    await using fixture = await withStream({ path });
    await fixture.rpc.append({ event: { type: "test.precondition", payload: { ok: true } } });

    await expect(
      fixture.rpc.append({
        event: { type: "test.precondition", offset: 99, payload: { ok: false } },
      }),
    ).rejects.toThrow(/Offset precondition failed/);
    expect(await fixture.rpc.maxOffset()).toBe(1);

    await using reader = await withStream({ path });
    const readable = await reader.rpc.stream();
    // @ts-expect-error capnweb@0.8.0 types only model ReadableStream<Uint8Array>
    const streamReader = (readable as ReadableStream<StreamEvent>).getReader();
    const [event] = await readEvents(streamReader, 1, 500);
    expect(event).toMatchObject({ offset: 1, payload: { ok: true } });

    const nextRead = streamReader.read();
    await expect(withTimeout(nextRead, 100)).rejects.toThrow(/timed out/);
    await streamReader.cancel("done checking precondition failure");
    await nextRead.catch(() => undefined);
  });

  it("defaults to confirmed appends and does not accumulate unconfirmed write debt", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await fixture.rpc.append({
      event: { type: "test.durability.default", payload: { mode: "confirmed" } },
    });

    expect(await fixture.rpc.maxOffset()).toBe(1);
    expect(await fixture.rpc.debug()).toMatchObject({
      settings: {
        defaultAppendDurabilityMode: "confirmed",
        checkpointEveryUnconfirmedAppends: 100,
        debugConfirmedSyncDelayMs: 0,
        debugCheckpointSyncDelayMs: 0,
      },
      unconfirmedWriteCount: 0,
      checkpointInProgress: false,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("allows a best-effort per-call override and clears it with an explicit sync barrier", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await fixture.rpc.append({
      event: { type: "test.durability.best-effort", payload: { n: 1 } },
      durability: "best-effort",
    });
    await fixture.rpc.append({
      event: { type: "test.durability.best-effort", payload: { n: 2 } },
      durability: { mode: "best-effort" },
    });

    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 2,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });

    expect(await fixture.rpc.sync()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("best-effort object thresholds are validated but do not schedule checkpoints", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await fixture.rpc.append({
      event: { type: "test.durability.best-effort-threshold", payload: { n: 1 } },
      durability: { mode: "best-effort", checkpointEveryUnconfirmedAppends: 1 },
    });

    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 1,
      checkpointInProgress: false,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("best-effort appends fan out while write debt is still unconfirmed", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const event: StreamEventInput = {
      type: "test.durability.best-effort-live",
      payload: { phase: "before-sync" },
    };

    await using reader = await withStream({ path });
    const readable = await reader.rpc.stream();
    // @ts-expect-error capnweb@0.8.0 types only model ReadableStream<Uint8Array>
    const streamReader = (readable as ReadableStream<StreamEvent>).getReader();
    const liveRead = streamReader.read();

    await using writer = await withStream({ path });
    const appended = await writer.rpc.append({ event, durability: "best-effort" });
    const delivered = await withTimeout(liveRead, 500);

    expect(delivered.done).toBe(false);
    expect(delivered.value).toEqual(appended);
    expect(await writer.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 1,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });

    streamReader.releaseLock();
  });

  it("does not count idempotent best-effort retries as new unconfirmed writes", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const idempotencyKey = crypto.randomUUID();
    await using fixture = await withStream({ path });

    const first = await fixture.rpc.append({
      event: { type: "test.durability.idempotent", idempotencyKey },
      durability: "best-effort",
    });
    const retry = await fixture.rpc.append({
      event: { type: "test.durability.idempotent", idempotencyKey },
      durability: "best-effort",
    });

    expect(retry).toEqual(first);
    expect(await fixture.rpc.maxOffset()).toBe(1);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 1,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("idempotent retries return before conflicting validation can reject them", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const idempotencyKey = crypto.randomUUID();
    await using fixture = await withStream({ path });

    const first = await fixture.rpc.append({
      event: { type: "test.durability.idempotent-validation", idempotencyKey },
      durability: "best-effort",
    });
    const retry = await fixture.rpc.append({
      event: {
        type: "test.durability.idempotent-validation",
        idempotencyKey,
        offset: 99,
      },
      durability: JSON.parse('"not-a-mode"'),
    });

    expect(retry).toEqual(first);
    expect(await fixture.rpc.maxOffset()).toBe(1);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 1,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("fails corrupted idempotent retries before conflicting validation can reject them", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const idempotencyKey = crypto.randomUUID();
    await using fixture = await withStream({ path });

    await fixture.rpc.append({
      event: { type: "test.durability.idempotent-corrupt", idempotencyKey },
      durability: "best-effort",
    });
    await fixture.rpc.debugDeleteEventForReplay({ offset: 1 });

    await expect(
      fixture.rpc.append({
        event: {
          type: "test.durability.idempotent-corrupt",
          idempotencyKey,
          offset: 99,
        },
        durability: JSON.parse('"not-a-mode"'),
      }),
    ).rejects.toThrow(/Idempotency index points at missing stream event offset 1/);

    expect(await fixture.rpc.maxOffset()).toBe(1);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 1,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("allows a confirmed per-call override on a best-effort stream", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });
    await fixture.rpc.patchSettings({ defaultAppendDurabilityMode: "best-effort" });

    await fixture.rpc.append({
      event: { type: "test.durability.override", payload: { mode: "best-effort" } },
    });
    await fixture.rpc.append({
      event: { type: "test.durability.override", payload: { mode: "confirmed" } },
      durability: "confirmed",
    });

    expect(await fixture.rpc.maxOffset()).toBe(2);
    expect(await fixture.rpc.debug()).toMatchObject({
      settings: { defaultAppendDurabilityMode: "best-effort" },
      unconfirmedWriteCount: 1,
    });
  });

  it("rejects invalid stream settings without changing append defaults", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect(
      fixture.rpc.patchSettings({ defaultAppendDurabilityMode: JSON.parse('"later"') }),
    ).rejects.toThrow(/Unknown append durability mode/);
    await expect(fixture.rpc.patchSettings({ checkpointEveryUnconfirmedAppends: 0 })).rejects
      .toThrow(/checkpointEveryUnconfirmedAppends/);
    await expect(fixture.rpc.patchSettings({ debugConfirmedSyncDelayMs: -1 })).rejects.toThrow(
      /debugConfirmedSyncDelayMs/,
    );
    await expect(fixture.rpc.patchSettings({ debugCheckpointSyncDelayMs: -1 })).rejects.toThrow(
      /debugCheckpointSyncDelayMs/,
    );
    await expect(
      fixture.rpc.patchSettings(JSON.parse('{"checkpointEveryUnconfirmedAppend":1}')),
    ).rejects.toThrow(/Unknown stream setting/);

    await fixture.rpc.append({
      event: { type: "test.settings.invalid-defaults", payload: { mode: "still-confirmed" } },
    });

    expect(await fixture.rpc.debug()).toMatchObject({
      settings: {
        defaultAppendDurabilityMode: "confirmed",
        checkpointEveryUnconfirmedAppends: 100,
        debugConfirmedSyncDelayMs: 0,
        debugCheckpointSyncDelayMs: 0,
      },
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("persists stream settings across durable object restart", async () => {
    const path = `stream-${crypto.randomUUID()}`;

    {
      await using fixture = await withStream({ path });
      await fixture.rpc.patchSettings({
        defaultAppendDurabilityMode: "checkpointed",
        checkpointEveryUnconfirmedAppends: 1,
      });
      await expect(fixture.rpc.kill({ reason: "restart settings persistence test" })).rejects
        .toThrow();
    }

    await sleep(250);

    await using restarted = await withStream({ path });
    expect(await restarted.rpc.debug()).toMatchObject({
      settings: {
        defaultAppendDurabilityMode: "checkpointed",
        checkpointEveryUnconfirmedAppends: 1,
      },
    });

    await restarted.rpc.append({
      event: { type: "test.settings.restart-default", payload: { mode: "checkpointed" } },
    });

    expect(await restarted.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 1,
      checkpointCompletedCount: 1,
    });
  });

  it("checkpointed appendBatch drains the whole same-event unconfirmed window", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const events: StreamEventInput[] = Array.from({ length: 5 }, (_, i) => ({
      type: "test.durability.checkpointed",
      payload: { n: i + 1 },
    }));
    await using fixture = await withStream({ path });

    await fixture.rpc.appendBatch({
      events,
      durability: { mode: "checkpointed", checkpointEveryUnconfirmedAppends: 2 },
    });

    expect(await fixture.rpc.maxOffset()).toBe(events.length);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointInProgress: false,
      checkpointStartedCount: 1,
      checkpointCompletedCount: 1,
    });
  });

  it("checkpointed appendBatch returns after scheduling but before awaiting the checkpoint", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const events: StreamEventInput[] = Array.from({ length: 5 }, (_, i) => ({
      type: "test.durability.checkpointed-debug",
      payload: { n: i + 1 },
    }));
    await using fixture = await withStream({ path });

    const result = await fixture.rpc.appendBatchDebug({
      events,
      durability: { mode: "checkpointed", checkpointEveryUnconfirmedAppends: 2 },
    });

    expect(result.events.map((event) => event.offset)).toEqual([1, 2, 3, 4, 5]);
    expect(result.debug).toMatchObject({
      unconfirmedWriteCount: 5,
      checkpointInProgress: true,
      checkpointStartedCount: 1,
      checkpointCompletedCount: 0,
    });

    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointInProgress: false,
      checkpointStartedCount: 1,
      checkpointCompletedCount: 1,
    });
  });

  it("checkpointed append schedules a delayed checkpoint that gates later RPC", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });
    await using probe = await withStream({ path });
    await fixture.rpc.patchSettings({ debugCheckpointSyncDelayMs: 1_000 });

    const checkpointingAppend = fixture.rpc.appendBatchDebug({
      events: [{ type: "test.durability.checkpoint-gate" }],
      durability: { mode: "checkpointed", checkpointEveryUnconfirmedAppends: 1 },
    });
    await new Promise((resolve) => setTimeout(resolve, 50));

    const gatedPing = probe.rpc.ping();
    await expectTimesOut(gatedPing, 250);

    const result = await checkpointingAppend;

    expect(result.debug).toMatchObject({
      unconfirmedWriteCount: 1,
      checkpointInProgress: true,
      checkpointStartedCount: 1,
      checkpointCompletedCount: 0,
    });

    await gatedPing;
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointInProgress: false,
      checkpointStartedCount: 1,
      checkpointCompletedCount: 1,
    });
  });

  it("checkpointed appends can schedule a second checkpoint after the first completes", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await fixture.rpc.append({
      event: { type: "test.durability.checkpoint-rearm", payload: { n: 1 } },
      durability: { mode: "checkpointed", checkpointEveryUnconfirmedAppends: 1 },
    });
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointInProgress: false,
      checkpointStartedCount: 1,
      checkpointCompletedCount: 1,
    });

    await fixture.rpc.append({
      event: { type: "test.durability.checkpoint-rearm", payload: { n: 2 } },
      durability: { mode: "checkpointed", checkpointEveryUnconfirmedAppends: 1 },
    });

    expect(await fixture.rpc.maxOffset()).toBe(2);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointInProgress: false,
      checkpointStartedCount: 2,
      checkpointCompletedCount: 2,
    });
  });

  it("checkpointed passes the live-before-durability probe that confirmed intentionally fails", async () => {
    const confirmedPath = `stream-${crypto.randomUUID()}`;
    await using confirmedWriter = await withStream({ path: confirmedPath });
    await confirmedWriter.rpc.patchSettings({ debugConfirmedSyncDelayMs: 300 });
    await using confirmedReader = await withStream({ path: confirmedPath });
    const confirmedReadable = await confirmedReader.rpc.stream();
    const confirmedStreamReader = (confirmedReadable as unknown as ReadableStream<StreamEvent>)
      .getReader();

    const confirmedAppend = confirmedWriter.rpc.append({
      event: { type: "test.durability.confirmed-live-before-sync" },
      durability: "confirmed",
    });
    const confirmedLiveRead = confirmedStreamReader.read();
    await expectReadTimesOut(confirmedLiveRead, 100);
    const confirmedAppended = await confirmedAppend;
    const confirmedDelivered = await withTimeout(confirmedLiveRead, 1_000);
    expect(confirmedDelivered.done).toBe(false);
    expect(confirmedDelivered.value).toEqual(confirmedAppended);
    confirmedStreamReader.releaseLock();

    const checkpointedPath = `stream-${crypto.randomUUID()}`;
    await using checkpointedWriter = await withStream({ path: checkpointedPath });
    await checkpointedWriter.rpc.patchSettings({ debugCheckpointSyncDelayMs: 2_000 });
    await using checkpointedReader = await withStream({ path: checkpointedPath });
    const checkpointedReadable = await checkpointedReader.rpc.stream();
    const checkpointedStreamReader = (checkpointedReadable as unknown as ReadableStream<StreamEvent>)
      .getReader();

    const checkpointedAppend = checkpointedWriter.rpc.append({
      event: { type: "test.durability.checkpointed-live-before-sync" },
      durability: { mode: "checkpointed", checkpointEveryUnconfirmedAppends: 1 },
    });
    const checkpointedLiveRead = checkpointedStreamReader.read();
    const checkpointedDelivered = await withTimeout(checkpointedLiveRead, 1_000);
    expect(checkpointedDelivered.done).toBe(false);
    expect(checkpointedDelivered.value).toMatchObject({
      type: "test.durability.checkpointed-live-before-sync",
      offset: 1,
    });
    await checkpointedAppend;
    expect(await checkpointedWriter.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 1,
      checkpointCompletedCount: 1,
    });
    checkpointedStreamReader.releaseLock();
  });

  it("rejects invalid per-call checkpoint thresholds", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect(
      fixture.rpc.append({
        event: { type: "test.durability.invalid-threshold" },
        durability: { mode: "checkpointed", checkpointEveryUnconfirmedAppends: 0 },
      }),
    ).rejects.toThrow();

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("rejects non-integer checkpoint thresholds before allocating an offset", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect(
      fixture.rpc.append({
        event: { type: "test.durability.fractional-threshold" },
        durability: { mode: "checkpointed", checkpointEveryUnconfirmedAppends: 1.5 },
      }),
    ).rejects.toThrow(/checkpointEveryUnconfirmedAppends must be a positive integer/);

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("rejects non-number checkpoint thresholds before allocating an offset", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect(
      fixture.rpc.append({
        event: { type: "test.durability.string-threshold" },
        durability: JSON.parse('{"mode":"checkpointed","checkpointEveryUnconfirmedAppends":"2"}'),
      }),
    ).rejects.toThrow(/checkpointEveryUnconfirmedAppends must be a positive integer/);

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("rejects invalid checkpoint thresholds even on non-checkpointed object durability", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect(
      fixture.rpc.append({
        event: { type: "test.durability.invalid-unused-threshold" },
        durability: { mode: "best-effort", checkpointEveryUnconfirmedAppends: 0 },
      }),
    ).rejects.toThrow(/checkpointEveryUnconfirmedAppends/);
    await expect(
      fixture.rpc.append({
        event: { type: "test.durability.invalid-confirmed-threshold" },
        durability: { mode: "confirmed", checkpointEveryUnconfirmedAppends: 0 },
      }),
    ).rejects.toThrow(/checkpointEveryUnconfirmedAppends/);

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("rejects invalid per-call durability modes before allocating an offset", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect(
      fixture.rpc.append({
        event: { type: "test.durability.invalid-mode" },
        durability: JSON.parse('"maybe-later"'),
      }),
    ).rejects.toThrow(/Unknown append durability mode/);

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("rejects null per-call durability before allocating an offset", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect(
      fixture.rpc.append({
        event: { type: "test.durability.null-mode" },
        durability: JSON.parse("null"),
      }),
    ).rejects.toThrow(/append durability must be a mode string or options object/);

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("rejects object durability without a mode before allocating an offset", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });
    await fixture.rpc.patchSettings({ defaultAppendDurabilityMode: "checkpointed" });

    await expect(
      fixture.rpc.append({
        event: { type: "test.durability.missing-mode" },
        durability: JSON.parse('{"checkpointEveryUnconfirmedAppends":1}'),
      }),
    ).rejects.toThrow(/append durability options must include mode/);

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("rejects non-string object durability modes before falling back to stream settings", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });
    await fixture.rpc.patchSettings({ defaultAppendDurabilityMode: "checkpointed" });

    await expect(
      fixture.rpc.append({
        event: { type: "test.durability.non-string-object-mode" },
        durability: JSON.parse('{"mode":null}'),
      }),
    ).rejects.toThrow(/Unknown append durability mode/);

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("rejects unknown durability option fields before allocating an offset", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await expect(
      fixture.rpc.append({
        event: { type: "test.durability.unknown-option" },
        durability: JSON.parse('{"mode":"checkpointed","checkpointEveryUnconfirmedAppend":1}'),
      }),
    ).rejects.toThrow(/Unknown append durability option/);

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("rejects primitive per-call durability before falling back to stream settings", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });
    await fixture.rpc.patchSettings({ defaultAppendDurabilityMode: "checkpointed" });

    await expect(
      fixture.rpc.append({
        event: { type: "test.durability.primitive-mode" },
        durability: JSON.parse("1"),
      }),
    ).rejects.toThrow(/append durability must be a mode string or options object/);

    expect(await fixture.rpc.maxOffset()).toBe(0);
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });
  });

  it("uses checkpointed stream settings when append does not pass a per-call override", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });
    await fixture.rpc.patchSettings({
      defaultAppendDurabilityMode: "checkpointed",
      checkpointEveryUnconfirmedAppends: 2,
    });

    await fixture.rpc.append({ event: { type: "test.durability.settings", payload: { n: 1 } } });
    expect(await fixture.rpc.debug()).toMatchObject({
      settings: {
        defaultAppendDurabilityMode: "checkpointed",
        checkpointEveryUnconfirmedAppends: 2,
      },
      unconfirmedWriteCount: 1,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });

    await fixture.rpc.append({ event: { type: "test.durability.settings", payload: { n: 2 } } });
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointInProgress: false,
      checkpointStartedCount: 1,
      checkpointCompletedCount: 1,
    });
  });

  it("uses stream checkpoint threshold for checkpointed string overrides", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });
    await fixture.rpc.patchSettings({
      defaultAppendDurabilityMode: "best-effort",
      checkpointEveryUnconfirmedAppends: 2,
    });

    await fixture.rpc.append({
      event: { type: "test.durability.string-checkpointed", payload: { n: 1 } },
      durability: "checkpointed",
    });
    expect(await fixture.rpc.debug()).toMatchObject({
      settings: {
        defaultAppendDurabilityMode: "best-effort",
        checkpointEveryUnconfirmedAppends: 2,
      },
      unconfirmedWriteCount: 1,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });

    await fixture.rpc.append({
      event: { type: "test.durability.string-checkpointed", payload: { n: 2 } },
      durability: "checkpointed",
    });
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 1,
      checkpointCompletedCount: 1,
    });
  });

  it("uses stream checkpoint threshold for checkpointed object overrides without a threshold", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });
    await fixture.rpc.patchSettings({
      defaultAppendDurabilityMode: "best-effort",
      checkpointEveryUnconfirmedAppends: 2,
    });

    await fixture.rpc.append({
      event: { type: "test.durability.object-checkpointed", payload: { n: 1 } },
      durability: { mode: "checkpointed" },
    });
    expect(await fixture.rpc.debug()).toMatchObject({
      settings: {
        defaultAppendDurabilityMode: "best-effort",
        checkpointEveryUnconfirmedAppends: 2,
      },
      unconfirmedWriteCount: 1,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });

    await fixture.rpc.append({
      event: { type: "test.durability.object-checkpointed", payload: { n: 2 } },
      durability: { mode: "checkpointed" },
    });
    expect(await fixture.rpc.debug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointStartedCount: 1,
      checkpointCompletedCount: 1,
    });
  });

  it("lets unrelated RPC resolve while confirmed append waits for durability", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using writer = await withStream({ path });
    await using probe = await withStream({ path });
    await writer.rpc.patchSettings({ debugConfirmedSyncDelayMs: 300 });

    const append = writer.rpc.append({
      event: { type: "test.causal.confirmed", payload: { path } },
      durability: "confirmed",
    });

    const winner = await Promise.race([
      probe.rpc.ping().then(() => "ping"),
      append.then(() => "append"),
    ]);

    expect(winner).toBe("ping");
    expect(await append).toMatchObject({
      type: "test.causal.confirmed",
      offset: 1,
      payload: { path },
    });
  });

  it("lets subscribers drain old events but not the new confirmed event before durability", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const history: StreamEventInput[] = Array.from({ length: 3 }, (_, i) => ({
      type: "test.causal.history",
      payload: { n: i + 1 },
    }));
    const live: StreamEventInput = {
      type: "test.causal.live",
      payload: { marker: "must-not-leak-before-confirm" },
    };

    await using writer = await withStream({ path });
    await writer.rpc.appendBatch({ events: history, durability: "best-effort" });
    await writer.rpc.sync();
    await writer.rpc.patchSettings({ debugConfirmedSyncDelayMs: 300 });

    await using reader = await withStream({ path });
    const readable = await reader.rpc.stream();
    // @ts-expect-error capnweb@0.8.0 types only model ReadableStream<Uint8Array>
    const streamReader = (readable as ReadableStream<StreamEvent>).getReader();

    const append = writer.rpc.append({ event: live, durability: "confirmed" });
    const backlog = await readEvents(streamReader, history.length, 100);
    expectContiguousOffsets(backlog, history.length);

    const liveRead = streamReader.read();
    await expectReadTimesOut(liveRead, 100);

    const appended = await append;
    expect(appended).toMatchObject({ offset: 4, payload: live.payload });

    const delivered = await withTimeout(liveRead, 1_000);
    expect(delivered.done).toBe(false);
    expect(delivered.value).toMatchObject({ offset: 4, payload: live.payload });

    streamReader.releaseLock();
  });
});
