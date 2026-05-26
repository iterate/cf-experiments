import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import { describe, expect, it } from "vitest";
import { withStream } from "./lib/with-stream.js";

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

describe("handwritten stream capnweb", () => {
  it("append returns committed event over capnweb", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const event: StreamEventInput = { type: "test.append", payload: { path } };

    await using fixture = await withStream({ path });

    await fixture.rpc.append({ event });

    expect(fixture.wireAnalysis().resultWaits).toHaveLength(1);
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
  });

  it("distinguishes sequential waits from concurrent waits", async () => {
    const sequentialName = `stream-${crypto.randomUUID()}`;
    const firstConcurrentName = `stream-${crypto.randomUUID()}`;
    const secondConcurrentName = `stream-${crypto.randomUUID()}`;

    await using sequential = await withStream({ path: sequentialName });
    await sequential.rpc.append({
      event: { type: "test.sequential", payload: { name: sequentialName } },
    });
    expect(await sequential.rpc.count()).toBe(1);
    expect(sequential.wireAnalysis().resultWaits).toHaveLength(2);
    expect(sequential.wireAnalysis().waves).toHaveLength(2);

    await using concurrent = await withStream({ path: firstConcurrentName });
    await using concurrent2 = await withStream({ path: secondConcurrentName });
    const firstAppend = concurrent.rpc.append({
      event: { type: "test.concurrent", payload: { name: firstConcurrentName } },
    });
    const secondAppend = concurrent2.rpc.append({
      event: { type: "test.concurrent", payload: { name: secondConcurrentName } },
    });
    await Promise.all([firstAppend, secondAppend]);

    expect(concurrent.wireAnalysis().resultWaits).toHaveLength(1);
    expect(concurrent2.wireAnalysis().resultWaits).toHaveLength(1);
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
    expect(await writer.rpc.count()).toBe(1);

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
    expect(await fixture.rpc.count()).toBe(1);

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

  it(
    "buffers a burst for a delayed client reader beyond desiredBufferedEvents",
    async () => {
      const path = `stream-${crypto.randomUUID()}`;
      const burstSize = 20;
      const events: StreamEventInput[] = Array.from({ length: burstSize }, (_, i) => ({
        type: "test.backpressure",
        payload: { n: i + 1 },
      }));

      await using reader = await withStream({ path });
      const readable = await reader.rpc.stream({ desiredBufferedEvents: 1 });

      await using writer = await withStream({ path });
      await writer.rpc.appendBatch({ events });

      const debugAfterBurst = await reader.rpc.streamDebug();
      expect(debugAfterBurst.subscribers).toHaveLength(1);
      expect(debugAfterBurst.subscribers[0]).toMatchObject({
        enqueuedEvents: burstSize,
        desiredBufferedEvents: 1,
      });

      // desiredBufferedEvents is a backpressure signal, not a hard cap:
      // enqueue() accepted the whole burst while the client had not started
      // reading yet.
      expect(debugAfterBurst.subscribers[0]!.desiredSize).toBeLessThanOrEqual(1);

      // @ts-expect-error capnweb@0.8.0 types only model ReadableStream<Uint8Array>
      const streamReader = (readable as ReadableStream<StreamEvent>).getReader();
      const delivered = await readEvents(streamReader, burstSize, 1_000);

      expectContiguousOffsets(delivered, burstSize);
      expect(delivered.map((event) => event.payload)).toEqual(events.map((event) => event.payload));

      streamReader.releaseLock();
    },
    5_000,
  );

  it("defaults to confirmed appends and does not accumulate unconfirmed write debt", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });

    await fixture.rpc.append({
      event: { type: "test.durability.default", payload: { mode: "confirmed" } },
    });

    expect(await fixture.rpc.count()).toBe(1);
    expect(await fixture.rpc.durabilityDebug()).toMatchObject({
      settings: {
        defaultAppendDurabilityMode: "confirmed",
        checkpointEveryUnconfirmedWrites: 100,
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

    expect(await fixture.rpc.durabilityDebug()).toMatchObject({
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
    expect(await fixture.rpc.count()).toBe(1);
    expect(await fixture.rpc.durabilityDebug()).toMatchObject({
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

    expect(await fixture.rpc.count()).toBe(2);
    expect(await fixture.rpc.durabilityDebug()).toMatchObject({
      settings: { defaultAppendDurabilityMode: "best-effort" },
      unconfirmedWriteCount: 1,
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
      durability: { mode: "checkpointed", checkpointEveryUnconfirmedWrites: 2 },
    });

    expect(await fixture.rpc.count()).toBe(events.length);
    expect(await fixture.rpc.durabilityDebug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointInProgress: false,
      checkpointStartedCount: 1,
      checkpointCompletedCount: 1,
    });
  });

  it("uses checkpointed stream settings when append does not pass a per-call override", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });
    await fixture.rpc.patchSettings({
      defaultAppendDurabilityMode: "checkpointed",
      checkpointEveryUnconfirmedWrites: 2,
    });

    await fixture.rpc.append({ event: { type: "test.durability.settings", payload: { n: 1 } } });
    expect(await fixture.rpc.durabilityDebug()).toMatchObject({
      settings: {
        defaultAppendDurabilityMode: "checkpointed",
        checkpointEveryUnconfirmedWrites: 2,
      },
      unconfirmedWriteCount: 1,
      checkpointStartedCount: 0,
      checkpointCompletedCount: 0,
    });

    await fixture.rpc.append({ event: { type: "test.durability.settings", payload: { n: 2 } } });
    expect(await fixture.rpc.durabilityDebug()).toMatchObject({
      unconfirmedWriteCount: 0,
      checkpointInProgress: false,
      checkpointStartedCount: 1,
      checkpointCompletedCount: 1,
    });
  });
});
