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
    expect(await sequential.rpc.count()).toEqual({ kv: 1 });
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

      const outboundWhileReading = reader.wsMessages
        .slice(outboundAfterSubscribe)
        .filter((frame) => frame.direction === "out")
        .map((frame) => JSON.parse(frame.data))
        .filter((data) => data[0] === "pull" || data[0] === "push");
      expect(outboundWhileReading).toEqual([]);

      streamReader.releaseLock();
    },
    2_000,
  );
});
