import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import { describe, expect, it } from "vitest";
import { withStream } from "./jonas-stream-client.js";

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";

describe("jonas stream websocket primitives", () => {
  it("rejects non-websocket requests at the durable object boundary", async () => {
    const path = `jonas-${crypto.randomUUID()}`;
    const response = await fetch(new URL(`/jonas/${path}`, workerUrl));

    expect(response.status).toBe(400);
    expect(await response.text()).toBe("WebSocket only");
  });

  it("streams events appended on the same websocket", async () => {
    const path = `jonas-${crypto.randomUUID()}`;
    const event: StreamEventInput = {
      type: "test.jonas.same-websocket",
      payload: { path },
    };

    await using stream = await withStream({ path });
    const events = stream.stream();
    stream.append(event);

    const delivered = await nextEvent(events);

    expect(delivered).toMatchObject({
      type: event.type,
      payload: event.payload,
      offset: 1,
      createdAt: expect.any(String),
    });
    expect(parsedFrames(stream.wsMessages)).toEqual([
      { direction: "out", data: { op: "start" } },
      { direction: "out", data: { op: "append", event } },
      { direction: "in", data: { op: "event", event: delivered } },
    ]);
  });

  it("append is fire-and-forget while another client receives the event", async () => {
    const path = `jonas-${crypto.randomUUID()}`;
    const event: StreamEventInput = {
      type: "test.jonas.fire-and-forget",
      payload: { path },
    };

    await using subscriber = await withStream({ path });
    await using publisher = await withStream({ path });
    const events = subscriber.stream();
    const framesAfterStart = subscriber.wsMessages.length;

    publisher.append(event);
    const delivered = await nextEvent(events);

    expect(delivered).toMatchObject({
      type: event.type,
      payload: event.payload,
      offset: 1,
      createdAt: expect.any(String),
    });
    expect(parsedFrames(publisher.wsMessages)).toEqual([
      { direction: "out", data: { op: "append", event } },
    ]);
    expect(outboundFramesAfter(subscriber.wsMessages, framesAfterStart)).toEqual([]);
  });

  it("appendAndWaitForResponse returns the appended event with an append key ack", async () => {
    const path = `jonas-${crypto.randomUUID()}`;
    const event: StreamEventInput = {
      type: "test.jonas.append-and-wait",
      payload: { path },
    };

    await using stream = await withStream({ path });
    const appended = await stream.appendAndWaitForResponse(event, { key: "append-1" });

    expect(appended).toMatchObject({
      type: event.type,
      payload: event.payload,
      offset: 1,
      createdAt: expect.any(String),
    });
    expect(parsedFrames(stream.wsMessages)).toEqual([
      {
        direction: "out",
        data: { op: "append", event, requestAck: { key: "append-1" } },
      },
      { direction: "in", data: { op: "append-ack", appendKey: "append-1", event: appended } },
    ]);
  });

  it("appendAndWaitForResponse can wait while the stream iterator still receives the event", async () => {
    const path = `jonas-${crypto.randomUUID()}`;
    const event: StreamEventInput = {
      type: "test.jonas.append-and-wait-subscribed",
      payload: { path },
    };

    await using stream = await withStream({ path });
    const events = stream.stream();
    const append = stream.appendAndWaitForResponse(event, { key: "append-1" });

    const [delivered, appended] = await Promise.all([nextEvent(events), append]);

    expect(appended).toEqual(delivered);
    expect(parsedFrames(stream.wsMessages)).toEqual([
      { direction: "out", data: { op: "start" } },
      {
        direction: "out",
        data: { op: "append", event, requestAck: { key: "append-1" } },
      },
      { direction: "in", data: { op: "event", event: delivered } },
      { direction: "in", data: { op: "append-ack", appendKey: "append-1", event: appended } },
    ]);
  });

  it("idempotent appendAndWaitForResponse retries return the original event without rebroadcasting", async () => {
    const path = `jonas-${crypto.randomUUID()}`;
    const idempotencyKey = crypto.randomUUID();
    const first: StreamEventInput = {
      type: "test.jonas.append-and-wait-idempotency",
      idempotencyKey,
      payload: { attempt: 1 },
    };
    const retry: StreamEventInput = {
      type: "test.jonas.append-and-wait-idempotency",
      idempotencyKey,
      payload: { attempt: 2 },
    };

    await using stream = await withStream({ path });
    const events = stream.stream();

    const appended = await stream.appendAndWaitForResponse(first, { key: "append-1" });
    const delivered = await nextEvent(events);
    const retried = await stream.appendAndWaitForResponse(retry, { key: "append-2" });

    expect(delivered).toEqual(appended);
    expect(retried).toEqual(appended);
    await expect(withTimeout(events.next(), 100)).rejects.toThrow(/timed out/);
  });

  it("idempotent retries do not emit duplicate live events", async () => {
    const path = `jonas-${crypto.randomUUID()}`;
    const idempotencyKey = crypto.randomUUID();
    const first: StreamEventInput = {
      type: "test.jonas.idempotency",
      idempotencyKey,
      payload: { attempt: 1 },
    };
    const retry: StreamEventInput = {
      type: "test.jonas.idempotency",
      idempotencyKey,
      payload: { attempt: 2 },
    };

    await using stream = await withStream({ path });
    const events = stream.stream();

    stream.append(first);
    const delivered = await nextEvent(events);
    stream.append(retry);

    expect(delivered).toMatchObject({
      type: first.type,
      payload: first.payload,
      offset: 1,
      createdAt: expect.any(String),
    });
    await expect(withTimeout(events.next(), 100)).rejects.toThrow(/timed out/);
  });
});

async function nextEvent(
  events: AsyncIterableIterator<StreamEvent>,
  timeoutMs = 1_000,
): Promise<StreamEvent> {
  const result = await withTimeout(events.next(), timeoutMs);
  if (result.done) throw new Error("stream ended before an event arrived");
  return result.value;
}

function parsedFrames(messages: { direction: "out" | "in"; data: string }[]) {
  return messages.map((frame) => ({
    direction: frame.direction,
    data: JSON.parse(frame.data),
  }));
}

function outboundFramesAfter(
  messages: { direction: "out" | "in"; data: string }[],
  afterFrameIndex: number,
) {
  return parsedFrames(messages.slice(afterFrameIndex)).filter((frame) => frame.direction === "out");
}

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  return Promise.race([
    promise,
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error(`timed out after ${ms}ms`)), ms),
    ),
  ]);
}
