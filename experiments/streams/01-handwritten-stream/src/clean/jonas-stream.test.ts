import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";
import { withStream, withStreamCapnweb, withStreamRaw } from "./jonas-stream-client.js";

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";

describe("jonas stream websocket primitives", () => {
  it("rejects non-websocket requests at the durable object boundary", async () => {
    const path = `jonas-${crypto.randomUUID()}`;
    const response = await fetch(new URL(`/jonas/${path}`, workerUrl));

    expect(response.status).toBe(400);
    expect(await response.text()).toBe("WebSocket only");
  });

  it("append uses the allowUnconfirmed write fast path", async () => {
    const source = await readFile(
      decodeURIComponent(new URL("./jonas-stream.ts", import.meta.url).pathname),
      "utf8",
    );

    expect(source).toMatch(
      /this\.ctx\.storage\.put\(writes,\s*\{\s*allowUnconfirmed: true,\s*noCache: true\s*\}\)/,
    );
  });

  it("streams events appended on the same websocket", async () => {
    const path = `jonas-${crypto.randomUUID()}`;
    const event: StreamEventInput = {
      type: "test.jonas.same-websocket",
      payload: { path },
    };

    await using stream = await withStreamRaw({ path });
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

    await using subscriber = await withStreamRaw({ path });
    await using publisher = await withStreamRaw({ path });
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

  it("raw appendAndWaitForResponse returns the appended event with an append key ack", async () => {
    const path = `jonas-${crypto.randomUUID()}`;
    const event: StreamEventInput = {
      type: "test.jonas.append-and-wait",
      payload: { path },
    };

    await using stream = await withStreamRaw({ path });
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

  it("raw appendAndWaitForResponse broadcasts before acknowledging the append", async () => {
    const path = `jonas-${crypto.randomUUID()}`;
    const event: StreamEventInput = {
      type: "test.jonas.append-and-wait-subscribed",
      payload: { path },
    };

    await using stream = await withStreamRaw({ path });
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

  it("simulated storage sync delay holds fan-out and append acknowledgement", async () => {
    const path = `jonas-${crypto.randomUUID()}`;
    const event: StreamEventInput = {
      type: "test.jonas.simulated-sync-delay",
      payload: { path },
    };

    await using fixture = await withStream({ path });
    const events = fixture.stream();

    await fixture.capnweb.simulateStorageSyncDelay(500);
    const append = fixture.append({ event });
    const delivery = nextEvent(events);

    await expect(withTimeout(append, 100)).rejects.toThrow(/timed out/);
    await expect(withTimeout(delivery, 100)).rejects.toThrow(/timed out/);

    const [appended, delivered] = await Promise.all([append, delivery]);
    expect(appended).toMatchObject({
      type: event.type,
      payload: event.payload,
      offset: 1,
      createdAt: expect.any(String),
    });
    expect(delivered).toEqual(appended);
  });

  it("clears simulated storage sync delay with null", async () => {
    const path = `jonas-${crypto.randomUUID()}`;
    const event: StreamEventInput = {
      type: "test.jonas.clear-simulated-sync-delay",
      payload: { path },
    };

    await using stream = await withStreamCapnweb({ path });
    await stream.capnweb.simulateStorageSyncDelay(500);
    expect(await stream.capnweb.simulateStorageSyncDelay(null)).toBeNull();

    const appended = await withTimeout(stream.capnweb.append({ event }), 200);
    expect(appended).toMatchObject({ type: event.type, offset: 1 });
  });

  it("exposes public methods over capnweb transport", async () => {
    const path = `jonas-${crypto.randomUUID()}`;
    const event: StreamEventInput = {
      type: "test.jonas.capnweb-append",
      payload: { path },
    };

    await using stream = await withStreamCapnweb({ path });

    expect(await stream.capnweb.simulateStorageSyncDelay(null)).toBeNull();
    expect(await stream.capnweb.append({ event })).toMatchObject({
      type: event.type,
      payload: event.payload,
      offset: 1,
      createdAt: expect.any(String),
    });
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

    await using stream = await withStreamRaw({ path });
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

    await using stream = await withStreamRaw({ path });
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
