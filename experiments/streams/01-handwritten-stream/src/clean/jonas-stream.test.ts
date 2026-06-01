import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import { readFile } from "node:fs/promises";
import { z } from "zod";
import { describe, expect, it } from "vitest";
import { echoProcessor } from "./demo-processors/echo-processor.js";
import {
  withStream,
  withStreamCapnweb,
  withStreamProcessorCapnweb,
  withStreamRaw,
} from "./jonas-stream-client.js";

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";
const deployedIt = workerUrl.includes("localhost") ? it.skip : it;
const hibernationWaitMs = Number(process.env.HIBERNATION_WAIT_MS ?? 15_000);

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
      { direction: "out", data: { op: "subscribe" } },
      { direction: "out", data: { op: "append", event } },
      { direction: "in", data: { op: "events", events: [delivered] } },
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
      { direction: "out", data: { op: "subscribe" } },
      {
        direction: "out",
        data: { op: "append", event, requestAck: { key: "append-1" } },
      },
      { direction: "in", data: { op: "events", events: [delivered] } },
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

  it("runs a simple processor in a vitest fixture", async () => {
    const path = `jonas-processor-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path });
    await using processor = await fixture.withProcessor<{ seen: number; blocked: number }>({
      stateSchema: z.object({
        seen: z.number().default(0),
        blocked: z.number().default(0),
      }),
      initialState: {},
      reduce({ state, event }) {
        if (event.type === "test.processor.input") {
          return { ...state, seen: state.seen + 1 };
        }
        if (event.type === "test.processor.block") {
          return { ...state, blocked: state.blocked + 1 };
        }
        return state;
      },
      afterAppend({ event, state, append, appendAndWait, blockProcessorUntil }) {
        if (event.type === "test.processor.input") {
          append({
            type: "test.processor.output",
            payload: { seen: state.seen },
          });
        }
        if (event.type === "test.processor.block") {
          blockProcessorUntil(async () => {
            await appendAndWait({
              type: "test.processor.blocked-output",
              payload: { blocked: state.blocked },
            });
          });
        }
      },
    });
    const events = fixture.stream();

    await fixture.append({
      event: {
        type: "test.processor.input",
        payload: {},
      },
    });
    expect(await eventOfType(events, "test.processor.output")).toMatchObject({
      type: "test.processor.output",
      payload: { seen: 1 },
    });
    expect(processor.snapshot().state).toEqual({ seen: 1, blocked: 0 });
    expect(processor.snapshot().offset).toBeGreaterThanOrEqual(1);

    await fixture.append({
      event: {
        type: "test.processor.block",
        payload: {},
      },
    });
    expect(await eventOfType(events, "test.processor.blocked-output")).toMatchObject({
      type: "test.processor.blocked-output",
      payload: { blocked: 1 },
    });
    await waitFor(() => {
      expect(processor.snapshot().state).toEqual({ seen: 1, blocked: 1 });
    });
    expect(processor.snapshot().offset).toBeGreaterThanOrEqual(3);
  });

  it.fails(
    "keeps delivering events to a stream processor after JonasStream hibernates with no open clients",
    async () => {
      // Hypothesis (see to-test.md): `#connectStreamProcessor()` runs during a capnweb `append()`
      // RPC and stores the outbound JonasStream → StreamProcessor socket in in-memory
      // `#outboundWebSockets`. Outbound websockets do not keep the DO alive. Once every request
      // context expires and JonasStream sleeps, that in-memory subscriber set is gone — we likely
      // need to (re)open processor connections from an alarm instead.
      //
      // Reproduces on deployed after ~120s idle with no JonasStream clients (HIBERNATION_WAIT_MS).
      const streamPath = `jonas-processor-hibernate-${crypto.randomUUID()}`;
      const processorPath = `${streamPath}:echo`;

      await using processor = await withStreamProcessorCapnweb({ path: processorPath });

      {
        await using subscriptionCapnweb = await withStreamCapnweb({ path: streamPath });
        await subscriptionCapnweb.capnweb.append({
          event: {
            type: "events.iterate.com/stream/processor-subscribed",
            payload: { processorSlug: "echo" },
          },
        });
      }

      await waitFor(async () => {
        const status = await processor.capnweb.status();
        expect(status.processorSlug).toBe("echo");
        expect(status.snapshot).toMatchObject({ state: { seen: 0 } });
      });

      // Outbound websockets do not pin the DO awake; after request contexts expire it sleeps.
      await new Promise((resolve) => setTimeout(resolve, hibernationWaitMs));

      await using publisher = await withStreamCapnweb({ path: streamPath });
      await publisher.capnweb.append({
        event: {
          type: "test.processor.input",
          payload: { afterHibernation: true },
        },
      });

      await waitFor(async () => {
        expect((await processor.capnweb.status()).snapshot?.state).toEqual({ seen: 1 });
      });
    },
    hibernationWaitMs + 60_000,
  );

  it("runs the same demo processor in vitest and through a processor durable object", async () => {
    const fixturePath = `jonas-fixture-processor-${crypto.randomUUID()}`;
    await using fixture = await withStream({ path: fixturePath });
    await using fixtureProcessor = await fixture.withProcessor(echoProcessor);
    const fixtureEvents = fixture.stream();

    await fixture.append({
      event: { type: "test.processor.input", payload: { runner: "vitest" } },
    });
    expect(await eventOfType(fixtureEvents, "test.processor.output")).toMatchObject({
      payload: { seen: 1 },
    });
    expect(fixtureProcessor.snapshot().state).toEqual({ seen: 1 });

    const durableStreamPath = `jonas-do-processor-${crypto.randomUUID()}`;
    const durableProcessorPath = `${durableStreamPath}:echo`;
    await using durableStream = await withStream({ path: durableStreamPath });
    await using durableProcessor = await withStreamProcessorCapnweb({ path: durableProcessorPath });
    const durableEvents = durableStream.stream();

    expect(await durableProcessor.capnweb.status()).toMatchObject({
      processorSlug: undefined,
      snapshot: undefined,
    });

    const subscription = await durableStream.append({
      event: {
        type: "events.iterate.com/stream/processor-subscribed",
        payload: { processorSlug: "echo" },
      },
    });
    await waitFor(async () => {
      const status = await durableProcessor.capnweb.status();
      expect(status.processorSlug).toBe("echo");
      expect(status.snapshot).toMatchObject({
        state: { seen: 0 },
        offset: subscription.offset,
      });
    });

    await durableStream.append({
      event: { type: "test.processor.input", payload: { runner: "durable-object" } },
    });

    expect(await eventOfType(durableEvents, "test.processor.output")).toMatchObject({
      payload: { seen: 1 },
    });
    await waitFor(async () => {
      const status = await durableProcessor.capnweb.status();
      expect(status.snapshot?.state).toEqual({ seen: 1 });
    });
  });

  deployedIt("keeps a raw websocket connected across deployed hibernation", async () => {
    const path = `jonas-hibernate-${crypto.randomUUID()}`;
    const event: StreamEventInput = {
      type: "test.jonas.deployed-hibernation",
      payload: { path },
    };

    await using raw = await withStreamRaw({ path });
    const events = raw.stream();

    const before = await withStreamCapnweb({ path });
    const beforePing = await before.capnweb.ping();
    await before[Symbol.asyncDispose]();

    await new Promise((resolve) => setTimeout(resolve, 15_000));

    const appended = await raw.appendAndWaitForResponse(event, { key: "after-hibernation" });
    expect(await nextEvent(events)).toEqual(appended);

    await using after = await withStreamCapnweb({ path });
    await expect(after.capnweb.ping()).resolves.not.toEqual(beforePing);
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
  timeoutMs = 5_000,
): Promise<StreamEvent> {
  const result = await withTimeout(events.next(), timeoutMs);
  if (result.done) throw new Error("stream ended before an event arrived");
  return result.value;
}

async function eventOfType(
  events: AsyncIterableIterator<StreamEvent>,
  type: string,
): Promise<StreamEvent> {
  for (let i = 0; i < 10; i++) {
    const event = await nextEvent(events);
    if (event.type === type) return event;
  }
  throw new Error(`timed out waiting for event type ${type}`);
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

async function waitFor(assertion: () => void | Promise<void>, timeoutMs = 5_000) {
  const startedAt = Date.now();
  let lastError: unknown;
  while (Date.now() - startedAt < timeoutMs) {
    try {
      await assertion();
      return;
    } catch (error) {
      lastError = error;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }
  throw lastError;
}
