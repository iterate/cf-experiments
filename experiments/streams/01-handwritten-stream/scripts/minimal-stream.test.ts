import type { StreamEventInput } from "@cf-experiments/shared/event";
import { describe, expect, it } from "vitest";
import { withMinimalStream } from "./lib/minimal-stream.js";

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";

function outboundFramesAfter(
  wsMessages: { direction: string; data: string }[],
  afterFrameIndex: number,
) {
  return wsMessages
    .slice(afterFrameIndex)
    .filter((frame) => frame.direction === "out")
    .map((frame) => JSON.parse(frame.data));
}

describe("minimal websocket stream baseline", () => {
  it("rejects non-websocket requests at the minimal stream boundary", async () => {
    const path = `minimal-${crypto.randomUUID()}`;
    const response = await fetch(new URL(`/minimal/${path}`, workerUrl));

    expect(response.status).toBe(400);
    expect(await response.text()).toBe("This endpoint only accepts WebSocket requests.");
  });

  it("appends, broadcasts, and acknowledges with the lightweight websocket protocol", async () => {
    const path = `minimal-${crypto.randomUUID()}`;
    const event: StreamEventInput = { type: "test.minimal", payload: { path } };

    await using subscriber = await withMinimalStream({ path });
    await using publisher = await withMinimalStream({ path });
    await subscriber.subscribe();

    const appended = await publisher.append({ event, requestId: "frame-1" });
    const delivered = await subscriber.readEvent();

    expect(appended).toMatchObject({
      type: event.type,
      offset: 1,
      payload: event.payload,
      createdAt: expect.any(String),
    });
    expect(delivered).toEqual(appended);
    expect(publisher.wsMessages.map((frame) => JSON.parse(frame.data))).toMatchObject([
      { op: "append", requestId: "frame-1", event },
      { op: "ack", requestId: "frame-1", event: appended },
    ]);
  });

  it("pure subscribers originate no websocket frames after the initial subscribe", async () => {
    const path = `minimal-${crypto.randomUUID()}`;
    await using subscriber = await withMinimalStream({ path });
    await using publisher = await withMinimalStream({ path });
    await subscriber.subscribe();
    const afterSubscribeFrameIndex = subscriber.wsMessages.length;

    const events: StreamEventInput[] = Array.from({ length: 3 }, (_, i) => ({
      type: "test.minimal.pure-subscriber",
      payload: { n: i + 1 },
    }));
    const appended = await Promise.all(
      events.map((event, i) => publisher.append({ event, requestId: `frame-${i + 1}` })),
    );
    const delivered = await Promise.all(events.map(() => subscriber.readEvent()));

    expect(delivered).toEqual(appended);
    expect(outboundFramesAfter(subscriber.wsMessages, afterSubscribeFrameIndex)).toEqual([]);
  });
});
