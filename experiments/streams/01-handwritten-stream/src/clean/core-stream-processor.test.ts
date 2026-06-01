import { describe, expect, it } from "vitest";
import {
  initialCoreStreamState,
  reduceCoreStreamState,
} from "./core-stream-processor.js";

describe("core stream processor", () => {
  it("tracks zero-based offsets for every event", () => {
    const state = initialCoreStreamState("2026-06-01T12:00:00.000Z");

    const next = reduceCoreStreamState({
      state,
      event: {
        offset: 0,
        type: "test.event",
        payload: {},
        createdAt: "2026-06-01T12:00:01.000Z",
      },
    });

    expect(next).toMatchObject({
      eventCount: 1,
      maxOffset: 0,
      subscriptionsByKey: {},
    });
  });

  it("keeps the latest subscription-configured event by subscription key", () => {
    const state = initialCoreStreamState("2026-06-01T12:00:00.000Z");

    const first = reduceCoreStreamState({
      state,
      event: {
        offset: 0,
        type: "events.iterate.com/stream/subscription-configured",
        idempotencyKey: "subscription:echo",
        payload: {
          subscriptionKey: "echo",
          subscriber: {
            type: "built-in",
            transport: "captainweb-websocket",
            processorSlug: "echo",
          },
        },
        createdAt: "2026-06-01T12:00:01.000Z",
      },
    });
    const second = reduceCoreStreamState({
      state: first,
      event: {
        offset: 1,
        type: "events.iterate.com/stream/subscription-configured",
        idempotencyKey: "subscription:echo",
        payload: {
          subscriptionKey: "echo",
          subscriber: {
            type: "built-in",
            transport: "captainweb-websocket",
            processorSlug: "echo",
          },
        },
        createdAt: "2026-06-01T12:00:02.000Z",
      },
    });

    expect(second.subscriptionsByKey.echo.latestConfiguredEvent.offset).toBe(1);
    expect(second).toMatchObject({
      eventCount: 2,
      maxOffset: 1,
    });
  });
});
