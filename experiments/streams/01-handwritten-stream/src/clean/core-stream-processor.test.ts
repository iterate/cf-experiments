import { describe, expect, it } from "vitest";
import {
  coreStreamProcessorContract,
  reduceCoreStreamState,
} from "./core-stream-processor.js";

describe("core stream processor", () => {
  it("reduces stream identity from the created event", () => {
    const state = reduceCoreStreamState({
      state: coreStreamProcessorContract.stateSchema.parse(coreStreamProcessorContract.initialState),
      event: {
        offset: 0,
        type: "events.iterate.com/stream/created",
        payload: {
          streamNamespace: "jonas",
          streamPath: "test",
        },
        createdAt: "2026-06-01T12:00:00.000Z",
      },
    });

    const woken = reduceCoreStreamState({
      state,
      event: {
        offset: 1,
        type: "events.iterate.com/stream/woken",
        payload: {
          incarnationId: "incarnation-1",
        },
        createdAt: "2026-06-01T12:00:00.001Z",
      },
    });

    const next = reduceCoreStreamState({
      state: woken,
      event: {
        offset: 2,
        type: "test.event",
        payload: {},
        createdAt: "2026-06-01T12:00:01.000Z",
      },
    });

    expect(next).toMatchObject({
      createdAt: "2026-06-01T12:00:00.000Z",
      eventCount: 3,
      incarnationId: "incarnation-1",
      maxOffset: 2,
      streamNamespace: "jonas",
      streamPath: "test",
      subscriptionsByKey: {},
    });
  });

  it("keeps the latest subscription-configured event by subscription key", () => {
    const state = reduceCoreStreamState({
      state: coreStreamProcessorContract.stateSchema.parse(coreStreamProcessorContract.initialState),
      event: {
        offset: 0,
        type: "events.iterate.com/stream/created",
        payload: {
          streamNamespace: "jonas",
          streamPath: "test",
        },
        createdAt: "2026-06-01T12:00:00.000Z",
      },
    });

    const woken = reduceCoreStreamState({
      state,
      event: {
        offset: 1,
        type: "events.iterate.com/stream/woken",
        payload: {
          incarnationId: "incarnation-1",
        },
        createdAt: "2026-06-01T12:00:00.001Z",
      },
    });

    const first = reduceCoreStreamState({
      state: woken,
      event: {
        offset: 2,
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
        offset: 3,
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

    expect(second.subscriptionsByKey.echo.latestConfiguredEvent.offset).toBe(3);
    expect(second).toMatchObject({
      eventCount: 4,
      maxOffset: 3,
    });
  });
});
