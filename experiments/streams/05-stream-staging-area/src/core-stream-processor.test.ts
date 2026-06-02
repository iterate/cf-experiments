import { describe, expect, it } from "vitest";
import { coreStreamProcessorContract } from "./core-stream-processor.js";

const reduce = coreStreamProcessorContract.reduce;
if (reduce === undefined) throw new Error("core stream processor must have a reducer");

describe("core stream processor", () => {
  it("reduces stream identity from the created event", () => {
    let state = coreStreamProcessorContract.stateSchema.parse(
      coreStreamProcessorContract.initialState,
    );
    state = coreStreamProcessorContract.stateSchema.parse(
      reduce({
        contract: coreStreamProcessorContract,
        state,
        event: {
          offset: 0,
          type: "events.iterate.com/stream/created",
          payload: {
            namespace: "stream",
            path: "test",
          },
          createdAt: "2026-06-01T12:00:00.000Z",
        },
      }),
    );
    state = coreStreamProcessorContract.stateSchema.parse(
      reduce({
        contract: coreStreamProcessorContract,
        state,
        event: {
          offset: 1,
          type: "events.iterate.com/stream/woken",
          payload: {
            incarnationId: "incarnation-1",
          },
          createdAt: "2026-06-01T12:00:00.001Z",
        },
      }),
    );
    expect(state).toMatchObject({
      createdAt: "2026-06-01T12:00:00.000Z",
      eventCount: 2,
      incarnationId: "incarnation-1",
      maxOffset: 1,
      namespace: "stream",
      path: "test",
      subscriptionsByKey: {},
    });
  });

  it("keeps the latest subscription-configured event by subscription key", () => {
    let state = coreStreamProcessorContract.stateSchema.parse(
      coreStreamProcessorContract.initialState,
    );
    state = coreStreamProcessorContract.stateSchema.parse(
      reduce({
        contract: coreStreamProcessorContract,
        state,
        event: {
          offset: 0,
          type: "events.iterate.com/stream/created",
          payload: {
            namespace: "stream",
            path: "test",
          },
          createdAt: "2026-06-01T12:00:00.000Z",
        },
      }),
    );
    state = coreStreamProcessorContract.stateSchema.parse(
      reduce({
        contract: coreStreamProcessorContract,
        state,
        event: {
          offset: 1,
          type: "events.iterate.com/stream/woken",
          payload: {
            incarnationId: "incarnation-1",
          },
          createdAt: "2026-06-01T12:00:00.001Z",
        },
      }),
    );
    state = coreStreamProcessorContract.stateSchema.parse(
      reduce({
        contract: coreStreamProcessorContract,
        state,
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
      }),
    );
    state = coreStreamProcessorContract.stateSchema.parse(
      reduce({
        contract: coreStreamProcessorContract,
        state,
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
      }),
    );

    expect(state.subscriptionsByKey.echo.latestConfiguredEvent.offset).toBe(3);
    expect(state).toMatchObject({
      eventCount: 4,
      maxOffset: 3,
    });
  });
});
