import { describe, expect, it } from "vitest";
import {
  buildProcessorRegisteredEvent,
  buildStreamErrorOccurredEvent,
  coreStreamProcessorContract,
} from "./core-stream-processor.js";

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
          offset: 1,
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
          offset: 2,
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
          offset: 3,
          type: "events.iterate.com/stream/configured",
          payload: {
            config: {
              simulatedStorageSyncDelayMs: 0,
            },
          },
          createdAt: "2026-06-01T12:00:01.000Z",
        },
      }),
    );
    expect(state).toMatchObject({
      createdAt: "2026-06-01T12:00:00.000Z",
      eventCount: 3,
      incarnationId: "incarnation-1",
      maxOffset: 3,
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
          offset: 1,
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
          offset: 2,
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
          offset: 3,
          type: "events.iterate.com/stream/subscription-configured",
          idempotencyKey: "subscription:echo",
          payload: {
            subscriptionKey: "echo",
            subscriber: {
              type: "built-in",
              transport: "capnweb-websocket",
              processorSlug: "echo-test",
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
          offset: 4,
          type: "events.iterate.com/stream/subscription-configured",
          idempotencyKey: "subscription:echo",
          payload: {
            subscriptionKey: "echo",
            subscriber: {
              type: "built-in",
              transport: "capnweb-websocket",
              processorSlug: "echo-test",
            },
          },
          createdAt: "2026-06-01T12:00:02.000Z",
        },
      }),
    );

    expect(state.subscriptionsByKey.echo.latestConfiguredEvent.offset).toBe(4);
    expect(state).toMatchObject({
      eventCount: 4,
      maxOffset: 4,
    });
  });

  it("keeps configuration in core state", () => {
    let state = coreStreamProcessorContract.stateSchema.parse(
      coreStreamProcessorContract.initialState,
    );
    state = coreStreamProcessorContract.stateSchema.parse(
      reduce({
        contract: coreStreamProcessorContract,
        state,
        event: {
          offset: 1,
          type: "events.iterate.com/stream/configured",
          payload: {
            config: {
              simulatedStorageSyncDelayMs: 25,
            },
          },
          createdAt: "2026-06-01T12:00:00.000Z",
        },
      }),
    );

    expect(state.config.simulatedStorageSyncDelayMs).toBe(25);
  });

  it("keeps the latest processor-registered event by processor slug", () => {
    let state = coreStreamProcessorContract.stateSchema.parse(
      coreStreamProcessorContract.initialState,
    );
    state = coreStreamProcessorContract.stateSchema.parse(
      reduce({
        contract: coreStreamProcessorContract,
        state,
        event: {
          offset: 1,
          createdAt: "2026-06-01T12:00:00.000Z",
          ...buildProcessorRegisteredEvent({
            contract: {
              slug: "echo-test",
              version: "0.1.0",
              description: "Echoes test inputs.",
              consumes: ["test.processor.input"],
              emits: ["test.processor.output"],
              events: {
                "test.processor.input": { description: "Input." },
                "test.processor.output": { description: "Output." },
              },
            },
          }),
        },
      }),
    );

    expect(state.processorsBySlug["echo-test"]?.latestRegisteredEvent.payload).toMatchObject({
      slug: "echo-test",
      version: "0.1.0",
      consumes: ["test.processor.input"],
      emits: ["test.processor.output"],
      ownedEvents: [
        { type: "test.processor.input", description: "Input." },
        { type: "test.processor.output", description: "Output." },
      ],
    });
  });

  it("accepts stream error-occurred events", () => {
    let state = coreStreamProcessorContract.stateSchema.parse(
      coreStreamProcessorContract.initialState,
    );
    state = coreStreamProcessorContract.stateSchema.parse(
      reduce({
        contract: coreStreamProcessorContract,
        state,
        event: {
          offset: 1,
          createdAt: "2026-06-01T12:00:00.000Z",
          ...buildStreamErrorOccurredEvent({
            idempotencyKey: "processor-error:echo-test:1",
            message: "Processor echo-test side effects failed at offset 1: boom",
            error: { name: "Error", message: "boom" },
          }),
        },
      }),
    );

    expect(state).toMatchObject({ eventCount: 1, maxOffset: 1 });
  });
});
