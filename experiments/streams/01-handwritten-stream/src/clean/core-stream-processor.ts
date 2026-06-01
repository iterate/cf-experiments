import { z } from "zod";
import type { StreamEvent } from "@cf-experiments/shared/event";
import { defineProcessorContract } from "@cf-experiments/shared/stream-processors";

export const coreStreamProcessorContract = defineProcessorContract({
  slug: "events.iterate.com/stream/core",
  version: "0.1.0",
  description: "Maintains the stream's own reduced state.",
  stateSchema: z.object({
    streamNamespace: z.string().trim().min(1),
    streamPath: z.string().trim().min(1),
    createdAt: z.string(),
    incarnationId: z.string().trim().min(1),
    eventCount: z.number().int().min(0),
    maxOffset: z.number().int().min(-1),
    subscriptionsByKey: z.record(
      z.string(),
      z.object({
        latestConfiguredEvent: z.object({
          offset: z.number().int().min(0),
          type: z.literal("events.iterate.com/stream/subscription-configured"),
          payload: z.object({
            subscriptionKey: z.string().trim().min(1),
            subscriber: z.discriminatedUnion("type", [
              z.object({
                type: z.literal("built-in"),
                transport: z.literal("captainweb-websocket"),
                processorSlug: z.string().trim().min(1),
              }),
              z.object({
                type: z.literal("dynamic-worker"),
                transport: z.literal("captainweb-websocket"),
                workerName: z.string().trim().min(1),
                entrypoint: z.string().trim().min(1),
              }),
              z.object({
                type: z.literal("external-url"),
                transport: z.literal("https-webhook"),
                url: z.url(),
              }),
            ]),
          }),
          createdAt: z.string(),
        }),
      }),
    ),
  }),
  initialState: {
    streamNamespace: "uninitialized",
    streamPath: "uninitialized",
    createdAt: "uninitialized",
    incarnationId: "uninitialized",
    eventCount: 0,
    maxOffset: -1,
    subscriptionsByKey: {},
  },
  events: {
    "events.iterate.com/stream/created": {
      description: "Initializes the core reduced state for a stream.",
      payloadSchema: z.object({
        streamNamespace: z.string().trim().min(1),
        streamPath: z.string().trim().min(1),
      }),
    },
    "events.iterate.com/stream/woken": {
      description: "Records that a Durable Object incarnation has started running this stream.",
      payloadSchema: z.object({
        incarnationId: z.string().trim().min(1),
      }),
    },
    "events.iterate.com/stream/subscription-configured": {
      description: "Configures or replaces an outbound subscription for this stream.",
      payloadSchema: z.object({
        subscriptionKey: z.string().trim().min(1),
        subscriber: z.discriminatedUnion("type", [
          z.object({
            type: z.literal("built-in"),
            transport: z.literal("captainweb-websocket"),
            processorSlug: z.string().trim().min(1),
          }),
          z.object({
            type: z.literal("dynamic-worker"),
            transport: z.literal("captainweb-websocket"),
            workerName: z.string().trim().min(1),
            entrypoint: z.string().trim().min(1),
          }),
          z.object({
            type: z.literal("external-url"),
            transport: z.literal("https-webhook"),
            url: z.url(),
          }),
        ]),
      }),
    },
  },
  consumes: [
    "*",
    "events.iterate.com/stream/created",
    "events.iterate.com/stream/woken",
    "events.iterate.com/stream/subscription-configured",
  ],
  emits: [],
  reduce({ state, event }) {
    // All events increment the event count and max offset
    const next = {
      ...state,
      eventCount: Math.max(state.eventCount, event.offset + 1),
      maxOffset: Math.max(state.maxOffset, event.offset),
    };

    if (event.type === "events.iterate.com/stream/created") {
      return {
        ...next,
        streamNamespace: event.payload.streamNamespace,
        streamPath: event.payload.streamPath,
        createdAt: event.createdAt,
      };
    }

    if (event.type === "events.iterate.com/stream/woken") {
      return {
        ...next,
        incarnationId: event.payload.incarnationId,
      };
    }

    if (event.type !== "events.iterate.com/stream/subscription-configured") {
      return next;
    }

    const latestConfiguredEvent = {
      ...event,
      type: "events.iterate.com/stream/subscription-configured" as const,
    };

    return {
      ...next,
      subscriptionsByKey: {
        ...next.subscriptionsByKey,
        [event.payload.subscriptionKey]: { latestConfiguredEvent },
      },
    };
  },
});

export type CoreStreamState = z.infer<typeof coreStreamProcessorContract.stateSchema>;

export type SubscriptionConfiguredEvent =
  CoreStreamState["subscriptionsByKey"][string]["latestConfiguredEvent"];

export function reduceCoreStreamState(args: {
  state: CoreStreamState;
  event: StreamEvent;
}): CoreStreamState {
  return coreStreamProcessorContract.stateSchema.parse(
    coreStreamProcessorContract.reduce?.({
      contract: coreStreamProcessorContract,
      state: args.state,
      event: args.event as Parameters<
        NonNullable<typeof coreStreamProcessorContract.reduce>
      >[0]["event"],
    }) ?? args.state,
  );
}
