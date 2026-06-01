import { z } from "zod";
import type { StreamEvent } from "@cf-experiments/shared/event";
import { defineProcessorContract } from "@cf-experiments/shared/stream-processors";

export const coreStreamProcessorContract = defineProcessorContract({
  slug: "events.iterate.com/stream/core",
  version: "0.1.0",
  description: "Maintains the stream's own reduced state.",
  stateSchema: z.object({
    createdAt: z.string(),
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
  events: {
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
  consumes: ["*"],
  emits: [],
  reduce({ state, event }) {
    const next = {
      ...state,
      eventCount: Math.max(state.eventCount, event.offset + 1),
      maxOffset: Math.max(state.maxOffset, event.offset),
    };

    if (event.type !== "events.iterate.com/stream/subscription-configured") {
      return next;
    }

    const payload = z
      .object({
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
      })
      .parse(event.payload);
    const latestConfiguredEvent = {
      ...event,
      type: "events.iterate.com/stream/subscription-configured" as const,
      payload,
    };

    return {
      ...next,
      subscriptionsByKey: {
        ...next.subscriptionsByKey,
        [payload.subscriptionKey]: { latestConfiguredEvent },
      },
    };
  },
});

export type CoreStreamState = z.infer<typeof coreStreamProcessorContract.stateSchema>;

export type SubscriptionConfiguredEvent =
  CoreStreamState["subscriptionsByKey"][string]["latestConfiguredEvent"];

export function initialCoreStreamState(createdAt: string): CoreStreamState {
  return coreStreamProcessorContract.stateSchema.parse({
    createdAt,
    eventCount: 0,
    maxOffset: -1,
    subscriptionsByKey: {},
  });
}

export function reduceCoreStreamState(args: {
  state: CoreStreamState;
  event: StreamEvent;
}): CoreStreamState {
  return coreStreamProcessorContract.stateSchema.parse(
    coreStreamProcessorContract.reduce?.({
      contract: coreStreamProcessorContract,
      state: args.state,
      event: args.event,
    }) ?? args.state,
  );
}
