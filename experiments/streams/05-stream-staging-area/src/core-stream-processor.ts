import { z } from "zod";
import { defineProcessorContract } from "@cf-experiments/shared/stream-processors";

const outboundSubscriberSchema = z.discriminatedUnion("type", [
  z.object({
    type: z.literal("built-in"),
    transport: z.literal("capnweb-websocket"),
    processorSlug: z.string().trim().min(1),
  }),
  z.object({
    type: z.literal("external-url"),
    transport: z.literal("capnweb-websocket"),
    url: z.url(),
    headers: z.record(z.string(), z.string()).optional(),
  }),
  // TODO: Add dynamic-worker when a worker-name/entrypoint dialer exists.
  // TODO: Add webhooks only if we want non-capnweb delivery semantics.
]);

export const coreStreamProcessorContract = defineProcessorContract({
  slug: "stream",
  version: "0.1.0",
  description: "Maintains the stream's own reduced state.",
  stateSchema: z.object({
    namespace: z.string().trim().min(1),
    path: z.string().trim().min(1),
    createdAt: z.string(),
    incarnationId: z.string().trim().min(1),
    config: z.object({
      simulatedStorageSyncDelayMs: z.number().int().min(0).default(0).nullable(),
    }),
    eventCount: z.number().int().min(0),
    maxOffset: z.number().int().min(0),
    subscriptionsByKey: z.record(
      z.string(),
      z.object({
        latestConfiguredEvent: z.object({
          offset: z.number().int().min(0),
          type: z.literal("events.iterate.com/stream/subscription-configured"),
          payload: z.object({
            subscriptionKey: z.string().trim().min(1),
            subscriber: outboundSubscriberSchema,
          }),
          createdAt: z.string(),
        }),
      }),
    ),
  }),
  initialState: {
    namespace: "uninitialized",
    path: "uninitialized",
    createdAt: "uninitialized",
    incarnationId: "uninitialized",
    config: {
      simulatedStorageSyncDelayMs: 0,
    },
    eventCount: 0,
    maxOffset: 0,
    subscriptionsByKey: {},
  },
  events: {
    "events.iterate.com/stream/created": {
      description: "Initializes the core reduced state for a stream.",
      payloadSchema: z.object({
        namespace: z.string().trim().min(1),
        path: z.string().trim().min(1),
      }),
    },
    "events.iterate.com/stream/woken": {
      description: "Records that a Durable Object incarnation has started running this stream.",
      payloadSchema: z.object({
        incarnationId: z.string().trim().min(1),
      }),
    },
    "events.iterate.com/stream/configured": {
      description: "Configures stream-level options.",
      payloadSchema: z.object({
        config: z.object({
          simulatedStorageSyncDelayMs: z.number().int().min(0),
        }),
      }),
    },
    "events.iterate.com/stream/subscription-configured": {
      description: "Configures or replaces an outbound subscription for this stream.",
      payloadSchema: z.object({
        subscriptionKey: z.string().trim().min(1),
        subscriber: outboundSubscriberSchema,
      }),
    },
  },
  consumes: [
    "*",
    "events.iterate.com/stream/created",
    "events.iterate.com/stream/woken",
    "events.iterate.com/stream/configured",
    "events.iterate.com/stream/subscription-configured",
  ],
  emits: [],
  reduce({ state, event }) {
    // All events increment the event count and max offset
    const next = {
      ...state,
      eventCount: state.eventCount + 1,
      maxOffset: event.offset,
    };

    switch (event.type) {
      // events.iterate.com/stream/created will only ever happen once and is always the first event
      case "events.iterate.com/stream/created":
        if (event.offset !== 1) {
          throw new Error(
            "events.iterate.com/stream/created must be the first event and have offset 1",
          );
        }
        return {
          ...next,
          namespace: event.payload.namespace,
          path: event.payload.path,
          createdAt: event.createdAt,
        };

      // events.iterate.com/stream/woken will fire each time the Stream durable object's javascript constructor
      // fires. Each time that happens, we get a new "incarnationId"
      case "events.iterate.com/stream/woken":
        return {
          ...next,
          incarnationId: event.payload.incarnationId,
        };

      // events.iterate.com/stream/configured is used for runtime configuration of the stream
      case "events.iterate.com/stream/configured":
        return {
          ...next,
          config: {
            ...next.config,
            ...event.payload.config,
          },
        };

      // events.iterate.com/stream/subscription-configured is used to configure outbound subscriptions
      case "events.iterate.com/stream/subscription-configured": {
        return {
          ...next,
          subscriptionsByKey: {
            ...next.subscriptionsByKey,
            [event.payload.subscriptionKey]: { latestConfiguredEvent: event },
          },
        };
      }

      default:
        return next;
    }
  },
});

export type CoreStreamState = z.infer<typeof coreStreamProcessorContract.stateSchema>;

export type SubscriptionConfiguredEvent =
  CoreStreamState["subscriptionsByKey"][string]["latestConfiguredEvent"];
