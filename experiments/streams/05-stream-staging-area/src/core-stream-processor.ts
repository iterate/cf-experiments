import { z } from "zod";
import { defineProcessorContract } from "@cf-experiments/shared/stream-processors";
import type { StreamEventInput } from "@cf-experiments/shared/event";

const DEFAULT_CIRCUIT_BREAKER_BURST_CAPACITY = 500;
const DEFAULT_CIRCUIT_BREAKER_REFILL_RATE_PER_MINUTE = 500;

type CircuitBreakerFields = {
  availableTokens: number;
  lastRefillAtMs: number | null;
  burstCapacity: number;
  refillRatePerMinute: number;
};

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
    childPaths: z.array(z.string().trim().min(1)),
    paused: z.boolean(),
    pauseReason: z.string().nullable(),
    availableTokens: z.number(),
    lastRefillAtMs: z.number().int().min(0).nullable(),
    burstCapacity: z.number().int().positive(),
    refillRatePerMinute: z.number().int().positive(),
    processorsBySlug: z.record(
      z.string(),
      z.object({
        latestRegisteredEvent: z.object({
          offset: z.number().int().min(0),
          type: z.literal("events.iterate.com/stream/processor-registered"),
          payload: z.object({
            slug: z.string().trim().min(1),
            version: z.string().trim().min(1),
            description: z.string(),
            consumes: z.array(z.string()),
            emits: z.array(z.string()),
            ownedEvents: z.array(
              z.object({
                type: z.string().trim().min(1),
                description: z.string().optional(),
                examples: z.array(z.unknown()).optional(),
              }),
            ),
          }),
          createdAt: z.string(),
        }),
      }),
    ),
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
    childPaths: [],
    paused: false,
    pauseReason: null,
    availableTokens: DEFAULT_CIRCUIT_BREAKER_BURST_CAPACITY,
    lastRefillAtMs: null,
    burstCapacity: DEFAULT_CIRCUIT_BREAKER_BURST_CAPACITY,
    refillRatePerMinute: DEFAULT_CIRCUIT_BREAKER_REFILL_RATE_PER_MINUTE,
    processorsBySlug: {},
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
    "events.iterate.com/stream/child-stream-created": {
      description: "Records an immediate child stream under this stream.",
      payloadSchema: z.object({
        childPath: z.string().trim().min(1),
      }),
    },
    "events.iterate.com/stream/subscription-configured": {
      description: "Configures or replaces an outbound subscription for this stream.",
      payloadSchema: z.object({
        subscriptionKey: z.string().trim().min(1),
        subscriber: outboundSubscriberSchema,
      }),
    },
    "events.iterate.com/stream/processor-registered": {
      description: "Records the public contract for a processor active on this stream.",
      payloadSchema: z.object({
        slug: z.string().trim().min(1),
        version: z.string().trim().min(1),
        description: z.string(),
        consumes: z.array(z.string()),
        emits: z.array(z.string()),
        ownedEvents: z.array(
          z.object({
            type: z.string().trim().min(1),
            description: z.string().optional(),
            examples: z.array(z.unknown()).optional(),
          }),
        ),
      }),
    },
    "events.iterate.com/stream/error-occurred": {
      description: "Records a structured stream or processor runner error.",
      payloadSchema: z.object({
        message: z.string().trim().min(1),
        error: z
          .object({
            name: z.string().trim().min(1).optional(),
            message: z.string().trim().min(1),
            code: z.string().trim().min(1).optional(),
            stack: z.string().trim().min(1).optional(),
          })
          .optional(),
      }),
    },
    "events.iterate.com/stream/circuit-breaker-configured": {
      description: "Configures the stream token-bucket circuit breaker.",
      payloadSchema: z.object({
        burstCapacity: z.number().int().positive(),
        refillRatePerMinute: z.number().int().positive(),
      }),
    },
    "events.iterate.com/stream/paused": {
      description: "Records that the stream is paused and should reject ordinary appends.",
      payloadSchema: z.object({
        reason: z.string().trim().min(1).optional(),
      }),
    },
    "events.iterate.com/stream/resumed": {
      description: "Records that the stream has resumed accepting ordinary appends.",
      payloadSchema: z.object({
        reason: z.string().trim().min(1).optional(),
      }),
    },
  },
  consumes: [
    "*",
    "events.iterate.com/stream/created",
    "events.iterate.com/stream/woken",
    "events.iterate.com/stream/configured",
    "events.iterate.com/stream/child-stream-created",
    "events.iterate.com/stream/subscription-configured",
    "events.iterate.com/stream/processor-registered",
    "events.iterate.com/stream/error-occurred",
    "events.iterate.com/stream/circuit-breaker-configured",
    "events.iterate.com/stream/paused",
    "events.iterate.com/stream/resumed",
  ],
  emits: [],
  reduce({ state, event }) {
    // All events increment the event count and max offset
    let next = {
      ...state,
      eventCount: state.eventCount + 1,
      maxOffset: event.offset,
    };

    switch (event.type) {
      case "events.iterate.com/stream/circuit-breaker-configured":
        return {
          ...next,
          burstCapacity: event.payload.burstCapacity,
          refillRatePerMinute: event.payload.refillRatePerMinute,
          availableTokens: event.payload.burstCapacity,
          lastRefillAtMs: Date.parse(event.createdAt),
        };

      case "events.iterate.com/stream/paused":
        return {
          ...next,
          paused: true,
          pauseReason: event.payload.reason ?? null,
          availableTokens: state.burstCapacity,
          lastRefillAtMs: Date.parse(event.createdAt),
        };

      case "events.iterate.com/stream/resumed":
        return {
          ...next,
          paused: false,
          pauseReason: null,
          availableTokens: state.burstCapacity,
          lastRefillAtMs: Date.parse(event.createdAt),
        };

      // events.iterate.com/stream/created will only ever happen once and is always the first event
      case "events.iterate.com/stream/created":
        next = spendCircuitBreakerToken({ state, event, next });
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
        next = spendCircuitBreakerToken({ state, event, next });
        return {
          ...next,
          config: {
            ...next.config,
            ...event.payload.config,
          },
        };

      case "events.iterate.com/stream/child-stream-created": {
        next = spendCircuitBreakerToken({ state, event, next });
        const childPath = getImmediateChildPath({
          parentPath: state.path,
          childPath: event.payload.childPath,
        });
        if (childPath === null || next.childPaths.includes(childPath)) return next;
        return {
          ...next,
          childPaths: [...next.childPaths, childPath],
        };
      }

      // events.iterate.com/stream/subscription-configured is used to configure outbound subscriptions
      case "events.iterate.com/stream/subscription-configured": {
        next = spendCircuitBreakerToken({ state, event, next });
        return {
          ...next,
          subscriptionsByKey: {
            ...next.subscriptionsByKey,
            [event.payload.subscriptionKey]: { latestConfiguredEvent: event },
          },
        };
      }

      case "events.iterate.com/stream/processor-registered":
        next = spendCircuitBreakerToken({ state, event, next });
        return {
          ...next,
          processorsBySlug: {
            ...next.processorsBySlug,
            [event.payload.slug]: { latestRegisteredEvent: event },
          },
        };

      default:
        return spendCircuitBreakerToken({ state, event, next });
    }
  },
});

export type CoreStreamState = z.infer<typeof coreStreamProcessorContract.stateSchema>;

export type SubscriptionConfiguredEvent =
  CoreStreamState["subscriptionsByKey"][string]["latestConfiguredEvent"];

export type ProcessorRegisteredEvent =
  CoreStreamState["processorsBySlug"][string]["latestRegisteredEvent"];

export function buildProcessorRegisteredEvent(args: {
  contract: {
    slug: string;
    version?: string;
    description: string;
    consumes: readonly string[];
    emits: readonly string[];
    events: Record<
      string,
      {
        description?: string;
        examples?: readonly unknown[];
      }
    >;
  };
}) {
  const version = args.contract.version ?? "0.0.0";
  return {
    type: "events.iterate.com/stream/processor-registered",
    idempotencyKey: `processor-registered:${args.contract.slug}:${version}`,
    payload: {
      slug: args.contract.slug,
      version,
      description: args.contract.description,
      consumes: [...args.contract.consumes],
      emits: [...args.contract.emits],
      ownedEvents: Object.entries(args.contract.events).map(([type, event]) => ({
        type,
        ...(event.description === undefined ? {} : { description: event.description }),
        ...(event.examples === undefined || event.examples.length === 0
          ? {}
          : { examples: [...event.examples] }),
      })),
    },
  } as const;
}

export function buildStreamErrorOccurredEvent(args: {
  message: string;
  error?: {
    name?: string;
    message: string;
    code?: string;
    stack?: string;
  };
  idempotencyKey?: string;
}) {
  return {
    type: "events.iterate.com/stream/error-occurred",
    ...(args.idempotencyKey === undefined ? {} : { idempotencyKey: args.idempotencyKey }),
    payload: {
      message: args.message,
      ...(args.error === undefined ? {} : { error: args.error }),
    },
  } as const;
}

export function buildChildStreamCreatedEvent(args: {
  parentPath: string;
  childPath: string;
}) {
  return {
    type: "events.iterate.com/stream/child-stream-created",
    idempotencyKey: `child-stream-created:${args.parentPath}:${args.childPath}`,
    payload: { childPath: args.childPath },
  } as const;
}

export function buildStreamPausedEvent(args: {
  reason: string;
  idempotencyKey?: string;
}) {
  return {
    type: "events.iterate.com/stream/paused",
    ...(args.idempotencyKey === undefined ? {} : { idempotencyKey: args.idempotencyKey }),
    payload: { reason: args.reason },
  } as const;
}

export function buildStreamResumedEvent(args: {
  reason?: string;
  idempotencyKey?: string;
}) {
  return {
    type: "events.iterate.com/stream/resumed",
    ...(args.idempotencyKey === undefined ? {} : { idempotencyKey: args.idempotencyKey }),
    payload: args.reason === undefined ? {} : { reason: args.reason },
  } as const;
}

export function assertCoreStreamAppendAllowed(args: {
  event: StreamEventInput;
  state: CoreStreamState;
}) {
  if (!args.state.paused) return;
  if (args.event.type === "events.iterate.com/stream/resumed") return;
  if (args.event.type === "events.iterate.com/stream/error-occurred") return;
  if (args.event.type === "events.iterate.com/stream/woken") return;
  throw new Error(`stream paused: ${args.state.pauseReason ?? "circuit breaker open"}`);
}

export function shouldPauseCoreStreamAfterAppend(state: CoreStreamState) {
  return !state.paused && state.availableTokens < 0;
}

function spendCircuitBreakerToken<State extends CircuitBreakerFields>(args: {
  state: CircuitBreakerFields;
  event: { createdAt: string };
  next: State;
}): State {
  const createdAtMs = Date.parse(args.event.createdAt);
  const refilled =
    args.state.lastRefillAtMs === null
      ? args.state.burstCapacity
      : Math.min(
          args.state.burstCapacity,
          args.state.availableTokens +
            (createdAtMs - args.state.lastRefillAtMs) *
              (args.state.refillRatePerMinute / 60_000),
        );

  return {
    ...args.next,
    availableTokens: refilled - 1,
    lastRefillAtMs: createdAtMs,
  };
}

export function getAncestorStreamPaths(path: string): string[] {
  if (path === "/") return [];
  const segments = path.split("/").filter(Boolean);
  const ancestors = ["/"];
  for (let index = 1; index < segments.length; index += 1) {
    ancestors.push(`/${segments.slice(0, index).join("/")}`);
  }
  return ancestors;
}

function getImmediateChildPath(args: {
  parentPath: string;
  childPath: string;
}): string | null {
  if (args.childPath === args.parentPath) return null;
  if (args.parentPath === "/") {
    const [firstSegment] = args.childPath.split("/").filter(Boolean);
    return firstSegment === undefined ? null : `/${firstSegment}`;
  }

  const parentPrefix = `${args.parentPath}/`;
  if (!args.childPath.startsWith(parentPrefix)) return null;
  const [firstSegment] = args.childPath.slice(parentPrefix.length).split("/").filter(Boolean);
  return firstSegment === undefined ? null : `${args.parentPath}/${firstSegment}`;
}
