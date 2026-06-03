import { expectTypeOf } from "expect-type";
import { z } from "zod";
import type { StreamEvent } from "./event.js";
import { defineProcessorContract, type ConsumedEvent } from "./stream-processors.js";

const localEvents = {
  "test/a": {
    description: "A",
    payloadSchema: z.object({ a: z.string() }),
  },
  "test/b": {
    description: "B",
    payloadSchema: z.object({ b: z.number() }),
  },
};

const noWildcard = defineProcessorContract({
  slug: "no-wildcard",
  version: "0.1.0",
  description: "Consumes explicitly declared events.",
  stateSchema: z.object({}),
  initialState: {},
  events: localEvents,
  consumes: ["test/a", "test/b"],
  emits: [],
});

type NoWildcardEvent = ConsumedEvent<typeof noWildcard>;

expectTypeOf<NoWildcardEvent["type"]>().toEqualTypeOf<"test/a" | "test/b">();

function expectNoWildcardNarrowing(event: NoWildcardEvent) {
  if (event.type === "test/a") {
    expectTypeOf(event.payload).toEqualTypeOf<{ a: string }>();
  } else {
    expectTypeOf(event.payload).toEqualTypeOf<{ b: number }>();
  }
}

const pureWildcard = defineProcessorContract({
  slug: "pure-wildcard",
  version: "0.1.0",
  description: "Consumes every stream event without a local event catalog.",
  stateSchema: z.object({}),
  initialState: {},
  events: {},
  consumes: ["*"],
  emits: [],
});

type PureWildcardEvent = ConsumedEvent<typeof pureWildcard>;

expectTypeOf<PureWildcardEvent>().toEqualTypeOf<StreamEvent>();
expectTypeOf<PureWildcardEvent["type"]>().toEqualTypeOf<string>();
expectTypeOf<PureWildcardEvent["payload"]>().toEqualTypeOf<unknown | undefined>();

const wildcardWithExplicitEvents = defineProcessorContract({
  slug: "wildcard-with-explicit-events",
  version: "0.1.0",
  description: "Consumes everything at runtime while retaining typed branches for known events.",
  stateSchema: z.object({}),
  initialState: {},
  events: localEvents,
  consumes: ["*", "test/a", "test/b"],
  emits: [],
});

type WildcardWithExplicitEvent = ConsumedEvent<typeof wildcardWithExplicitEvents>;

expectTypeOf<WildcardWithExplicitEvent["type"]>().toEqualTypeOf<"test/a" | "test/b">();

function expectWildcardWithExplicitNarrowing(event: WildcardWithExplicitEvent) {
  if (event.type === "test/a") {
    expectTypeOf(event.payload).toEqualTypeOf<{ a: string }>();
  } else {
    expectTypeOf(event.payload).toEqualTypeOf<{ b: number }>();
  }
}

void expectNoWildcardNarrowing;
void expectWildcardWithExplicitNarrowing;
