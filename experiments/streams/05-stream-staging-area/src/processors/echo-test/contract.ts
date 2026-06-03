// Defines the "echo-test" processor contract.
// This test processor proves that one processor implementation can run in
// Node/Vitest, browser, and the StreamProcessorRunner Durable Object.

import { z } from "zod";
import { defineProcessorContract } from "@cf-experiments/shared/stream-processors";
import { coreStreamProcessorContract } from "../../core-stream-processor.js";

export const echoTestProcessorContract = defineProcessorContract({
  slug: "echo-test",
  version: "0.1.0",
  description: "Counts inputs and echoes each back as an output carrying the running count.",
  stateSchema: z.object({
    seen: z.number().int().min(0).default(0),
    hasRegisteredCurrentVersion: z.boolean().default(false),
  }),
  initialState: {},
  processorDeps: [coreStreamProcessorContract],
  events: {
    "test.processor.input": { description: "Input to echo.", payloadSchema: z.unknown() },
    "test.processor.output": {
      description: "Echoed output carrying the running input count.",
      payloadSchema: z.object({ seen: z.number() }),
    },
  },
  consumes: ["events.iterate.com/stream/processor-registered", "test.processor.input"],
  emits: ["events.iterate.com/stream/processor-registered", "test.processor.output"],
  reduce({ state, event }) {
    if (
      event.type === "events.iterate.com/stream/processor-registered" &&
      event.payload.slug === "echo-test" &&
      event.payload.version === "0.1.0"
    ) {
      return { ...state, hasRegisteredCurrentVersion: true };
    }
    return event.type === "test.processor.input" ? { ...state, seen: state.seen + 1 } : state;
  },
});

export type EchoTestState = z.infer<typeof echoTestProcessorContract.stateSchema>;
