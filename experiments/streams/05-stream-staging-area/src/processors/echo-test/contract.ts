// Defines the "echo-test" processor contract.
// This test processor proves that one processor implementation can run in
// Node/Vitest, browser, and the StreamProcessorRunner Durable Object.

import { z } from "zod";
import { defineProcessorContract } from "@cf-experiments/shared/stream-processors";

export const echoTestProcessorContract = defineProcessorContract({
  slug: "echo-test",
  version: "0.1.0",
  description: "Counts inputs and echoes each back as an output carrying the running count.",
  stateSchema: z.object({ seen: z.number().int().min(0).default(0) }),
  initialState: {},
  events: {
    "test.processor.input": { description: "Input to echo.", payloadSchema: z.unknown() },
    "test.processor.output": {
      description: "Echoed output carrying the running input count.",
      payloadSchema: z.object({ seen: z.number() }),
    },
  },
  consumes: ["test.processor.input"],
  emits: ["test.processor.output"],
  reduce({ state, event }) {
    return event.type === "test.processor.input" ? { seen: state.seen + 1 } : state;
  },
});

export type EchoTestState = z.infer<typeof echoTestProcessorContract.stateSchema>;
