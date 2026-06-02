// ONE stream processor definition, imported unchanged by every host:
//   - the StreamProcessorRunner DO (outbound capnweb)
//   - the Node/vitest e2e (inbound capnweb)
//   - a browser tab (inbound capnweb)
//
// Proves "same processor + same runner code, three runtimes."

import { z } from "zod";
import { defineProcessorContract } from "@cf-experiments/shared/stream-processors";
import { implementProcessor } from "./stream-processor.js";

export const echoContract = defineProcessorContract({
  slug: "echo",
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

export type EchoState = z.infer<typeof echoContract.stateSchema>;

export const echo = implementProcessor(echoContract, () => ({
  afterAppend({ event, state, append }) {
    if (event.type !== "test.processor.input") return;
    append({ type: "test.processor.output", payload: { seen: state.seen } });
  },
}));
