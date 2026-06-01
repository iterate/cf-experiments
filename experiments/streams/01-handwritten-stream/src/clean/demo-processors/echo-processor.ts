import { z } from "zod";
import type { SimpleStreamProcessor } from "@cf-experiments/shared/simple-stream-processor";

export const echoProcessor = {
  stateSchema: z.object({
    seen: z.number().default(0),
  }),
  initialState: {},
  reduce({ state, event }) {
    if (event.type !== "test.processor.input") return state;
    return { seen: state.seen + 1 };
  },
  afterAppend({ event, state, append }) {
    if (event.type !== "test.processor.input") return;
    append({
      type: "test.processor.output",
      payload: { seen: state.seen },
    });
  },
} satisfies SimpleStreamProcessor<{ seen: number }, { env: Env }>;
