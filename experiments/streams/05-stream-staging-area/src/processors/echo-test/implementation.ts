// Implements the "echo-test" processor.
// It appends a test output for every test input and is intentionally tiny so
// runtime tests can focus on subscription/runner behavior instead of business logic.

import { implementProcessor } from "../../processor.js";
import { buildProcessorRegisteredEvent } from "../../core-stream-processor.js";
import { echoTestProcessorContract } from "./contract.js";

export const echoTestProcessor = implementProcessor(echoTestProcessorContract, () => ({
  afterAppend({ event, state, stream, shouldApplySideEffects, keepAlive }) {
    if (!shouldApplySideEffects({ event })) return;
    if (!state.hasRegisteredCurrentVersion) {
      keepAlive(stream.append({ event: buildProcessorRegisteredEvent({ contract: echoTestProcessorContract }) }));
    }
    if (event.type !== "test.processor.input") return;
    keepAlive(stream.append({ event: { type: "test.processor.output", payload: { seen: state.seen } } }));
  },
}));
