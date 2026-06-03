// Implements the "raw-events-browser" processor.
// The browser runtime supplies a buffered `storeEvent` dependency so this
// synchronous afterAppend hook can stay fire-and-forget while SQLite writes are
// coalesced outside the processor.

import type { StreamEvent } from "@cf-experiments/shared/event";
import { implementProcessor } from "../../processor.js";
import { rawEventsBrowserProcessorContract } from "./contract.js";

export const rawEventsBrowserProcessor = implementProcessor(
  rawEventsBrowserProcessorContract,
  (deps: { storeEvent(event: StreamEvent): void }) => ({
    afterAppend({ event }) {
      deps.storeEvent(event);
    },
  }),
);
