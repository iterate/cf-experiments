// Defines the "raw-events-browser" processor contract.
// This browser-only processor consumes every stream event and mirrors the raw
// append log into the local OPFS SQLite database used by stream view components.

import { z } from "zod";
import { defineProcessorContract } from "@cf-experiments/shared/stream-processors";

export const rawEventsBrowserProcessorContract = defineProcessorContract({
  slug: "raw-events-browser",
  version: "0.1.0",
  description: "Mirrors raw stream events into the browser SQLite events table.",
  stateSchema: z.object({}),
  initialState: {},
  events: {},
  consumes: ["*"],
  emits: [],
});
