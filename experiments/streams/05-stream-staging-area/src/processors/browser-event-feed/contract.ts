// Defines the "browser-event-feed" processor contract.
// A browser-only reducing processor that consumes every stream event and folds
// consecutive events lacking a specific renderer into grouped feed_items rows.
// The reduced state is just the open, extendable group plus the dense row counter;
// the grouping itself lives in the pure planFeedOps helper so reduce and
// afterAppendBatch stay in lockstep.

import { z } from "zod";
import { defineProcessorContract } from "@cf-experiments/shared/stream-processors";
import { INITIAL_FEED_STATE, planFeedOps } from "./grouping.js";

export const browserEventFeedContract = defineProcessorContract({
  slug: "browser-event-feed",
  version: "0.1.0",
  description: "Groups consecutive stream events of the same type into the browser feed_items table.",
  stateSchema: z.object({
    open: z
      .object({
        localIndex: z.number().int().min(0),
        firstOffset: z.number().int().min(1),
        lastOffset: z.number().int().min(1),
        eventCount: z.number().int().min(1),
        eventType: z.string(),
      })
      .nullable(),
    nextLocalIndex: z.number().int().min(0),
  }),
  initialState: INITIAL_FEED_STATE,
  events: {},
  consumes: ["*"],
  emits: [],
  reduce({ state, event }) {
    return planFeedOps(state, [event]).endState;
  },
});
