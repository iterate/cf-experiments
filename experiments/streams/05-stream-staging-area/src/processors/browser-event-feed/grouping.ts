// Pure grouping logic for the "browser-event-feed" processor.
//
// The Event feed reframes the raw stream as a stream of UI components. Each event
// either has a SPECIFIC RENDERER (a dedicated component for its type) — then it is
// written as its own feed_items row and closes any open group — or it has none, in
// which case it is folded into the current open "group" row when the type matches.
// A new type always starts a fresh group row.
//
// This is deliberately a pure function of (state, events): the reducer uses it to
// advance state, and afterAppendBatch re-folds it over the same batch to derive the
// exact SQLite ops. Same input => same ops => idempotent replay.

import type { StreamEvent } from "@cf-experiments/shared/event";

/** Maps an event type to its specific renderer component, or null to fall into the group. */
export function componentForEventType(type: string): string | null {
  switch (type) {
    case "events.iterate.com/stream/created":
      return "stream.created";
    case "events.iterate.com/stream/woken":
      return "stream.woken";
    default:
      return null;
  }
}

/** Component name used for the catch-all group row. */
export const GROUP_COMPONENT = "group";

export type OpenGroup = {
  localIndex: number;
  firstOffset: number;
  lastOffset: number;
  eventCount: number;
  eventType: string;
};

export type FeedState = {
  /** The current open, extendable group row, or null when the last row is a singleton. */
  open: OpenGroup | null;
  /** Dense, monotonically increasing next feed_items local_index. */
  nextLocalIndex: number;
};

export const INITIAL_FEED_STATE: FeedState = { open: null, nextLocalIndex: 0 };

export type FeedOp =
  | {
      kind: "insert";
      localIndex: number;
      component: string;
      firstOffset: number;
      lastOffset: number;
      eventCount: number;
      data: unknown;
    }
  | {
      kind: "update";
      localIndex: number;
      lastOffset: number;
      eventCount: number;
      data: unknown;
    };

/**
 * Fold a batch of events into feed ops + the resulting state, starting from `start`.
 * The reducer calls this one event at a time; afterAppendBatch calls it with the whole
 * delivered batch to produce one transaction.
 */
export function planFeedOps(
  start: FeedState,
  events: readonly StreamEvent[],
): { ops: FeedOp[]; endState: FeedState } {
  let open = start.open;
  let nextLocalIndex = start.nextLocalIndex;
  const ops: FeedOp[] = [];

  for (const event of events) {
    const renderer = componentForEventType(event.type);

    if (renderer !== null) {
      // Specific renderer: its own singleton row, and it closes any open group.
      ops.push({
        kind: "insert",
        localIndex: nextLocalIndex,
        component: renderer,
        firstOffset: event.offset,
        lastOffset: event.offset,
        eventCount: 1,
        data: event.payload ?? {},
      });
      nextLocalIndex += 1;
      open = null;
      continue;
    }

    if (open !== null && open.eventType === event.type) {
      // Extend the open group for this event type.
      open = { ...open, lastOffset: event.offset, eventCount: open.eventCount + 1 };
      ops.push({
        kind: "update",
        localIndex: open.localIndex,
        lastOffset: open.lastOffset,
        eventCount: open.eventCount,
        data: { eventType: open.eventType },
      });
      continue;
    }

    // Start a new group (no open row, or the type changed).
    open = {
      localIndex: nextLocalIndex,
      firstOffset: event.offset,
      lastOffset: event.offset,
      eventCount: 1,
      eventType: event.type,
    };
    nextLocalIndex += 1;
    ops.push({
      kind: "insert",
      localIndex: open.localIndex,
      component: GROUP_COMPONENT,
      firstOffset: open.firstOffset,
      lastOffset: open.lastOffset,
      eventCount: open.eventCount,
      data: { eventType: event.type },
    });
  }

  return { ops, endState: { open, nextLocalIndex } };
}
