import type { z } from "zod";
import type { StreamEvent, StreamEventInput } from "./event.js";

export type SimpleStreamProcessorSnapshot<State> = {
  state: State;
  /** Highest stream offset fully handled by this processor. */
  offset: number;
};

export type SimpleStreamProcessor<State, Deps = undefined> = {
  stateSchema: z.ZodType<State>;
  initialState?: unknown;
  reduce?(args: {
    state: State;
    event: StreamEvent;
  }): State;
  afterAppend?(args: {
    event: StreamEvent;
    previousState: State;
    state: State;
    deps: Deps;
    append(event: StreamEventInput): void;
    appendAndWait(event: StreamEventInput): Promise<StreamEvent>;
    /**
     * Takes a callback, not a promise, so the runner can reject attempts to
     * start blocking work after `afterAppend` has returned. Late async work can
     * use `waitUntil`, but it cannot retroactively pause event processing.
     */
    blockProcessorUntil(work: () => Promise<unknown>): void;
    waitUntil(promise: Promise<unknown>): void;
  }): void;
};

export async function createSimpleStreamProcessorRunner<State, Deps = undefined>(args: {
  processor: SimpleStreamProcessor<State, Deps>;
  deps: Deps;
  append(event: StreamEventInput): void;
  appendAndWait(event: StreamEventInput): Promise<StreamEvent>;
  loadSnapshot?(): Promise<SimpleStreamProcessorSnapshot<State> | undefined> | SimpleStreamProcessorSnapshot<State> | undefined;
  saveSnapshot?(snapshot: SimpleStreamProcessorSnapshot<State>): Promise<void> | void;
  signal?: AbortSignal;
  waitUntil?(promise: Promise<unknown>): void;
}) {
  let snapshot =
    (await args.loadSnapshot?.()) ?? {
      state: args.processor.stateSchema.parse(args.processor.initialState),
      offset: 0,
    };
  await args.saveSnapshot?.(snapshot);
  console.log("SimpleStreamProcessor runner started", { offset: snapshot.offset });

  const runner = {
    snapshot() {
      return snapshot;
    },
    async processEvent(event: StreamEvent) {
      if (args.signal?.aborted) {
        console.log("SimpleStreamProcessor ignoring event because signal is aborted", {
          eventOffset: event.offset,
          currentOffset: snapshot.offset,
        });
        return snapshot;
      }

      if (event.offset <= snapshot.offset) {
        console.warn("SimpleStreamProcessor received already-processed event", {
          eventOffset: event.offset,
          currentOffset: snapshot.offset,
          eventType: event.type,
        });
        return snapshot;
      }

      console.log("SimpleStreamProcessor processing event", {
        eventOffset: event.offset,
        currentOffset: snapshot.offset,
        eventType: event.type,
      });
      const previousState = snapshot.state;
      const reducedState =
        args.processor.reduce?.({ state: previousState, event }) ?? previousState;
      const state = args.processor.stateSchema.parse(reducedState);
      const blockers: Promise<unknown>[] = [];
      let acceptsProcessorBlockers = true;
      const waitUntil = (promise: Promise<unknown>) => {
        if (args.waitUntil === undefined) {
          void promise.catch(() => undefined);
          return;
        }
        args.waitUntil(promise);
      };

      try {
        args.processor.afterAppend?.({
          event,
          previousState,
          state,
          deps: args.deps,
          append: args.append,
          appendAndWait: args.appendAndWait,
          blockProcessorUntil(work) {
            if (!acceptsProcessorBlockers) {
              throw new Error(
                "blockProcessorUntil() can only be called synchronously during afterAppend(). " +
                  "Call it before afterAppend returns; use waitUntil() for detached background work.",
              );
            }
            const blocker = work();
            blockers.push(blocker);
            waitUntil(blocker);
            console.log("SimpleStreamProcessor registered processor blocker", {
              eventOffset: event.offset,
              eventType: event.type,
              blockerCount: blockers.length,
            });
          },
          waitUntil,
        });
      } finally {
        acceptsProcessorBlockers = false;
      }

      await Promise.all(blockers);

      snapshot = { state, offset: event.offset };
      await args.saveSnapshot?.(snapshot);
      console.log("SimpleStreamProcessor advanced offset", {
        offset: snapshot.offset,
        eventType: event.type,
      });
      return snapshot;
    },
    async run(events: AsyncIterable<StreamEvent>) {
      for await (const event of events) {
        if (args.signal?.aborted) {
          break;
        }
        await runner.processEvent(event);
      }
      return snapshot;
    },
  };

  return runner;
}
