// Runtime-agnostic processor runner.
// It owns processor state/checkpointing and can run over any StreamSubscription,
// whether events arrive from browser/node inbound subscribe or from a Durable
// Object outbound requestSubscription handshake.

import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import {
  getInitialProcessorState,
  runProcessorReduce,
  type ProcessorState,
} from "@cf-experiments/shared/stream-processors";
import type { StreamSubscription } from "./subscription.js";
import type { Processor, RunnableContract } from "./processor.js";

export type Snapshot<State> = { state: State; offset: number };

export type ProcessorStorage<State> = {
  load(): Promise<Snapshot<State> | undefined> | Snapshot<State> | undefined;
  save(snapshot: Snapshot<State>): Promise<void> | void;
};

export type ProcessorStream = {
  append(args: { streamPath?: string; event: StreamEventInput }): unknown;
  appendBatch(args: { streamPath?: string; events: StreamEventInput[] }): unknown;
};

export function createProcessorRunner<Contract extends RunnableContract<Contract>, Deps>(args: {
  processor: Processor<Contract, Deps>;
  deps: Deps;
  storage?: ProcessorStorage<ProcessorState<Contract>>;
  stream: ProcessorStream;
}) {
  const { contract } = args.processor;
  const implementation = args.processor.build(args.deps);
  let loaded = false;
  let snapshot: Snapshot<ProcessorState<Contract>> | undefined = undefined;
  let state = getInitialProcessorState(contract);
  const keptAlive = new Set<Promise<unknown>>();

  function keepAlive(work: unknown) {
    if (work === undefined || work === null || typeof (work as Promise<unknown>).then !== "function") {
      return;
    }
    const promise = work as Promise<unknown>;
    keptAlive.add(promise);
    promise.finally(() => keptAlive.delete(promise)).catch((error: unknown) => {
      console.error("processor keepAlive promise failed", error);
    });
  }

  async function loadSnapshot() {
    if (!loaded) {
      snapshot = await args.storage?.load();
      if (snapshot !== undefined) state = snapshot.state;
      loaded = true;
    }
    return snapshot;
  }

  async function saveSnapshot(nextSnapshot: Snapshot<ProcessorState<Contract>>) {
    snapshot = nextSnapshot;
    await args.storage?.save(nextSnapshot);
  }

  async function processEvent(argsForEvent: { event: StreamEvent; streamMaxOffset: number }) {
    await loadSnapshot();
    if (argsForEvent.event.offset <= (snapshot?.offset ?? -1)) return;

    const previousState = state;
    const reduction = runProcessorReduce({
      processor: { contract },
      event: argsForEvent.event,
      state: previousState,
    });
    const nextState = reduction?.state ?? previousState;

    const blockers: Promise<unknown>[] = [];
    if (reduction !== undefined && implementation.afterAppend !== undefined) {
      let acceptsBlockers = true;
      implementation.afterAppend({
        event: reduction.event,
        previousState,
        state: nextState,
        streamMaxOffset: argsForEvent.streamMaxOffset,
        stream: args.stream,
        blockProcessorUntil: (work) => {
          if (!acceptsBlockers) throw new Error("blockProcessorUntil must be synchronous");
          const blocker = work();
          blockers.push(blocker);
          keepAlive(blocker);
        },
        keepAlive,
      });
      acceptsBlockers = false;
    }

    state = nextState;
    const nextSnapshot = { state, offset: argsForEvent.event.offset };
    if (blockers.length > 0) await Promise.all(blockers);
    await saveSnapshot(nextSnapshot);
  }

  return {
    async snapshot() {
      return loadSnapshot();
    },
    processEvent,
    run(argsForRun: { subscription: StreamSubscription }) {
      let stopped = false;
      const processing = (async () => {
        for await (const event of argsForRun.subscription) {
          if (stopped) break;
          const streamMaxOffset = argsForRun.subscription.streamMaxOffset;
          if (streamMaxOffset === undefined) {
            throw new Error("subscription yielded an event before streamMaxOffset was known");
          }
          await processEvent({ event, streamMaxOffset });
        }
      })();

      return {
        async [Symbol.asyncDispose]() {
          stopped = true;
          await argsForRun.subscription[Symbol.asyncDispose]();
          await processing;
        },
      };
    },
  };
}

export type ProcessorRunner = ReturnType<typeof createProcessorRunner>;
