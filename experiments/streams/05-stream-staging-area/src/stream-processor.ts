// Independent, runtime-agnostic stream-processor implementation.
//
// One processor model, one runner, three runtimes (Node/vitest, browser,
// Cloudflare DO) — runtimes differ only in the two ports the runner is given
// (storage + stream) plus how the subscription socket is opened.
//
//   1. Contract        pure data + pure reduce (defineProcessorContract, shared).
//   2. Implementation  build(deps) -> { afterAppend }. afterAppend is a per-event
//                      switch; side effects are fire-and-forget by default, with
//                      blockProcessorUntil for durable at-least-once processing.
//   3. Runner          createProcessorRunner(...) IS the subscription sink
//                      (processEventBatch). Builtin processors additionally get a
//                      pre-commit beforeAppend gate and run inline in the Stream.

import { RpcTarget, type RpcStub } from "capnweb";
import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import {
  getInitialProcessorState,
  runProcessorReduce,
  type ConsumedEvent,
  type EmittedInput,
  type EventCatalog,
  type ProcessorState,
} from "@cf-experiments/shared/stream-processors";
import type { z } from "zod";
import type { StreamRpc, SubscriptionSink } from "./stream-types.js";

// ===========================================================================
// Contract shape
// ===========================================================================

/** F-bounded so reduce narrows on the same contract; matches runProcessorReduce. */
export type RunnableContract<Self> = {
  slug: string;
  stateSchema: z.ZodType;
  initialState?: unknown;
  events: EventCatalog;
  processorDeps?: readonly unknown[];
  consumes: readonly string[];
  emits: readonly string[];
  reduce?: (args: {
    contract: Self;
    state: ProcessorState<Self>;
    event: ConsumedEvent<Self>;
  }) => ProcessorState<Self> | null | undefined;
};

// ===========================================================================
// Implementation shape
// ===========================================================================

export type ProcessorCapabilities<Contract> = {
  /** Fire-and-forget background append. The runner keeps it alive; does not block. */
  append(event: EmittedInput<Contract>): void;
  /** Awaitable append — use inside blockProcessorUntil/waitUntil when you need the result. */
  appendAndWait(event: EmittedInput<Contract>): Promise<StreamEvent>;
  /**
   * Durable opt-in: "do not checkpoint past this event until `work` completes."
   * Must be called synchronously during afterAppend. Crash before completion =>
   * the event is re-delivered and re-processed (at-least-once).
   */
  blockProcessorUntil(work: () => Promise<unknown>): void;
  /** Keep the runtime alive for detached work; does not gate the checkpoint. */
  waitUntil(promise: Promise<unknown>): void;
};

/**
 * The stream's high-water-mark as of this batch delivery — the only raw fact the
 * runner adds. Everything else is deducible: offset lag = head.offset - event.offset,
 * time lag = Date.parse(head.createdAt) - Date.parse(event.createdAt),
 * caughtUp = event.offset >= head.offset.
 */
export type StreamHead = { offset: number; createdAt: string };

/** Per-event hook. Everything is an argument -> trivially testable. */
export type AfterAppendArgs<Contract> = ProcessorCapabilities<Contract> & {
  event: ConsumedEvent<Contract>;
  previousState: ProcessorState<Contract>;
  state: ProcessorState<Contract>;
  head: StreamHead;
};

export type ProcessorImplementation<Contract> = {
  afterAppend?(args: AfterAppendArgs<Contract>): void;
};

/** Pre-commit gate args. The event has no offset/createdAt yet. */
export type BeforeAppendArgs<Contract> = {
  event: StreamEventInput;
  state: ProcessorState<Contract>;
};

/** Builtin (inline, in-Stream) implementation: adds the pre-commit gate. */
export type BuiltinImplementation<Contract> = ProcessorImplementation<Contract> & {
  beforeAppend?(args: BeforeAppendArgs<Contract>): void;
};

export type Processor<Contract, Deps> = {
  contract: Contract;
  build(deps: Deps): ProcessorImplementation<Contract>;
};

export type BuiltinProcessor<Contract, Deps> = {
  contract: Contract;
  build(deps: Deps): BuiltinImplementation<Contract>;
};

/**
 * Bind an implementation to a contract. Object-literal `afterAppend` passed
 * through here gets contextual typing, so the override needs NO arg annotation.
 * `build(deps)` is the only place runtime clients are constructed.
 */
export function implementProcessor<Contract extends RunnableContract<Contract>, Deps = void>(
  contract: Contract,
  build: (deps: Deps) => ProcessorImplementation<Contract>,
): Processor<Contract, Deps> {
  return { contract, build };
}

export function implementBuiltinProcessor<Contract extends RunnableContract<Contract>, Deps = void>(
  contract: Contract,
  build: (deps: Deps) => BuiltinImplementation<Contract>,
): BuiltinProcessor<Contract, Deps> {
  return { contract, build };
}

// ===========================================================================
// Runner — two ports, one process path, IS the subscription sink
// ===========================================================================

export type Snapshot<State> = { state: State; offset: number };

export type ProcessorStorage<State> = {
  load(): Promise<Snapshot<State> | undefined> | Snapshot<State> | undefined;
  save(snapshot: Snapshot<State>): Promise<void> | void;
};

/** The stream the runner appends through. No readHistory: the stream replays. */
export type StreamPort = {
  append(event: StreamEventInput): Promise<StreamEvent>;
};

/**
 * Reserved metadata key for the append->delivery round-trip metric. The runner
 * stamps a wall-clock send time on appends (only when instrumented), and reads it
 * back when the same event is delivered. No await on the append, no correlation map.
 */
export const APPENDED_AT_MS = "events.iterate.com/instrument/appended-at-ms";

export type DeliveredBatch = { events: StreamEvent[]; headOffset?: number; headCreatedAt?: string };

export function createProcessorRunner<Contract extends RunnableContract<Contract>, Deps>(args: {
  processor: Processor<Contract, Deps>;
  deps: Deps;
  storage: ProcessorStorage<ProcessorState<Contract>>;
  stream: StreamPort;
  waitUntil?(promise: Promise<unknown>): void;
  /** Opt-in: called when an event THIS runner appended is delivered back. */
  onAppendRoundTrip?(sample: { event: StreamEvent; ms: number }): void;
}) {
  const { contract } = args.processor;
  const implementation = args.processor.build(args.deps);
  let snapshot: Snapshot<ProcessorState<Contract>> | undefined;
  let tail = Promise.resolve();

  async function loadSnapshot() {
    if (snapshot === undefined) {
      snapshot = (await args.storage.load()) ?? {
        state: getInitialProcessorState(contract),
        offset: -1,
      };
    }
    return snapshot;
  }

  // Stamps the send time only when instrumented, so normal events stay clean.
  const sendAppend = (event: EmittedInput<Contract>) =>
    args.onAppendRoundTrip === undefined
      ? args.stream.append(event)
      : args.stream.append({ ...event, metadata: { ...event.metadata, [APPENDED_AT_MS]: Date.now() } });

  async function processBatch(events: StreamEvent[], head: StreamHead) {
    const current = await loadSnapshot();
    let advancedWithoutSave = false;

    for (const event of events) {
      if (event.offset <= current.offset) continue; // idempotent resume / dedup

      // Append->delivery round-trip: measured on receipt, no await on the append.
      const appendedAtMs = event.metadata?.[APPENDED_AT_MS];
      if (typeof appendedAtMs === "number") {
        args.onAppendRoundTrip?.({ event, ms: Date.now() - appendedAtMs });
      }

      const previousState = current.state;
      const reduction = runProcessorReduce({ processor: { contract }, event, state: previousState });
      const state = reduction?.state ?? previousState;

      const blockers: Promise<unknown>[] = [];
      if (reduction !== undefined && implementation.afterAppend !== undefined) {
        let acceptsBlockers = true;
        implementation.afterAppend({
          event: reduction.event,
          previousState,
          state,
          head,
          append: (e) => {
            const appended = sendAppend(e).then(
              () => undefined,
              (error: unknown) => console.error("append failed", error),
            );
            args.waitUntil?.(appended);
          },
          appendAndWait: (e) => sendAppend(e),
          blockProcessorUntil: (work) => {
            if (!acceptsBlockers) throw new Error("blockProcessorUntil must be synchronous");
            const blocker = work();
            blockers.push(blocker);
            args.waitUntil?.(blocker);
          },
          waitUntil: (promise) => args.waitUntil?.(promise),
        });
        acceptsBlockers = false;
      }

      snapshot = { state, offset: event.offset };
      advancedWithoutSave = true;

      if (blockers.length > 0) {
        // Durable: hold the checkpoint at this event until the work is done.
        await Promise.all(blockers);
        await args.storage.save(snapshot);
        advancedWithoutSave = false;
      }
    }

    // Fire-and-forget progress: one coalesced checkpoint per batch.
    if (advancedWithoutSave && snapshot !== undefined) await args.storage.save(snapshot);
  }

  return {
    async snapshot() {
      return loadSnapshot();
    },
    /** Offset to hand to stream.subscribe() so the stream resumes correctly. */
    async afterOffset() {
      return (await loadSnapshot()).offset;
    },
    /**
     * The subscription sink. The stream calls this for both replay and live and
     * piggybacks its current maxOffset as `headOffset` (the high-water-mark) so the
     * runner can report lag. Falls back to the batch's last offset if absent.
     */
    processEventBatch(batch: DeliveredBatch) {
      const last = batch.events.at(-1);
      const head: StreamHead = {
        offset: batch.headOffset ?? last?.offset ?? -1,
        createdAt: batch.headCreatedAt ?? last?.createdAt ?? new Date(0).toISOString(),
      };
      tail = tail.then(() => processBatch(batch.events, head));
      return tail;
    },
  };
}

export type ProcessorRunner = ReturnType<typeof createProcessorRunner>;

// ===========================================================================
// Subscription glue (inbound: browser / node / vitest)
// ===========================================================================

/** The capnweb sink: returns undefined to the stream (no ack traffic), kicks the runner. */
export class ProcessorSink extends RpcTarget implements SubscriptionSink {
  readonly #deliver: (args: DeliveredBatch) => unknown;
  constructor(deliver: (args: DeliveredBatch) => unknown) {
    super();
    this.#deliver = deliver;
  }
  processEventBatch(args: DeliveredBatch): undefined {
    void this.#deliver(args);
  }
}

export function streamPortFromRpc(rpc: RpcStub<StreamRpc>): StreamPort {
  return { append: (event) => rpc.append({ event }) };
}

/**
 * Inbound host. Builds the runner, wires its processEventBatch as the subscription
 * sink, hands over afterOffset(). The ONLY connection-specific glue; the runner
 * itself is identical to the DO.
 */
export async function withStreamProcessor<Contract extends RunnableContract<Contract>, Deps>(args: {
  connection: { rpc: RpcStub<StreamRpc> };
  subscriptionKey: string;
  processor: Processor<Contract, Deps>;
  deps: Deps;
  storage: ProcessorStorage<ProcessorState<Contract>>;
  onAppendRoundTrip?(sample: { event: StreamEvent; ms: number }): void;
}) {
  const runner = createProcessorRunner({
    processor: args.processor,
    deps: args.deps,
    storage: args.storage,
    stream: streamPortFromRpc(args.connection.rpc),
    onAppendRoundTrip: args.onAppendRoundTrip,
  });
  const sink = new ProcessorSink((batch) => runner.processEventBatch(batch));
  const handle = await args.connection.rpc.subscribe({
    subscriptionKey: args.subscriptionKey,
    sink,
    afterOffset: await runner.afterOffset(),
  });
  return {
    runner,
    async [Symbol.asyncDispose]() {
      await handle.unsubscribe();
    },
  };
}
