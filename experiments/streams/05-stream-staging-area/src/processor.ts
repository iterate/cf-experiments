// Processor authoring API.
// Contracts are pure data/reducer definitions; implementations bind a contract
// to synchronous afterAppend side effects and optional built-in beforeAppend gates.

import type { StreamEventInput } from "@cf-experiments/shared/event";
import type {
  ConsumedEvent,
  EventCatalog,
  ProcessorState,
} from "@cf-experiments/shared/stream-processors";
import type { z } from "zod";
import type { ProcessorStream } from "./processor-runner.js";

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

export type ProcessorCapabilities<Contract> = {
  /** The exact stream RPC API this processor is running against. */
  stream: ProcessorStream;
  /**
   * Durable opt-in: "do not checkpoint past this event until `work` completes."
   * Must be called synchronously during afterAppend. Crash before completion =>
   * the event is re-delivered and re-processed (at-least-once).
   */
  blockProcessorUntil(work: () => Promise<unknown>): void;
  /**
   * Track detached work without making it part of the checkpoint. In the Durable
   * Object runner this should eventually be backed by alarms; for now it keeps a
   * local reference and reports failures.
   */
  keepAlive(work: unknown): void;
};

/** Per-event hook. Everything is an argument -> trivially testable. */
export type AfterAppendArgs<Contract> = ProcessorCapabilities<Contract> & {
  event: ConsumedEvent<Contract>;
  previousState: ProcessorState<Contract>;
  state: ProcessorState<Contract>;
  streamMaxOffset: number;
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
