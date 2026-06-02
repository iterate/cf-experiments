// TRIAL — a clean, testable decomposition. Not wired into the worker.
// Run: pnpm typecheck  (look for errors in THIS file only)
//
// Three crisp roles, each independently testable:
//
//   1. Contract        pure data + pure reduce. Import anywhere. (unchanged)
//   2. Implementation  build(deps) -> { afterAppend }. The "runtime closure"
//                      is just `build`: it closes over clients DERIVED FROM
//                      deps. It never holds business state — that lives in the
//                      reduced `state`. afterAppend gets free type narrowing
//                      because it's an object-literal method in a generic fn.
//   3. Runner          connects an implementation to two small ports (storage
//                      + stream). One `consume()` path; catch-up is reduce-only,
//                      live also runs afterAppend. No closures over hidden state.

import { z } from "zod";
import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import {
  defineProcessorContract,
  getInitialProcessorState,
  runProcessorReduce,
  type ConsumedEvent,
  type EmittedInput,
  type EventCatalog,
  type ProcessorState,
} from "@cf-experiments/shared/stream-processors";

// ===========================================================================
// Shared shapes
// ===========================================================================

/** F-bounded so reduce narrows on the same contract; matches runProcessorReduce. */
type RunnableContract<Self> = {
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

/** Everything afterAppend can touch is an argument -> trivially testable. */
type AfterAppendArgs<Contract> = {
  event: ConsumedEvent<Contract>;
  previousState: ProcessorState<Contract>;
  state: ProcessorState<Contract>;
  append(event: EmittedInput<Contract>): void;
  appendAndWait(event: EmittedInput<Contract>): Promise<StreamEvent>;
  blockProcessorUntil(work: () => Promise<unknown>): void;
  waitUntil(promise: Promise<unknown>): void;
};

type ProcessorImplementation<Contract> = {
  afterAppend?(args: AfterAppendArgs<Contract>): void;
};

type Processor<Contract, Deps> = {
  contract: Contract;
  build(deps: Deps): ProcessorImplementation<Contract>;
};

/**
 * Bind an implementation to a contract. `build(deps)` is the only place runtime
 * clients are constructed; it closes over them. Object-literal `afterAppend`
 * passed through here gets contextual typing, so NO arg annotation is needed.
 */
function implementProcessor<Contract extends RunnableContract<Contract>, Deps = void>(
  contract: Contract,
  build: (deps: Deps) => ProcessorImplementation<Contract>,
): Processor<Contract, Deps> {
  return { contract, build };
}

// ===========================================================================
// Runner — two small ports, one consume path
// ===========================================================================

type Snapshot<State> = { state: State; offset: number };

type ProcessorStorage<State> = {
  load(): Promise<Snapshot<State> | undefined> | Snapshot<State> | undefined;
  save(snapshot: Snapshot<State>): Promise<void> | void;
};

// The stream is the only thing the runner appends through. No readHistory:
// the stream replays history through the SAME processEventBatch channel.
type StreamPort = {
  append(event: StreamEventInput): void;
  appendAndWait(event: StreamEventInput): Promise<StreamEvent>;
};

async function createProcessorRunner<Contract extends RunnableContract<Contract>, Deps>(args: {
  processor: Processor<Contract, Deps>;
  deps: Deps;
  storage: ProcessorStorage<ProcessorState<Contract>>;
  stream: StreamPort;
  waitUntil?(promise: Promise<unknown>): void;
}) {
  const { contract } = args.processor;
  const implementation = args.processor.build(args.deps);
  let snapshot: Snapshot<ProcessorState<Contract>> = (await args.storage.load()) ?? {
    state: getInitialProcessorState(contract),
    offset: -1,
  };
  // Batches arrive faster than we can process them (processEventBatch is not
  // awaited by the stream). Serialize so events apply in offset order.
  let tail = Promise.resolve();

  async function consume(event: StreamEvent) {
    if (event.offset <= snapshot.offset) return; // idempotent resume / dedup
    const previousState = snapshot.state;
    const reduction = runProcessorReduce({ processor: { contract }, event, state: previousState });
    const state = reduction?.state ?? previousState;

    if (reduction !== undefined) {
      const blockers: Promise<unknown>[] = [];
      let acceptsBlockers = true;
      implementation.afterAppend?.({
        event: reduction.event,
        previousState,
        state,
        append: (e) => args.stream.append(e),
        appendAndWait: (e) => args.stream.appendAndWait(e),
        blockProcessorUntil: (work) => {
          if (!acceptsBlockers) throw new Error("blockProcessorUntil must be synchronous");
          const blocker = work();
          blockers.push(blocker);
          args.waitUntil?.(blocker);
        },
        waitUntil: (promise) => args.waitUntil?.(promise),
      });
      acceptsBlockers = false;
      await Promise.all(blockers);
    }

    snapshot = { state, offset: event.offset };
    await args.storage.save(snapshot);
  }

  return {
    snapshot: () => snapshot,
    /** Offset to hand to stream.subscribe() so the stream resumes correctly. */
    afterOffset: () => snapshot.offset,
    /** The subscription sink. The stream calls this for both replay and live. */
    processEventBatch({ events }: { events: StreamEvent[] }) {
      tail = tail.then(async () => {
        for (const event of events) await consume(event);
      });
      return tail;
    },
  };
}

// ===========================================================================
// Example processors
// ===========================================================================

const echoContract = defineProcessorContract({
  slug: "trial.echo",
  version: "0.1.0",
  description: "Echoes input events back as output events.",
  stateSchema: z.object({ seen: z.number().int().min(0).default(0) }),
  initialState: {},
  events: {
    "test.processor.input": { description: "in", payloadSchema: z.object({ path: z.string() }) },
    "test.processor.output": { description: "out", payloadSchema: z.object({ seen: z.number() }) },
  },
  consumes: ["test.processor.input"],
  emits: ["test.processor.output"],
  reduce({ state, event }) {
    if (event.type !== "test.processor.input") return state;
    return { seen: state.seen + 1 };
  },
});

// No deps, no runtime state: build() takes nothing, afterAppend is 2 lines.
const echo = implementProcessor(echoContract, () => ({
  afterAppend({ event, state, append }) {
    if (event.type !== "test.processor.input") return;
    const _path: string = event.payload.path; // proves narrowing
    void _path;
    append({ type: "test.processor.output", payload: { seen: state.seen } });
  },
}));

const transcribeContract = defineProcessorContract({
  slug: "trial.transcribe",
  version: "0.1.0",
  description: "Transcribes uploaded audio.",
  stateSchema: z.object({ transcripts: z.number().int().min(0).default(0) }),
  initialState: {},
  events: {
    "trial.audio-uploaded": { description: "in", payloadSchema: z.object({ url: z.string() }) },
    "trial.transcript-ready": {
      description: "out",
      payloadSchema: z.object({ url: z.string(), text: z.string() }),
    },
  },
  consumes: ["trial.audio-uploaded"],
  emits: ["trial.transcript-ready"],
  reduce({ state, event }) {
    if (event.type !== "trial.audio-uploaded") return state;
    return { transcripts: state.transcripts + 1 };
  },
});

type TranscribeDeps = { transcribe(url: string): Promise<string> };

// Runtime "closure": the client is built from deps inside build(). It is a
// cache/connection, NOT business state. Testable by passing a fake `deps`.
const transcribe = implementProcessor(transcribeContract, (deps: TranscribeDeps) => ({
  afterAppend({ event, append, blockProcessorUntil }) {
    if (event.type !== "trial.audio-uploaded") return;
    const url = event.payload.url; // narrows to { url: string }
    blockProcessorUntil(async () => {
      append({ type: "trial.transcript-ready", payload: { url, text: await deps.transcribe(url) } });
    });
  },
}));

// ===========================================================================
// What the tests look like
// ===========================================================================

// (1) Unit-test afterAppend in ISOLATION — no runner, no storage, no stream.
export async function exampleAfterAppendUnitTest() {
  const appended: StreamEventInput[] = [];
  const impl = transcribe.build({ transcribe: async (url) => `transcript:${url}` });
  const blockers: Promise<unknown>[] = [];

  impl.afterAppend?.({
    event: { type: "trial.audio-uploaded", payload: { url: "/a.wav" }, offset: 3, createdAt: "t" },
    previousState: { transcripts: 0 },
    state: { transcripts: 1 },
    append: (e) => appended.push(e),
    appendAndWait: async (e) => ({ ...e, offset: 0, createdAt: "t" }),
    blockProcessorUntil: (work) => blockers.push(work()),
    waitUntil: () => {},
  });
  await Promise.all(blockers);
  return appended; // assert: [{ type: "trial.transcript-ready", payload: { url: "/a.wav", text: "transcript:/a.wav" } }]
}

// (2) Integration-test the runner with in-memory ports.
export async function exampleRunnerTest() {
  const appended: StreamEventInput[] = [];
  let saved: Snapshot<{ seen: number }> | undefined;

  const runner = await createProcessorRunner({
    processor: echo,
    deps: undefined,
    storage: { load: () => saved, save: (s) => void (saved = s) },
    stream: {
      append: (e) => void appended.push(e),
      appendAndWait: async (e) => ({ ...e, offset: 0, createdAt: "t" }),
    },
  });

  // Exactly what the stream DO does to the sink, including replay-as-a-batch.
  await runner.processEventBatch({
    events: [{ type: "test.processor.input", payload: { path: "/x" }, offset: 2, createdAt: "t" }],
  });
  return { appended, snapshot: runner.snapshot() };
}
