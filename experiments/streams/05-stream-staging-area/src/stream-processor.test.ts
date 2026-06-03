// Runnable in the Node/vitest runtime, fully in-process (no worker needed).
// Proves the processor model: per-event afterAppend, durable blockProcessorUntil,
// the builtin beforeAppend gate folded into a core processor (child-stream
// topology + circuit breaker), and the SQLite-projector shape.

import { describe, expect, it } from "vitest";
import { z } from "zod";
import { defineProcessorContract, getInitialProcessorState, runProcessorReduce } from "@cf-experiments/shared/stream-processors";
import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import {
  implementBuiltinProcessor,
  implementProcessor,
} from "./processor.js";
import { createProcessorRunner, type Snapshot } from "./processor-runner.js";
import type { StreamRpc } from "./types.js";

const iso = (ms = 0) => new Date(ms).toISOString();

function event(args: {
  type: string;
  payload?: unknown;
  offset: number;
  createdAtMs?: number;
  idempotencyKey?: string;
}): StreamEvent {
  return {
    type: args.type,
    payload: args.payload,
    offset: args.offset,
    createdAt: iso(args.createdAtMs),
    ...(args.idempotencyKey === undefined ? {} : { idempotencyKey: args.idempotencyKey }),
  };
}

// A stream stub that commits appends in memory and (optionally) fans them back.
function memoryStream(options: { onCommit?: (e: StreamEvent) => void; startOffset?: number } = {}) {
  let nextOffset = options.startOffset ?? 100;
  const committed: StreamEvent[] = [];
  const stream: StreamRpc = {
    append: (args) => {
      const e: StreamEvent = { ...args.event, offset: nextOffset++, createdAt: iso(1) };
      committed.push(e);
      options.onCommit?.(e);
      return e;
    },
    appendBatch: (batch) =>
      batch.events.map((input) => {
        const e: StreamEvent = { ...input, offset: nextOffset++, createdAt: iso(1) };
        committed.push(e);
        options.onCommit?.(e);
        return e;
      }),
    getEvent: () => undefined,
    getEvents: () => [],
    subscribe: () => {
      throw new Error("memoryStream does not implement subscribe");
    },
    runtimeState: () => {
      throw new Error("memoryStream does not implement runtimeState");
    },
    kill: () => {},
    reset: async () => {},
    reduce: () => {
      throw new Error("memoryStream does not implement reduce");
    },
  };
  return { stream, committed };
}

// ---------------------------------------------------------------------------
// echo — default fire-and-forget pattern
// ---------------------------------------------------------------------------

const echoContract = defineProcessorContract({
  slug: "test.echo",
  version: "0.1.0",
  description: "echo",
  stateSchema: z.object({ seen: z.number().int().min(0).default(0) }),
  initialState: {},
  events: {
    "test.input": { description: "in", payloadSchema: z.object({ path: z.string() }) },
    "test.output": { description: "out", payloadSchema: z.object({ seen: z.number() }) },
  },
  consumes: ["test.input"],
  emits: ["test.output"],
  reduce({ state, event }) {
    return event.type === "test.input" ? { seen: state.seen + 1 } : state;
  },
});

const echo = implementProcessor(echoContract, () => ({
  afterAppend({ event, state, stream, keepAlive }) {
    if (event.type !== "test.input") return;
    keepAlive(stream.append({ event: { type: "test.output", payload: { seen: state.seen } } }));
  },
}));

describe("subscription processor (node, in-process)", () => {
  it("echo reduces, emits, and advances the snapshot", async () => {
    const { stream, committed } = memoryStream();
    let saved: Snapshot<{ seen: number }> | undefined;
    const runner = createProcessorRunner({
      processor: echo,
      deps: undefined,
      storage: { load: () => saved, save: (s) => void (saved = s) },
      stream,
    });

    await runner.processEvent({
      event: event({ type: "test.input", payload: { path: "/x" }, offset: 2 }),
      streamMaxOffset: 2,
    });

    expect(committed).toMatchObject([{ type: "test.output", payload: { seen: 1 } }]);
    expect((await runner.snapshot())?.offset).toBe(2);
    expect(saved?.offset).toBe(2);
  });

  it("dedups already-processed offsets on replay", async () => {
    const { stream, committed } = memoryStream();
    const runner = createProcessorRunner({
      processor: echo,
      deps: undefined,
      storage: { load: () => ({ state: { seen: 0 }, offset: 5 }), save: () => {} },
      stream,
    });
    await runner.processEvent({
      event: event({ type: "test.input", payload: { path: "/x" }, offset: 3 }),
      streamMaxOffset: 5,
    });
    expect(committed).toHaveLength(0); // offset 3 <= snapshot 5
  });
});

// ---------------------------------------------------------------------------
// transcribe — durable blockProcessorUntil (at-least-once)
// ---------------------------------------------------------------------------

const transcribeContract = defineProcessorContract({
  slug: "test.transcribe",
  version: "0.1.0",
  description: "transcribe",
  stateSchema: z.object({ done: z.number().int().min(0).default(0) }),
  initialState: {},
  events: {
    "test.audio": { description: "in", payloadSchema: z.object({ url: z.string() }) },
    "test.transcript": { description: "out", payloadSchema: z.object({ url: z.string(), text: z.string() }) },
  },
  consumes: ["test.audio"],
  emits: ["test.transcript"],
  reduce({ state, event }) {
    return event.type === "test.audio" ? { done: state.done + 1 } : state;
  },
});

const transcribe = implementProcessor(transcribeContract, (deps: { transcribe(url: string): Promise<string> }) => ({
  afterAppend({ event, stream, blockProcessorUntil }) {
    if (event.type !== "test.audio") return;
    const url = event.payload.url;
    blockProcessorUntil(async () => {
      const text = await deps.transcribe(url);
      await stream.append({ event: { type: "test.transcript", payload: { url, text } } });
    });
  },
}));

describe("durable processor (blockProcessorUntil)", () => {
  it("holds the checkpoint until the side effect completes", async () => {
    const { stream, committed } = memoryStream();
    const saves: number[] = [];
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));

    const runner = createProcessorRunner({
      processor: transcribe,
      deps: {
        transcribe: async (url) => {
          await gate;
          return `transcript:${url}`;
        },
      },
      storage: { load: () => undefined, save: (s) => void saves.push(s.offset) },
      stream,
    });

    const processed = runner.processEvent({
      event: event({ type: "test.audio", payload: { url: "/a" }, offset: 2 }),
      streamMaxOffset: 2,
    });
    await new Promise((r) => setTimeout(r, 0));
    expect(saves).toEqual([]); // blocked: not yet checkpointed

    release();
    await processed;
    expect(committed).toMatchObject([{ type: "test.transcript", payload: { url: "/a", text: "transcript:/a" } }]);
    expect(saves).toContain(2); // checkpointed only after the work completed
  });
});

// ---------------------------------------------------------------------------
// SQLite projector shape — fire-and-forget bulk write
// ---------------------------------------------------------------------------

const projectorContract = defineProcessorContract({
  slug: "test.sqlite-projector",
  version: "0.1.0",
  description: "project",
  stateSchema: z.object({}),
  initialState: {},
  events: {},
  consumes: ["*"],
  emits: [],
});

describe("projector processor (consumes everything, writes to a db port)", () => {
  it("writes every delivered event", async () => {
    const written: number[] = [];
    const projector = implementProcessor(projectorContract, (deps: { write(e: StreamEvent): void }) => ({
      afterAppend({ event }) {
        deps.write(event);
      },
    }));
    const { stream } = memoryStream();
    const runner = createProcessorRunner({
      processor: projector,
      deps: { write: (e) => written.push(e.offset) },
      storage: { load: () => undefined, save: () => {} },
      stream,
    });
    await runner.processEvent({ event: event({ type: "a", offset: 0, payload: {} }), streamMaxOffset: 1 });
    await runner.processEvent({ event: event({ type: "b", offset: 1, payload: {} }), streamMaxOffset: 1 });
    expect(written).toEqual([0, 1]);
  });
});

// ---------------------------------------------------------------------------
// core builtin processor — bookkeeping + child-stream topology + circuit breaker
// ---------------------------------------------------------------------------

const coreContract = defineProcessorContract({
  slug: "stream",
  version: "0.1.0",
  description: "core",
  stateSchema: z.object({
    namespace: z.string(),
    path: z.string(),
    eventCount: z.number().int().min(0).default(0),
    maxOffset: z.number().int().min(-1).default(-1),
    childPaths: z.array(z.string()).default([]),
    paused: z.boolean().default(false),
    pauseReason: z.string().nullable().default(null),
    availableTokens: z.number().default(500),
    lastRefillAtMs: z.number().int().min(0).nullable().default(null),
    burstCapacity: z.number().int().positive().default(500),
    refillRatePerMinute: z.number().int().positive().default(500),
  }),
  initialState: { namespace: "uninitialized", path: "uninitialized" },
  events: {
    "events.iterate.com/stream/created": { description: "c", payloadSchema: z.object({ namespace: z.string(), path: z.string() }) },
    "events.iterate.com/stream/child-stream-created": { description: "cc", payloadSchema: z.object({ childPath: z.string() }) },
    "events.iterate.com/stream/circuit-breaker-configured": { description: "cb", payloadSchema: z.object({ burstCapacity: z.number().int().positive(), refillRatePerMinute: z.number().int().positive() }) },
    "events.iterate.com/stream/paused": { description: "p", payloadSchema: z.object({ reason: z.string().optional() }) },
    "events.iterate.com/stream/resumed": { description: "r", payloadSchema: z.object({ reason: z.string().optional() }) },
  },
  consumes: [
    "*",
    "events.iterate.com/stream/created",
    "events.iterate.com/stream/child-stream-created",
    "events.iterate.com/stream/circuit-breaker-configured",
    "events.iterate.com/stream/paused",
    "events.iterate.com/stream/resumed",
  ],
  emits: ["events.iterate.com/stream/paused", "events.iterate.com/stream/child-stream-created"],
  reduce({ state, event }) {
    const createdAtMs = Date.parse(event.createdAt);
    let next = { ...state, eventCount: state.eventCount + 1, maxOffset: event.offset };
    switch (event.type) {
      case "events.iterate.com/stream/circuit-breaker-configured":
        next = { ...next, burstCapacity: event.payload.burstCapacity, refillRatePerMinute: event.payload.refillRatePerMinute, availableTokens: event.payload.burstCapacity, lastRefillAtMs: createdAtMs };
        break;
      case "events.iterate.com/stream/paused":
        next = { ...next, paused: true, pauseReason: event.payload.reason ?? null, availableTokens: state.burstCapacity, lastRefillAtMs: createdAtMs };
        break;
      case "events.iterate.com/stream/resumed":
        next = { ...next, paused: false, pauseReason: null, availableTokens: state.burstCapacity, lastRefillAtMs: createdAtMs };
        break;
      default: {
        const refilled = state.lastRefillAtMs == null ? state.burstCapacity : Math.min(state.burstCapacity, state.availableTokens + (createdAtMs - state.lastRefillAtMs) * (state.refillRatePerMinute / 60_000));
        next = { ...next, availableTokens: refilled - 1, lastRefillAtMs: createdAtMs };
      }
    }
    switch (event.type) {
      case "events.iterate.com/stream/created":
        return { ...next, namespace: event.payload.namespace, path: event.payload.path };
      case "events.iterate.com/stream/child-stream-created": {
        const child = immediateChildPath({ parentPath: state.path, childPath: event.payload.childPath });
        if (child == null || next.childPaths.includes(child)) return next;
        return { ...next, childPaths: [...next.childPaths, child] };
      }
      default:
        return next;
    }
  },
});

const core = implementBuiltinProcessor(coreContract, (deps: { appendToStream(path: string, e: StreamEventInput): void }) => ({
  beforeAppend({ event, state }) {
    if (!state.paused) return;
    if (event.type === "events.iterate.com/stream/resumed") return;
    throw new Error(`stream paused: ${state.pauseReason ?? "circuit breaker open"}`);
  },
  afterAppend({ event, state, stream, keepAlive }) {
    if (!state.paused && state.availableTokens < 0) {
      keepAlive(stream.append({ event: { type: "events.iterate.com/stream/paused", payload: { reason: "circuit breaker tripped" } } }));
    }
    if (event.type === "events.iterate.com/stream/created") {
      for (const ancestor of ancestorStreamPaths(state.path)) {
        deps.appendToStream(ancestor, {
          type: "events.iterate.com/stream/child-stream-created",
          payload: { childPath: state.path },
          idempotencyKey: `child-stream-created:${ancestor}:${state.path}`,
        });
      }
    }
  },
}));

function ancestorStreamPaths(path: string): string[] {
  if (path === "/") return [];
  const segments = path.split("/").filter(Boolean);
  const ancestors: string[] = ["/"];
  for (let i = 1; i < segments.length; i++) ancestors.push(`/${segments.slice(0, i).join("/")}`);
  return ancestors;
}

function immediateChildPath(args: { parentPath: string; childPath: string }): string | null {
  const { parentPath, childPath } = args;
  if (childPath === parentPath) return null;
  const prefix = parentPath === "/" ? "/" : `${parentPath}/`;
  if (!childPath.startsWith(prefix)) return null;
  const first = childPath.slice(prefix.length).split("/").filter(Boolean)[0];
  return first == null ? null : `${parentPath === "/" ? "" : parentPath}/${first}`;
}

type CoreState = z.infer<typeof coreContract.stateSchema>;

class CoreStreamSim {
  readonly streams = new Map<string, { state: CoreState; offset: number }>();
  readonly #impl = core.build({ appendToStream: (path, e) => void this.append(path, e) });

  #entry(path: string) {
    let entry = this.streams.get(path);
    if (entry === undefined) {
      entry = { state: { ...getInitialProcessorState(coreContract), namespace: "stream", path }, offset: -1 };
      this.streams.set(path, entry);
    }
    return entry;
  }

  append(path: string, input: StreamEventInput, createdAtMs = 0): StreamEvent {
    const entry = this.#entry(path);
    this.#impl.beforeAppend?.({ event: input, state: entry.state });
    const committed: StreamEvent = { ...input, offset: entry.offset + 1, createdAt: iso(createdAtMs) };
    const previousState = entry.state;
    const reduction = runProcessorReduce({ processor: { contract: coreContract }, event: committed, state: previousState });
    entry.offset = committed.offset;
    if (reduction === undefined) return committed;
    entry.state = reduction.state;
    const stream: StreamRpc = {
      append: (args) => this.append(path, args.event, createdAtMs),
      appendBatch: (args) => args.events.map((input) => this.append(path, input, createdAtMs)),
      getEvent: () => undefined,
      getEvents: () => [],
      subscribe: () => {
        throw new Error("CoreStreamSim does not implement subscribe");
      },
      runtimeState: () => {
        throw new Error("CoreStreamSim does not implement runtimeState");
      },
      kill: () => {},
      reset: async () => {},
      reduce: () => {
        throw new Error("CoreStreamSim does not implement reduce");
      },
    };
    this.#impl.afterAppend?.({
      event: reduction.event,
      previousState,
      state: entry.state,
      streamMaxOffset: committed.offset,
      stream,
      blockProcessorUntil: (work) => void work(),
      keepAlive: () => {},
    });
    return committed;
  }
}

describe("core builtin processor (inline)", () => {
  it("propagates child-stream-created up the ancestor chain", () => {
    const sim = new CoreStreamSim();
    sim.append("/a/b/c", { type: "events.iterate.com/stream/created", payload: { namespace: "stream", path: "/a/b/c" } });
    expect(sim.streams.get("/")?.state.childPaths).toEqual(["/a"]);
    expect(sim.streams.get("/a")?.state.childPaths).toEqual(["/a/b"]);
    expect(sim.streams.get("/a/b")?.state.childPaths).toEqual(["/a/b/c"]);
  });

  it("circuit breaker trips after the burst budget and rejects further appends", () => {
    const sim = new CoreStreamSim();
    sim.append("/cb", { type: "events.iterate.com/stream/created", payload: { namespace: "stream", path: "/cb" } });
    sim.append("/cb", { type: "events.iterate.com/stream/circuit-breaker-configured", payload: { burstCapacity: 2, refillRatePerMinute: 1 } });

    let rejected = 0;
    for (let i = 0; i < 5; i++) {
      try {
        sim.append("/cb", { type: "test.widget", payload: { i } });
      } catch {
        rejected += 1;
      }
    }
    expect(sim.streams.get("/cb")?.state.paused).toBe(true);
    expect(rejected).toBeGreaterThan(0);
  });
});
