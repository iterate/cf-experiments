import { OpenAPIReferencePlugin } from "@orpc/openapi/plugins";
import { os } from "@orpc/server";
import { CORSPlugin } from "@orpc/server/plugins";
import { ZodToJsonSchemaConverter } from "@orpc/zod/zod4";
import { parseRouter, type AnyRouter } from "trpc-cli/dist/parse-router.js";
import { z } from "zod";
import { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import type { BenchmarkMode, RunBenchmarkArgs } from "./benchmark-runner.js";

const CountOutput = z.object({
  sqlite: z.number().int().nonnegative(),
});

const BenchmarkModeInput = z.enum(["rpc-serial", "rpc-batch", "rpc-pipelined"]);

const BenchmarkInput = z.object({
  stream: z.string().default("default"),
  mode: BenchmarkModeInput.optional(),
  messages: z.number().int().positive().default(1_000),
  payloadBytes: z.number().int().nonnegative().default(256),
  batchSize: z.number().int().positive().default(100),
  runId: z.string().optional(),
});

const BenchmarkResult = z.object({
  runner: z.string(),
  stream: z.string(),
  mode: BenchmarkModeInput,
  messages: z.number().int().positive(),
  payloadBytes: z.number().int().nonnegative(),
  committed: z.number().int().nonnegative(),
  elapsedMs: z.number().nonnegative(),
  eventsPerSecond: z.number().nonnegative(),
  serverCount: z.number().int().nonnegative(),
  runId: z.string(),
  dispatchMs: z.number().nonnegative().optional(),
});

const BenchmarkFanoutInput = BenchmarkInput.extend({
  runners: z.number().int().positive().default(1),
  streamPrefix: z.string().optional(),
});

const BenchmarkFanoutResult = z.object({
  type: z.literal("benchmark-fanout-result"),
  runId: z.string(),
  runners: z.number().int().positive(),
  streamPrefix: z.string(),
  mode: BenchmarkModeInput,
  messages: z.number().int().positive(),
  payloadBytes: z.number().int().nonnegative(),
  aggregateCommitted: z.number().int().nonnegative(),
  elapsedMs: z.number().nonnegative(),
  aggregateEventsPerSecond: z.number().nonnegative(),
  results: z.array(BenchmarkResult),
});

const NamedStreamInput = z.object({
  name: z.string().default("default"),
});

const AppendInput = StreamEventInput.extend({
  name: z.string().default("default"),
});

const AppendBatchInput = z.object({
  name: z.string().default("default"),
  events: z.array(StreamEventInput),
});

const TrpcCliProceduresOutput = z.object({
  procedures: z.array(z.unknown()),
});

const base = os.$context<{ env: Env }>();

let streamRouterForDiscovery: AnyRouter | undefined;

export const streamOrpcRouter = base.router({
  append: base
    .route({
      method: "POST",
      path: "/append",
      tags: ["stream"],
      summary: "Append one event to a stream Durable Object",
    })
    .input(AppendInput)
    .output(StreamEvent)
    .handler(async ({ context, input }) => {
      const { name, ...event } = input;
      return context.env.SUPER_SIMPLE_STREAM.getByName(name).append(event);
    }),

  appendBatch: base
    .route({
      method: "POST",
      path: "/append-batch",
      tags: ["stream"],
      summary: "Append a batch of events to a stream Durable Object",
    })
    .input(AppendBatchInput)
    .output(z.array(StreamEvent))
    .handler(async ({ context, input }) =>
      context.env.SUPER_SIMPLE_STREAM.getByName(input.name).appendBatch(input.events),
    ),

  count: base
    .route({
      method: "GET",
      path: "/count",
      tags: ["stream"],
      summary: "Read the committed event count for a stream Durable Object",
    })
    .input(NamedStreamInput)
    .output(CountOutput)
    .handler(async ({ context, input }) =>
      context.env.SUPER_SIMPLE_STREAM.getByName(input.name).count(),
    ),

  benchmark: {
    run: base
      .route({
        method: "POST",
        path: "/benchmark/run",
        tags: ["benchmark"],
        summary: "Run one append-throughput benchmark",
      })
      .input(
        BenchmarkInput.extend({
          runner: z.string().optional(),
        }),
      )
      .output(BenchmarkResult)
      .handler(async ({ context, input }) => {
        const runnerName = input.runner ?? `runner-${crypto.randomUUID().slice(0, 8)}`;
        return context.env.BENCHMARK_RUNNER.getByName(runnerName).runBenchmark(
          toBenchmarkArgs(input),
        );
      }),

    fanout: base
      .route({
        method: "POST",
        path: "/benchmark/fanout",
        tags: ["benchmark"],
        summary: "Run the same append-throughput benchmark across many runner Durable Objects",
      })
      .input(BenchmarkFanoutInput)
      .output(BenchmarkFanoutResult)
      .handler(async ({ context, input }) => {
        const streamPrefix = input.streamPrefix ?? input.stream;
        const runId = input.runId ?? crypto.randomUUID();
        const startedAt = Date.now();
        const results = await Promise.all(
          Array.from({ length: input.runners }, (_, i) =>
            context.env.BENCHMARK_RUNNER.getByName(runnerNameForFanout(runId, i)).runBenchmark({
              ...toBenchmarkArgs(input),
              stream: `${streamPrefix}-${i}`,
              runId,
            }),
          ),
        );
        const aggregateCommitted = results.reduce((n, r) => n + r.committed, 0);
        const elapsedMs = Date.now() - startedAt;
        return {
          type: "benchmark-fanout-result" as const,
          runId,
          runners: input.runners,
          streamPrefix,
          mode: input.mode ?? "rpc-serial",
          messages: input.messages,
          payloadBytes: input.payloadBytes,
          aggregateCommitted,
          elapsedMs,
          aggregateEventsPerSecond: aggregateCommitted / (elapsedMs / 1_000),
          results,
        };
      }),
  },

  __internal: {
    trpcCliProcedures: base
      .route({
        method: "GET",
        path: "/__internal/trpc-cli-procedures",
        tags: ["internal"],
        summary: "Return parsed oRPC procedure metadata for the remote CLI proxy",
      })
      .output(TrpcCliProceduresOutput)
      .handler(() => {
        if (!streamRouterForDiscovery) {
          throw new Error("stream oRPC router is not ready");
        }
        return {
          procedures: parseRouter({ router: streamRouterForDiscovery }).filter(
            ([path]) => path !== "__internal.trpcCliProcedures",
          ),
        };
      }),
  },
});

streamRouterForDiscovery = streamOrpcRouter as AnyRouter;

export const streamRpcPlugins = [new CORSPlugin({ origin: "*" })];

export const streamOpenApiPlugins = [
  ...streamRpcPlugins,
  new OpenAPIReferencePlugin({
    docsProvider: "scalar",
    docsPath: "/docs",
    specPath: "/openapi.json",
    schemaConverters: [new ZodToJsonSchemaConverter()],
    specGenerateOptions: {
      info: {
        title: "00-max-throughput-stream API",
        version: "0.0.0",
      },
      servers: [{ url: "/api" }],
      tags: [{ name: "stream" }, { name: "benchmark" }, { name: "internal" }],
    },
  }),
];

function toBenchmarkArgs(input: z.output<typeof BenchmarkInput>): RunBenchmarkArgs {
  return {
    stream: input.stream,
    ...(input.mode === undefined ? {} : { mode: input.mode as BenchmarkMode }),
    messages: input.messages,
    payloadBytes: input.payloadBytes,
    batchSize: input.batchSize,
    ...(input.runId === undefined ? {} : { runId: input.runId }),
  };
}

function runnerNameForFanout(runId: string, index: number) {
  return `runner-${runId.slice(0, 8)}-${index}`;
}
