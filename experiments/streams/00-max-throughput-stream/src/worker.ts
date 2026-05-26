import { StreamEventInput } from "@cf-experiments/shared/event";
import { OpenAPIHandler } from "@orpc/openapi/fetch";
import { RPCHandler } from "@orpc/server/fetch";
import { newWorkersRpcResponse } from "capnweb";
import { BenchmarkRunner, type RunBenchmarkArgs } from "./benchmark-runner.js";
import { streamOpenApiPlugins, streamOrpcRouter, streamRpcPlugins } from "./orpc.js";
import { SuperSimpleStream } from "./super-simple-stream.js";
import { RpcTarget as CfRpcTarget, WorkerEntrypoint } from "cloudflare:workers";

export { BenchmarkRunner, SuperSimpleStream };

const streamOpenApiHandler = new OpenAPIHandler(streamOrpcRouter, {
  plugins: streamOpenApiPlugins,
});
const streamRpcHandler = new RPCHandler(streamOrpcRouter, {
  plugins: streamRpcPlugins,
});

class DurableObjectStubCapnwebTarget extends CfRpcTarget {
  constructor(private readonly stub: DurableObjectStub<SuperSimpleStream>) {
    super();
  }

  get() {
    return this.stub;
  }

  getDurableObjectStub() {
    return this.stub;
  }
}

interface StreamCapabilityProps {
  path: string;
}

class StreamRpcTarget extends CfRpcTarget {
  constructor(
    private readonly env: Env,
    private readonly path: string,
  ) {
    super();
  }

  #stub() {
    return this.env.SUPER_SIMPLE_STREAM.getByName(this.path);
  }

  get() {
    return this.#stub();
  }

  append(event: StreamEventInput) {
    return this.#stub().append(event);
  }

  appendBatch(events: StreamEventInput[]) {
    return this.#stub().appendBatch(events);
  }

  count() {
    return this.#stub().count();
  }
}

class StreamsRpcTarget extends CfRpcTarget {
  constructor(private readonly env: Env) {
    super();
  }

  get(path: string) {
    // Could in future check if this stream is permitted to be accessed etc
    return new StreamRpcTarget(this.env, path);
  }
}

class ProjectRpcTarget extends CfRpcTarget {
  constructor(private readonly env: Env) {
    super();
  }

  get streams() {
    return new StreamsRpcTarget(this.env);
  }
}

export class StreamsCapability extends WorkerEntrypoint<Env> {
  get(path: string) {
    return new StreamRpcTarget(this.env, path);
  }
}

export class ProjectCapability extends WorkerEntrypoint<Env> {
  get streams() {
    return new StreamsRpcTarget(this.env);
  }
}

export class StreamCapability extends WorkerEntrypoint<Env, StreamCapabilityProps> {
  get() {
    return new StreamRpcTarget(this.env, this.ctx.props.path).get();
  }

  append(event: StreamEventInput) {
    return new StreamRpcTarget(this.env, this.ctx.props.path).append(event);
  }

  appendBatch(events: StreamEventInput[]) {
    return new StreamRpcTarget(this.env, this.ctx.props.path).appendBatch(events);
  }

  count() {
    return new StreamRpcTarget(this.env, this.ctx.props.path).count();
  }
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const name = url.searchParams.get("name") ?? "default";
    const stub = env.SUPER_SIMPLE_STREAM.getByName(name);

    if (url.pathname.startsWith("/orpc")) {
      const { matched, response } = await streamRpcHandler.handle(request, {
        prefix: "/orpc",
        context: { env },
      });
      if (matched && response) return response;
    }

    if (url.pathname.startsWith("/api")) {
      const { matched, response } = await streamOpenApiHandler.handle(request, {
        prefix: "/api",
        context: { env },
      });
      if (matched && response) return response;
    }

    if (url.pathname === "/stream") {
      return stub.fetch(request);
    }

    if (url.pathname === "/worker-entrypoint") {
      const path = url.searchParams.get("path") ?? "default";
      return newWorkersRpcResponse(request, ctx.exports.StreamCapability({ props: { path } }));
    }

    if (url.pathname === "/capnweb-project") {
      return newWorkersRpcResponse(request, new ProjectRpcTarget(env));
    }

    if (url.pathname === "/capnweb") {
      const capnwebUrl = new URL(request.url);
      capnwebUrl.pathname = "/_capnweb";
      return stub.fetch(new Request(capnwebUrl, request));
    }

    if (url.pathname === "/capnweb-cf-target") {
      const capnwebUrl = new URL(request.url);
      capnwebUrl.pathname = "/_capnweb-cf-target";
      return stub.fetch(new Request(capnwebUrl, request));
    }

    if (url.pathname === "/capnweb-do-stub") {
      return newWorkersRpcResponse(request, stub);
    }

    if (url.pathname === "/capnweb-return-do-stub") {
      return newWorkersRpcResponse(request, new DurableObjectStubCapnwebTarget(stub));
    }

    if (url.pathname === "/append" && request.method === "POST") {
      const parsed = StreamEventInput.safeParse(await request.json());
      if (!parsed.success) {
        return new Response("body must be a StreamEventInput", { status: 400 });
      }
      return Response.json(await stub.append(parsed.data));
    }

    if (url.pathname === "/append-batch" && request.method === "POST") {
      const parsed = StreamEventInput.array().safeParse(await request.json());
      if (!parsed.success) {
        return new Response("body must be a StreamEventInput[]", { status: 400 });
      }
      return Response.json(await stub.appendBatch(parsed.data));
    }

    if (url.pathname === "/count") {
      return Response.json(await stub.count());
    }

    if (url.pathname === "/benchmark/run" && request.method === "POST") {
      const runnerName =
        url.searchParams.get("runner") ?? `runner-${crypto.randomUUID().slice(0, 8)}`;
      const result = await env.BENCHMARK_RUNNER.getByName(runnerName).runBenchmark(
        parseBenchmarkArgs(url.searchParams),
      );
      return Response.json(result);
    }

    if (url.pathname === "/benchmark/fanout" && request.method === "POST") {
      const params = parseBenchmarkArgs(url.searchParams);
      const runners = parsePositiveInt(url.searchParams.get("runners"), 1);
      const streamPrefix = url.searchParams.get("stream-prefix") ?? params.stream;
      const runId = params.runId ?? crypto.randomUUID();
      const startedAt = Date.now();
      const results = await Promise.all(
        Array.from({ length: runners }, (_, i) =>
          env.BENCHMARK_RUNNER.getByName(`${runnerNameForFanout(runId, i)}`).runBenchmark({
            ...params,
            stream: `${streamPrefix}-${i}`,
            runId,
          }),
        ),
      );
      const aggregateCommitted = results.reduce((n, r) => n + r.committed, 0);
      const elapsedMs = Date.now() - startedAt;
      return Response.json({
        type: "benchmark-fanout-result",
        runId,
        runners,
        streamPrefix,
        mode: params.mode ?? "rpc-serial",
        messages: params.messages ?? 1_000,
        payloadBytes: params.payloadBytes ?? 256,
        aggregateCommitted,
        elapsedMs,
        aggregateEventsPerSecond: aggregateCommitted / (elapsedMs / 1_000),
        results,
      });
    }

    return new Response(
      [
        "00-max-throughput-stream",
        "",
        "GET  /stream?name=default&count          — DO count (or WebSocket upgrade)",
        "POST /capnweb-project                   — Cap'n Web RPC with ProjectCapability as main",
        "POST /capnweb?name=default               — Cap'n Web RPC via DO fetch + capnweb RpcTarget",
        "POST /capnweb-cf-target?name=default     — Cap'n Web RPC via DO fetch + Workers RpcTarget",
        "POST /capnweb-do-stub?name=default       — Cap'n Web RPC in Worker with DO stub as main",
        "POST /capnweb-return-do-stub?name=default — Cap'n Web RPC in Worker returning the DO stub",
        "GET  /count?name=default                 — RPC count",
        "POST /append?name=default                — StreamEventInput JSON body",
        "POST /append-batch?name=default          — StreamEventInput[] JSON body",
        "GET  /api/docs                           — Scalar docs for the oRPC OpenAPI routes",
        "POST /orpc/*                             — oRPC RPC transport for the proxied CLI",
        "GET  /api/__internal/trpc-cli-procedures — trpc-cli procedure metadata",
        "",
        "POST /benchmark/run?stream=bench&messages=1000&mode=rpc-serial&runner=r0",
        "POST /benchmark/fanout?runners=8&stream-prefix=bench&messages=1000&mode=rpc-serial",
        "       mode=rpc-serial|rpc-batch|rpc-pipelined",
        "",
        'WebSocket frames: { op: "append", event } or { op: "appendBatch", events }',
      ].join("\n"),
      { headers: { "content-type": "text/plain; charset=utf-8" } },
    );
  },
} satisfies ExportedHandler<Env>;

function parseBenchmarkArgs(params: URLSearchParams): RunBenchmarkArgs {
  const mode = params.get("mode");
  return {
    stream: params.get("stream") ?? "default",
    ...(mode === "rpc-serial" || mode === "rpc-batch" || mode === "rpc-pipelined" ? { mode } : {}),
    ...(params.has("messages")
      ? { messages: parsePositiveInt(params.get("messages"), 1_000) }
      : {}),
    ...(params.has("payload-bytes")
      ? { payloadBytes: parsePositiveInt(params.get("payload-bytes"), 256) }
      : {}),
    ...(params.has("batch-size")
      ? { batchSize: parsePositiveInt(params.get("batch-size"), 100) }
      : {}),
    ...(params.get("run-id") ? { runId: params.get("run-id")! } : {}),
  };
}

function parsePositiveInt(raw: string | null, fallback: number) {
  const n = Number(raw);
  return Number.isInteger(n) && n > 0 ? n : fallback;
}

function runnerNameForFanout(runId: string, index: number) {
  return `runner-${runId.slice(0, 8)}-${index}`;
}
