import { parseWriteVariant, WriteSink } from "./write-sink.js";
import { WsBenchmarkRunner } from "./ws-benchmark-runner.js";

export { WriteSink, WsBenchmarkRunner };

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const name = url.searchParams.get("name") ?? `sink-${crypto.randomUUID().slice(0, 8)}`;

    if (url.pathname === "/write-loop" && request.method === "POST") {
      const result = await env.WRITE_SINK.getByName(name).writeLoop({
        messages: parsePositiveInt(url.searchParams.get("messages"), 10_000),
        payloadBytes: parseNonNegativeInt(url.searchParams.get("payload-bytes"), 256),
        variant: parseWriteVariant(url.searchParams.get("variant")),
      });
      return Response.json(result);
    }

    if (url.pathname === "/ws-benchmark" && request.method === "POST") {
      const runner = url.searchParams.get("runner") ?? `runner-${crypto.randomUUID().slice(0, 8)}`;
      const result = await env.WS_BENCHMARK_RUNNER.getByName(runner).runBenchmark({
        stream: name,
        messages: parsePositiveInt(url.searchParams.get("messages"), 10_000),
        payloadBytes: parsePositiveInt(url.searchParams.get("payload-bytes"), 256),
        maxDrainMs: parsePositiveInt(url.searchParams.get("drain-ms"), 120_000),
      });
      return Response.json(result);
    }

    if (url.pathname === "/count") {
      return Response.json(await env.WRITE_SINK.getByName(name).count());
    }

    if (url.pathname === "/stream") {
      return env.WRITE_SINK.getByName(name).fetch(request);
    }

    return new Response(
      [
        "02-do-write-ceiling",
        "",
        "POST /write-loop?name=sink&messages=100000&payload-bytes=256&variant=shared",
        "  variant: shared | autoinc | blob | tiny",
        "POST /ws-benchmark?name=sink&messages=100000&drain-ms=120000 — runner DO → WS → WriteSink",
        "GET  /count?name=sink",
        "GET  /stream?name=sink                                     — WebSocket upgrade (external client)",
        "",
        "Sweep: pnpm ceiling <url> --messages 10000,50000 --payload-bytes 64,256,1024",
      ].join("\n"),
      { headers: { "content-type": "text/plain; charset=utf-8" } },
    );
  },
} satisfies ExportedHandler<Env>;

function parsePositiveInt(raw: string | null, fallback: number) {
  const n = Number(raw);
  return Number.isInteger(n) && n > 0 ? n : fallback;
}

function parseNonNegativeInt(raw: string | null, fallback: number) {
  const n = Number(raw);
  return Number.isInteger(n) && n >= 0 ? n : fallback;
}
