#!/usr/bin/env node
/**
 * Compare clean Stream DO durability modes for allowUnconfirmed benefit.
 *
 * Runs append throughput, egress contention, and audio-shaped fan-out sweeps
 * edge-side via BenchmarkRunner DOs (no laptop WiFi in the timing path).
 *
 *   node scripts/clean-unconfirmed-benchmark.ts http://localhost:8787
 *   node scripts/clean-unconfirmed-benchmark.ts https://01-handwritten-stream.iterate-dev-preview.workers.dev \
 *     --simulated-sync-delay-ms 0 --append-messages 1000 --audio-publishers 10 --audio-subscribers 36
 *
 * Quick single-mode probes:
 *   curl 'http://localhost:8787/benchmark/clean/append-throughput?mode=best-effort&messages=1000&payload-bytes=4800'
 *   curl 'http://localhost:8787/benchmark/clean/egress-contention?durability=confirmed-sync&simulated-sync-delay-ms=200'
 *   curl 'http://localhost:8787/benchmark/clean/audio-chaos?durability=best-effort&publishers=10&subscribers=36&frames-per-publisher=50&pace-ms=20&measure-append-ack=true'
 */

const args = parseArgs(process.argv.slice(2));
const baseUrl = (args.url ?? process.env.WORKER_URL ?? "http://localhost:8787").replace(/\/$/, "");
const sweepOnly = args.sweep !== "false";
const modes = (args.modes ?? "best-effort,confirmed-sync,output-gated")
  .split(",")
  .map((mode) => mode.trim());

console.error(
  JSON.stringify({
    type: "clean-unconfirmed-benchmark-start",
    baseUrl,
    sweepOnly,
    modes,
  }),
);

if (sweepOnly) {
  const params = new URLSearchParams();
  if (args["simulated-sync-delay-ms"] !== undefined) {
    params.set("simulated-sync-delay-ms", args["simulated-sync-delay-ms"]);
  }
  if (args["append-messages"] !== undefined) params.set("append-messages", args["append-messages"]);
  if (args["append-payload-bytes"] !== undefined) {
    params.set("append-payload-bytes", args["append-payload-bytes"]);
  }
  if (args["audio-publishers"] !== undefined) params.set("audio-publishers", args["audio-publishers"]);
  if (args["audio-subscribers"] !== undefined) {
    params.set("audio-subscribers", args["audio-subscribers"]);
  }
  if (args["audio-frames-per-publisher"] !== undefined) {
    params.set("audio-frames-per-publisher", args["audio-frames-per-publisher"]);
  }
  if (args["audio-pace-ms"] !== undefined) params.set("audio-pace-ms", args["audio-pace-ms"]);
  const runId = args["run-id"] ?? crypto.randomUUID();
  params.set("run-id", runId);

  const response = await fetch(`${baseUrl}/benchmark/clean/unconfirmed-sweep?${params}`, {
    method: "POST",
  });
  if (!response.ok) throw new Error(`sweep failed: ${response.status} ${await response.text()}`);
  const result = (await response.json()) as Parameters<typeof printSweepSummary>[0];
  printSweepSummary(result);
  console.log(JSON.stringify(result, null, 2));
} else {
  for (const mode of modes) {
    for (const kind of ["append", "egress", "audio"] as const) {
      const params = new URLSearchParams({ durability: mode, mode });
      if (args["simulated-sync-delay-ms"] !== undefined) {
        params.set("simulated-sync-delay-ms", args["simulated-sync-delay-ms"]);
      }
      if (kind === "append") {
        if (args["append-messages"] !== undefined) params.set("messages", args["append-messages"]);
        if (args["append-payload-bytes"] !== undefined) {
          params.set("payload-bytes", args["append-payload-bytes"]);
        }
      }
      if (kind === "audio") {
        if (args["audio-publishers"] !== undefined) params.set("publishers", args["audio-publishers"]);
        if (args["audio-subscribers"] !== undefined) {
          params.set("subscribers", args["audio-subscribers"]);
        }
        if (args["audio-frames-per-publisher"] !== undefined) {
          params.set("frames-per-publisher", args["audio-frames-per-publisher"]);
        }
        if (args["audio-pace-ms"] !== undefined) params.set("pace-ms", args["audio-pace-ms"]);
        params.set("measure-append-ack", "true");
      }
      if (kind === "egress" && args["simulated-sync-delay-ms"] === undefined) {
        params.set("simulated-sync-delay-ms", "200");
      }

      const path =
        kind === "append"
          ? "/benchmark/clean/append-throughput"
          : kind === "egress"
            ? "/benchmark/clean/egress-contention"
            : "/benchmark/clean/audio-chaos";
      const response = await fetch(`${baseUrl}${path}?${params}`, { method: "POST" });
      if (!response.ok) {
        throw new Error(`${kind}/${mode} failed: ${response.status} ${await response.text()}`);
      }
      console.log(JSON.stringify(await response.json()));
    }
  }
}

function printSweepSummary(result: {
  appendThroughput: {
    durability: string;
    eventsPerSecond: number;
    appendAckLatencyMs: { p50: number; p95: number };
  }[];
  egressContention: {
    durability: string;
    pingDuringAppendLatencyMs: { p50: number; p95: number };
    appendAckLatencyMs: { p50: number; p95: number };
  }[];
  audioChaos: {
    durability: string;
    eventsPerSecond: number;
    publisherAppendAckLatencyMs: { p50: number; p95: number };
    allSubscribersCreatedAtLatencyMs: { p50: number; p95: number };
    publisherAppendStartToSelfEchoLatencyMs: { p50: number; p95: number };
  }[];
}) {
  console.error("\n=== append throughput (events/s, append-ack p95 ms) ===");
  for (const row of result.appendThroughput) {
    console.error(
      `${row.durability.padEnd(16)} ${row.eventsPerSecond.toFixed(1).padStart(8)}/s  ack p95=${row.appendAckLatencyMs.p95.toFixed(1)}ms`,
    );
  }

  console.error("\n=== egress contention during slow append (ping p95 ms, append-ack p95 ms) ===");
  for (const row of result.egressContention) {
    console.error(
      `${row.durability.padEnd(16)} ping p95=${row.pingDuringAppendLatencyMs.p95.toFixed(1)}ms  append p95=${row.appendAckLatencyMs.p95.toFixed(1)}ms`,
    );
  }

  console.error("\n=== audio chaos (events/s, append-ack p95, all-subscribers p95, self-echo p95 ms) ===");
  for (const row of result.audioChaos) {
    console.error(
      `${row.durability.padEnd(16)} ${row.eventsPerSecond.toFixed(1).padStart(8)}/s  ack=${row.publisherAppendAckLatencyMs.p95.toFixed(1)}  all=${row.allSubscribersCreatedAtLatencyMs.p95.toFixed(1)}  echo=${row.publisherAppendStartToSelfEchoLatencyMs.p95.toFixed(1)}`,
    );
  }
}

function parseArgs(argv: string[]) {
  const out: Record<string, string> = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i]!;
    if (arg.startsWith("--")) {
      const key = arg.slice(2);
      const next = argv[i + 1];
      if (next && !next.startsWith("--")) {
        out[key] = next;
        i += 1;
      } else {
        out[key] = "true";
      }
    } else if (!out.url) {
      out.url = arg;
    }
  }
  return out;
}

export {};
