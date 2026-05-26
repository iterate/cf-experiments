#!/usr/bin/env node
/**
 * Trigger edge-side BenchmarkRunner DOs from outside Cloudflare.
 *
 *   pnpm benchmark:runners http://localhost:8789 --runners 8 --messages 1000
 *   pnpm benchmark:runners https://00-max-throughput-stream.iterate-dev-preview.workers.dev --runners 16 --mode rpc-batch
 */

const args = parseArgs(process.argv.slice(2));

if (!args.url) {
  console.error(`Usage: pnpm benchmark:runners <base-url> \\
  [--runners 8] [--stream-prefix bench] [--messages 1000] [--payload-bytes 256] \\
  [--mode rpc-serial|rpc-batch] [--batch-size 100] [--runner r0]`);
  process.exit(1);
}

const baseUrl = args.url.replace(/\/$/, "");
const runners = Number(args.runners ?? 1);
const params = new URLSearchParams({
  stream: args.stream ?? "bench",
  messages: String(args.messages ?? 1_000),
  "payload-bytes": String(args["payload-bytes"] ?? 256),
  mode: args.mode ?? "rpc-serial",
});

if (args["batch-size"]) params.set("batch-size", args["batch-size"]);
if (args["stream-prefix"]) params.set("stream-prefix", args["stream-prefix"]);
if (args["run-id"]) params.set("run-id", args["run-id"]);

const path =
  runners > 1
    ? `/benchmark/fanout?runners=${runners}&${params}`
    : `/benchmark/run?runner=${encodeURIComponent(args.runner ?? "runner-0")}&${params}`;

console.error(JSON.stringify({ type: "benchmark-runners-start", url: `${baseUrl}${path}`, runners }));

const response = await fetch(`${baseUrl}${path}`, { method: "POST" });
const body = await response.text();
if (!response.ok) {
  console.error(body);
  process.exit(1);
}

console.log(body);

function parseArgs(argv: string[]) {
  const parsed: Record<string, string | undefined> = {};
  const positional: string[] = [];
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (!arg.startsWith("--")) {
      positional.push(arg);
      continue;
    }
    const key = arg.slice(2);
    const next = argv[i + 1];
    if (next && !next.startsWith("--")) {
      parsed[key] = next;
      i += 1;
    } else {
      parsed[key] = "true";
    }
  }
  parsed.url = positional[0];
  return parsed;
}
