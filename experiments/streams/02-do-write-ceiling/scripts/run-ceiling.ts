#!/usr/bin/env node
import { appendFileSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

/**
 * Sweep WriteSink ceiling benchmarks (in-DO loop + runner→WS).
 *
 *   pnpm ceiling https://02-do-write-ceiling.iterate-dev-preview.workers.dev
 *   pnpm ceiling <url> --modes in-do --messages 100000 --payload-bytes 0,64,256,1024 --repeats 5
 *   pnpm ceiling <url> --modes in-do --variants shared,autoinc,blob,tiny --messages 50000
 */

const args = parseArgs(process.argv.slice(2));
const outPath = args.out;

if (!args.url) {
  console.error(`Usage: pnpm ceiling <base-url> \\
  [--messages 100000] [--payload-bytes 0,64,256,1024] \\
  [--variants shared,autoinc,blob,tiny] [--repeats 5] \\
  [--modes in-do,ws] [--drain-ms 120000] [--out findings/deployed-sweep.jsonl]`);
  process.exit(1);
}

const baseUrl = args.url.replace(/\/$/, "");
const messageCounts = parseCsvInts(args.messages ?? "100000");
const payloadSizes = parseCsvNonNegativeInts(args["payload-bytes"] ?? "256");
const variants = (args.variants ?? "shared").split(",").map((s) => s.trim());
const repeats = Number(args.repeats ?? 1);
const maxDrainMs = Number(args["drain-ms"] ?? 120_000);
const modes = (args.modes ?? "in-do,ws").split(",").map((s) => s.trim());

console.error(
  JSON.stringify({
    type: "ceiling-sweep-start",
    baseUrl,
    messageCounts,
    payloadSizes,
    variants,
    repeats,
    drainMs: maxDrainMs,
    modes,
    outPath,
  }),
);

if (outPath) {
  mkdirSync(dirname(outPath), { recursive: true });
  writeFileSync(outPath, "");
}

for (const messages of messageCounts) {
  for (const payloadBytes of payloadSizes) {
    for (const variant of variants) {
      for (let run = 1; run <= repeats; run++) {
        const name = `ceiling-${crypto.randomUUID().slice(0, 8)}`;

        if (modes.includes("in-do")) {
          const result = await postJson(
            `${baseUrl}/write-loop?name=${encodeURIComponent(name)}&messages=${messages}&payload-bytes=${payloadBytes}&variant=${variant}`,
          );
          const wallPerSecond = result.messages / (result.wallMs / 1_000);
          const mbPerSecond = (result.bytesWritten / 1_048_576) / (result.wallMs / 1_000);
          const row = {
            ...result,
            run,
            verified: result.serverCount === result.committed,
            wallPerSecond,
            mbPerSecond,
          };
          console.log(JSON.stringify(row));
          if (outPath) appendLine(outPath, row);
        }

        if (modes.includes("ws")) {
          const wsName = `${name}-ws`;
          const result = await postJson(
            `${baseUrl}/ws-benchmark?name=${encodeURIComponent(wsName)}&messages=${messages}&payload-bytes=${payloadBytes}&drain-ms=${maxDrainMs}`,
          );
          console.log(
            JSON.stringify({
              ...result,
              run,
              verified: result.verified ?? result.serverCount === result.sent,
              wallPerSecond: result.sent / (result.wallMs / 1_000),
            }),
          );
        }
      }
    }
  }
}

async function postJson(url: string) {
  const startedAt = performance.now();
  const res = await fetch(url, { method: "POST" });
  const wallMs = performance.now() - startedAt;
  if (!res.ok) throw new Error(`${url} → ${res.status} ${await res.text()}`);
  const body = await res.json();
  return { ...body, wallMs };
}

function parseCsvInts(raw: string) {
  return raw.split(",").map((s) => {
    const n = Number(s.trim());
    if (!Number.isInteger(n) || n <= 0) throw new Error(`Invalid integer: ${s}`);
    return n;
  });
}

function parseCsvNonNegativeInts(raw: string) {
  return raw.split(",").map((s) => {
    const n = Number(s.trim());
    if (!Number.isInteger(n) || n < 0) throw new Error(`Invalid non-negative integer: ${s}`);
    return n;
  });
}

function appendLine(path: string, row: unknown) {
  appendFileSync(path, `${JSON.stringify(row)}\n`);
}

function parseArgs(argv: string[]) {
  const out: Record<string, string> = {};
  for (let i = 0; i < argv.length; i++) {
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
