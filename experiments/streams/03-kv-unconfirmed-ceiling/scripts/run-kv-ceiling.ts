#!/usr/bin/env node
/**
 * Compare SQL vs KV gated vs KV allowUnconfirmed on deployed worker.
 *
 *   pnpm ceiling https://03-kv-unconfirmed-ceiling.iterate-dev-preview.workers.dev
 */

const args = parseArgs(process.argv.slice(2));
const baseUrl = (args.url ?? "https://03-kv-unconfirmed-ceiling.iterate-dev-preview.workers.dev").replace(
  /\/$/,
  "",
);
const messageCounts = parseCsvInts(args.messages ?? "100000");
const payloadSizes = parseCsvNonNegativeInts(args["payload-bytes"] ?? "256,4800");
const modes = (args.modes ?? "sql,kv-gated,kv-unconfirmed").split(",").map((s) => s.trim());
const repeats = Number(args.repeats ?? 2);

console.error(JSON.stringify({ type: "kv-ceiling-start", baseUrl, messageCounts, payloadSizes, modes, repeats }));

for (const messages of messageCounts) {
  for (const payloadBytes of payloadSizes) {
    for (const mode of modes) {
      for (let run = 1; run <= repeats; run++) {
        const name = `kv-${crypto.randomUUID().slice(0, 8)}`;
        const startedAt = performance.now();
        const res = await fetch(
          `${baseUrl}/write-loop?name=${encodeURIComponent(name)}&messages=${messages}&payload-bytes=${payloadBytes}&mode=${mode}`,
          { method: "POST" },
        );
        const wallMs = performance.now() - startedAt;
        if (!res.ok) throw new Error(`${mode} → ${res.status} ${await res.text()}`);
        const result = await res.json();
        const loopWallMs = Math.max(wallMs - (result.syncMs ?? 0), 0.001);
        console.log(
          JSON.stringify({
            ...result,
            run,
            wallMs,
            loopWallMs,
            loopPerSecond: result.messages / (loopWallMs / 1_000),
            wallPerSecond: result.messages / (wallMs / 1_000),
            syncPerSecond: result.syncMs > 0 ? result.messages / (result.syncMs / 1_000) : undefined,
          }),
        );
      }
    }
  }
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
