#!/usr/bin/env node
/**
 * No-sync throughput, manual flush, and memory pressure tests.
 *
 *   pnpm test:no-sync
 *   pnpm test:flush
 *   pnpm test:pressure
 */

const baseUrl = (
  parseArgs(process.argv.slice(2)).url ?? "https://03-kv-unconfirmed-ceiling.iterate-dev-preview.workers.dev"
).replace(/\/$/, "");
const suite = parseArgs(process.argv.slice(2)).suite ?? "no-sync";

if (suite === "no-sync") await runNoSyncSuite();
else if (suite === "flush") await runFlushSuite();
else if (suite === "pressure") await runPressureSuite();
else {
  console.error("Usage: node scripts/run-kv-extended.ts [--url U] --suite no-sync|flush|pressure");
  process.exit(1);
}

async function runNoSyncSuite() {
  console.error(JSON.stringify({ type: "suite", name: "no-sync", baseUrl }));
  for (const payloadBytes of [256, 4800]) {
    for (const messages of [100_000, 500_000]) {
      for (let run = 1; run <= 2; run++) {
        const name = `nosync-${crypto.randomUUID().slice(0, 8)}`;
        try {
          const result = await post(
            `${baseUrl}/write-loop?name=${name}&messages=${messages}&payload-bytes=${payloadBytes}&mode=kv-unconfirmed&sync=0`,
          );
          log({
            suite: "no-sync",
            run,
            ...result,
            loopPerSecond: rate(result.messages, result.loopMs || result.wallMs),
            wallPerSecond: rate(result.messages, result.wallMs),
          });

          const flushed = await post(`${baseUrl}/flush?name=${name}`);
          log({
            suite: "no-sync-flush-after",
            name,
            ...flushed,
            flushPerSecond: rate(result.metaCount, flushed.syncMs || flushed.wallMs),
          });
        } catch (cause) {
          log({ suite: "no-sync-fail", run, messages, payloadBytes, error: String(cause) });
        }
      }
    }
  }
}

async function runFlushSuite() {
  console.error(JSON.stringify({ type: "suite", name: "flush-every", baseUrl }));
  const messages = 100_000;
  const payloadBytes = 4800;
  for (const flushEvery of [0, 1_000, 10_000, 100_000]) {
    const name = `flush-${flushEvery}-${crypto.randomUUID().slice(0, 8)}`;
    const flushParam = flushEvery > 0 ? `&flush-every=${flushEvery}` : "";
    const result = await post(
      `${baseUrl}/write-loop?name=${name}&messages=${messages}&payload-bytes=${payloadBytes}&mode=kv-unconfirmed&sync=0${flushParam}`,
    );
    log({
      suite: "flush-every",
      flushEvery,
      ...result,
      loopPerSecond: rate(result.messages, result.loopMs),
      syncPerSecond: result.syncMs > 0 ? rate(result.messages, result.syncMs) : undefined,
      wallPerSecond: rate(result.messages, result.wallMs),
    });
  }
}

async function runPressureSuite() {
  console.error(JSON.stringify({ type: "suite", name: "pressure", baseUrl }));
  const name = `pressure-${crypto.randomUUID().slice(0, 8)}`;
  const payloadBytes = 4800;
  const batch = 50_000;

  let totalMeta = 0;
  for (let batchNum = 1; batchNum <= 200; batchNum++) {
    try {
      const ping = await get(`${baseUrl}/ping?name=${name}`);
      const result = await post(
        `${baseUrl}/append?name=${name}&messages=${batch}&payload-bytes=${payloadBytes}&mode=kv-unconfirmed&sync=0`,
      );
      const prevMeta = totalMeta;
      totalMeta = result.metaCount;
      log({
        suite: "pressure-batch",
        batchNum,
        batchMessages: batch,
        totalMeta,
        deltaMeta: totalMeta - prevMeta,
        ...result,
        loopPerSecond: rate(result.messages, result.loopMs),
        wallPerSecond: rate(result.messages, result.wallMs),
        pingMeta: ping.metaCount,
      });
      if (totalMeta - prevMeta < batch) break;
    } catch (cause) {
      log({
        suite: "pressure-batch-fail",
        batchNum,
        totalMeta,
        error: String(cause),
      });
      break;
    }
  }

  try {
    const flushed = await post(`${baseUrl}/flush?name=${name}`);
    const count = await get(`${baseUrl}/count?name=${name}`);
    log({ suite: "pressure-final-flush", name, ...flushed, ...count, flushPerSecond: rate(count.metaCount, flushed.syncMs) });
  } catch (cause) {
    log({ suite: "pressure-final-flush-fail", name, error: String(cause) });
  }

  try {
    const pressure = await post(
      `${baseUrl}/pressure?name=${name}-single&max-messages=2000000&payload-bytes=${payloadBytes}&mode=kv-unconfirmed`,
    );
    log({
      suite: "pressure-single",
      ...pressure,
      loopPerSecond: rate(pressure.written, pressure.loopMs),
    });
  } catch (cause) {
    log({ suite: "pressure-single-fail", error: String(cause) });
  }
}

async function post(url: string) {
  const startedAt = performance.now();
  const res = await fetch(url, { method: "POST" });
  const wallMs = performance.now() - startedAt;
  if (!res.ok) throw new Error(`${url} → ${res.status} ${await res.text()}`);
  return { ...(await res.json()), wallMs };
}

async function get(url: string) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${url} → ${res.status} ${await res.text()}`);
  return res.json();
}

function rate(count: number, ms: number) {
  return count / (Math.max(ms, 0.001) / 1_000);
}

function log(row: Record<string, unknown>) {
  console.log(JSON.stringify(row));
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
