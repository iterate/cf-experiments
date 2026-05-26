#!/usr/bin/env node
/**
 * Measure append throughput into SuperSimpleStream.
 *
 *   pnpm benchmark http://localhost:8787
 *   pnpm benchmark http://localhost:8787 --mode compare --messages 10000 --verify
 *   pnpm benchmark https://00-max-throughput-stream.iterate-dev-preview.workers.dev --mode ws --name fresh-run
 */

const args = parseArgs(process.argv.slice(2));

if (!args.url) {
  console.error(`Usage: pnpm benchmark <base-url> \\
  [--mode ws|rpc|compare] [--messages 10000] [--payload-bytes 256] \\
  [--name bench] [--verify] [--drain-ms 3000]`);
  process.exit(1);
}

if (typeof WebSocket === "undefined") {
  console.error("Requires Node.js 22+ with global WebSocket.");
  process.exit(1);
}

const baseUrl = args.url.replace(/\/$/, "");
const mode = args.mode ?? "ws";
const messages = Number(args.messages ?? 10_000);
const payloadBytes = Number(args["payload-bytes"] ?? 256);
const name = args.name ?? `bench-${crypto.randomUUID().slice(0, 8)}`;
const verify = args.verify === "true";
const drainMs = Number(args["drain-ms"] ?? 3_000);
const runId = crypto.randomUUID();

if (mode === "compare") {
  const ws = await runWebSocket({ baseUrl, name, messages, payloadBytes, runId, verify, drainMs });
  const rpc = await runRpc({ baseUrl, name: `${name}-rpc`, messages, payloadBytes, runId, verify, drainMs });
  console.log(JSON.stringify({ type: "benchmark-compare-result", name, messages, payloadBytes, ws, rpc }));
  process.exit(0);
}

if (mode === "rpc") {
  console.log(JSON.stringify(await runRpc({ baseUrl, name, messages, payloadBytes, runId, verify, drainMs })));
  process.exit(0);
}

console.log(JSON.stringify(await runWebSocket({ baseUrl, name, messages, payloadBytes, runId, verify, drainMs })));

async function runWebSocket(args: {
  baseUrl: string;
  name: string;
  messages: number;
  payloadBytes: number;
  runId: string;
  verify: boolean;
  drainMs: number;
}) {
  const sampleFrame = buildAppendFrame(buildEvent(1, args.runId, args.payloadBytes));
  const frameBytes = Buffer.byteLength(sampleFrame, "utf8");
  const wsUrl = `${toWebSocketBase(args.baseUrl)}/stream?name=${encodeURIComponent(args.name)}`;

  console.error(
    JSON.stringify({
      type: "benchmark-start",
      mode: "ws-fire-forget",
      wsUrl,
      messages: args.messages,
      payloadBytes: args.payloadBytes,
      frameBytes,
    }),
  );

  const startedAt = performance.now();
  const sent = await pumpWebSocket({ wsUrl, messages: args.messages, runId: args.runId, payloadBytes: args.payloadBytes });
  const elapsedSec = (performance.now() - startedAt) / 1_000;

  let serverCount: number | undefined;
  if (args.verify) {
    await sleep(args.drainMs);
    serverCount = await fetchCount({ baseUrl: args.baseUrl, name: args.name });
  }

  return {
    type: "benchmark-result",
    mode: "ws-fire-forget",
    name: args.name,
    messages: args.messages,
    payloadBytes: args.payloadBytes,
    frameBytes,
    sent,
    elapsedSec,
    eventsPerSecond: sent / elapsedSec,
    serverCount,
  };
}

async function runRpc(args: {
  baseUrl: string;
  name: string;
  messages: number;
  payloadBytes: number;
  runId: string;
  verify: boolean;
  drainMs: number;
}) {
  const appendUrl = `${args.baseUrl}/append?name=${encodeURIComponent(args.name)}`;

  console.error(
    JSON.stringify({
      type: "benchmark-start",
      mode: "rpc-serial",
      appendUrl,
      messages: args.messages,
      payloadBytes: args.payloadBytes,
    }),
  );

  const startedAt = performance.now();
  let committed = 0;
  let lastRay: string | undefined;

  for (let n = 1; n <= args.messages; n++) {
    const response = await fetch(appendUrl, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(buildEvent(n, args.runId, args.payloadBytes)),
    });
    lastRay = response.headers.get("cf-ray") ?? lastRay;
    if (!response.ok) {
      throw new Error(`append failed at n=${n}: ${response.status} ${await response.text()}`);
    }
    committed += 1;
  }

  const elapsedSec = (performance.now() - startedAt) / 1_000;

  let serverCount: number | undefined;
  if (args.verify) {
    await sleep(args.drainMs);
    serverCount = await fetchCount({ baseUrl: args.baseUrl, name: args.name });
  }

  return {
    type: "benchmark-result",
    mode: "rpc-serial",
    name: args.name,
    messages: args.messages,
    payloadBytes: args.payloadBytes,
    committed,
    elapsedSec,
    eventsPerSecond: committed / elapsedSec,
    serverCount,
    cfRay: lastRay,
  };
}

function buildEvent(n: number, runId: string, payloadBytes: number) {
  return {
    type: "benchmark.append",
    payload: { n, runId, pad: "x".repeat(Math.max(0, payloadBytes)) },
    metadata: { runId },
  };
}

function buildAppendFrame(event: ReturnType<typeof buildEvent>) {
  return JSON.stringify({ op: "append", event });
}

function toWebSocketBase(raw: string) {
  const url = new URL(raw);
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";
  return url.origin;
}

async function pumpWebSocket(args: {
  wsUrl: string;
  messages: number;
  runId: string;
  payloadBytes: number;
}) {
  return new Promise<number>((resolve, reject) => {
    let openedAt = 0;
    let sent = 0;
    const ws = new WebSocket(args.wsUrl) as WebSocket & { bufferedAmount: number };

    const finish = () => resolve(sent);

    ws.addEventListener("open", () => {
      openedAt = performance.now();
      pump();
    });

    ws.addEventListener("error", () => {
      reject(new Error(`WebSocket error after sending ${sent}/${args.messages}`));
    });

    ws.addEventListener("close", () => {
      if (sent < args.messages) finish();
    });

    setTimeout(() => {
      if (sent < args.messages) finish();
    }, 120_000);

    function pump() {
      while (
        sent < args.messages &&
        ws.readyState === WebSocket.OPEN &&
        ws.bufferedAmount < 8_000_000
      ) {
        sent += 1;
        ws.send(buildAppendFrame(buildEvent(sent, args.runId, args.payloadBytes)));
      }
      if (sent < args.messages && ws.readyState === WebSocket.OPEN) {
        setImmediate(pump);
        return;
      }
      if (sent >= args.messages) {
        ws.close(1000, "done");
        finish();
      }
      if (openedAt === 0) return;
    }
  });
}

async function fetchCount(args: { baseUrl: string; name: string }) {
  const response = await fetch(`${args.baseUrl}/count?name=${encodeURIComponent(args.name)}`);
  if (!response.ok) {
    console.error(`count failed: ${response.status} ${await response.text()}`);
    return undefined;
  }
  const body = (await response.json()) as { sqlite?: number };
  return body.sqlite;
}

function sleep(ms: number) {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

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
