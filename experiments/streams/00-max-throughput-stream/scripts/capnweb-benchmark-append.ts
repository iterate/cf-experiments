#!/usr/bin/env node
/**
 * Measure append throughput into SuperSimpleStream via Cap'n Web RPC.
 *
 *   pnpm capnweb-benchmark http://localhost:8787
 *   pnpm capnweb-benchmark http://localhost:8787 --mode compare --messages 10000 --verify
 *   pnpm capnweb-benchmark https://00-max-throughput-stream.iterate-dev-preview.workers.dev --endpoint /capnweb-cf-target
 */

import { newHttpBatchRpcSession, newWebSocketRpcSession } from "capnweb";

type StreamEventInput = {
  type: string;
  payload?: unknown;
  metadata?: Record<string, unknown>;
};

interface SuperSimpleStreamApi {
  append(event: StreamEventInput): unknown;
  appendBatch(events: StreamEventInput[]): unknown[];
  count(): { sqlite: number };
}

interface DurableObjectStubApi {
  get(): SuperSimpleStreamApi;
  getDurableObjectStub(): SuperSimpleStreamApi;
}

interface ProjectApi {
  streams: {
    get(path: string): SuperSimpleStreamApi;
  };
}

const args = parseArgs(process.argv.slice(2));

if (!args.url) {
  console.error(`Usage: pnpm capnweb-benchmark <base-url> \\
  [--mode ws|batch|serial|compare] [--messages 10000] [--payload-bytes 256] \\
  [--name bench] [--endpoint /capnweb] [--target root|get|project|returned-stub] [--verify] [--drain-ms 3000] [--debug-wire]`);
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
const endpoint = normalizeEndpoint(args.endpoint ?? "/capnweb");
const target = args.target ?? "root";
const streamPath = name;
const debugWire = args["debug-wire"] === "true";

if (mode === "compare") {
  const ws = await runWebSocket({
    capnwebUrl: buildCapnwebUrl(baseUrl, endpoint, name),
    target,
    name,
    messages,
    payloadBytes,
    runId,
    verify,
    drainMs,
    baseUrl,
    debugWire,
  });
  const batchName = `${name}-batch`;
  const batch = await runBatch({
    capnwebUrl: buildCapnwebUrl(baseUrl, endpoint, batchName),
    target,
    name: batchName,
    messages,
    payloadBytes,
    runId,
    verify,
    drainMs,
    baseUrl,
  });
  const serialName = `${name}-serial`;
  const serial = await runSerial({
    capnwebUrl: buildCapnwebUrl(baseUrl, endpoint, serialName),
    target,
    name: serialName,
    messages,
    payloadBytes,
    runId,
    verify,
    drainMs,
    baseUrl,
  });
  console.log(
    JSON.stringify({
      type: "capnweb-benchmark-compare-result",
      endpoint,
      target,
      name,
      messages,
      payloadBytes,
      ws,
      batch,
      serial,
    }),
  );
  process.exit(0);
}

if (mode === "batch") {
  console.log(
    JSON.stringify(
      await runBatch({
        capnwebUrl: buildCapnwebUrl(baseUrl, endpoint, name),
        target,
        name,
        messages,
        payloadBytes,
        runId,
        verify,
        drainMs,
        baseUrl,
      }),
    ),
  );
  process.exit(0);
}

if (mode === "serial") {
  console.log(
    JSON.stringify(
      await runSerial({
        capnwebUrl: buildCapnwebUrl(baseUrl, endpoint, name),
        target,
        name,
        messages,
        payloadBytes,
        runId,
        verify,
        drainMs,
        baseUrl,
      }),
    ),
  );
  process.exit(0);
}

console.log(
  JSON.stringify(
    await runWebSocket({
      capnwebUrl: buildCapnwebUrl(baseUrl, endpoint, name),
      target,
      name,
      messages,
      payloadBytes,
      runId,
      verify,
      drainMs,
      baseUrl,
      debugWire,
    }),
  ),
);

async function runWebSocket(args: {
  capnwebUrl: string;
  target: string;
  baseUrl: string;
  name: string;
  messages: number;
  payloadBytes: number;
  runId: string;
  verify: boolean;
  drainMs: number;
  debugWire: boolean;
}) {
  const wsUrl = toWebSocketUrl(args.capnwebUrl);

  console.error(
    JSON.stringify({
      type: "capnweb-benchmark-start",
      mode: "ws-fire-forget",
      wsUrl,
      target: args.target,
      messages: args.messages,
      payloadBytes: args.payloadBytes,
      debugWire: args.debugWire,
    }),
  );

  const startedAt = performance.now();
  let sent = 0;

  const webSocket = newDebuggableWebSocket(wsUrl, args.debugWire);
  using root = newWebSocketRpcSession<SuperSimpleStreamApi & DurableObjectStubApi>(webSocket);
  const stub = getRpcTarget(root, args.target);
  for (let n = 1; n <= args.messages; n += 1) {
    stub.append(buildEvent(n, args.runId, args.payloadBytes));
    sent += 1;
  }

  const elapsedSec = (performance.now() - startedAt) / 1_000;

  let serverCount: number | undefined;
  if (args.verify) {
    await sleep(args.drainMs);
    serverCount = await fetchCount({ baseUrl: args.baseUrl, name: args.name });
  }

  return {
    type: "capnweb-benchmark-result",
    mode: "ws-fire-forget",
    target: args.target,
    name: args.name,
    messages: args.messages,
    payloadBytes: args.payloadBytes,
    sent,
    elapsedSec,
    eventsPerSecond: sent / elapsedSec,
    serverCount,
  };
}

async function runBatch(args: {
  capnwebUrl: string;
  target: string;
  baseUrl: string;
  name: string;
  messages: number;
  payloadBytes: number;
  runId: string;
  verify: boolean;
  drainMs: number;
}) {
  console.error(
    JSON.stringify({
      type: "capnweb-benchmark-start",
      mode: "http-batch",
      capnwebUrl: args.capnwebUrl,
      target: args.target,
      messages: args.messages,
      payloadBytes: args.payloadBytes,
    }),
  );

  const startedAt = performance.now();
  let committed = 0;

  using root = newHttpBatchRpcSession<SuperSimpleStreamApi & DurableObjectStubApi>(args.capnwebUrl);
  const stub = getRpcTarget(root, args.target);
  for (let n = 1; n <= args.messages; n += 1) {
    stub.append(buildEvent(n, args.runId, args.payloadBytes));
    committed += 1;
  }
  await stub.count();

  const elapsedSec = (performance.now() - startedAt) / 1_000;

  let serverCount: number | undefined;
  if (args.verify) {
    await sleep(args.drainMs);
    serverCount = await fetchCount({ baseUrl: args.baseUrl, name: args.name });
  }

  return {
    type: "capnweb-benchmark-result",
    mode: "http-batch",
    target: args.target,
    name: args.name,
    messages: args.messages,
    payloadBytes: args.payloadBytes,
    committed,
    elapsedSec,
    eventsPerSecond: committed / elapsedSec,
    serverCount,
  };
}

async function runSerial(args: {
  capnwebUrl: string;
  target: string;
  baseUrl: string;
  name: string;
  messages: number;
  payloadBytes: number;
  runId: string;
  verify: boolean;
  drainMs: number;
}) {
  console.error(
    JSON.stringify({
      type: "capnweb-benchmark-start",
      mode: "http-batch-serial",
      capnwebUrl: args.capnwebUrl,
      target: args.target,
      messages: args.messages,
      payloadBytes: args.payloadBytes,
    }),
  );

  const startedAt = performance.now();
  let committed = 0;

  for (let n = 1; n <= args.messages; n += 1) {
    using root = newHttpBatchRpcSession<SuperSimpleStreamApi & DurableObjectStubApi>(
      args.capnwebUrl,
    );
    const stub = getRpcTarget(root, args.target);
    stub.append(buildEvent(n, args.runId, args.payloadBytes));
    await stub.count();
    committed += 1;
  }

  const elapsedSec = (performance.now() - startedAt) / 1_000;

  let serverCount: number | undefined;
  if (args.verify) {
    await sleep(args.drainMs);
    serverCount = await fetchCount({ baseUrl: args.baseUrl, name: args.name });
  }

  return {
    type: "capnweb-benchmark-result",
    mode: "http-batch-serial",
    target: args.target,
    name: args.name,
    messages: args.messages,
    payloadBytes: args.payloadBytes,
    committed,
    elapsedSec,
    eventsPerSecond: committed / elapsedSec,
    serverCount,
  };
}

function buildEvent(n: number, runId: string, payloadBytes: number): StreamEventInput {
  return {
    type: "benchmark.append",
    payload: { n, runId, pad: "x".repeat(Math.max(0, payloadBytes)) },
    metadata: { runId },
  };
}

function getRpcTarget(root: unknown, target: string): SuperSimpleStreamApi {
  const api = root as SuperSimpleStreamApi & DurableObjectStubApi & ProjectApi;
  if (target === "project") return api.streams.get(streamPath);
  if (target === "get") return api.get();
  if (target === "returned-stub") return api.getDurableObjectStub();
  if (target !== "root") throw new Error(`unknown target: ${target}`);
  return api;
}

function toWebSocketUrl(raw: string) {
  const url = new URL(raw);
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";
  return url.toString();
}

function normalizeEndpoint(endpoint: string) {
  return endpoint.startsWith("/") ? endpoint : `/${endpoint}`;
}

function buildCapnwebUrl(baseUrl: string, endpoint: string, name: string) {
  const encodedName = encodeURIComponent(name);
  return `${baseUrl}${endpoint}?name=${encodedName}&path=${encodedName}`;
}

function newDebuggableWebSocket(url: string, debugWire: boolean): WebSocket {
  const webSocket = new WebSocket(url);
  if (!debugWire) return webSocket;

  const send = webSocket.send.bind(webSocket);
  webSocket.send = ((data: Parameters<WebSocket["send"]>[0]) => {
    logWebSocketFrame("out", data);
    return send(data);
  }) as WebSocket["send"];

  webSocket.addEventListener("message", (event) => {
    logWebSocketFrame("in", event.data);
  });

  return webSocket;
}

function logWebSocketFrame(direction: "in" | "out", data: unknown) {
  const arrow = direction === "out" ? "->" : "<-";
  if (typeof data === "string") {
    console.error(`[capnweb ws ${arrow}] ${data}`);
    return;
  }

  console.error(`[capnweb ws ${arrow}] ${describeWebSocketFrameData(data)}`);
}

function describeWebSocketFrameData(data: unknown) {
  if (data instanceof ArrayBuffer) return `ArrayBuffer(${data.byteLength})`;
  if (ArrayBuffer.isView(data)) return `${data.constructor.name}(${data.byteLength})`;
  if (data instanceof Blob) return `Blob(${data.size})`;
  return String(data);
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
