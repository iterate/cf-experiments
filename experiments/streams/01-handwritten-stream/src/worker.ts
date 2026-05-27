import { BenchmarkRunner } from "./benchmark-runner.js";
import { OrpcDurableStream } from "./orpc-durable-stream.js";
import { Stream } from "./stream.js";

export { BenchmarkRunner, OrpcDurableStream, Stream };

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/benchmark/audio-chaos") {
      const runId = url.searchParams.get("run-id") ?? crypto.randomUUID();
      const result = await env.BENCHMARK_RUNNER.getByName(`${runId}:orchestrator`).runAudioChaos({
        stream: url.searchParams.get("stream") ?? undefined,
        streamKind: streamKindParam(url),
        runId,
        publishers: positiveIntParam(url, "publishers"),
        subscribers: nonNegativeIntParam(url, "subscribers"),
        slowSubscribers: nonNegativeIntParam(url, "slow-subscribers"),
        framesPerPublisher: positiveIntParam(url, "frames-per-publisher"),
        frameMs: positiveIntParam(url, "frame-ms"),
        paceMs: nonNegativeIntParam(url, "pace-ms"),
        sampleRate: positiveIntParam(url, "sample-rate"),
        channels: positiveIntParam(url, "channels"),
        bytesPerSample: positiveIntParam(url, "bytes-per-sample"),
        timeoutMs: positiveIntParam(url, "timeout-ms"),
        durability: durabilityParam(url),
        checkpointEveryUnconfirmedAppends: positiveIntParam(url, "checkpoint-every"),
        measureAppendAck: url.searchParams.get("measure-append-ack") === "true",
        measureSelfEcho: booleanParam(url, "measure-self-echo"),
      });
      return Response.json(result);
    }

    const name = url.pathname.slice(1) || "default";
    return env.STREAM.getByName(name).fetch(request);
  },
} satisfies ExportedHandler<Env>;

function positiveIntParam(url: URL, name: string) {
  const raw = url.searchParams.get(name);
  if (raw === null) return undefined;
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) throw new Error(`${name} must be a positive integer`);
  return value;
}

function nonNegativeIntParam(url: URL, name: string) {
  const raw = url.searchParams.get(name);
  if (raw === null) return undefined;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }
  return value;
}

function streamKindParam(url: URL) {
  const raw = url.searchParams.get("stream-kind");
  if (raw === null) return undefined;
  if (
    raw !== "durable" &&
    raw !== "volatile" &&
    raw !== "json-volatile" &&
    raw !== "orpc-durable-iterator" &&
    raw !== "raw-volatile"
  ) {
    throw new Error(
      "stream-kind must be durable, volatile, json-volatile, orpc-durable-iterator, or raw-volatile",
    );
  }
  return raw;
}

function booleanParam(url: URL, name: string) {
  const raw = url.searchParams.get(name);
  if (raw === null) return undefined;
  if (raw === "true") return true;
  if (raw === "false") return false;
  throw new Error(`${name} must be true or false`);
}

function durabilityParam(url: URL) {
  const raw = url.searchParams.get("durability");
  if (raw === null) return undefined;
  if (raw !== "confirmed" && raw !== "best-effort" && raw !== "checkpointed") {
    throw new Error("durability must be confirmed, best-effort, or checkpointed");
  }
  return raw;
}
