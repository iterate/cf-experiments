import { DurableObject } from "cloudflare:workers";
import type { StreamEventInput } from "@cf-experiments/shared/event";

export type BenchmarkMode = "rpc-serial" | "rpc-batch" | "rpc-pipelined";

export type RunBenchmarkArgs = {
  stream: string;
  mode?: BenchmarkMode;
  messages?: number;
  payloadBytes?: number;
  batchSize?: number;
  runId?: string;
};

export type BenchmarkResult = {
  runner: string;
  stream: string;
  mode: BenchmarkMode;
  messages: number;
  payloadBytes: number;
  committed: number;
  elapsedMs: number;
  eventsPerSecond: number;
  serverCount: number;
  runId: string;
  dispatchMs?: number;
};

export class BenchmarkRunner extends DurableObject {
  async runBenchmark(args: RunBenchmarkArgs): Promise<BenchmarkResult> {
    const stream = args.stream;
    const mode = args.mode ?? "rpc-serial";
    const messages = args.messages ?? 1_000;
    const payloadBytes = args.payloadBytes ?? 256;
    const batchSize = args.batchSize ?? 100;
    const runId = args.runId ?? crypto.randomUUID();
    const stub = this.env.STREAM.getByName(stream);

    const startedAt = Date.now();
    let committed = 0;
    let dispatchMs: number | undefined;

    if (mode === "rpc-serial") {
      for (let n = 1; n <= messages; n++) {
        await stub.append({ event: buildEvent(n, runId, payloadBytes) });
        committed += 1;
      }
    } else if (mode === "rpc-pipelined") {
      const pending: Promise<unknown>[] = [];
      for (let n = 1; n <= messages; n++) {
        pending.push(stub.append({ event: buildEvent(n, runId, payloadBytes) }));
      }
      dispatchMs = Date.now() - startedAt;
      await Promise.all(pending);
      committed = messages;
    } else {
      for (let offset = 0; offset < messages; offset += batchSize) {
        const count = Math.min(batchSize, messages - offset);
        const events = Array.from({ length: count }, (_, i) =>
          buildEvent(offset + i + 1, runId, payloadBytes),
        );
        await stub.appendBatch({ events });
        committed += count;
      }
    }

    const elapsedMs = Date.now() - startedAt;
    const serverCount = await stub.count();

    return {
      runner: this.ctx.id.name ?? this.ctx.id.toString(),
      stream,
      mode,
      messages,
      payloadBytes,
      committed,
      elapsedMs,
      eventsPerSecond: committed / (elapsedMs / 1_000),
      serverCount,
      runId,
      ...(dispatchMs !== undefined ? { dispatchMs } : {}),
    };
  }
}

function buildEvent(n: number, runId: string, payloadBytes: number): StreamEventInput {
  return {
    type: "benchmark.append",
    payload: { n, runId, pad: "x".repeat(Math.max(0, payloadBytes)) },
    metadata: { runId },
  };
}
