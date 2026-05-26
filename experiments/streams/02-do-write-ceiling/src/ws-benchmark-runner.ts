import { DurableObject } from "cloudflare:workers";
import type { StreamEventInput } from "@cf-experiments/shared/event";

export type WsBenchmarkResult = {
  mode: "ws-from-runner";
  stream: string;
  messages: number;
  payloadBytes: number;
  sent: number;
  dispatchMs: number;
  drainMs: number;
  elapsedMs: number;
  dispatchPerSecond: number;
  commitPerSecond: number;
  serverCount: number;
  bytesSent: number;
  verified: boolean;
};

/** Opens a WebSocket to WriteSink and pumps append frames (no server replies). */
export class WsBenchmarkRunner extends DurableObject {
  async runBenchmark(args: {
    stream: string;
    messages?: number;
    payloadBytes?: number;
    maxDrainMs?: number;
  }) {
    const messages = args.messages ?? 10_000;
    const payloadBytes = args.payloadBytes ?? 256;
    const maxDrainMs = args.maxDrainMs ?? 120_000;
    const stub = this.env.WRITE_SINK.getByName(args.stream);

    const response = await stub.fetch(new Request("http://do/stream", { headers: { Upgrade: "websocket" } }));
    const ws = response.webSocket;
    if (ws === null) throw new Error("WriteSink did not return a WebSocket");

    ws.accept();
    const sample = buildFrame(1, payloadBytes);
    const bytesSent = sample.length * messages;
    const startedAt = performance.now();
    let sent = 0;

    for (let n = 1; n <= messages; n++) {
      ws.send(buildFrame(n, payloadBytes));
      sent += 1;
    }

    const dispatchMs = Math.max(performance.now() - startedAt, 0.001);
    const drainStartedAt = performance.now();
    let serverCount = 0;

    while (performance.now() - drainStartedAt < maxDrainMs) {
      ({ sqlite: serverCount } = await stub.count());
      if (serverCount >= sent) break;
      await sleep(50);
    }

    ws.close(1000, "done");
    const drainMs = performance.now() - drainStartedAt;
    const elapsedMs = performance.now() - startedAt;

    if (serverCount < sent) {
      ({ sqlite: serverCount } = await stub.count());
    }

    return {
      mode: "ws-from-runner",
      stream: args.stream,
      messages,
      payloadBytes,
      sent,
      dispatchMs,
      drainMs,
      elapsedMs,
      dispatchPerSecond: sent / (dispatchMs / 1_000),
      commitPerSecond: serverCount / (elapsedMs / 1_000),
      serverCount,
      bytesSent,
      verified: serverCount === sent,
    } satisfies WsBenchmarkResult;
  }
}

function buildFrame(n: number, payloadBytes: number) {
  const event: StreamEventInput = {
    type: "bench",
    payload: payloadBytes > 0 ? { n, pad: "x".repeat(payloadBytes) } : { n },
  };
  return JSON.stringify({ op: "append", event });
}

function sleep(ms: number) {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}
