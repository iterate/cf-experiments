// Node runtime over a REAL WebSocket subscription: hosts a stream processor
// in-process via withStreamProcessor against a running worker. Gated like the
// other e2e — set STREAM_STAGING_E2E=true (and WORKER_URL) with `wrangler dev`
// running. Typecheck-verified always.

import { describe, expect, it } from "vitest";
import { connectStreamFromNode } from "./client-libraries/stream-node-worker.js";
import { withStreamProcessor, type Snapshot } from "./stream-processor.js";
// The SAME processor the DO (outbound) and the browser tab (inbound) run.
import { echo, type EchoState } from "./demo-processor.js";

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";
const e2eIt = process.env.STREAM_STAGING_E2E === "true" ? it : it.skip;

describe("node-hosted stream processor (e2e)", () => {
  e2eIt("hosts echo in-process over an inbound subscription", async () => {
    const path = `node-echo-${crypto.randomUUID()}`;
    await using connection = await connectStreamFromNode({ path, workerUrl });

    let saved: Snapshot<EchoState> | undefined;
    await using _runner = await withStreamProcessor({
      connection,
      subscriptionKey: "node-echo",
      processor: echo,
      deps: undefined,
      storage: { load: () => saved, save: (snapshot) => void (saved = snapshot) },
    });

    await connection.rpc.append({ event: { type: "test.processor.input", payload: { path } } });

    // echo appends test.processor.output back into the stream; poll for it.
    const startedAt = Date.now();
    let outputs: number[] = [];
    while (Date.now() - startedAt < 4_000) {
      const events = await connection.rpc.getEvents({});
      outputs = events.filter((e) => e.type === "test.processor.output").map((e) => e.offset);
      if (outputs.length > 0) break;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    expect(outputs.length).toBeGreaterThan(0);
    expect(saved?.state.seen).toBe(1);
  });

  e2eIt("reconnects and resumes from its snapshot without reprocessing", async () => {
    const path = `node-resume-${crypto.randomUUID()}`;
    let saved: Snapshot<EchoState> | undefined;
    const storage = { load: () => saved, save: (s: Snapshot<EchoState>) => void (saved = s) };

    // Session 1: process one input, then drop the connection + runner.
    {
      await using connection = await connectStreamFromNode({ path, workerUrl });
      await using _runner = await withStreamProcessor({
        connection, subscriptionKey: "resume", processor: echo, deps: undefined, storage,
      });
      await connection.rpc.append({ event: { type: "test.processor.input", payload: { path } } });
      await waitUntil(() => saved?.state.seen === 1, 5_000);
    }
    const offsetAfterFirst = saved?.offset ?? -1;
    expect(saved?.state.seen).toBe(1);

    // Session 2: fresh connection + fresh runner, SAME persisted snapshot. It must
    // resume (subscribe afterOffset = stored offset), not reprocess the first input.
    {
      await using connection = await connectStreamFromNode({ path, workerUrl });
      await using _runner = await withStreamProcessor({
        connection, subscriptionKey: "resume", processor: echo, deps: undefined, storage,
      });
      await connection.rpc.append({ event: { type: "test.processor.input", payload: { path } } });
      await waitUntil(() => (saved?.state.seen ?? 0) === 2, 5_000);
    }
    expect(saved?.state.seen).toBe(2); // resumed from 1; second input counted exactly once
    expect(saved?.offset ?? -1).toBeGreaterThan(offsetAfterFirst);
  });
});

async function waitUntil(predicate: () => boolean, timeoutMs: number) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error("waitUntil timed out");
}
