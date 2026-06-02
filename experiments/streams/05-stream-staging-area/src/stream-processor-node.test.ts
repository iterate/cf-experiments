// Node runtime over a REAL WebSocket subscription: hosts a stream processor
// in-process via withStreamProcessor against a running worker. Gated like the
// other e2e — set STREAM_STAGING_E2E=true (and WORKER_URL) with `wrangler dev`
// running. Typecheck-verified always.

import { describe, expect, it } from "vitest";
import { z } from "zod";
import { defineProcessorContract } from "@cf-experiments/shared/stream-processors";
import { connectStreamFromNode } from "./client-libraries/stream-node-worker.js";
import { implementProcessor, withStreamProcessor, type Snapshot } from "./stream-processor.js";

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";
const e2eIt = process.env.STREAM_STAGING_E2E === "true" ? it : it.skip;

const echoContract = defineProcessorContract({
  slug: "node.echo",
  version: "0.1.0",
  description: "echo",
  stateSchema: z.object({ seen: z.number().int().min(0).default(0) }),
  initialState: {},
  events: {
    "test.processor.input": { description: "in", payloadSchema: z.unknown() },
    "test.processor.output": { description: "out", payloadSchema: z.object({ seen: z.number() }) },
  },
  consumes: ["test.processor.input"],
  emits: ["test.processor.output"],
  reduce({ state, event }) {
    return event.type === "test.processor.input" ? { seen: state.seen + 1 } : state;
  },
});

const echo = implementProcessor(echoContract, () => ({
  afterAppend({ event, state, append }) {
    if (event.type !== "test.processor.input") return;
    append({ type: "test.processor.output", payload: { seen: state.seen } });
  },
}));

describe("node-hosted stream processor (e2e)", () => {
  e2eIt("hosts echo in-process over an inbound subscription", async () => {
    const path = `node-echo-${crypto.randomUUID()}`;
    await using connection = await connectStreamFromNode({ path, workerUrl });

    let saved: Snapshot<{ seen: number }> | undefined;
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
});
