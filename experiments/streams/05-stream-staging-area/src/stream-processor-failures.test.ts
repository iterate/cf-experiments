// Failure-condition tests for the runner, in-process (deterministic, no worker).
// Proves: resume-from-snapshot + dedup, idempotent re-delivery, and durable
// at-least-once via blockProcessorUntil when a crash happens before the checkpoint.

import { describe, expect, it } from "vitest";
import type { StreamEvent } from "@cf-experiments/shared/event";
import { createProcessorRunner, implementProcessor, type Snapshot, type StreamPort } from "./stream-processor.js";
import { echo, echoContract, type EchoState } from "./demo-processor.js";

const iso = (ms = 0) => new Date(ms).toISOString();
const input = (offset: number): StreamEvent => ({ type: "test.processor.input", payload: {}, offset, createdAt: iso() });

function memoryStream() {
  const committed: StreamEvent[] = [];
  let nextOffset = 1000;
  const stream: StreamPort = {
    append: async (e) => {
      const ev: StreamEvent = { ...e, offset: nextOffset++, createdAt: iso(1) };
      committed.push(ev);
      return ev;
    },
  };
  return { stream, committed };
}

describe("failure conditions (in-process runner)", () => {
  it("resumes from a persisted snapshot and dedups already-processed offsets", async () => {
    const { stream, committed } = memoryStream();
    let saved: Snapshot<EchoState> | undefined = { state: { seen: 2 }, offset: 5 };
    const runner = createProcessorRunner({
      processor: echo,
      deps: undefined,
      storage: { load: () => saved, save: (s) => void (saved = s) },
      stream,
    });

    // A re-delivered historical event (offset 4 <= snapshot 5) must be ignored.
    await runner.processEventBatch({ events: [input(4)], headOffset: 6 });
    expect(committed).toHaveLength(0);

    // A genuinely new event resumes from the persisted count.
    await runner.processEventBatch({ events: [input(6)], headOffset: 6 });
    expect(committed).toMatchObject([{ type: "test.processor.output", payload: { seen: 3 } }]);
    expect(saved?.offset).toBe(6);
  });

  it("does not double-process a re-delivered batch (idempotent)", async () => {
    const { stream, committed } = memoryStream();
    let saved: Snapshot<EchoState> | undefined;
    const runner = createProcessorRunner({
      processor: echo,
      deps: undefined,
      storage: { load: () => saved, save: (s) => void (saved = s) },
      stream,
    });

    const batch = { events: [input(1)], headOffset: 1 };
    await runner.processEventBatch(batch);
    await runner.processEventBatch(batch); // exact re-delivery (e.g. after a reconnect)
    expect(committed).toHaveLength(1);
    expect(saved?.state.seen).toBe(1);
  });

  it("durable blockProcessorUntil: a crash before checkpoint reprocesses the event (at-least-once)", async () => {
    let attempts = 0;
    // Durable processor: the side effect is gated by blockProcessorUntil, so the
    // checkpoint must not advance until it succeeds.
    const durable = implementProcessor(echoContract, () => ({
      afterAppend({ event, append, blockProcessorUntil }) {
        if (event.type !== "test.processor.input") return;
        blockProcessorUntil(async () => {
          attempts += 1;
          if (attempts === 1) throw new Error("transient failure before checkpoint");
          append({ type: "test.processor.output", payload: { seen: 0 } });
        });
      },
    }));

    const { stream, committed } = memoryStream();
    let saved: Snapshot<EchoState> | undefined;
    const storage = { load: () => saved, save: (s: Snapshot<EchoState>) => void (saved = s) };

    // Runner 1: the blocker throws, so the batch rejects and nothing is checkpointed.
    const runner1 = createProcessorRunner({ processor: durable, deps: undefined, storage, stream });
    await expect(runner1.processEventBatch({ events: [input(1)], headOffset: 1 })).rejects.toThrow();
    expect(saved).toBeUndefined();
    expect(committed).toHaveLength(0);

    // Runner 2 (restart): the same event is re-delivered; this time the work succeeds.
    const runner2 = createProcessorRunner({ processor: durable, deps: undefined, storage, stream });
    await runner2.processEventBatch({ events: [input(1)], headOffset: 1 });
    expect(committed).toHaveLength(1); // side effect happened exactly once, after retry
    expect(saved?.offset).toBe(1);
    expect(attempts).toBe(2);
  });
});
