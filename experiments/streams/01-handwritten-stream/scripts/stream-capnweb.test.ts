import type { StreamEventInput } from "@cf-experiments/shared/event";
import { describe, expect, it } from "vitest";
import { withProject } from "./lib/with-project.js";

describe("handwritten stream capnweb", () => {
  it("pipelines dependent calls without pulling intermediate promises", async () => {
    const path = `stream-${crypto.randomUUID()}`;
    const event: StreamEventInput = { type: "test.append", payload: { path } };

    await using fixture = await withProject({ projectId: "vitest" });

    await fixture.rpc.streams.get(path).append({ event });

    // Cap'n Web promises also behave as stubs for their eventual value:
    // https://github.com/cloudflare/capnweb/blob/main/README.md#rpcpromiset
    //
    // `project.streams.get(path)` creates import #1. We then call `.append(...)`
    // on that unresolved promise, so the client sends a second pipelined call
    // against #1 and only pulls #2 (the append result). Pulling #1 would mean
    // we accidentally waited for the stream capability before appending, adding
    // a latency window.
    const analysis = fixture.wireAnalysis();
    expect(analysis.resultWaits).toHaveLength(1);
    expect(analysis.waves).toHaveLength(1);
    expect(fixture.parsedWsMessages()).not.toContainEqual({
      direction: "out",
      data: ["pull", 1],
    });
    expect(fixture.parsedWsMessages()).toMatchObject([
      { direction: "out", data: ["push", ["pipeline", 0, ["streams", "get"], [path]]] },
      { direction: "out", data: ["push", ["pipeline", 1, ["append"], [{ event }]]] },
      { direction: "out", data: ["pull", 2] },
      {
        direction: "in",
        data: [
          "resolve",
          2,
          {
            type: event.type,
            offset: expect.any(Number),
            createdAt: expect.any(String),
          },
        ],
      },
      { direction: "out", data: ["release", 2, expect.any(Number)] },
    ]);
  });

  it("distinguishes sequential waits from concurrent waits", async () => {
    const sequentialPath = `stream-${crypto.randomUUID()}`;
    const firstConcurrentPath = `stream-${crypto.randomUUID()}`;
    const secondConcurrentPath = `stream-${crypto.randomUUID()}`;

    await using sequential = await withProject({ projectId: "vitest" });
    await sequential.rpc.streams
      .get(sequentialPath)
      .append({ event: { type: "test.sequential", payload: { path: sequentialPath } } });
    expect(await sequential.rpc.streams.get(sequentialPath).count()).toEqual({ kv: 1 });

    // A pulled result is a Cap'n Web `pull -> resolve/reject` protocol wait:
    // https://github.com/cloudflare/capnweb/blob/main/protocol.md#push-and-pull
    //
    // A latency wave groups waits that overlap in time. Two separate `await`s
    // below create two waves: append completes before count is even sent.
    expect(sequential.wireAnalysis().resultWaits).toHaveLength(2);
    expect(sequential.wireAnalysis().waves).toHaveLength(2);

    await using concurrent = await withProject({ projectId: "vitest" });
    const firstAppend = concurrent.rpc.streams
      .get(firstConcurrentPath)
      .append({ event: { type: "test.concurrent", payload: { path: firstConcurrentPath } } });
    const secondAppend = concurrent.rpc.streams
      .get(secondConcurrentPath)
      .append({ event: { type: "test.concurrent", payload: { path: secondConcurrentPath } } });
    await Promise.all([firstAppend, secondAppend]);

    // Both results are pulled, but both pulls are sent before either resolve
    // arrives. On a high-latency link this is the difference between two
    // Atlantic crossings and one crossing with two in-flight requests.
    expect(concurrent.wireAnalysis().resultWaits).toHaveLength(2);
    expect(concurrent.wireAnalysis().waves).toHaveLength(1);
    expect(concurrent.wireAnalysis().waves[0]?.waits).toHaveLength(2);
  });

  it("avoids pulls for unobserved results and for .map() source arrays", async () => {
    const fireAndForgetPath = `stream-${crypto.randomUUID()}`;
    const mapPath = `stream-${crypto.randomUUID()}`;
    const events = [
      { type: "test.map", payload: { n: 1 } },
      { type: "test.map", payload: { n: 2 } },
    ];

    await using fireAndForget = await withProject({ projectId: "vitest" });
    {
      // Cap'n Web only sends `pull` if the application observes the promise
      // (`await`, `.then`, etc). If you truly do not need the result, dispose
      // the promise so the server can release it:
      // https://github.com/cloudflare/capnweb/blob/main/protocol.md#push-and-pull
      // https://github.com/cloudflare/capnweb/blob/main/README.md#automatic-disposal
      using _append = fireAndForget.rpc.streams
        .get(fireAndForgetPath)
        .append({ event: { type: "test.fire-and-forget", payload: { path: fireAndForgetPath } } });
      await new Promise((resolve) => setTimeout(resolve, 0));
    }

    expect(fireAndForget.parsedWsMessages()).toMatchObject([
      {
        direction: "out",
        data: ["push", ["pipeline", 0, ["streams", "get"], [fireAndForgetPath]]],
      },
      {
        direction: "out",
        data: [
          "push",
          [
            "pipeline",
            1,
            ["append"],
            [
              {
                event: { type: "test.fire-and-forget", payload: { path: fireAndForgetPath } },
              },
            ],
          ],
        ],
      },
      { direction: "out", data: ["release", 2, expect.any(Number)] },
    ]);
    expect(fireAndForget.wireAnalysis().resultWaits).toHaveLength(0);
    expect(fireAndForget.wireAnalysis().waves).toHaveLength(0);

    await using mapped = await withProject({ projectId: "vitest" });

    const appended = mapped.rpc.streams.get(mapPath).appendBatch({ events });
    const offsets = await appended.map((event) => event.offset);

    // `.map()` records a synchronous callback and replays it remotely. The docs'
    // array example says this lets you transform an array result without first
    // pulling the whole array back to the client:
    // https://github.com/cloudflare/capnweb/blob/main/README.md#the-magic-map-method
    //
    // The source `appendBatch` result is import #2, but the client pulls only
    // the `remap` result (#3). The returned array contains promise placeholders,
    // so the analyzer waits for the exported element promises (-1, -2) before
    // considering the mapped value usable by application code.
    expect(offsets).toEqual([1, 2]);
    expect(mapped.parsedWsMessages()).toContainEqual({
      direction: "out",
      data: ["push", ["remap", 2, [], [], [["pipeline", 0, ["offset"]]]]],
    });
    expect(mapped.parsedWsMessages()).not.toContainEqual({
      direction: "out",
      data: ["pull", 2],
    });
    expect(mapped.parsedWsMessages()).toContainEqual({
      direction: "out",
      data: ["pull", 3],
    });

    const analysis = mapped.wireAnalysis();
    expect(analysis.resultWaits).toHaveLength(1);
    expect(analysis.waves).toHaveLength(1);
    expect(analysis.resultWaits[0]?.awaitedPromiseIds).toEqual([-1, -2]);
  });
});
