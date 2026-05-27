import type { StreamEventInput } from "@cf-experiments/shared/event";
import { describe, expect, it } from "vitest";
import {
  connectCleanCapnwebStream,
  connectCleanOrpcStream,
  connectCleanRawwsStream,
  connectCleanStream,
  type CleanStreamClient,
  type CleanStreamEndpoint,
  type CleanStreamTransport,
} from "../src/clean/client.js";

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";
const transports = [
  { transport: "capnweb", connect: connectCleanCapnwebStream },
  { transport: "orpc", connect: connectCleanOrpcStream },
  { transport: "rawws", connect: connectCleanRawwsStream },
] as const satisfies readonly {
  transport: CleanStreamTransport;
  connect: (endpoint: CleanStreamEndpoint) => Promise<CleanStreamClient>;
}[];

type SmokeResult = {
  transport: CleanStreamTransport;
  matched: boolean;
  appended: unknown;
  delivered: unknown;
};

function cleanStreamUrl(path: string) {
  return new URL(`/clean/${path}`, workerUrl);
}

describe("clean stream transport comparison", () => {
  for (const { transport, connect } of transports) {
    it(`${transport} client appends and subscribes through the URL endpoint`, async () => {
      const path = `clean-${transport}-${crypto.randomUUID()}`;
      const event: StreamEventInput = {
        type: "test.clean-stream.url-client",
        payload: { transport, path },
      };

      await using subscriber = await connect({ url: cleanStreamUrl(path) });
      await using publisher = await connect({ url: cleanStreamUrl(path) });
      await using subscription = await subscriber.subscribe();

      const appended = await publisher.append(event);
      const delivered = await subscription.read();

      expect(appended).toMatchObject({
        type: event.type,
        payload: event.payload,
        offset: 1,
        createdAt: expect.any(String),
      });
      expect(delivered).toEqual(appended);
    });

    it(`${transport} client works from a Durable Object with fetch input`, async () => {
      const path = `clean-fetch-${transport}-${crypto.randomUUID()}`;
      const url = new URL("/clean-client-smoke", workerUrl);
      url.searchParams.set("stream", path);
      url.searchParams.set("transport", transport);

      const response = await fetch(url);
      expect(response.ok).toBe(true);
      const result = (await response.json()) as SmokeResult;

      expect(result).toMatchObject({
        transport,
        matched: true,
        appended: {
          type: "test.clean-stream.fetch-client",
          offset: 1,
          createdAt: expect.any(String),
        },
      });
      expect(result.delivered).toEqual(result.appended);
    });
  }

  it("generic clean client dispatches to the selected transport", async () => {
    const path = `clean-generic-${crypto.randomUUID()}`;
    const event: StreamEventInput = {
      type: "test.clean-stream.generic-client",
      payload: { path },
    };

    await using subscriber = await connectCleanStream({
      transport: "rawws",
      endpoint: { url: cleanStreamUrl(path) },
    });
    await using publisher = await connectCleanStream({
      transport: "rawws",
      endpoint: { url: cleanStreamUrl(path) },
    });
    await using subscription = await subscriber.subscribe();

    const appended = await publisher.append(event);
    expect(await subscription.read()).toEqual(appended);
  });

  it("requires an explicit clean stream transport", async () => {
    const path = `clean-${crypto.randomUUID()}`;
    const response = await fetch(cleanStreamUrl(path));

    expect(response.status).toBe(400);
    expect(await response.text()).toContain("transport must be capnweb, orpc, or rawws");
  });
});
