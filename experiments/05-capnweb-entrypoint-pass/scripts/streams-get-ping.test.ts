/**
 * Exact repros for:
 *   await streams.get("/some/stream").ping()
 *
 * Same client call, two server implementations, two different errors:
 *
 *   /streams-do   — get() returns env.STREAMS.getByName(path)
 *   /streams      — get() returns ctx.exports.StreamEntrypoint({ props: { path } })
 *
 *   WORKER_URL=http://localhost:8787 pnpm test:streams-get-ping
 */
import { newWebSocketRpcSession } from "capnweb";
import { describe, expect, it } from "vitest";

interface StreamDoApi {
  ping(): Promise<{ message: "pong"; incarnationId: string }>;
}

interface StreamEntrypointApi {
  ping(): Promise<{ path: string; message: "pong" }>;
}

interface StreamsApi {
  get(path: string): StreamDoApi | StreamEntrypointApi;
}

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";

describe(`streams.get(path).ping() @ ${workerUrl}`, () => {
  it("DurableObject error: get() returns env.STREAMS.getByName(path)", async () => {
    const path = "/some/stream";
    using streams = newWebSocketRpcSession<StreamsApi>(toWebSocketUrl(`${workerUrl}/streams-do`));

    const outcome = await streams.get(path).ping().then(
      (value) => ({ ok: true as const, value }),
      (error: unknown) => ({ ok: false as const, error: String(error) }),
    );

    if (outcome.ok) {
      expect(outcome.value).toMatchObject({ message: "pong" });
      console.log("unexpected success — experimental flag may be enabled");
      return;
    }

    expect(outcome.error).toContain('Could not serialize object of type "DurableObject"');
    console.log(`DO stub: ${outcome.error}`);
  }, 30_000);

  it("ServiceStub error: get() returns ctx.exports.StreamEntrypoint({ props: { path } })", async () => {
    const path = "/some/stream";
    using streams = newWebSocketRpcSession<StreamsApi>(toWebSocketUrl(`${workerUrl}/streams`));

    const outcome = await streams.get(path).ping().then(
      (value) => ({ ok: true as const, value }),
      (error: unknown) => ({ ok: false as const, error: String(error) }),
    );

    if (outcome.ok) {
      expect(outcome.value).toMatchObject({ path, message: "pong" });
      console.log("unexpected success — experimental flag may be enabled");
      return;
    }

    expect(outcome.error).toContain(
      "ServiceStub serialization requires the 'experimental' compat flag",
    );
    console.log(`named entrypoint stub: ${outcome.error}`);
  }, 30_000);
});

function toWebSocketUrl(raw: string) {
  const url = new URL(raw);
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";
  return url.toString();
}
