/**
 * Cap'n Web ↔ Workers RPC interop: can we pass a named WorkerEntrypoint stub
 * across a Cap'n Web session?
 *
 * Finding (2026-05-27): yes, all three shapes work in Miniflare and production.
 * ctx.exports.GreeterEntrypoint({ props }) stubs proxy correctly when used as the
 * Cap'n Web root, returned from RpcTarget methods, or passed back as RPC args.
 *
 *   pnpm dev   # in another terminal; use the "Ready on ..." URL
 *   WORKER_URL=http://localhost:8788 pnpm test
 *
 * Deployed:
 *   WORKER_URL=https://05-capnweb-entrypoint-pass.iterate-dev-preview.workers.dev pnpm test
 */
import { newWebSocketRpcSession } from "capnweb";
import { describe, expect, it } from "vitest";

interface GreeterApi {
  ping(): Promise<{ label: string; message: string }>;
  getCapability(name: string): CounterApi;
}

interface CounterApi {
  bump(): Promise<{ name: string; count: number }>;
}

interface RelayApi {
  createGreeter(label: string): GreeterApi;
  callPing(greeter: GreeterApi): Promise<{ label: string; message: string }>;
}

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";

describe(`WorkerEntrypoint across Cap'n Web @ ${workerUrl}`, () => {
  it("uses ctx.exports.GreeterEntrypoint as the Cap'n Web session root", async () => {
    const label = `root-${crypto.randomUUID()}`;
    using greeter = newWebSocketRpcSession<GreeterApi>(
      toWebSocketUrl(`${workerUrl}/entrypoint?label=${encodeURIComponent(label)}`),
    );

    await expect(greeter.ping()).resolves.toEqual({ label, message: "pong" });
  }, 30_000);

  it("returns a GreeterEntrypoint stub from an RpcTarget method", async () => {
    const label = `returned-${crypto.randomUUID()}`;
    using relay = newWebSocketRpcSession<RelayApi>(toWebSocketUrl(`${workerUrl}/relay`));

    using greeter = relay.createGreeter(label);
    await expect(greeter.ping()).resolves.toEqual({ label, message: "pong" });
  }, 30_000);

  it("accepts a GreeterEntrypoint stub passed back into an RpcTarget method", async () => {
    const label = `passed-${crypto.randomUUID()}`;
    using relay = newWebSocketRpcSession<RelayApi>(toWebSocketUrl(`${workerUrl}/relay`));

    using greeter = relay.createGreeter(label);
    await expect(relay.callPing(greeter)).resolves.toEqual({ label, message: "pong" });
  }, 30_000);

  it("nested ctx.exports from getCapability() — needs experimental compat flag", async () => {
    const label = `nested-${crypto.randomUUID()}`;
    const counterName = `counter-${crypto.randomUUID()}`;
    using greeter = newWebSocketRpcSession<GreeterApi>(
      toWebSocketUrl(`${workerUrl}/entrypoint?label=${encodeURIComponent(label)}`),
    );

    using counter = greeter.getCapability(counterName);
    const outcome = await counter.bump().then(
      (value) => ({ ok: true as const, value }),
      (error: unknown) => ({ ok: false as const, error: String(error) }),
    );

    if (outcome.ok) {
      expect(outcome.value).toEqual({ name: counterName, count: 1 });
      console.log("nested entrypoint: works (experimental compat flag enabled in wrangler.jsonc)");
      return;
    }

    expect(outcome.error).toContain(
      "ServiceStub serialization requires the 'experimental' compat flag",
    );
    console.log(
      "nested entrypoint: blocked without experimental flag (default on deploy; see README)",
    );
  }, 30_000);
});

function toWebSocketUrl(raw: string) {
  const url = new URL(raw);
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";
  return url.toString();
}
