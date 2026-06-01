/**
 * Cap'n Web ↔ Workers RPC: DO stubs and service binding stubs.
 *
 *   pnpm dev   # starts upstream + main; use printed URL
 *   WORKER_URL=http://localhost:8787 pnpm test:stubs
 */
import { newWebSocketRpcSession } from "capnweb";
import { describe, expect, it } from "vitest";

interface PingDoApi {
  ping(name: string): Promise<{ message: "pong"; name: string; incarnationId: string }>;
}

interface DoRelayApi {
  getDo(name: string): PingDoApi;
  callPing(stub: PingDoApi, name: string): Promise<{ message: "pong"; name: string; incarnationId: string }>;
}

interface EchoApi {
  echo(label: string): Promise<{ label: string; origin: string; message: string }>;
}

interface ServiceRelayApi {
  getUpstream(): EchoApi;
  callEcho(stub: EchoApi, label: string): Promise<{ label: string; origin: string; message: string }>;
}

interface GreeterApi {
  getDo(name: string): PingDoApi;
  getUpstream(): EchoApi;
}

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";

describe(`DO stub across Cap'n Web @ ${workerUrl}`, () => {
  it("uses PING_DO.getByName as the Cap'n Web session root", async () => {
    const name = `do-root-${crypto.randomUUID()}`;
    using stub = newWebSocketRpcSession<PingDoApi>(
      toWebSocketUrl(`${workerUrl}/do-stub?name=${encodeURIComponent(name)}`),
    );

    const result = await stub.ping(name);
    expect(result.message).toBe("pong");
    expect(result.name).toBe(name);
    expect(result.incarnationId).toMatch(/^[0-9a-f-]{36}$/);
  }, 30_000);

  it("returns a DO stub from an RpcTarget method", async () => {
    const name = `do-returned-${crypto.randomUUID()}`;
    using relay = newWebSocketRpcSession<DoRelayApi>(toWebSocketUrl(`${workerUrl}/do-relay`));

    using stub = relay.getDo(name);
    const result = await stub.ping(name);
    expect(result.message).toBe("pong");
    expect(result.name).toBe(name);
  }, 30_000);

  it("accepts a DO stub passed back into an RpcTarget method", async () => {
    const name = `do-passed-${crypto.randomUUID()}`;
    using relay = newWebSocketRpcSession<DoRelayApi>(toWebSocketUrl(`${workerUrl}/do-relay`));

    using stub = relay.getDo(name);
    const result = await relay.callPing(stub, name);
    expect(result.message).toBe("pong");
    expect(result.name).toBe(name);
  }, 30_000);

  it("nested DO from GreeterEntrypoint.getDo() — needs experimental compat flag", async () => {
    const label = `greeter-${crypto.randomUUID()}`;
    const name = `do-nested-${crypto.randomUUID()}`;
    using greeter = newWebSocketRpcSession<GreeterApi>(
      toWebSocketUrl(`${workerUrl}/entrypoint?label=${encodeURIComponent(label)}`),
    );

    using stub = greeter.getDo(name);
    const outcome = await stub.ping(name).then(
      (value) => ({ ok: true as const, value }),
      (error: unknown) => ({ ok: false as const, error: String(error) }),
    );

    if (outcome.ok) {
      expect(outcome.value.message).toBe("pong");
      console.log("nested DO: works (experimental compat flag enabled)");
      return;
    }

    expect(
      outcome.error.includes("DurableObjectClass serialization requires the 'experimental' compat flag") ||
        outcome.error.includes('Could not serialize object of type "DurableObject"'),
    ).toBe(true);
    console.log(`nested DO: blocked — ${outcome.error}`);
  }, 30_000);
});

describe(`service binding stub across Cap'n Web @ ${workerUrl}`, () => {
  it("uses UPSTREAM service binding as the Cap'n Web session root", async () => {
    const label = `svc-root-${crypto.randomUUID()}`;
    using stub = newWebSocketRpcSession<EchoApi>(toWebSocketUrl(`${workerUrl}/service-stub`));

    await expect(stub.echo(label)).resolves.toEqual({
      label,
      origin: "service-binding",
      message: "from-upstream",
    });
  }, 30_000);

  it("returns a service binding stub from an RpcTarget method", async () => {
    const label = `svc-returned-${crypto.randomUUID()}`;
    using relay = newWebSocketRpcSession<ServiceRelayApi>(
      toWebSocketUrl(`${workerUrl}/service-relay`),
    );

    using stub = relay.getUpstream();
    await expect(stub.echo(label)).resolves.toEqual({
      label,
      origin: "service-binding",
      message: "from-upstream",
    });
  }, 30_000);

  it("accepts a service binding stub passed back into an RpcTarget method", async () => {
    const label = `svc-passed-${crypto.randomUUID()}`;
    using relay = newWebSocketRpcSession<ServiceRelayApi>(
      toWebSocketUrl(`${workerUrl}/service-relay`),
    );

    using stub = relay.getUpstream();
    await expect(relay.callEcho(stub, label)).resolves.toEqual({
      label,
      origin: "service-binding",
      message: "from-upstream",
    });
  }, 30_000);

  it("nested service binding from GreeterEntrypoint.getUpstream() — needs experimental compat flag", async () => {
    const label = `greeter-${crypto.randomUUID()}`;
    const echoLabel = `svc-nested-${crypto.randomUUID()}`;
    using greeter = newWebSocketRpcSession<GreeterApi>(
      toWebSocketUrl(`${workerUrl}/entrypoint?label=${encodeURIComponent(label)}`),
    );

    using stub = greeter.getUpstream();
    const outcome = await stub.echo(echoLabel).then(
      (value) => ({ ok: true as const, value }),
      (error: unknown) => ({ ok: false as const, error: String(error) }),
    );

    if (outcome.ok) {
      expect(outcome.value).toEqual({
        label: echoLabel,
        origin: "service-binding",
        message: "from-upstream",
      });
      console.log("nested service binding: works (experimental compat flag enabled)");
      return;
    }

    expect(outcome.error).toContain(
      "ServiceStub serialization requires the 'experimental' compat flag",
    );
    console.log("nested service binding: blocked without experimental flag");
  }, 30_000);
});

function toWebSocketUrl(raw: string) {
  const url = new URL(raw);
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";
  return url.toString();
}
