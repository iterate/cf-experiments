/**
 * Cap'n Web promise pipelining over Workers RPC stubs.
 *
 * Asserts wire frames for chained calls without awaiting intermediate stubs.
 *
 *   WORKER_URL=http://localhost:8787 pnpm test:pipelining
 */
import { newWebSocketRpcSession } from "capnweb";
import { describe, expect, it } from "vitest";

interface GreeterApi {
  ping(): Promise<{ label: string; message: string }>;
  getCapability(name: string): CounterApi;
  getDo(name: string): PingDoApi;
  getUpstream(): EchoApi;
}

interface CounterApi {
  bump(): Promise<{ name: string; count: number }>;
}

interface RelayApi {
  createGreeter(label: string): GreeterApi;
  callPing(greeter: GreeterApi): Promise<{ label: string; message: string }>;
}

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

type CapturedFrame = {
  direction: "out" | "in";
  data: string;
};

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";

describe(`Cap'n Web pipelining @ ${workerUrl}`, () => {
  it("pipelines relay.createGreeter(label).ping() over a WorkerEntrypoint stub", async () => {
    const label = `pipe-entry-${crypto.randomUUID()}`;
    const capture = captureFrames(`${workerUrl}/relay`);

    using relay = newWebSocketRpcSession<RelayApi>(capture.webSocket);
    await expect(relay.createGreeter(label).ping()).resolves.toEqual({ label, message: "pong" });

    expect(conversation(capture.frames)).toMatchObject([
      { direction: "out", data: ["push", ["pipeline", 0, ["createGreeter"], [label]]] },
      { direction: "out", data: ["push", ["pipeline", 1, ["ping"], []]] },
      { direction: "out", data: ["pull", 2] },
      { direction: "in", data: ["resolve", 2, { label, message: "pong" }] },
      { direction: "out", data: ["release", 2, expect.any(Number)] },
    ]);
  }, 30_000);

  it("pipelines relay.callPing(relay.createGreeter(label)) with stub as pipelined arg", async () => {
    const label = `pipe-entry-pass-${crypto.randomUUID()}`;
    const capture = captureFrames(`${workerUrl}/relay`);

    using relay = newWebSocketRpcSession<RelayApi>(capture.webSocket);
    await expect(relay.callPing(relay.createGreeter(label))).resolves.toEqual({
      label,
      message: "pong",
    });

    expect(conversation(capture.frames)).toMatchObject([
      { direction: "out", data: ["push", ["pipeline", 0, ["createGreeter"], [label]]] },
      {
        direction: "out",
        data: ["push", ["pipeline", 0, ["callPing"], [["pipeline", 1]]]],
      },
      { direction: "out", data: ["pull", 2] },
      { direction: "in", data: ["resolve", 2, { label, message: "pong" }] },
      { direction: "out", data: ["release", 2, expect.any(Number)] },
    ]);
  }, 30_000);

  it("pipelines relay.getDo(name).ping(name) over a DO stub", async () => {
    const name = `pipe-do-${crypto.randomUUID()}`;
    const capture = captureFrames(`${workerUrl}/do-relay`);

    using relay = newWebSocketRpcSession<DoRelayApi>(capture.webSocket);
    const result = await relay.getDo(name).ping(name);
    expect(result.message).toBe("pong");
    expect(result.name).toBe(name);

    expect(conversation(capture.frames)).toMatchObject([
      { direction: "out", data: ["push", ["pipeline", 0, ["getDo"], [name]]] },
      { direction: "out", data: ["push", ["pipeline", 1, ["ping"], [name]]] },
      { direction: "out", data: ["pull", 2] },
      {
        direction: "in",
        data: ["resolve", 2, { message: "pong", name, incarnationId: expect.any(String) }],
      },
      { direction: "out", data: ["release", 2, expect.any(Number)] },
    ]);
  }, 30_000);

  it("pipelines relay.callPing(relay.getDo(name), name) with DO stub as pipelined arg", async () => {
    const name = `pipe-do-pass-${crypto.randomUUID()}`;
    const capture = captureFrames(`${workerUrl}/do-relay`);

    using relay = newWebSocketRpcSession<DoRelayApi>(capture.webSocket);
    const result = await relay.callPing(relay.getDo(name), name);
    expect(result.message).toBe("pong");
    expect(result.name).toBe(name);

    expect(conversation(capture.frames)).toMatchObject([
      { direction: "out", data: ["push", ["pipeline", 0, ["getDo"], [name]]] },
      {
        direction: "out",
        data: ["push", ["pipeline", 0, ["callPing"], [["pipeline", 1], name]]],
      },
      { direction: "out", data: ["pull", 2] },
      {
        direction: "in",
        data: ["resolve", 2, { message: "pong", name, incarnationId: expect.any(String) }],
      },
      { direction: "out", data: ["release", 2, expect.any(Number)] },
    ]);
  }, 30_000);

  it("pipelines relay.getUpstream().echo(label) over a service binding stub", async () => {
    const label = `pipe-svc-${crypto.randomUUID()}`;
    const capture = captureFrames(`${workerUrl}/service-relay`);

    using relay = newWebSocketRpcSession<ServiceRelayApi>(capture.webSocket);
    await expect(relay.getUpstream().echo(label)).resolves.toEqual({
      label,
      origin: "service-binding",
      message: "from-upstream",
    });

    expect(conversation(capture.frames)).toMatchObject([
      { direction: "out", data: ["push", ["pipeline", 0, ["getUpstream"], []]] },
      { direction: "out", data: ["push", ["pipeline", 1, ["echo"], [label]]] },
      { direction: "out", data: ["pull", 2] },
      {
        direction: "in",
        data: ["resolve", 2, { label, origin: "service-binding", message: "from-upstream" }],
      },
      { direction: "out", data: ["release", 2, expect.any(Number)] },
    ]);
  }, 30_000);

  it("pipelines relay.callEcho(relay.getUpstream(), label) with service stub as pipelined arg", async () => {
    const label = `pipe-svc-pass-${crypto.randomUUID()}`;
    const capture = captureFrames(`${workerUrl}/service-relay`);

    using relay = newWebSocketRpcSession<ServiceRelayApi>(capture.webSocket);
    await expect(relay.callEcho(relay.getUpstream(), label)).resolves.toEqual({
      label,
      origin: "service-binding",
      message: "from-upstream",
    });

    expect(conversation(capture.frames)).toMatchObject([
      { direction: "out", data: ["push", ["pipeline", 0, ["getUpstream"], []]] },
      {
        direction: "out",
        data: ["push", ["pipeline", 0, ["callEcho"], [["pipeline", 1], label]]],
      },
      { direction: "out", data: ["pull", 2] },
      {
        direction: "in",
        data: ["resolve", 2, { label, origin: "service-binding", message: "from-upstream" }],
      },
      { direction: "out", data: ["release", 2, expect.any(Number)] },
    ]);
  }, 30_000);

  it("pipelines client-side but fails on greeter.getCapability(name).bump()", async () => {
    const label = `pipe-nested-entry-${crypto.randomUUID()}`;
    const counterName = `counter-${crypto.randomUUID()}`;
    const capture = captureFrames(`${workerUrl}/entrypoint?label=${encodeURIComponent(label)}`);

    using greeter = newWebSocketRpcSession<GreeterApi>(capture.webSocket);
    const outcome = await greeter.getCapability(counterName).bump().then(
      (value) => ({ ok: true as const, value }),
      (error: unknown) => ({ ok: false as const, error: String(error) }),
    );

    expect(conversation(capture.frames)).toMatchObject([
      { direction: "out", data: ["push", ["pipeline", 0, ["getCapability"], [counterName]]] },
      { direction: "out", data: ["push", ["pipeline", 1, ["bump"], []]] },
      { direction: "out", data: ["pull", 2] },
      { direction: "in", data: ["reject", 2, expect.any(Array)] },
      { direction: "out", data: ["release", 2, expect.any(Number)] },
    ]);

    if (outcome.ok) {
      expect(outcome.value).toEqual({ name: counterName, count: 1 });
      return;
    }

    expect(outcome.error).toContain(
      "ServiceStub serialization requires the 'experimental' compat flag",
    );
  }, 30_000);

  it("pipelines client-side but fails on greeter.getDo(name).ping(name)", async () => {
    const label = `pipe-nested-do-${crypto.randomUUID()}`;
    const name = `do-${crypto.randomUUID()}`;
    const capture = captureFrames(`${workerUrl}/entrypoint?label=${encodeURIComponent(label)}`);

    using greeter = newWebSocketRpcSession<GreeterApi>(capture.webSocket);
    const outcome = await greeter.getDo(name).ping(name).then(
      (value) => ({ ok: true as const, value }),
      (error: unknown) => ({ ok: false as const, error: String(error) }),
    );

    expect(conversation(capture.frames)).toMatchObject([
      { direction: "out", data: ["push", ["pipeline", 0, ["getDo"], [name]]] },
      { direction: "out", data: ["push", ["pipeline", 1, ["ping"], [name]]] },
      { direction: "out", data: ["pull", 2] },
      { direction: "in", data: ["reject", 2, expect.any(Array)] },
      { direction: "out", data: ["release", 2, expect.any(Number)] },
    ]);

    if (outcome.ok) {
      expect(outcome.value.message).toBe("pong");
      return;
    }

    expect(
      outcome.error.includes("DurableObjectClass serialization requires the 'experimental' compat flag") ||
        outcome.error.includes('Could not serialize object of type "DurableObject"'),
    ).toBe(true);
  }, 30_000);

  it("pipelines client-side but fails on greeter.getUpstream().echo(label)", async () => {
    const label = `pipe-nested-svc-${crypto.randomUUID()}`;
    const echoLabel = `echo-${crypto.randomUUID()}`;
    const capture = captureFrames(`${workerUrl}/entrypoint?label=${encodeURIComponent(label)}`);

    using greeter = newWebSocketRpcSession<GreeterApi>(capture.webSocket);
    const outcome = await greeter.getUpstream().echo(echoLabel).then(
      (value) => ({ ok: true as const, value }),
      (error: unknown) => ({ ok: false as const, error: String(error) }),
    );

    expect(conversation(capture.frames)).toMatchObject([
      { direction: "out", data: ["push", ["pipeline", 0, ["getUpstream"], []]] },
      { direction: "out", data: ["push", ["pipeline", 1, ["echo"], [echoLabel]]] },
      { direction: "out", data: ["pull", 2] },
      { direction: "in", data: ["reject", 2, expect.any(Array)] },
      { direction: "out", data: ["release", 2, expect.any(Number)] },
    ]);

    if (outcome.ok) {
      expect(outcome.value.message).toBe("from-upstream");
      return;
    }

    expect(outcome.error).toContain(
      "ServiceStub serialization requires the 'experimental' compat flag",
    );
  }, 30_000);
});

function conversation(frames: CapturedFrame[]) {
  return frames.map((frame) => ({
    direction: frame.direction,
    data: JSON.parse(frame.data) as unknown,
  }));
}

function captureFrames(httpUrl: string) {
  const frames: CapturedFrame[] = [];
  const webSocket = newCapturingWebSocket(toWebSocketUrl(httpUrl), frames);
  return { frames, webSocket };
}

function newCapturingWebSocket(url: string, frames: CapturedFrame[]): WebSocket {
  const webSocket = new WebSocket(url);
  const send = webSocket.send.bind(webSocket);

  webSocket.send = ((data: Parameters<WebSocket["send"]>[0]) => {
    frames.push({ direction: "out", data: describeWebSocketFrameData(data) });
    return send(data);
  }) as WebSocket["send"];

  webSocket.addEventListener("message", (event) => {
    frames.push({ direction: "in", data: describeWebSocketFrameData(event.data) });
  });

  return webSocket;
}

function describeWebSocketFrameData(data: unknown) {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (ArrayBuffer.isView(data)) return new TextDecoder().decode(data);
  throw new TypeError(`unexpected WebSocket frame data: ${String(data)}`);
}

function toWebSocketUrl(raw: string) {
  const url = new URL(raw);
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";
  return url.toString();
}
