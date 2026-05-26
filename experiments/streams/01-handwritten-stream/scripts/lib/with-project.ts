/**
 * Cap'n Web test fixture: typed ProjectCapability RPC + recorded WebSocket frames.
 * Used by vitest scripts in this experiment.
 *
 * `using` vs `await using`:
 * - `using` — sync [Symbol.dispose] at scope exit
 * - `await using` — async teardown; use here because we await WebSocket open/close
 *
 *   await using fixture = await withProject({ projectId: "vitest" });
 *   await fixture.rpc.streams.get("foo").append({ type: "test" });
 *   fixture.printWire(); // round trips + timings
 */

import { newWebSocketRpcSession, type RpcStub } from "capnweb";
import type { ProjectCapability } from "../../src/worker.js";
import { analyzeCapnwebWire, formatCapnwebWire, type WireAnalysis } from "./capnweb-wire.js";

const defaultWorkerUrl = process.env.WORKER_URL ?? "http://localhost:8787";

export type WsMessage = {
  direction: "out" | "in";
  data: string;
  /** ms since recording started (performance.now()) */
  tMs: number;
};

export type ParsedWsMessage = {
  direction: WsMessage["direction"];
  data: unknown;
};

export type ProjectFixture = AsyncDisposable & {
  rpc: RpcStub<ProjectCapability>;
  wsMessages: WsMessage[];
  parsedWsMessages(): ParsedWsMessage[];
  wireAnalysis(): WireAnalysis;
  formatWire(): string;
  printWire(): string;
};

export type WithProjectOptions = {
  projectId: string;
  workerUrl?: string;
  log?: Pick<Console, "log">;
  /** print wire timeline on fixture dispose (default false) */
  printWireOnDispose?: boolean;
};

export async function withProject({
  projectId,
  workerUrl = defaultWorkerUrl,
  log = console,
  printWireOnDispose = false,
}: WithProjectOptions): Promise<ProjectFixture> {
  const url = toWebSocketUrl(workerUrl, projectId);
  log.log(`[with-project] connecting ${url}`);

  const startedAt = performance.now();
  const wsMessages: WsMessage[] = [];
  const webSocket = newRecordingWebSocket(url, wsMessages, startedAt);
  await waitForWebSocketOpen(webSocket);
  log.log(`[with-project] connected projectId=${projectId}`);

  const rpc = newWebSocketRpcSession<ProjectCapability>(webSocket);

  const fixture: ProjectFixture = {
    rpc,
    wsMessages,
    parsedWsMessages: () =>
      wsMessages.map((frame) => ({
        direction: frame.direction,
        data: JSON.parse(frame.data) as unknown,
      })),
    wireAnalysis: () => analyzeCapnwebWire(wsMessages),
    formatWire: () => formatCapnwebWire(wsMessages),
    printWire: () => {
      const report = fixture.formatWire();
      log.log(report);
      return report;
    },
    async [Symbol.asyncDispose]() {
      log.log(`[with-project] disconnecting projectId=${projectId}`);
      if (printWireOnDispose) fixture.printWire();
      rpc[Symbol.dispose]();
      await closeWebSocket(webSocket);
      const { resultWaits, waves, spanMs } = fixture.wireAnalysis();
      log.log(
        `[with-project] disconnected projectId=${projectId} (${wsMessages.length} frames, ${resultWaits.length} pulled results, ${waves.length} latency waves, ${spanMs.toFixed(1)}ms span)`,
      );
    },
  };

  return fixture;
}

function newRecordingWebSocket(url: string, wsMessages: WsMessage[], startedAt: number) {
  const webSocket = new WebSocket(url);
  const send = webSocket.send.bind(webSocket);

  webSocket.send = ((data: Parameters<WebSocket["send"]>[0]) => {
    wsMessages.push({
      direction: "out",
      data: describeWebSocketFrameData(data),
      tMs: performance.now() - startedAt,
    });
    return send(data);
  }) as WebSocket["send"];

  webSocket.addEventListener("message", (event) => {
    wsMessages.push({
      direction: "in",
      data: describeWebSocketFrameData(event.data),
      tMs: performance.now() - startedAt,
    });
  });

  return webSocket;
}

function describeWebSocketFrameData(data: unknown) {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (ArrayBuffer.isView(data)) return new TextDecoder().decode(data);
  throw new TypeError(`unexpected WebSocket frame data: ${String(data)}`);
}

function toWebSocketUrl(raw: string, projectId: string) {
  const url = new URL(raw);
  url.searchParams.set("projectId", projectId);
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";
  return url.toString();
}

function waitForWebSocketOpen(webSocket: WebSocket) {
  if (webSocket.readyState === WebSocket.OPEN) return Promise.resolve();
  return new Promise<void>((resolve, reject) => {
    webSocket.addEventListener("open", () => resolve(), { once: true });
    webSocket.addEventListener(
      "error",
      () => reject(new Error("WebSocket connection failed")),
      { once: true },
    );
  });
}

function closeWebSocket(webSocket: WebSocket) {
  if (webSocket.readyState === WebSocket.CLOSED) return Promise.resolve();
  return new Promise<void>((resolve) => {
    webSocket.addEventListener("close", () => resolve(), { once: true });
    webSocket.close();
  });
}
