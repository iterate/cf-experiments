import { newWebSocketRpcSession, type RpcStub } from "capnweb";
import type { StreamProcessorRunnerRpc, StreamRpc } from "../stream-types.js";

type FetchEndpoint = (request: Request) => Promise<Response>;

export type StreamClient = AsyncDisposable & {
  rpc: RpcStub<StreamRpc>;
  onWebSocketFrame(
    listener: (frame: {
      direction: "in" | "out";
      data: string;
      byteLength: number;
      timestamp: number;
    }) => void,
  ): Disposable;
};

export type StreamProcessorRunnerClient = AsyncDisposable & {
  rpc: RpcStub<StreamProcessorRunnerRpc>;
};

export type StreamEndpoint = {
  path: string;
  workerUrl?: string | URL;
};

export type WorkerStreamEndpoint = StreamEndpoint & {
  fetch: FetchEndpoint;
};

/** Connects from Node.js using the runtime's global WebSocket. */
export async function connectStreamFromNode(endpoint: StreamEndpoint): Promise<StreamClient> {
  return connectStreamWebSocket(streamWebSocketUrl(endpoint));
}

/** Connects from a Worker or Durable Object using fetch plus a WebSocket upgrade. */
export async function connectStreamFromWorker(
  endpoint: WorkerStreamEndpoint,
): Promise<StreamClient> {
  const webSocket = await openWorkerWebSocket(endpoint.fetch, streamWebSocketUrl(endpoint));
  return streamClientFromWebSocket(webSocket);
}

/** Connects to a stream processor runner from Node.js. Used by end-to-end fixtures. */
export async function connectStreamProcessorRunnerFromNode(
  endpoint: StreamEndpoint,
): Promise<StreamProcessorRunnerClient> {
  const webSocket = new WebSocket(streamProcessorRunnerWebSocketUrl(endpoint));
  await waitForOpen(webSocket);
  return streamProcessorRunnerClientFromWebSocket(webSocket);
}

/** Connects to a stream processor runner from a Worker or Durable Object. */
export async function connectStreamProcessorRunnerFromWorker(
  endpoint: WorkerStreamEndpoint,
): Promise<StreamProcessorRunnerClient> {
  const webSocket = await openWorkerWebSocket(
    endpoint.fetch,
    streamProcessorRunnerWebSocketUrl(endpoint),
  );
  return streamProcessorRunnerClientFromWebSocket(webSocket);
}

function connectStreamWebSocket(url: string) {
  const webSocket = new WebSocket(url);
  return waitForOpen(webSocket).then(() => streamClientFromWebSocket(webSocket));
}

function streamClientFromWebSocket(webSocket: WebSocket): StreamClient {
  const frameListeners = new Set<
    (frame: {
      direction: "in" | "out";
      data: string;
      byteLength: number;
      timestamp: number;
    }) => void
  >();
  const send = webSocket.send.bind(webSocket);
  webSocket.send = ((data: Parameters<WebSocket["send"]>[0]) => {
    emitFrame(frameListeners, "out", data);
    return send(data);
  }) as WebSocket["send"];
  webSocket.addEventListener("message", (event) => emitFrame(frameListeners, "in", event.data));

  const rpc = newWebSocketRpcSession<StreamRpc>(webSocket);
  return {
    rpc,
    onWebSocketFrame(listener) {
      frameListeners.add(listener);
      return {
        [Symbol.dispose]() {
          frameListeners.delete(listener);
        },
      };
    },
    async [Symbol.asyncDispose]() {
      rpc[Symbol.dispose]();
      await closeWebSocket(webSocket);
    },
  };
}

function streamProcessorRunnerClientFromWebSocket(
  webSocket: WebSocket,
): StreamProcessorRunnerClient {
  const rpc = newWebSocketRpcSession<StreamProcessorRunnerRpc>(webSocket);
  return {
    rpc,
    async [Symbol.asyncDispose]() {
      rpc[Symbol.dispose]();
      await closeWebSocket(webSocket);
    },
  };
}

async function openWorkerWebSocket(fetchEndpoint: FetchEndpoint, url: string) {
  const response = await fetchEndpoint(new Request(url, { headers: { Upgrade: "websocket" } }));
  const webSocket = response.webSocket;
  if (webSocket === null) throw new Error("endpoint did not return a WebSocket");
  webSocket.accept();
  return webSocket;
}

function streamWebSocketUrl(endpoint: StreamEndpoint) {
  const url = new URL(endpoint.workerUrl ?? "http://localhost:8787");
  url.pathname = `/stream/${encodeURIComponent(endpoint.path)}`;
  return toWebSocketUrl(url).toString();
}

function streamProcessorRunnerWebSocketUrl(endpoint: StreamEndpoint) {
  const url = new URL(endpoint.workerUrl ?? "http://localhost:8787");
  url.pathname = `/stream-processor-runner/${encodeURIComponent(endpoint.path)}`;
  return toWebSocketUrl(url).toString();
}

function toWebSocketUrl(url: URL) {
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";
  return url;
}

function waitForOpen(webSocket: WebSocket) {
  if (webSocket.readyState === WebSocket.OPEN) return Promise.resolve();
  return new Promise<void>((resolve, reject) => {
    webSocket.addEventListener("open", () => resolve(), { once: true });
    webSocket.addEventListener("error", () => reject(new Error("WebSocket connection failed")), {
      once: true,
    });
  });
}

function closeWebSocket(webSocket: WebSocket) {
  if (webSocket.readyState === WebSocket.CLOSED) return Promise.resolve();
  return new Promise<void>((resolve) => {
    webSocket.addEventListener("close", () => resolve(), { once: true });
    webSocket.close();
  });
}

function emitFrame(
  listeners: Set<
    (frame: {
      direction: "in" | "out";
      data: string;
      byteLength: number;
      timestamp: number;
    }) => void
  >,
  direction: "in" | "out",
  data: unknown,
) {
  if (listeners.size === 0) return;
  const text = describeWebSocketFrameData(data);
  const frame = {
    direction,
    data: text,
    byteLength: new TextEncoder().encode(text).byteLength,
    timestamp: Date.now(),
  };
  for (const listener of listeners) listener(frame);
}

function describeWebSocketFrameData(data: unknown) {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (ArrayBuffer.isView(data)) return new TextDecoder().decode(data);
  throw new TypeError(`unexpected WebSocket frame data: ${String(data)}`);
}
