import { newWebSocketRpcSession, type RpcStub } from "capnweb";
import type { StreamRpc } from "../stream-types.js";

export type StreamBrowserConnectionStatus = "connecting" | "connected" | "closed" | "error";

/** Browser stream client. CapnWeb owns the WebSocket and queues sends while it is connecting. */
export type StreamBrowserClient = Disposable & {
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

/** Connects browser JavaScript to one stream URL over capnweb-WebSocket. */
export function connectStream(args: {
  url: string | URL;
  onConnectionStatusChange?: (
    status: StreamBrowserConnectionStatus,
    error: string | undefined,
  ) => void;
}): StreamBrowserClient {
  const url = new URL(args.url);
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";

  const frameListeners = new Set<
    (frame: {
      direction: "in" | "out";
      data: string;
      byteLength: number;
      timestamp: number;
    }) => void
  >();
  const webSocket = new WebSocket(url.toString());
  const send = webSocket.send.bind(webSocket);
  webSocket.send = ((data: Parameters<WebSocket["send"]>[0]) => {
    emitFrame(frameListeners, "out", data);
    return send(data);
  }) as WebSocket["send"];
  args.onConnectionStatusChange?.("connecting", undefined);
  webSocket.addEventListener("open", () =>
    args.onConnectionStatusChange?.("connected", undefined),
  );
  webSocket.addEventListener("close", (event) =>
    args.onConnectionStatusChange?.(
      "closed",
      event.reason === ""
        ? `WebSocket closed with code ${event.code}`
        : `WebSocket closed with code ${event.code}: ${event.reason}`,
    ),
  );
  webSocket.addEventListener("error", () =>
    args.onConnectionStatusChange?.("error", "WebSocket error"),
  );
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
    [Symbol.dispose]() {
      rpc[Symbol.dispose]();
      webSocket.close();
    },
  };
}

export const withStream = connectStream;

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
