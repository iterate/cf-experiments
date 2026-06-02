import { newWebSocketRpcSession, type RpcStub } from "capnweb";
import type { StreamRpc } from "../stream-types.js";

export type StreamBrowserConnectionStatus = "connecting" | "connected" | "closed" | "error";

/** Browser stream client. CapnWeb owns the WebSocket and queues sends while it is connecting. */
export type StreamBrowserClient = Disposable & {
  rpc: RpcStub<StreamRpc>;
};

/** Connects browser JavaScript to one stream URL over CaptainWeb-WebSocket. */
export function withStream(args: {
  url: string | URL;
  onConnectionStatusChange?: (status: StreamBrowserConnectionStatus) => void;
}): StreamBrowserClient {
  const url = new URL(args.url);
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";

  const webSocket = new WebSocket(url.toString());
  args.onConnectionStatusChange?.("connecting");
  webSocket.addEventListener("open", () => args.onConnectionStatusChange?.("connected"));
  webSocket.addEventListener("close", () => args.onConnectionStatusChange?.("closed"));
  webSocket.addEventListener("error", () => args.onConnectionStatusChange?.("error"));

  const rpc = newWebSocketRpcSession<StreamRpc>(webSocket);
  return {
    rpc,
    [Symbol.dispose]() {
      rpc[Symbol.dispose]();
    },
  };
}
