import {
  streamConnectionFromWebSocket,
  toWebSocketUrl,
  type StreamConnection,
} from "../connection.js";

export type StreamBrowserConnectionStatus = "connecting" | "connected" | "closed" | "error";

/** Connects browser JavaScript to one stream URL over capnweb-WebSocket. */
export async function connectStream(args: {
  url: string | URL;
  onConnectionStatusChange?: (
    status: StreamBrowserConnectionStatus,
    error: string | undefined,
  ) => void;
}): Promise<StreamConnection> {
  const webSocket = new WebSocket(toWebSocketUrl(args.url));
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
  return streamConnectionFromWebSocket(webSocket);
}
