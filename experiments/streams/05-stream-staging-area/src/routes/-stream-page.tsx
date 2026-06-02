import { useEffect, useState } from "react";
import { RpcTarget } from "capnweb";
import { useNavigate } from "@tanstack/react-router";
import type { StreamEvent } from "@cf-experiments/shared/event";
import { withStream, type StreamBrowserConnectionStatus } from "../client-libraries/stream-browser.js";
import type { SubscriptionRpcTarget } from "../stream-types.js";

export function StreamPage({ streamPath }: { streamPath: string }) {
  const navigate = useNavigate();
  const [draftPath, setDraftPath] = useState(streamPath);
  const [events, setEvents] = useState<StreamEvent[]>([]);
  const [connectionStatus, setConnectionStatus] = useState<
    StreamBrowserConnectionStatus | "subscribing" | "subscribed"
  >("connecting");

  const normalizedDraftPath = normalizeStreamPath(draftPath);
  const showGoToStream = normalizedDraftPath !== streamPath;

  useEffect(() => {
    setDraftPath(streamPath);
  }, [streamPath]);

  useEffect(() => {
    let active = true;
    setEvents([]);
    setConnectionStatus("connecting");

    const streamUrl = new URL(
      `/stream/${encodeURIComponent(streamPath)}`,
      window.location.href,
    );
    const subscriptionRpcTarget = new BrowserSubscriptionRpcTarget((batch) => {
      if (active) setEvents((existing) => [...existing, ...batch]);
    });
    const stream = withStream({
      url: streamUrl,
      onConnectionStatusChange(status) {
        if (active) setConnectionStatus(status);
      },
    });

    setConnectionStatus("subscribing");
    void stream.rpc
      .initInboundSubscription({ subscriptionRpcTarget })
      .then(() => {
        if (active) setConnectionStatus("subscribed");
      })
      .catch(() => {
        if (active) setConnectionStatus("error");
      });

    return () => {
      active = false;
      stream[Symbol.dispose]();
    };
  }, [streamPath]);

  return (
    <main
      style={{
        minHeight: "100vh",
        background: "#f6f7f8",
        color: "#16181d",
        fontFamily:
          'Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
      }}
    >
      <form
        onSubmit={(event) => {
          event.preventDefault();
          if (normalizedDraftPath === "/") {
            void navigate({ href: "/streams/" });
          } else {
            void navigate({
              to: "/streams/$",
              params: { _splat: normalizedDraftPath.slice(1) },
            });
          }
        }}
        style={{
          alignItems: "center",
          background: "#ffffff",
          borderBottom: "1px solid #d8dde4",
          display: "grid",
          gap: 12,
          gridTemplateColumns: "auto minmax(160px, 1fr) auto auto",
          padding: "12px 16px",
        }}
      >
        <label htmlFor="stream-path" style={{ fontSize: 13, fontWeight: 650 }}>
          Stream
        </label>
        <input
          id="stream-path"
          value={draftPath}
          onChange={(event) => setDraftPath(event.currentTarget.value)}
          style={{
            border: "1px solid #bac2cf",
            borderRadius: 6,
            font: "14px ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
            minWidth: 0,
            padding: "8px 10px",
          }}
        />
        {showGoToStream ? (
          <button
            type="submit"
            style={{
              background: "#1f6feb",
              border: 0,
              borderRadius: 6,
              color: "#ffffff",
              cursor: "pointer",
              fontSize: 13,
              fontWeight: 650,
              padding: "9px 12px",
              whiteSpace: "nowrap",
            }}
          >
            Go to stream
          </button>
        ) : null}
        <output
          style={{
            color: connectionStatus === "error" ? "#b42318" : "#344054",
            font: "13px ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
            justifySelf: "end",
            textTransform: "uppercase",
          }}
        >
          {connectionStatus}
        </output>
      </form>
      <pre
        style={{
          font: "13px/1.5 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
          margin: 0,
          overflow: "auto",
          padding: 16,
          whiteSpace: "pre-wrap",
          wordBreak: "break-word",
        }}
      >
        {events.length === 0
          ? "[]"
          : events.map((event) => JSON.stringify(event, null, 2)).join("\n")}
      </pre>
    </main>
  );
}

class BrowserSubscriptionRpcTarget extends RpcTarget implements SubscriptionRpcTarget {
  readonly #consumeBatch: (batch: StreamEvent[]) => void;

  constructor(consumeBatch: (batch: StreamEvent[]) => void) {
    super();
    this.#consumeBatch = consumeBatch;
  }

  consumeEvents(args: { events: StreamEvent[] }): undefined {
    this.#consumeBatch(args.events);
  }
}

function normalizeStreamPath(path: string) {
  const trimmed = path.trim();
  if (trimmed === "") return "/";
  if (trimmed.startsWith("/")) return trimmed;
  return `/${trimmed}`;
}
