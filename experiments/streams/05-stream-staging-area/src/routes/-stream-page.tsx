import {
  useLayoutEffect,
  useMemo,
  useReducer,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import { ClientOnly, useNavigate } from "@tanstack/react-router";
import { useVirtualizer, type VirtualItem } from "@tanstack/react-virtual";
import {
  createStreamBrowserStore,
  type StreamBrowserSnapshot,
  type StreamBrowserStore,
} from "../client-libraries/stream-browser-store.js";
import {
  type StreamBrowserDatabase,
  type StreamEventRow,
} from "../client-libraries/stream-browser-db.js";
import { useStreamQuery } from "../client-libraries/use-stream-query.js";
import "./-stream-page.css";

export function StreamPage({ streamPath }: { streamPath: string }) {
  return (
    <ClientOnly fallback={<StreamHydrationFallback streamPath={streamPath} />}>
      <HydratedStreamPage streamPath={streamPath} />
    </ClientOnly>
  );
}

export function StreamCompactView({ streamPath }: { streamPath: string }) {
  return (
    <ClientOnly fallback={<StreamHydrationFallback streamPath={streamPath} />}>
      <HydratedStreamCompactView streamPath={streamPath} />
    </ClientOnly>
  );
}

function HydratedStreamPage({ streamPath }: { streamPath: string }) {
  const runtime = useStreamRuntime(streamPath);

  return (
    <StreamPageWithDatabase
      snapshot={runtime.snapshot}
      streamDatabase={runtime.streamDatabase}
      streamStore={runtime.streamStore}
      streamPath={streamPath}
      databaseReady={runtime.countResult.status === "ok"}
      databaseError={runtime.countResult.error}
      databaseStatus={runtime.countResult.status}
      eventCount={runtime.eventCount}
    />
  );
}

function HydratedStreamCompactView({ streamPath }: { streamPath: string }) {
  const runtime = useStreamRuntime(streamPath);

  return (
    <section
      className="stream-page__compact-view"
      aria-label={`Stream ${streamPath}`}
      data-stream-path={streamPath}
    >
      <div className="stream-page__compact-status">
        <span className="stream-page__path-display">{streamPath}</span>
        <output className="stream-page__state" data-testid="stream-status">{runtime.snapshot.connectionStatus}</output>
        <output className="stream-page__state" data-testid="subscription-status">{runtime.snapshot.subscriptionStatus}</output>
        <output className="stream-page__state" data-testid="event-count">{runtime.eventCount}</output>
      </div>
      {runtime.countResult.status !== "ok" ? (
        <StreamLoadingPanel
          message={
            runtime.countResult.status === "error"
              ? `sqlite DB error at ${runtime.streamDatabase.databasePath}: ${runtime.countResult.error?.message ?? "unknown error"}`
              : `opening sqlite DB at ${runtime.streamDatabase.databasePath}`
          }
        />
      ) : (
        <EventRows
          eventCount={runtime.eventCount}
          key={`events:${streamPath}:${runtime.snapshot.clearVersion}`}
          snapshot={runtime.snapshot}
          streamDatabase={runtime.streamDatabase}
          streamPath={streamPath}
          streamStore={runtime.streamStore}
        />
      )}
    </section>
  );
}

function useStreamRuntime(streamPath: string) {
  const streamStore = useMemo(
    () => createStreamBrowserStore({ streamPath }),
    [streamPath],
  );
  const snapshot = useSyncExternalStore(
    streamStore.subscribe,
    streamStore.getSnapshot,
    streamStore.getServerSnapshot,
  );
  const streamDatabase = streamStore.streamDatabase;
  const countResult = useStreamQuery(
    streamDatabase,
    `SELECT COUNT(*) AS count FROM events`,
  );
  const eventCount = Number(countResult.data[0]?.count ?? 0);
  return { countResult, eventCount, snapshot, streamDatabase, streamStore };
}

function StreamHydrationFallback({ streamPath }: { streamPath: string }) {
  return (
    <div className="stream-page__hydrate-frame">
      <div className="stream-page__hydrate">
        <div className="stream-page__spinner" aria-hidden="true" />
        <span>SSR done, hydrating client for {streamPath}</span>
      </div>
    </div>
  );
}

function StreamPageWithDatabase({
  streamPath,
  snapshot,
  streamDatabase,
  streamStore,
  eventCount,
  databaseReady,
  databaseError,
  databaseStatus,
}: {
  streamPath: string;
  snapshot: StreamBrowserSnapshot;
  streamDatabase: StreamBrowserDatabase;
  streamStore: StreamBrowserStore;
  eventCount: number;
  databaseReady: boolean;
  databaseError: Error | undefined;
  databaseStatus: "pending" | "ok" | "error";
}) {

  return (
    <StreamPageLayout
      databaseReady={databaseReady}
      databaseError={databaseError}
      databaseStatus={databaseStatus}
      eventCount={eventCount}
      snapshot={snapshot}
      streamDatabase={streamDatabase}
      streamStore={streamStore}
      streamPath={streamPath}
    />
  );
}

function StreamPageLayout({
  streamPath,
  snapshot,
  streamDatabase,
  streamStore,
  eventCount,
  databaseReady,
  databaseError,
  databaseStatus,
}: {
  streamPath: string;
  snapshot: StreamBrowserSnapshot;
  streamDatabase: StreamBrowserDatabase;
  streamStore: StreamBrowserStore;
  eventCount: number;
  databaseReady: boolean;
  databaseError: Error | undefined;
  databaseStatus: "pending" | "ok" | "error";
}) {
  const [sidebarOpen, setSidebarOpen] = useState(
    () => globalThis.matchMedia("(min-width: 761px)").matches,
  );

  return (
    <main className="stream-page">
      <div className="stream-page__body">
        <StreamSidebar
          className={sidebarOpen ? undefined : "stream-page__sidebar--hidden"}
          eventCount={eventCount}
          key={`sidebar:${streamPath}`}
          snapshot={snapshot}
          streamDatabase={streamDatabase}
          streamStore={streamStore}
          streamPath={streamPath}
          onSidebarOpenChange={setSidebarOpen}
        />
        <div
          className={
            sidebarOpen
              ? "stream-page__main"
              : "stream-page__main stream-page__main--sidebar-hidden"
          }
        >
          {sidebarOpen ? null : (
            <button
              aria-controls="stream-sidebar"
              aria-label="Show sidebar"
              className="stream-page__sidebar-button stream-page__show-sidebar-button"
              title="Show sidebar"
              type="button"
              onClick={() => setSidebarOpen(true)}
            >
              <SidebarIcon />
            </button>
          )}
          {!databaseReady ? (
            <StreamLoadingPanel
              message={
                databaseStatus === "error"
                  ? `client hydrated, sqlite DB error at ${streamDatabase.databasePath}: ${databaseError?.message ?? "unknown error"}`
                  : `client hydrated, opening sqlite DB at ${streamDatabase.databasePath}`
              }
            />
          ) : (
            <EventRows
              eventCount={eventCount}
              key={`events:${streamPath}:${snapshot.clearVersion}`}
              snapshot={snapshot}
              streamDatabase={streamDatabase}
              streamPath={streamPath}
              streamStore={streamStore}
            />
          )}
        </div>
      </div>
    </main>
  );
}

function StreamLoadingPanel({ message }: { message: string }) {
  return (
    <section
      aria-label="Stream events"
      className="stream-page__stream stream-page__stream--loading"
    >
      <div className="stream-page__hydrate">
        <div className="stream-page__spinner" aria-hidden="true" />
        <span>{message}</span>
      </div>
    </section>
  );
}

function EditStreamIcon() {
  return (
    <svg
      aria-hidden
      className="stream-page__edit-icon"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M12 20h9" />
      <path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4Z" />
    </svg>
  );
}

function SidebarIcon() {
  return (
    <svg
      aria-hidden
      className="stream-page__sidebar-icon"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <rect height="18" rx="2" width="18" x="3" y="3" />
      <path d="M9 3v18" />
    </svg>
  );
}

function AppendEventIcon() {
  return (
    <svg
      aria-hidden
      className="stream-page__append-icon"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M22 2 11 13" />
      <path d="m22 2-7 20-4-9-9-4Z" />
    </svg>
  );
}

function StreamTopBar({
  streamPath,
  onSidebarOpenChange,
}: {
  streamPath: string;
  onSidebarOpenChange(open: boolean): void;
}) {
  const navigate = useNavigate();
  const pathInputRef = useRef<HTMLInputElement>(null);
  const [editingPath, setEditingPath] = useState(false);
  const [editedPath, setEditedPath] = useState(streamPath);
  const trimmedDraftPath = editedPath.trim();
  const normalizedDraftPath =
    trimmedDraftPath === ""
      ? "/"
      : trimmedDraftPath.startsWith("/")
        ? trimmedDraftPath
        : `/${trimmedDraftPath}`;
  const pathChanged = normalizedDraftPath !== streamPath;

  useLayoutEffect(() => {
    if (!editingPath) return;
    pathInputRef.current?.focus();
    pathInputRef.current?.select();
  }, [editingPath]);

  function goToDraftPath() {
    if (!pathChanged) {
      setEditingPath(false);
      setEditedPath(streamPath);
      return;
    }
    setEditingPath(false);
    if (normalizedDraftPath === "/") {
      void navigate({ to: "/streams" });
      return;
    }
    void navigate({
      to: "/streams/$",
      params: { _splat: normalizedDraftPath.slice(1) },
    });
  }

  function startEditingPath() {
    setEditedPath(streamPath);
    setEditingPath(true);
  }

  function cancelEditingPath() {
    setEditedPath(streamPath);
    setEditingPath(false);
  }

  return (
    <header className="stream-page__top-bar">
      <button
        aria-controls="stream-sidebar"
        aria-label="Hide sidebar"
        className="stream-page__sidebar-button stream-page__hide-sidebar-button"
        title="Hide sidebar"
        type="button"
        onClick={() => onSidebarOpenChange(false)}
      >
        <SidebarIcon />
      </button>
      <div className="stream-page__controls">
        <div className="stream-page__path-area">
          {editingPath ? (
            <>
              <input
                aria-label="Stream path"
                className="stream-page__input stream-page__path-input"
                id="stream-path"
                ref={pathInputRef}
                value={editedPath}
                onChange={(event) => setEditedPath(event.currentTarget.value)}
                onKeyDown={(event) => {
                  if (event.key === "Escape") {
                    event.preventDefault();
                    cancelEditingPath();
                    return;
                  }
                  if (event.key !== "Enter") return;
                  event.preventDefault();
                  goToDraftPath();
                }}
              />
              <button
                className="stream-page__button stream-page__path-submit"
                disabled={!pathChanged}
                type="button"
                onClick={() => goToDraftPath()}
              >
                Go to stream
              </button>
            </>
          ) : (
            <span className="stream-page__path-display">{streamPath}</span>
          )}
        </div>
        {editingPath ? (
          <button
            className="stream-page__text-link"
            type="button"
            onClick={() => cancelEditingPath()}
          >
            Cancel
          </button>
        ) : (
          <button
            aria-label="Edit stream path"
            className="stream-page__edit-button"
            title="Edit stream path"
            type="button"
            onClick={() => startEditingPath()}
          >
            <EditStreamIcon />
          </button>
        )}
      </div>
    </header>
  );
}

function EventRows({
  streamDatabase,
  eventCount,
  snapshot,
  streamPath,
  streamStore,
}: {
  streamDatabase: StreamBrowserDatabase;
  eventCount: number;
  snapshot: StreamBrowserSnapshot;
  streamPath: string;
  streamStore: StreamBrowserStore;
}) {
  const topScrollAffordanceHeight = 48;
  const parentRef = useRef<HTMLDivElement>(null);
  const footerRef = useRef<HTMLDivElement>(null);
  const settledInitialEndScroll = useRef(false);
  const previousEventCount = useRef(eventCount);
  const userHasMovedAwayFromEnd = useRef(false);
  const [footerHeight, setFooterHeight] = useState(0);
  const [expandedOffsets, setExpandedOffsets] = useState(() => new Set<number>());
  const [scrollState, dispatchScrollState] = useReducer(
    (
      state,
      action:
        | { type: "clear-unread" }
        | { type: "add-unread"; count: number }
        | { type: "set-scroll-position"; scrollPosition: { isAtTop: boolean; isAtEnd: boolean } },
    ) => {
      switch (action.type) {
        case "clear-unread":
          return state.unreadEventCount === 0 ? state : { ...state, unreadEventCount: 0 };
        case "add-unread":
          return {
            ...state,
            unreadEventCount: state.unreadEventCount + action.count,
          };
        case "set-scroll-position":
          return state.scrollPosition.isAtTop === action.scrollPosition.isAtTop &&
            state.scrollPosition.isAtEnd === action.scrollPosition.isAtEnd
            ? state
            : {
                ...state,
                scrollPosition: action.scrollPosition,
                unreadEventCount: action.scrollPosition.isAtEnd ? 0 : state.unreadEventCount,
              };
      }
    },
    {
      scrollPosition: { isAtTop: true, isAtEnd: true },
      unreadEventCount: 0,
    },
  );
  const virtualizer = useVirtualizer({
    count: eventCount,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 40,
    // This stream is append-only, so the virtual index is a stable item key.
    // If this grows older-history prepends later, switch this to a persisted row id.
    getItemKey: (index) => index,
    anchorTo: "end",
    followOnAppend: true,
    paddingStart: topScrollAffordanceHeight,
    paddingEnd: footerHeight,
    scrollEndThreshold: 80,
    overscan: 6,
    onChange(instance) {
      const scrollElement = parentRef.current;
      const distanceFromEnd =
        scrollElement === null
          ? 0
          : scrollElement.scrollHeight - scrollElement.clientHeight - scrollElement.scrollTop;
      const nextScrollPosition = {
        isAtTop: (instance.scrollOffset ?? 0) <= 4,
        isAtEnd: distanceFromEnd <= 4 || !userHasMovedAwayFromEnd.current,
      };
      if (nextScrollPosition.isAtEnd) userHasMovedAwayFromEnd.current = false;
      dispatchScrollState({ type: "set-scroll-position", scrollPosition: nextScrollPosition });
    },
  });
  const virtualItems = virtualizer.getVirtualItems();

  useLayoutEffect(() => {
    const footerElement = footerRef.current;
    if (footerElement === null) return;

    function updateFooterHeight(element: HTMLDivElement) {
      setFooterHeight(Math.ceil(element.getBoundingClientRect().height));
    }

    updateFooterHeight(footerElement);
    const resizeObserver = new ResizeObserver(() => updateFooterHeight(footerElement));
    resizeObserver.observe(footerElement);
    return () => resizeObserver.disconnect();
  }, []);

  useLayoutEffect(() => {
    const appendedEventCount = Math.max(0, eventCount - previousEventCount.current);
    previousEventCount.current = eventCount;

    if (!settledInitialEndScroll.current) {
      if (eventCount === 0) return;
      virtualizer.scrollToEnd();
      dispatchScrollState({ type: "clear-unread" });
      if (virtualizer.isAtEnd()) {
        settledInitialEndScroll.current = true;
      }
      return;
    }

    if (appendedEventCount === 0) return;

    if (userHasMovedAwayFromEnd.current && !scrollState.scrollPosition.isAtEnd) {
      dispatchScrollState({ type: "add-unread", count: appendedEventCount });
    } else {
      virtualizer.scrollToEnd();
      dispatchScrollState({ type: "clear-unread" });
    }
  }, [eventCount, scrollState.scrollPosition.isAtEnd, virtualizer]);

  const showBottomFade = eventCount > 0 && !scrollState.scrollPosition.isAtEnd;
  const showScrollToBottom = eventCount > 0 && !scrollState.scrollPosition.isAtEnd;
  const showScrollToTop = eventCount > 0 && !scrollState.scrollPosition.isAtTop;

  function stopFollowingEnd() {
    userHasMovedAwayFromEnd.current = true;
  }

  return (
    <div className="stream-page__feed">
      {showScrollToTop ? (
        <div className="stream-page__feed-top-fade">
          <div className="stream-page__feed-top-fade-mask" aria-hidden />
          <div className="stream-page__scroll-affordance stream-page__scroll-affordance--in-top-fade">
            <button
              aria-label="Scroll to top"
              className="stream-page__scroll-button"
              type="button"
              onClick={() => {
                stopFollowingEnd();
                virtualizer.scrollToOffset(0);
              }}
            >
              ↑
            </button>
          </div>
        </div>
      ) : null}
      <section
        aria-label="Stream events"
        data-testid="stream-events"
        className={
          showBottomFade
            ? "stream-page__stream stream-page__stream--fade-bottom"
            : "stream-page__stream"
        }
        ref={parentRef}
        onTouchStart={stopFollowingEnd}
        onPointerDown={(event) => {
          if (event.target === event.currentTarget) {
            stopFollowingEnd();
          }
        }}
        onWheel={(event) => {
          if (event.deltaY < 0) stopFollowingEnd();
        }}
      >
        {eventCount === 0 ? (
          <div className="stream-page__stream-placeholder">
            {snapshot.connectionStatus === "subscribed" ? null : (
              <div className="stream-page__spinner" aria-hidden="true" />
            )}
            <span>
              {snapshot.connectionError === undefined
                ? snapshot.connectionStatus === "subscribed"
                  ? "SQLite is ready; no events are stored locally yet"
                  : `SQLite is ready; stream connection is ${snapshot.connectionStatus}`
                : `SQLite is ready; stream connection is ${snapshot.connectionStatus}: ${snapshot.connectionError}`}
            </span>
          </div>
        ) : (
          <>
            <div
              className="stream-page__virtual-content"
              style={{ height: virtualizer.getTotalSize() }}
            >
              <EventRowWindow
                eventCount={eventCount}
                expandedOffsets={expandedOffsets}
                streamDatabase={streamDatabase}
                virtualItems={virtualItems}
                measureElement={virtualizer.measureElement}
                onToggleOffset={(offset) => {
                  setExpandedOffsets((current) => {
                    const next = new Set(current);
                    if (next.has(offset)) {
                      next.delete(offset);
                    } else {
                      next.add(offset);
                    }
                    return next;
                  });
                }}
              />
            </div>
          </>
        )}
      </section>
      <div className="stream-page__feed-footer" ref={footerRef}>
        {showBottomFade ? (
          <div className="stream-page__feed-fade">
            <div className="stream-page__feed-fade-mask" aria-hidden />
            {showScrollToBottom ? (
              <div className="stream-page__scroll-affordance stream-page__scroll-affordance--in-fade">
                <button
                  aria-label="Scroll to bottom"
                  className={
                    scrollState.unreadEventCount > 0
                      ? "stream-page__scroll-button stream-page__scroll-button--has-unread"
                      : "stream-page__scroll-button"
                  }
                  type="button"
                  onClick={() => {
                    userHasMovedAwayFromEnd.current = false;
                    dispatchScrollState({ type: "clear-unread" });
                    virtualizer.scrollToEnd();
                  }}
                >
                  <span className="stream-page__scroll-button-arrow" aria-hidden>
                    ↓
                  </span>
                  {scrollState.unreadEventCount > 0 ? (
                    <span className="stream-page__scroll-button-unread" aria-live="polite">
                      {scrollState.unreadEventCount} new
                    </span>
                  ) : null}
                </button>
              </div>
            ) : null}
          </div>
        ) : null}
        <StreamComposer key={`composer:${streamPath}`} streamStore={streamStore} />
      </div>
    </div>
  );
}

function EventRowWindow({
  streamDatabase,
  virtualItems,
  expandedOffsets,
  eventCount,
  measureElement,
  onToggleOffset,
}: {
  streamDatabase: StreamBrowserDatabase;
  virtualItems: VirtualItem[];
  expandedOffsets: Set<number>;
  eventCount: number;
  measureElement: (node: Element | null) => void;
  onToggleOffset(offset: number): void;
}) {
  const firstIndex = virtualItems[0]?.index ?? 0;
  const lastIndex = virtualItems.at(-1)?.index ?? -1;
  // local_index is the dense, zero-based browser list position TanStack Virtual reads.
  // Today it is offset - 1 and SQLite rejects gaps. If server-side TTL later ages out
  // old offsets, this column can remain the local dense index while offset keeps its
  // original stream identity.
  const rowQueryResult = useStreamQuery(
    streamDatabase,
    `SELECT local_index, offset, type, idempotency_key, created_at, inserted_at, json_pretty(raw_jsonb) AS raw_json
     FROM events
     WHERE local_index >= ? AND local_index < ?
     ORDER BY local_index ASC`,
    [firstIndex, lastIndex + 1],
  );
  const rowsByLocalIndex = useMemo(() => {
    const rows = new Map<number, StreamEventRow>();
    for (const row of rowQueryResult.data) {
      const event = streamEventRowFromSql(row);
      if (event !== undefined) rows.set(event.local_index, event);
    }
    return rows;
  }, [rowQueryResult.data]);

  return virtualItems.map((virtualItem) => {
    const event = rowsByLocalIndex.get(virtualItem.index);
    const isExpanded = event !== undefined && expandedOffsets.has(event.offset);
    const isLastEventRow = virtualItem.index === eventCount - 1;

    return (
      <div
        className={
          isLastEventRow
            ? "stream-page__virtual-row stream-page__virtual-row--last"
            : "stream-page__virtual-row"
        }
        data-index={virtualItem.index}
        key={virtualItem.key}
        ref={measureElement}
        style={{ transform: `translateY(${virtualItem.start}px)` }}
      >
        {event === undefined ? (
          <article className="stream-page__event-row stream-page__event-row--pending" />
        ) : (
          <article
            className={
              isExpanded
                ? "stream-page__event-row stream-page__event-row--expanded"
                : "stream-page__event-row stream-page__event-row--collapsed"
            }
          >
            <button
              aria-expanded={isExpanded}
              className="stream-page__event-meta"
              data-event-offset={event.offset}
              data-event-type={event.type}
              data-local-index={event.local_index}
              data-testid="event-meta"
              type="button"
              onClick={() => onToggleOffset(event.offset)}
            >
              <span>{event.offset}</span>
              <span>{event.type}</span>
              <time dateTime={event.created_at}>{event.created_at}</time>
            </button>
            {isExpanded ? (
              <pre className="stream-page__event-json">{event.raw_json}</pre>
            ) : null}
          </article>
        )}
      </div>
    );
  });
}

function streamEventRowFromSql(row: Record<string, unknown>): StreamEventRow | undefined {
  if (
    typeof row.local_index !== "number" ||
    typeof row.offset !== "number" ||
    typeof row.type !== "string" ||
    typeof row.created_at !== "string" ||
    typeof row.inserted_at !== "string" ||
    typeof row.raw_json !== "string" ||
    !(row.idempotency_key === null || typeof row.idempotency_key === "string")
  ) {
    return undefined;
  }
  return {
    local_index: row.local_index,
    offset: row.offset,
    type: row.type,
    idempotency_key: row.idempotency_key,
    created_at: row.created_at,
    inserted_at: row.inserted_at,
    raw_json: row.raw_json,
  };
}

function StreamSidebar({
  className,
  streamPath,
  snapshot,
  streamDatabase,
  streamStore,
  eventCount,
  onSidebarOpenChange,
}: {
  className?: string;
  streamPath: string;
  snapshot: StreamBrowserSnapshot;
  streamDatabase: StreamBrowserDatabase;
  streamStore: StreamBrowserStore;
  eventCount: number;
  onSidebarOpenChange(open: boolean): void;
}) {
  return (
    <aside
      className={className === undefined ? "stream-page__sidebar" : `stream-page__sidebar ${className}`}
      id="stream-sidebar"
    >
      <StreamTopBar streamPath={streamPath} onSidebarOpenChange={onSidebarOpenChange} />
      <SubscriptionTool
        eventCount={eventCount}
        snapshot={snapshot}
        streamDatabase={streamDatabase}
        streamStore={streamStore}
      />
      <InsertEventsTool
        streamStore={streamStore}
        streamPath={streamPath}
      />
    </aside>
  );
}

function SubscriptionTool({
  snapshot,
  streamDatabase,
  streamStore,
  eventCount,
}: {
  snapshot: StreamBrowserSnapshot;
  streamDatabase: StreamBrowserDatabase;
  streamStore: StreamBrowserStore;
  eventCount: number;
}) {
  const [actionFeedback, setActionFeedback] = useState<
    "idle" | "downloading" | "clearing" | "killing" | "resetting" | "done" | "error"
  >("idle");
  const serverActionBusy = actionFeedback === "killing" || actionFeedback === "resetting";

  return (
    <section className="stream-page__tool">
      <h2 className="stream-page__tool-title">Subscription</h2>
      <dl className="stream-page__facts">
        <div title="Connection to the stream Durable Object over a capnweb WebSocket: connecting → connected → subscribing → subscribed (or reconnecting / error).">
          <dt>Status</dt>
          <dd>
            <output
              className={
                snapshot.connectionStatus === "error"
                  ? "stream-page__state stream-page__state--error"
                  : "stream-page__state"
              }
              data-testid="stream-status"
            >
              {snapshot.connectionStatus}
            </output>
          </dd>
        </div>
        <div title="This tab's role in the Web Locks election. leader = the single writer (it subscribes to the stream and writes events into the shared local DB); follower = a reader that mirrors the leader's writes from the same on-disk DB; electing/idle = before a role is assigned.">
          <dt>Subscription</dt>
          <dd>
            <output className="stream-page__state" data-testid="subscription-status">{snapshot.subscriptionStatus}</output>
          </dd>
        </div>
        {snapshot.connectionError === undefined ? null : (
          <div title="The most recent connection or subscription error, if any.">
            <dt>Error</dt>
            <dd>
              <output className="stream-page__state stream-page__state--error stream-page__state--wrap">
                {snapshot.connectionError}
              </output>
            </dd>
          </div>
        )}
        <div title="Number of events stored in this tab's local SQLite mirror (one row per stream offset).">
          <dt>Events</dt>
          <dd>
            <output className="stream-page__state" data-testid="event-count">{eventCount}</output>
          </dd>
        </div>
        <div title="Whether the page is crossOriginIsolated (the COOP+COEP / SharedArrayBuffer mode). Deliberately false: wa-sqlite's OPFSCoopSyncVFS needs no isolation, and enabling it would re-introduce the SharedArrayBuffer OPFS deadlock. Not a problem — it's expected.">
          <dt>Cross-origin isolated</dt>
          <dd>
            <output className="stream-page__state">
              {String(snapshot.databaseInfo?.crossOriginIsolated ?? false)}
            </output>
          </dd>
        </div>
        <div title="Where the SQLite database lives. opfs = the browser's Origin Private File System — a real file on disk that survives reloads.">
          <dt>Storage</dt>
          <dd>
            <output className="stream-page__state">
              {snapshot.databaseInfo?.storageType ?? "pending"}
            </output>
          </dd>
        </div>
        <div title="Whether the browser granted eviction-protected ('persistent') storage to this origin. false = best-effort: the data IS saved to disk, but the browser may evict it under storage pressure. Chrome only grants this to engaged/installed origins; there's no API to force it.">
          <dt>Persisted</dt>
          <dd>
            <output className="stream-page__state">
              {String(snapshot.databaseInfo?.persisted ?? false)}
            </output>
          </dd>
        </div>
        <div title="On-disk size of this tab's local SQLite database file.">
          <dt>DB file size</dt>
          <dd>
            <output className="stream-page__state">
              {formatByteSize(snapshot.databaseInfo?.databaseSizeBytes ?? 0)}
            </output>
          </dd>
        </div>
      </dl>
      <div className="stream-page__action-grid">
        <button
          className="stream-page__button"
          disabled={actionFeedback === "downloading" || serverActionBusy}
          type="button"
          onClick={() => {
            setActionFeedback("downloading");
            void streamDatabase.download().then(
              () => setActionFeedback("done"),
              () => setActionFeedback("error"),
            );
          }}
        >
          Download
        </button>
        <button
          className="stream-page__button"
          disabled={actionFeedback === "clearing" || serverActionBusy}
          type="button"
          onClick={() => {
            setActionFeedback("clearing");
            void streamStore.clearLocalDatabase().then(
              () => setActionFeedback("done"),
              () => setActionFeedback("error"),
            );
          }}
        >
          Clear local
        </button>
        <button
          className="stream-page__button"
          disabled={serverActionBusy}
          title="Abort the stream DO; durable log is kept and a woken event is appended on restart."
          type="button"
          onClick={() => {
            setActionFeedback("killing");
            void streamStore.kill().then(
              () => setActionFeedback("done"),
              () => setActionFeedback("error"),
            );
          }}
        >
          Kill
        </button>
        <button
          className="stream-page__button"
          disabled={serverActionBusy}
          title="Wipe all stream DO storage, then abort — next connection starts a fresh stream."
          type="button"
          onClick={() => {
            setActionFeedback("resetting");
            void streamStore.reset().then(
              () => setActionFeedback("done"),
              () => setActionFeedback("error"),
            );
          }}
        >
          Reset
        </button>
      </div>
      {actionFeedback === "idle" ? null : (
        <output
          className={
            actionFeedback === "error"
              ? "stream-page__insert-state stream-page__insert-state--error"
              : "stream-page__insert-state"
          }
        >
          {actionFeedback}
        </output>
      )}
    </section>
  );
}

function formatByteSize(bytes: number) {
  const units = ["B", "KB", "MB", "GB"];
  let size = bytes;
  let unitIndex = 0;

  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }

  return `${size.toLocaleString(undefined, {
    maximumFractionDigits: unitIndex === 0 ? 0 : 1,
  })} ${units[unitIndex]}`;
}

function InsertEventsTool({
  streamPath,
  streamStore,
}: {
  streamPath: string;
  streamStore: StreamBrowserStore;
}) {
  const [insertState, dispatchInsertState] = useReducer(
    (
      state: {
        insertEventCount: string;
        insertBatchSize: string;
        periodSeconds: string;
        appendResponseMode: "await" | "background" | "dispose";
        insertState: "idle" | "inserting" | "done" | "error";
      },
      action:
        | { type: "set-insert-event-count"; value: string }
        | { type: "set-insert-batch-size"; value: string }
        | { type: "set-period-seconds"; value: string }
        | { type: "set-append-response-mode"; value: "await" | "background" | "dispose" }
        | { type: "set-insert-state"; value: "idle" | "inserting" | "done" | "error" },
    ) => {
      switch (action.type) {
        case "set-insert-event-count":
          return { ...state, insertEventCount: action.value };
        case "set-insert-batch-size":
          return { ...state, insertBatchSize: action.value };
        case "set-period-seconds":
          return { ...state, periodSeconds: action.value };
        case "set-append-response-mode":
          return { ...state, appendResponseMode: action.value };
        case "set-insert-state":
          return { ...state, insertState: action.value };
      }
    },
    {
      insertEventCount: "1000",
      insertBatchSize: "100",
      periodSeconds: "5",
      appendResponseMode: "await",
      insertState: "idle",
    },
  );

  async function insertRandomEvents() {
    const count = Math.max(0, Math.floor(Number(insertState.insertEventCount)));
    const batchSize = Math.max(1, Math.floor(Number(insertState.insertBatchSize)));
    const periodMs = Math.max(0, Number(insertState.periodSeconds) * 1_000);
    if (
      !Number.isFinite(count) ||
      !Number.isFinite(batchSize) ||
      !Number.isFinite(periodMs) ||
      count === 0
    ) {
      return;
    }

    dispatchInsertState({ type: "set-insert-state", value: "inserting" });

    try {
      const pendingResponses: Promise<unknown>[] = [];
      const batchCount = Math.ceil(count / batchSize);
      const batchDelayMs = batchCount <= 1 ? 0 : periodMs / (batchCount - 1);

      await Promise.all(
        Array.from({ length: batchCount }, (_, batchIndex) =>
          new Promise<void>((resolve) => {
            setTimeout(resolve, batchDelayMs * batchIndex);
          }).then(() => {
            const startIndex = batchIndex * batchSize;
            const result = streamStore.appendBatch({
              events: Array.from(
                { length: Math.min(batchSize, count - startIndex) },
                (_, indexInBatch) => {
                  const index = startIndex + indexInBatch;
                  return {
                    type: "events.iterate.com/debug/random-event",
                    payload: {
                      streamPath,
                      index,
                      count,
                      batchIndex,
                      batchSize,
                      value: crypto.randomUUID(),
                    },
                  };
                },
              ),
            });

            if (insertState.appendResponseMode === "await") {
              pendingResponses.push(result);
            } else if (insertState.appendResponseMode === "background") {
              // CapnWeb RpcPromises are lazy. Calling `.then()` starts the
              // append without making this form wait for the returned events.
              void result.then(undefined, (error: unknown) => {
                console.error("background appendBatch failed", error);
              });
            } else if (insertState.appendResponseMode === "dispose") {
              // CapnWeb says callers should dispose RpcPromises they do not
              // await. This option tests whether that still lets a write-only
              // append reach the Durable Object before cancellation wins.
              result[Symbol.dispose]();
            }
          }),
        ),
      );
      if (insertState.appendResponseMode === "await") await Promise.all(pendingResponses);
      dispatchInsertState({ type: "set-insert-state", value: "done" });
    } catch {
      dispatchInsertState({ type: "set-insert-state", value: "error" });
    }
  }

  return (
    <section className="stream-page__tool">
      <h2 className="stream-page__tool-title">Insert events</h2>
      <label className="stream-page__field">
        <span>Count</span>
        <input
          className="stream-page__input"
          min="1"
          step="1"
          type="number"
          value={insertState.insertEventCount}
          onChange={(event) =>
            dispatchInsertState({
              type: "set-insert-event-count",
              value: event.currentTarget.value,
            })
          }
        />
      </label>
      <label className="stream-page__field">
        <span>Seconds</span>
        <input
          className="stream-page__input"
          min="0"
          step="0.1"
          type="number"
          value={insertState.periodSeconds}
          onChange={(event) =>
            dispatchInsertState({
              type: "set-period-seconds",
              value: event.currentTarget.value,
            })
          }
        />
      </label>
      <label className="stream-page__field">
        <span>Batch size</span>
        <input
          className="stream-page__input"
          min="1"
          step="1"
          type="number"
          value={insertState.insertBatchSize}
          onChange={(event) =>
            dispatchInsertState({
              type: "set-insert-batch-size",
              value: event.currentTarget.value,
            })
          }
        />
      </label>
      <label className="stream-page__field">
        <span>appendBatch response</span>
        <select
          className="stream-page__input"
          value={insertState.appendResponseMode}
          onChange={(event) =>
            dispatchInsertState({
              type: "set-append-response-mode",
              value:
                event.currentTarget.value === "dispose"
                  ? "dispose"
                  : event.currentTarget.value === "background"
                    ? "background"
                    : "await",
            })
          }
        >
          <option value="await">Await</option>
          <option value="background">Background</option>
          <option value="dispose">Dispose</option>
        </select>
      </label>
      <button
        className="stream-page__button"
        disabled={insertState.insertState === "inserting"}
        type="button"
        onClick={() => void insertRandomEvents()}
      >
        Stream random events
      </button>
      <output className="stream-page__insert-state">{insertState.insertState}</output>
    </section>
  );
}

function StreamComposer({ streamStore }: { streamStore: StreamBrowserStore }) {
  const [composerText, setComposerText] = useState(() =>
    JSON.stringify(
      {
        type: "events.iterate.com/debug/manual-event",
        payload: {
          message: "hello from the browser composer",
        },
      },
      null,
      2,
    ),
  );
  const [appendState, setAppendState] = useState<"idle" | "appending" | "done" | "error">("idle");

  async function appendComposerEvent() {
    setAppendState("appending");

    try {
      await streamStore.appendBatch({
        events: [JSON.parse(composerText)],
      });
      setAppendState("done");
    } catch {
      setAppendState("error");
    }
  }

  return (
    <section className="stream-page__composer" aria-label="Append event">
      <div className="stream-page__composer-input">
        <textarea
          aria-label="Event JSON"
          className="stream-page__textarea"
          spellCheck={false}
          value={composerText}
          onChange={(event) => setComposerText(event.currentTarget.value)}
        />
        <button
          aria-label={
            appendState === "error" ? "Append failed; retry append event" : "Append event"
          }
          title={appendState === "appending" ? "Appending" : "Append event"}
          className={
            appendState === "error"
              ? "stream-page__composer-submit stream-page__composer-submit--error"
              : "stream-page__composer-submit"
          }
          disabled={appendState === "appending"}
          type="button"
          onClick={() => void appendComposerEvent()}
        >
          <AppendEventIcon />
        </button>
      </div>
    </section>
  );
}
