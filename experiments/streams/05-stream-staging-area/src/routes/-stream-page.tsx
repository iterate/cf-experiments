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
  const [eventTypeFilter, setEventTypeFilter] = useState("");
  const runtime = useStreamRuntime(streamPath, eventTypeFilter);

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
      eventTypeFilter={eventTypeFilter}
      eventTypes={runtime.eventTypes}
      totalEventCount={runtime.totalEventCount}
      onEventTypeFilterChange={setEventTypeFilter}
    />
  );
}

function HydratedStreamCompactView({ streamPath }: { streamPath: string }) {
  const [eventTypeFilter, setEventTypeFilter] = useState("");
  const runtime = useStreamRuntime(streamPath, eventTypeFilter);

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
          eventTypeFilter={eventTypeFilter}
          eventTypes={runtime.eventTypes}
          key={`events:${streamPath}:${runtime.snapshot.clearVersion}:${eventTypeFilter}`}
          snapshot={runtime.snapshot}
          streamDatabase={runtime.streamDatabase}
          streamPath={streamPath}
          streamStore={runtime.streamStore}
          totalEventCount={runtime.totalEventCount}
          onEventTypeFilterChange={setEventTypeFilter}
        />
      )}
    </section>
  );
}

function useStreamRuntime(streamPath: string, eventTypeFilter: string) {
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
  const totalCountResult = useStreamQuery(
    streamDatabase,
    `SELECT COUNT(*) AS count FROM events`,
  );
  const countResult = useStreamQuery(
    streamDatabase,
    eventTypeFilter === ""
      ? `SELECT COUNT(*) AS count FROM events`
      : `SELECT COUNT(*) AS count FROM events WHERE type = ?`,
    eventTypeFilter === "" ? [] : [eventTypeFilter],
  );
  const eventTypesResult = useStreamQuery(
    streamDatabase,
    `SELECT type, COUNT(*) AS count
     FROM events
     GROUP BY type
     ORDER BY type ASC`,
  );
  const eventCount = Number(countResult.data[0]?.count ?? 0);
  const totalEventCount = Number(totalCountResult.data[0]?.count ?? 0);
  const eventTypes = eventTypesResult.data.flatMap((row) => {
    if (typeof row.type !== "string" || typeof row.count !== "number") return [];
    return [{ count: row.count, type: row.type }];
  });
  return { countResult, eventCount, eventTypes, snapshot, streamDatabase, streamStore, totalEventCount };
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
  eventTypeFilter,
  eventTypes,
  totalEventCount,
  databaseReady,
  databaseError,
  databaseStatus,
  onEventTypeFilterChange,
}: {
  streamPath: string;
  snapshot: StreamBrowserSnapshot;
  streamDatabase: StreamBrowserDatabase;
  streamStore: StreamBrowserStore;
  eventCount: number;
  eventTypeFilter: string;
  eventTypes: { count: number; type: string }[];
  totalEventCount: number;
  databaseReady: boolean;
  databaseError: Error | undefined;
  databaseStatus: "pending" | "ok" | "error";
  onEventTypeFilterChange(eventType: string): void;
}) {

  return (
    <StreamPageLayout
      databaseReady={databaseReady}
      databaseError={databaseError}
      databaseStatus={databaseStatus}
      eventCount={eventCount}
      eventTypeFilter={eventTypeFilter}
      eventTypes={eventTypes}
      totalEventCount={totalEventCount}
      snapshot={snapshot}
      streamDatabase={streamDatabase}
      streamStore={streamStore}
      streamPath={streamPath}
      onEventTypeFilterChange={onEventTypeFilterChange}
    />
  );
}

function StreamPageLayout({
  streamPath,
  snapshot,
  streamDatabase,
  streamStore,
  eventCount,
  eventTypeFilter,
  eventTypes,
  totalEventCount,
  databaseReady,
  databaseError,
  databaseStatus,
  onEventTypeFilterChange,
}: {
  streamPath: string;
  snapshot: StreamBrowserSnapshot;
  streamDatabase: StreamBrowserDatabase;
  streamStore: StreamBrowserStore;
  eventCount: number;
  eventTypeFilter: string;
  eventTypes: { count: number; type: string }[];
  totalEventCount: number;
  databaseReady: boolean;
  databaseError: Error | undefined;
  databaseStatus: "pending" | "ok" | "error";
  onEventTypeFilterChange(eventType: string): void;
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
              eventTypeFilter={eventTypeFilter}
              eventTypes={eventTypes}
              key={`events:${streamPath}:${snapshot.clearVersion}:${eventTypeFilter}`}
              snapshot={snapshot}
              streamDatabase={streamDatabase}
              streamPath={streamPath}
              streamStore={streamStore}
              totalEventCount={totalEventCount}
              onEventTypeFilterChange={onEventTypeFilterChange}
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
  eventTypeFilter,
  eventTypes,
  totalEventCount,
  snapshot,
  streamPath,
  streamStore,
  onEventTypeFilterChange,
}: {
  streamDatabase: StreamBrowserDatabase;
  eventCount: number;
  eventTypeFilter: string;
  eventTypes: { count: number; type: string }[];
  totalEventCount: number;
  snapshot: StreamBrowserSnapshot;
  streamPath: string;
  streamStore: StreamBrowserStore;
  onEventTypeFilterChange(eventType: string): void;
}) {
  const topScrollAffordanceHeight = 48;
  const parentRef = useRef<HTMLDivElement>(null);
  const previousEventCount = useRef(eventCount);
  const settledInitialEndScroll = useRef(false);
  const [expandedOffsets, setExpandedOffsets] = useState(() => new Set<number>());
  const [newEventCount, setNewEventCount] = useState(0);
  const [scrollPosition, setScrollPosition] = useState({ isAtTop: true, isAtEnd: true });
  const virtualizer = useVirtualizer({
    count: eventCount,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 38,
    // This stream is append-only, so the virtual index is a stable item key.
    // If this grows older-history prepends later, switch this to a persisted row id.
    getItemKey: (index) => index,
    anchorTo: "end",
    followOnAppend: true,
    paddingStart: topScrollAffordanceHeight,
    scrollEndThreshold: 80,
    overscan: 6,
    // The official React chat example uses direct DOM updates. Without this,
    // a tiny/non-scrollable list that receives a huge same-render append can
    // scroll to the previous max offset and stop following the end.
    directDomUpdates: true,
    onChange(instance) {
      const nextScrollPosition = {
        isAtTop: (instance.scrollOffset ?? 0) <= 4,
        isAtEnd: instance.isAtEnd(),
      };
      setScrollPosition((current) =>
        current.isAtTop === nextScrollPosition.isAtTop &&
          current.isAtEnd === nextScrollPosition.isAtEnd
          ? current
          : nextScrollPosition
      );
    },
  });
  const virtualItems = virtualizer.getVirtualItems();

  useLayoutEffect(() => {
    if (settledInitialEndScroll.current || eventCount === 0) return;
    settledInitialEndScroll.current = true;
    virtualizer.scrollToEnd();
  }, [eventCount, virtualizer]);

  useLayoutEffect(() => {
    const appendedCount = eventCount - previousEventCount.current;
    previousEventCount.current = eventCount;
    if (appendedCount <= 0) {
      if (eventCount === 0) setNewEventCount(0);
      return;
    }
    if (!scrollPosition.isAtEnd) {
      setNewEventCount((current) => current + appendedCount);
    }
  }, [eventCount, scrollPosition.isAtEnd]);

  useLayoutEffect(() => {
    if (scrollPosition.isAtEnd) setNewEventCount(0);
  }, [scrollPosition.isAtEnd]);

  const showScrollToBottom = eventCount > 0 && !scrollPosition.isAtEnd;
  const showScrollToTop = eventCount > 0 && !scrollPosition.isAtTop;

  return (
    <div className="stream-page__feed">
      <EventTypeFilterBar
        eventCount={eventCount}
        eventTypeFilter={eventTypeFilter}
        eventTypes={eventTypes}
        totalEventCount={totalEventCount}
        onEventTypeFilterChange={onEventTypeFilterChange}
      />
      <StreamRuntimeNotice eventCount={eventCount} snapshot={snapshot} />
      {showScrollToTop ? (
        <div className="stream-page__feed-top-fade">
          <div className="stream-page__feed-top-fade-mask" aria-hidden />
          <div className="stream-page__scroll-affordance stream-page__scroll-affordance--in-top-fade">
            <button
              aria-label="Scroll to top"
              className="stream-page__scroll-button"
              type="button"
              onClick={() => {
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
        className="stream-page__stream"
        ref={parentRef}
      >
        {eventCount === 0 ? (
          <div className="stream-page__stream-placeholder">
            {snapshot.connectionStatus === "subscribed" ? null : (
              <div className="stream-page__spinner" aria-hidden="true" />
            )}
            <span>
              {eventTypeFilter !== ""
                ? "SQLite is ready; no events match the selected event type"
                : snapshot.connectionError === undefined
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
                eventTypeFilter={eventTypeFilter}
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
        <div className="stream-page__feed-footer">
          {showScrollToBottom ? (
            <div className="stream-page__feed-fade">
              <div className="stream-page__feed-fade-mask" aria-hidden />
              <div className="stream-page__scroll-affordance stream-page__scroll-affordance--in-fade">
                <button
                  aria-label={
                    newEventCount === 0
                      ? "Scroll to bottom"
                      : `Scroll to bottom, ${newEventCount} new ${
                          newEventCount === 1 ? "event" : "events"
                        }`
                  }
                  className={
                    newEventCount === 0
                      ? "stream-page__scroll-button"
                      : "stream-page__scroll-button stream-page__scroll-button--with-count"
                  }
                  type="button"
                  onClick={() => {
                    setNewEventCount(0);
                    virtualizer.scrollToEnd();
                  }}
                >
                  <span className="stream-page__scroll-button-arrow">↓</span>
                  {newEventCount === 0 ? null : (
                    <span className="stream-page__scroll-button-count">{newEventCount}</span>
                  )}
                </button>
              </div>
            </div>
          ) : null}
          <StreamComposer key={`composer:${streamPath}`} streamStore={streamStore} />
        </div>
      </section>
    </div>
  );
}

function EventTypeFilterBar({
  eventCount,
  eventTypeFilter,
  eventTypes,
  totalEventCount,
  onEventTypeFilterChange,
}: {
  eventCount: number;
  eventTypeFilter: string;
  eventTypes: { count: number; type: string }[];
  totalEventCount: number;
  onEventTypeFilterChange(eventType: string): void;
}) {
  const totalLabel = `${totalEventCount.toLocaleString()} total ${
    totalEventCount === 1 ? "event" : "events"
  }`;
  const filteredLabel = `${eventCount.toLocaleString()} filtered ${
    eventCount === 1 ? "event" : "events"
  }`;

  return (
    <div className="stream-page__filter-bar">
      <label className="stream-page__filter-field">
        <span>Event type</span>
        <select
          aria-label="Event type filter"
          className="stream-page__input stream-page__filter-select"
          value={eventTypeFilter}
          onChange={(event) => onEventTypeFilterChange(event.currentTarget.value)}
        >
          <option value="">All event types</option>
          {eventTypes.map((eventType) => (
            <option key={eventType.type} value={eventType.type}>
              {eventType.type} ({eventType.count})
            </option>
          ))}
        </select>
      </label>
      <output className="stream-page__filter-count" data-testid="filter-count">
        {eventTypeFilter === "" ? totalLabel : `${filteredLabel} / ${totalLabel}`}
      </output>
    </div>
  );
}

function StreamRuntimeNotice({
  eventCount,
  snapshot,
}: {
  eventCount: number;
  snapshot: StreamBrowserSnapshot;
}) {
  if (snapshot.connectionError !== undefined) {
    return (
      <div className="stream-page__notice stream-page__notice--error" data-testid="stream-error" role="alert">
        <strong>Stream error</strong>
        <span>{snapshot.connectionError}</span>
      </div>
    );
  }

  if (eventCount === 0 && snapshot.subscriptionStatus === "follower") {
    // This is the exact failure shape seen after a deploy with stale tabs:
    // the tab is connected but intentionally not subscribing, and its local
    // SQLite mirror is empty. Surface it loudly instead of showing only a
    // spinner, because there may be no exception in this tab's console.
    return (
      <div className="stream-page__notice stream-page__notice--warning" data-testid="stream-warning" role="status">
        <strong>Follower with empty SQLite mirror</strong>
        <span>
          This tab is waiting for the elected writer tab to mirror events into local SQLite.
          Reload or close older tabs if this does not resolve.
        </span>
      </div>
    );
  }

  return null;
}

function EventRowWindow({
  streamDatabase,
  virtualItems,
  expandedOffsets,
  eventCount,
  eventTypeFilter,
  measureElement,
  onToggleOffset,
}: {
  streamDatabase: StreamBrowserDatabase;
  virtualItems: VirtualItem[];
  expandedOffsets: Set<number>;
  eventCount: number;
  eventTypeFilter: string;
  measureElement: (node: Element | null) => void;
  onToggleOffset(offset: number): void;
}) {
  const firstIndex = virtualItems[0]?.index ?? 0;
  const lastIndex = virtualItems.at(-1)?.index ?? -1;
  const windowSize = Math.max(0, lastIndex - firstIndex + 1);
  // local_index is the dense, zero-based browser list position TanStack Virtual reads.
  // Today it is offset - 1 and SQLite rejects gaps. If server-side TTL later ages out
  // old offsets, this column can remain the local dense index while offset keeps its
  // original stream identity.
  //
  // With no filter, the virtual index and local_index are the same. With an event-type
  // filter, TanStack Virtual indexes the filtered list, so SQLite walks the
  // events_type_local_index index in local order and returns LIMIT/OFFSET rows from
  // that filtered list.
  const rowQueryResult = useStreamQuery(
    streamDatabase,
    eventTypeFilter === ""
      ? `SELECT local_index AS virtual_index, '' AS query_event_type,
           local_index, offset, type, idempotency_key, created_at, inserted_at, json_pretty(raw_jsonb) AS raw_json
         FROM events
         WHERE local_index >= ? AND local_index < ?
         ORDER BY local_index ASC`
      : `SELECT ? + ROW_NUMBER() OVER (ORDER BY local_index) - 1 AS virtual_index,
           ? AS query_event_type,
           local_index, offset, type, idempotency_key, created_at, inserted_at, raw_json
         FROM (
           SELECT local_index, offset, type, idempotency_key, created_at, inserted_at, json_pretty(raw_jsonb) AS raw_json
           FROM events
           WHERE type = ?
           ORDER BY local_index ASC
           LIMIT ? OFFSET ?
         )
         ORDER BY local_index ASC`,
    eventTypeFilter === ""
      ? [firstIndex, lastIndex + 1]
      : [firstIndex, eventTypeFilter, eventTypeFilter, windowSize, firstIndex],
  );
  const rowsByLocalIndex = useMemo(() => {
    const rows = new Map<number, StreamEventRow>();
    for (const row of rowQueryResult.data) {
      const event = streamEventRowFromSql(row);
      if (
        event !== undefined &&
        row.query_event_type === eventTypeFilter &&
        typeof row.virtual_index === "number"
      ) {
        rows.set(row.virtual_index, event);
      }
    }
    return rows;
  }, [eventTypeFilter, rowQueryResult.data]);
  const markedFirstRowDraw = useRef(false);
  useLayoutEffect(() => {
    if (markedFirstRowDraw.current || rowsByLocalIndex.size === 0) return;
    markedFirstRowDraw.current = true;
    performance.mark("stream:first-event-row");
  }, [rowsByLocalIndex.size]);

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
        ref={event === undefined ? undefined : measureElement}
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
                  return randomStreamEventInput({
                    batchIndex,
                    batchSize,
                    count,
                    index,
                    streamPath,
                  });
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

function randomStreamEventInput(args: {
  batchIndex: number;
  batchSize: number;
  count: number;
  index: number;
  streamPath: string;
}) {
  const randomValues = new Uint32Array(3);
  crypto.getRandomValues(randomValues);
  const random = randomValues[0] ?? 0;
  const suffix = `${args.index}-${crypto.randomUUID().slice(0, 8)}`;
  const actors = ["Ada", "Grace", "Linus", "Margaret", "Katherine", "Dennis"];
  const nouns = ["invoice", "deployment", "workspace", "artifact", "subscription", "trace"];
  const adjectives = ["urgent", "draft", "verified", "archived", "reviewed", "blocked"];
  const actor = actors[random % actors.length] ?? "Ada";
  const noun = nouns[(randomValues[1] ?? 0) % nouns.length] ?? "artifact";
  const adjective = adjectives[(randomValues[2] ?? 0) % adjectives.length] ?? "reviewed";
  const eventTypes = [
    "events.iterate.com/core/metadata-updated",
    "events.iterate.com/core/child-stream-created",
    "events.iterate.com/core/error-occurred",
    "events.iterate.com/core/circuit-breaker-configured",
    "events.iterate.com/core/paused",
    "events.iterate.com/core/resumed",
    "https://events.iterate.com/manual-event-appended",
  ];
  const type = eventTypes[random % eventTypes.length] ?? "https://events.iterate.com/manual-event-appended";
  const debug = {
    batchIndex: args.batchIndex,
    batchSize: args.batchSize,
    count: args.count,
    generatedAt: new Date().toISOString(),
    index: args.index,
    streamPath: args.streamPath,
  };

  if (type === "events.iterate.com/core/metadata-updated") {
    return {
      type,
      payload: {
        ...debug,
        description: `${actor} marked the ${noun} as ${adjective}.`,
        labels: [adjective, noun],
        title: `${adjective} ${noun}`,
        updatedBy: actor,
      },
    };
  }

  if (type === "events.iterate.com/core/child-stream-created") {
    return {
      type,
      payload: {
        ...debug,
        childStreamPath: `${args.streamPath}/children/${suffix}`,
        createdBy: actor,
        reason: `follow-up for ${noun}`,
      },
    };
  }

  if (type === "events.iterate.com/core/error-occurred") {
    return {
      type,
      payload: {
        ...debug,
        errorId: crypto.randomUUID(),
        failedEventType: "https://events.iterate.com/manual-event-appended",
        message: `${actor} could not process ${adjective} ${noun}.`,
        severity: random % 3 === 0 ? "high" : random % 3 === 1 ? "medium" : "low",
      },
    };
  }

  if (type === "events.iterate.com/core/circuit-breaker-configured") {
    return {
      type,
      payload: {
        ...debug,
        burstCapacity: 50 + (random % 500),
        configuredBy: actor,
        refillRatePerMinute: 10 + (random % 90),
      },
    };
  }

  if (type === "events.iterate.com/core/paused" || type === "events.iterate.com/core/resumed") {
    return {
      type,
      payload: {
        ...debug,
        actor,
        reason: `${adjective} ${noun} maintenance`,
      },
    };
  }

  return {
    type,
    payload: {
      ...debug,
      actor,
      body: `${actor} appended a ${adjective} note about the ${noun}.`,
      id: crypto.randomUUID(),
      tags: [noun, adjective],
    },
  };
}

function StreamComposer({ streamStore }: { streamStore: StreamBrowserStore }) {
  const textareaRef = useRef<HTMLTextAreaElement>(null);
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

  useLayoutEffect(() => {
    const textarea = textareaRef.current;
    if (textarea === null) return;
    textarea.style.height = "auto";
    textarea.style.height = `${textarea.scrollHeight}px`;
  }, [composerText]);

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
          ref={textareaRef}
          spellCheck={false}
          value={composerText}
          onChange={(event) => {
            setAppendState("idle");
            setComposerText(event.currentTarget.value);
          }}
        />
        {appendState === "idle" ? null : (
          <output
            aria-live="polite"
            className={
              appendState === "error"
                ? "stream-page__composer-state stream-page__composer-state--error"
                : "stream-page__composer-state"
            }
          >
            {appendState === "appending" ? "appending" : appendState === "done" ? "appended" : "error"}
          </output>
        )}
        <button
          aria-label={
            appendState === "error" ? "Append failed; retry append event" : "Append event"
          }
          title={
            appendState === "appending"
              ? "Appending"
              : appendState === "done"
                ? "Appended"
                : "Append event"
          }
          className={
            appendState === "error"
              ? "stream-page__composer-submit stream-page__composer-submit--error"
              : appendState === "done"
                ? "stream-page__composer-submit stream-page__composer-submit--done"
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
