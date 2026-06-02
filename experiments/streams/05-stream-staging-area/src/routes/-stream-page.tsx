import {
  useLayoutEffect,
  useMemo,
  useReducer,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import { ClientOnly, Link } from "@tanstack/react-router";
import { useVirtualizer, type VirtualItem } from "@tanstack/react-virtual";
import type { SQLocal, SqlTag, StatementInput } from "sqlocal";
import {
  createStreamBrowserStore,
  type StreamBrowserSnapshot,
} from "../client-libraries/stream-browser-store.js";
import {
  getStreamBrowserDatabase,
  type StreamBrowserDatabase,
  type StreamEventRow,
} from "../client-libraries/stream-browser-db.js";
import { withStream } from "../client-libraries/stream-browser.js";
import "./-stream-page.css";

export function StreamPage({ streamPath }: { streamPath: string }) {
  return (
    <ClientOnly fallback={<StreamHydrationFallback streamPath={streamPath} />}>
      <HydratedStreamPage streamPath={streamPath} />
    </ClientOnly>
  );
}

function HydratedStreamPage({ streamPath }: { streamPath: string }) {
  const streamStore = useMemo(
    () => createStreamBrowserStore({ streamPath }),
    [streamPath],
  );
  const snapshot = useSyncExternalStore(
    streamStore.subscribe,
    streamStore.getSnapshot,
    streamStore.getServerSnapshot,
  );
  const streamDatabase = useMemo(() => getStreamBrowserDatabase(streamPath), [streamPath]);

  return (
    <StreamPageWithDatabase
      snapshot={snapshot}
      streamDatabase={streamDatabase}
      streamPath={streamPath}
    />
  );
}

function StreamHydrationFallback({ streamPath }: { streamPath: string }) {
  return (
    <main className="stream-page">
      <StreamTopBar streamPath={streamPath} />
      <div className="stream-page__hydrate">
        <div className="stream-page__spinner" aria-hidden="true" />
        <span>Hydrating stream viewer</span>
      </div>
    </main>
  );
}

function StreamPageWithDatabase({
  streamPath,
  snapshot,
  streamDatabase,
}: {
  streamPath: string;
  snapshot: StreamBrowserSnapshot;
  streamDatabase: StreamBrowserDatabase;
}) {
  // SQLocal 0.18's reactive table analyzer currently does not recognize
  // `SELECT count(*) FROM events` as reading `events`, so keep the schema to
  // the single requested table and count projected integer keys in React.
  const countQuery = useMemo(
    () => (sql: SqlTag) => sql`
      SELECT virtual_index
      FROM events
      ORDER BY virtual_index ASC
    `,
    [],
  );
  const countRows = useStreamReactiveQuery<{ virtual_index: number }>(
    streamDatabase.sqlocal,
    countQuery,
  );
  const eventCount = countRows.length;

  return (
    <StreamPageLayout
      eventCount={eventCount}
      snapshot={snapshot}
      streamDatabase={streamDatabase}
      streamPath={streamPath}
    />
  );
}

function StreamPageLayout({
  streamPath,
  snapshot,
  streamDatabase,
  eventCount,
}: {
  streamPath: string;
  snapshot: StreamBrowserSnapshot;
  streamDatabase: StreamBrowserDatabase | undefined;
  eventCount: number;
}) {

  return (
    <main className="stream-page">
      <StreamTopBar key={`top:${streamPath}`} streamPath={streamPath} />
      <div className="stream-page__body">
        <StreamSidebar
          eventCount={eventCount}
          key={`sidebar:${streamPath}`}
          snapshot={snapshot}
          streamDatabase={streamDatabase}
          streamPath={streamPath}
        />
        <div className="stream-page__main">
          {streamDatabase === undefined ? (
            <section className="stream-page__stream" aria-label="Stream events">
              <pre className="stream-page__empty">[]</pre>
            </section>
          ) : (
            <EventRows
              eventCount={eventCount}
              key={`events:${streamPath}`}
              streamDatabase={streamDatabase}
            />
          )}
          <StreamComposer key={`composer:${streamPath}`} streamPath={streamPath} />
        </div>
      </div>
    </main>
  );
}

function StreamTopBar({ streamPath }: { streamPath: string }) {
  const [editedPath, setEditedPath] = useState<string | undefined>();
  const draftPath = editedPath ?? streamPath;
  const trimmedDraftPath = draftPath.trim();
  const normalizedDraftPath =
    trimmedDraftPath === ""
      ? "/"
      : trimmedDraftPath.startsWith("/")
        ? trimmedDraftPath
        : `/${trimmedDraftPath}`;
  const showGoToStream = normalizedDraftPath !== streamPath;

  return (
    <header className="stream-page__top-bar">
      <div className="stream-page__controls">
        <label className="stream-page__label" htmlFor="stream-path">
          Stream
        </label>
        <input
          className="stream-page__input"
          id="stream-path"
          value={draftPath}
          onChange={(event) => setEditedPath(event.currentTarget.value)}
        />
        {showGoToStream ? (
          normalizedDraftPath === "/" ? (
            <Link className="stream-page__button" to="/streams">
              Go to stream
            </Link>
          ) : (
            <Link
              className="stream-page__button"
              to="/streams/$"
              params={{ _splat: normalizedDraftPath.slice(1) }}
            >
              Go to stream
            </Link>
          )
        ) : null}
      </div>
    </header>
  );
}

function EventRows({
  streamDatabase,
  eventCount,
}: {
  streamDatabase: StreamBrowserDatabase;
  eventCount: number;
}) {
  const parentRef = useRef<HTMLDivElement>(null);
  const scrolledToInitialEnd = useRef(false);
  const previousEventCount = useRef(eventCount);
  const [expandedOffsets, setExpandedOffsets] = useState(() => new Set<number>());
  const [scrollState, dispatchScrollState] = useReducer(
    (
      state,
      action:
        | { type: "follow-end" }
        | { type: "stop-following-end" }
        | { type: "clear-unread" }
        | { type: "add-unread"; count: number }
        | { type: "set-scroll-position"; scrollPosition: { isAtTop: boolean; isAtEnd: boolean } }
        | { type: "set-query-window"; queryWindow: { startIndex: number; limit: number } },
    ) => {
      switch (action.type) {
        case "follow-end":
          return { ...state, isFollowingEnd: true, unreadEventCount: 0 };
        case "stop-following-end":
          return state.isFollowingEnd ? { ...state, isFollowingEnd: false } : state;
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
            : { ...state, scrollPosition: action.scrollPosition };
        case "set-query-window":
          return state.queryWindow.startIndex === action.queryWindow.startIndex &&
            state.queryWindow.limit === action.queryWindow.limit
            ? state
            : { ...state, queryWindow: action.queryWindow };
      }
    },
    {
      isFollowingEnd: true,
      queryWindow: { startIndex: 0, limit: 80 },
      scrollPosition: { isAtTop: true, isAtEnd: true },
      unreadEventCount: 0,
    },
  );
  const virtualizer = useVirtualizer({
    count: eventCount,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 40,
    getItemKey: (index) => index,
    anchorTo: "end",
    followOnAppend: true,
    scrollEndThreshold: 4,
    overscan: 8,
    onChange(instance) {
      const virtualItems = instance.getVirtualItems();
      const firstVirtualItem = virtualItems[0];
      const lastVirtualItem = virtualItems.at(-1);
      if (firstVirtualItem !== undefined && lastVirtualItem !== undefined) {
        const nextQueryWindow = {
          startIndex: firstVirtualItem.index,
          limit: lastVirtualItem.index - firstVirtualItem.index + 1,
        };
        dispatchScrollState({ type: "set-query-window", queryWindow: nextQueryWindow });
      }

      const nextScrollPosition = {
        isAtTop: (instance.scrollOffset ?? 0) <= 4,
        isAtEnd: instance.isAtEnd(),
      };
      if (nextScrollPosition.isAtEnd) {
        dispatchScrollState({ type: "follow-end" });
      }
      dispatchScrollState({ type: "set-scroll-position", scrollPosition: nextScrollPosition });
    },
  });
  const virtualItems = virtualizer.getVirtualItems();

  useLayoutEffect(() => {
    const appendedEventCount = Math.max(0, eventCount - previousEventCount.current);
    previousEventCount.current = eventCount;

    if (!scrolledToInitialEnd.current) {
      if (eventCount === 0) return;
      scrolledToInitialEnd.current = true;
      virtualizer.scrollToEnd();
      dispatchScrollState({ type: "clear-unread" });
      return;
    }

    // TanStack Virtual owns the end measurement. This state is just the user's
    // intent: keep following the live end until they deliberately move away.
    if (scrollState.isFollowingEnd && eventCount > 0) {
      virtualizer.scrollToEnd();
      dispatchScrollState({ type: "clear-unread" });
    } else if (appendedEventCount > 0 && !scrollState.scrollPosition.isAtEnd) {
      dispatchScrollState({ type: "add-unread", count: appendedEventCount });
    }
  }, [eventCount, scrollState.isFollowingEnd, scrollState.scrollPosition.isAtEnd, virtualizer]);

  return (
    <section
      className="stream-page__stream"
      aria-label="Stream events"
      ref={parentRef}
      onTouchStart={() => dispatchScrollState({ type: "stop-following-end" })}
      onPointerDown={(event) => {
        if (event.target === event.currentTarget) {
          dispatchScrollState({ type: "stop-following-end" });
        }
      }}
      onWheel={(event) => {
        if (event.deltaY < 0) dispatchScrollState({ type: "stop-following-end" });
      }}
    >
      {eventCount === 0 ? (
        <pre className="stream-page__empty">[]</pre>
      ) : (
        <>
          <div
            className="stream-page__virtual-content"
            style={{ height: virtualizer.getTotalSize() }}
          >
            <EventRowWindow
              expandedOffsets={expandedOffsets}
              queryWindow={scrollState.queryWindow}
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
          <div className="stream-page__scroll-affordances">
            {!scrollState.scrollPosition.isAtTop ? (
              <button
                aria-label="Scroll to top"
                className="stream-page__scroll-button"
                type="button"
                onClick={() => {
                  dispatchScrollState({ type: "stop-following-end" });
                  virtualizer.scrollToIndex(0, { align: "start" });
                }}
              >
                ↑
              </button>
            ) : null}
            {!scrollState.isFollowingEnd && !scrollState.scrollPosition.isAtEnd ? (
              <div className="stream-page__bottom-affordance">
                {scrollState.unreadEventCount > 0 ? (
                  <output className="stream-page__unread-badge" aria-live="polite">
                    {scrollState.unreadEventCount} new
                  </output>
                ) : null}
                <button
                  aria-label="Scroll to bottom"
                  className="stream-page__scroll-button"
                  type="button"
                  onClick={() => {
                    dispatchScrollState({ type: "follow-end" });
                    virtualizer.scrollToEnd();
                  }}
                >
                  ↓
                </button>
              </div>
            ) : null}
          </div>
        </>
      )}
    </section>
  );
}

function EventRowWindow({
  streamDatabase,
  queryWindow,
  virtualItems,
  expandedOffsets,
  measureElement,
  onToggleOffset,
}: {
  streamDatabase: StreamBrowserDatabase;
  queryWindow: { startIndex: number; limit: number };
  virtualItems: VirtualItem[];
  expandedOffsets: Set<number>;
  measureElement: (node: Element | null) => void;
  onToggleOffset(offset: number): void;
}) {
  const rowQuery = useMemo(
    () => (sql: SqlTag) => sql`
      SELECT virtual_index, offset, type, idempotency_key, created_at, raw_json
      FROM events
      ORDER BY virtual_index ASC
      LIMIT ${queryWindow.limit}
      OFFSET ${queryWindow.startIndex}
    `,
    [queryWindow.limit, queryWindow.startIndex],
  );
  const rows = useStreamReactiveQuery<StreamEventRow>(streamDatabase.sqlocal, rowQuery);

  return virtualItems.map((virtualItem) => {
    const event = rows[virtualItem.index - queryWindow.startIndex];
    const isExpanded = event !== undefined && expandedOffsets.has(event.offset);

    return (
      <div
        className="stream-page__virtual-row"
        data-index={virtualItem.index}
        key={virtualItem.key}
        ref={measureElement}
        style={{ transform: `translateY(${virtualItem.start}px)` }}
      >
        {event === undefined ? (
          <article className="stream-page__event-row stream-page__event-row--pending" />
        ) : (
          <article className="stream-page__event-row">
            <button
              aria-expanded={isExpanded}
              className="stream-page__event-meta"
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

function useStreamReactiveQuery<Result extends Record<string, unknown>>(
  sqlocal: SQLocal,
  query: StatementInput<Result>,
) {
  const reactiveQuery = useMemo(() => sqlocal.reactiveQuery<Result>(query), [query, sqlocal]);
  return useSyncExternalStore(
    (onStoreChange) => {
      const subscription = reactiveQuery.subscribe(
        () => onStoreChange(),
        (error) => {
          console.error("SQLocal reactive query failed", error);
          onStoreChange();
        },
      );
      return () => subscription.unsubscribe();
    },
    () => reactiveQuery.value,
    () => [],
  );
}

function StreamSidebar({
  streamPath,
  snapshot,
  streamDatabase,
  eventCount,
}: {
  streamPath: string;
  snapshot: StreamBrowserSnapshot;
  streamDatabase: StreamBrowserDatabase | undefined;
  eventCount: number;
}) {
  const [insertEventCount, setInsertEventCount] = useState("10");
  const [periodSeconds, setPeriodSeconds] = useState("5");
  const [insertState, setInsertState] = useState<"idle" | "inserting" | "done" | "error">("idle");
  const [databaseActionState, setDatabaseActionState] = useState<
    "idle" | "downloading" | "clearing" | "done" | "error"
  >("idle");

  async function insertRandomEvents() {
    const count = Math.max(0, Math.floor(Number(insertEventCount)));
    const periodMs = Math.max(0, Number(periodSeconds) * 1_000);
    if (!Number.isFinite(count) || !Number.isFinite(periodMs) || count === 0) return;

    setInsertState("inserting");
    const stream = withStream({
      url: new URL(`/stream/${encodeURIComponent(streamPath)}`, window.location.href),
    });

    try {
      if (periodMs === 0 || count === 1) {
        await stream.rpc.appendBatch({
          events: Array.from({ length: count }, (_, index) => ({
            type: "events.iterate.com/debug/random-event",
            payload: {
              streamPath,
              index,
              count,
              value: crypto.randomUUID(),
            },
          })),
        });
        setInsertState("done");
        return;
      }

      await Promise.all(
        Array.from({ length: count }, (_, index) =>
          new Promise<void>((resolve) => {
            setTimeout(resolve, (periodMs / (count - 1)) * index);
          }).then(() =>
            stream.rpc.append({
              event: {
                type: "events.iterate.com/debug/random-event",
                payload: {
                  streamPath,
                  index,
                  count,
                  value: crypto.randomUUID(),
                },
              },
            }),
          ),
        ),
      );
      setInsertState("done");
    } catch {
      setInsertState("error");
    } finally {
      stream[Symbol.dispose]();
    }
  }

  return (
    <aside className="stream-page__sidebar">
      <section className="stream-page__tool">
        <h2 className="stream-page__tool-title">Subscription</h2>
        <dl className="stream-page__facts">
          <div>
            <dt>Status</dt>
            <dd>
              <output
                className={
                  snapshot.connectionStatus === "error"
                    ? "stream-page__state stream-page__state--error"
                    : "stream-page__state"
                }
              >
                {snapshot.connectionStatus}
              </output>
            </dd>
          </div>
          <div>
            <dt>Events</dt>
            <dd>
              <output className="stream-page__state">{eventCount}</output>
            </dd>
          </div>
          <div>
            <dt>OPFS isolated</dt>
            <dd>
              <output className="stream-page__state">
                {String(snapshot.databaseInfo?.crossOriginIsolated ?? false)}
              </output>
            </dd>
          </div>
          <div>
            <dt>Storage</dt>
            <dd>
              <output className="stream-page__state">
                {snapshot.databaseInfo?.storageType ?? "pending"}
              </output>
            </dd>
          </div>
          <div>
            <dt>Persisted</dt>
            <dd>
              <output className="stream-page__state">
                {String(snapshot.databaseInfo?.persisted ?? false)}
              </output>
            </dd>
          </div>
          <div>
            <dt>DB bytes</dt>
            <dd>
              <output className="stream-page__state">
                {snapshot.databaseInfo?.databaseSizeBytes ?? 0}
              </output>
            </dd>
          </div>
        </dl>
        <div className="stream-page__button-row">
          <button
            className="stream-page__button"
            disabled={streamDatabase === undefined || databaseActionState === "downloading"}
            type="button"
            onClick={() => {
              if (streamDatabase === undefined) return;
              setDatabaseActionState("downloading");
              void streamDatabase.download().then(
                () => setDatabaseActionState("done"),
                () => setDatabaseActionState("error"),
              );
            }}
          >
            Download DB
          </button>
          <button
            className="stream-page__button stream-page__button--secondary"
            disabled={streamDatabase === undefined || databaseActionState === "clearing"}
            type="button"
            onClick={() => {
              if (streamDatabase === undefined) return;
              setDatabaseActionState("clearing");
              void streamDatabase.clear().then(
                () => setDatabaseActionState("done"),
                () => setDatabaseActionState("error"),
              );
            }}
          >
            Clear local DB
          </button>
        </div>
        <output
          className={
            databaseActionState === "error"
              ? "stream-page__insert-state stream-page__insert-state--error"
              : "stream-page__insert-state"
          }
        >
          {databaseActionState}
        </output>
      </section>
      <section className="stream-page__tool">
        <h2 className="stream-page__tool-title">Insert events</h2>
        <label className="stream-page__field">
          <span>Count</span>
          <input
            className="stream-page__input"
            min="1"
            step="1"
            type="number"
            value={insertEventCount}
            onChange={(event) => setInsertEventCount(event.currentTarget.value)}
          />
        </label>
        <label className="stream-page__field">
          <span>Seconds</span>
          <input
            className="stream-page__input"
            min="0"
            step="0.1"
            type="number"
            value={periodSeconds}
            onChange={(event) => setPeriodSeconds(event.currentTarget.value)}
          />
        </label>
        <button
          className="stream-page__button"
          disabled={insertState === "inserting"}
          type="button"
          onClick={() => void insertRandomEvents()}
        >
          Stream random events
        </button>
        <output className="stream-page__insert-state">{insertState}</output>
      </section>
    </aside>
  );
}

function StreamComposer({ streamPath }: { streamPath: string }) {
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
    const stream = withStream({
      url: new URL(`/stream/${encodeURIComponent(streamPath)}`, window.location.href),
    });

    try {
      await stream.rpc.append({
        event: JSON.parse(composerText),
      });
      setAppendState("done");
    } catch {
      setAppendState("error");
    } finally {
      stream[Symbol.dispose]();
    }
  }

  return (
    <section className="stream-page__composer" aria-label="Append event">
      <textarea
        aria-label="Event JSON"
        className="stream-page__textarea"
        spellCheck={false}
        value={composerText}
        onChange={(event) => setComposerText(event.currentTarget.value)}
      />
      <div className="stream-page__composer-actions">
        <button
          className="stream-page__button"
          disabled={appendState === "appending"}
          type="button"
          onClick={() => void appendComposerEvent()}
        >
          Append
        </button>
        <output
          className={
            appendState === "error"
              ? "stream-page__insert-state stream-page__insert-state--error"
              : "stream-page__insert-state"
          }
        >
          {appendState}
        </output>
      </div>
    </section>
  );
}
