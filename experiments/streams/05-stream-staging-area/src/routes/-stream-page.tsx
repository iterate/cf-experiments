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
  type StreamBrowserStore,
} from "../client-libraries/stream-browser-store.js";
import {
  getStreamBrowserDatabase,
  type StreamBrowserDatabase,
  type StreamDatabaseWriteMode,
  type StreamEventRow,
} from "../client-libraries/stream-browser-db.js";
import "./-stream-page.css";

export function StreamPage({ streamPath }: { streamPath: string }) {
  return (
    <ClientOnly fallback={<StreamHydrationFallback streamPath={streamPath} />}>
      <HydratedStreamPage streamPath={streamPath} />
    </ClientOnly>
  );
}

function HydratedStreamPage({ streamPath }: { streamPath: string }) {
  const [sqliteWriteMode, setSqliteWriteMode] = useState<StreamDatabaseWriteMode>("batch");
  const streamStore = useMemo(
    () => createStreamBrowserStore({ sqliteWriteMode, streamPath }),
    [sqliteWriteMode, streamPath],
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
      sqliteWriteMode={sqliteWriteMode}
      streamDatabase={streamDatabase}
      streamStore={streamStore}
      streamPath={streamPath}
      onSqliteWriteModeChange={setSqliteWriteMode}
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
  sqliteWriteMode,
  streamDatabase,
  streamStore,
  onSqliteWriteModeChange,
}: {
  streamPath: string;
  snapshot: StreamBrowserSnapshot;
  sqliteWriteMode: StreamDatabaseWriteMode;
  streamDatabase: StreamBrowserDatabase;
  streamStore: StreamBrowserStore;
  onSqliteWriteModeChange(writeMode: StreamDatabaseWriteMode): void;
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
      sqliteWriteMode={sqliteWriteMode}
      streamDatabase={streamDatabase}
      streamStore={streamStore}
      streamPath={streamPath}
      onSqliteWriteModeChange={onSqliteWriteModeChange}
    />
  );
}

function StreamPageLayout({
  streamPath,
  snapshot,
  sqliteWriteMode,
  streamDatabase,
  streamStore,
  eventCount,
  onSqliteWriteModeChange,
}: {
  streamPath: string;
  snapshot: StreamBrowserSnapshot;
  sqliteWriteMode: StreamDatabaseWriteMode;
  streamDatabase: StreamBrowserDatabase | undefined;
  streamStore: StreamBrowserStore;
  eventCount: number;
  onSqliteWriteModeChange(writeMode: StreamDatabaseWriteMode): void;
}) {

  return (
    <main className="stream-page">
      <StreamTopBar key={`top:${streamPath}`} streamPath={streamPath} />
      <div className="stream-page__body">
        <StreamSidebar
          eventCount={eventCount}
          key={`sidebar:${streamPath}`}
          snapshot={snapshot}
          sqliteWriteMode={sqliteWriteMode}
          streamDatabase={streamDatabase}
          streamStore={streamStore}
          streamPath={streamPath}
          onSqliteWriteModeChange={onSqliteWriteModeChange}
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
          <StreamComposer key={`composer:${streamPath}`} streamStore={streamStore} />
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
  sqliteWriteMode,
  streamDatabase,
  streamStore,
  eventCount,
  onSqliteWriteModeChange,
}: {
  streamPath: string;
  snapshot: StreamBrowserSnapshot;
  sqliteWriteMode: StreamDatabaseWriteMode;
  streamDatabase: StreamBrowserDatabase | undefined;
  streamStore: StreamBrowserStore;
  eventCount: number;
  onSqliteWriteModeChange(writeMode: StreamDatabaseWriteMode): void;
}) {
  return (
    <aside className="stream-page__sidebar">
      <SubscriptionTool
        eventCount={eventCount}
        snapshot={snapshot}
        streamDatabase={streamDatabase}
      />
      <InsertEventsTool
        sqliteWriteMode={sqliteWriteMode}
        streamStore={streamStore}
        streamPath={streamPath}
        onSqliteWriteModeChange={onSqliteWriteModeChange}
      />
    </aside>
  );
}

function SubscriptionTool({
  snapshot,
  streamDatabase,
  eventCount,
}: {
  snapshot: StreamBrowserSnapshot;
  streamDatabase: StreamBrowserDatabase | undefined;
  eventCount: number;
}) {
  const [databaseActionState, setDatabaseActionState] = useState<
    "idle" | "downloading" | "clearing" | "done" | "error"
  >("idle");

  return (
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
  );
}

function InsertEventsTool({
  streamPath,
  sqliteWriteMode,
  streamStore,
  onSqliteWriteModeChange,
}: {
  streamPath: string;
  sqliteWriteMode: StreamDatabaseWriteMode;
  streamStore: StreamBrowserStore;
  onSqliteWriteModeChange(writeMode: StreamDatabaseWriteMode): void;
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
      insertEventCount: "10",
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
      <label className="stream-page__field">
        <span>SQLite writes</span>
        <select
          className="stream-page__input"
          value={sqliteWriteMode}
          onChange={(event) =>
            onSqliteWriteModeChange(
              event.currentTarget.value === "row" ? "row" : "batch",
            )
          }
        >
          <option value="batch">Batch</option>
          <option value="row">Row at a time</option>
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
