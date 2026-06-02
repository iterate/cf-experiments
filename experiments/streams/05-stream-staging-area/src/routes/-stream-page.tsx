import {
  useCallback,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import { Link } from "@tanstack/react-router";
import { useVirtualizer } from "@tanstack/react-virtual";
import {
  createStreamBrowserStore,
  type StreamBrowserSnapshot,
} from "../client-libraries/stream-browser-store.js";
import { withStream } from "../client-libraries/stream-browser.js";
import "./-stream-page.css";

export function StreamPage({ streamPath }: { streamPath: string }) {
  const streamStore = useMemo(
    () => createStreamBrowserStore({ streamPath }),
    [streamPath],
  );
  const snapshot = useSyncExternalStore(
    streamStore.subscribe,
    streamStore.getSnapshot,
    streamStore.getServerSnapshot,
  );

  return (
    <main className="stream-page">
      <StreamTopBar key={`top:${streamPath}`} streamPath={streamPath} />
      <div className="stream-page__body">
        <StreamSidebar key={`sidebar:${streamPath}`} streamPath={streamPath} snapshot={snapshot} />
        <div className="stream-page__main">
          <EventRows key={`events:${streamPath}`} snapshot={snapshot} />
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

function EventRows({ snapshot }: { snapshot: StreamBrowserSnapshot }) {
  const events = snapshot.events;
  const parentRef = useRef<HTMLDivElement>(null);
  const previousEventCount = useRef(events.length);
  const [expandedOffsets, setExpandedOffsets] = useState(() => new Set<number>());
  const [isFollowingEnd, setIsFollowingEnd] = useState(true);
  const [unreadEventCount, setUnreadEventCount] = useState(0);
  const [scrollPosition, setScrollPosition] = useState({
    isAtTop: true,
    isAtEnd: true,
  });
  const virtualizer = useVirtualizer({
    count: events.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 220,
    getItemKey: (index) => events[index]?.offset ?? index,
    anchorTo: "end",
    overscan: 8,
  });
  const virtualItems = virtualizer.getVirtualItems();

  const syncScrollPosition = useCallback(() => {
    const scrollElement = parentRef.current;
    if (scrollElement === null || events.length === 0) {
      setScrollPosition((current) =>
        current.isAtTop && current.isAtEnd ? current : { isAtTop: true, isAtEnd: true },
      );
      return;
    }
    const distanceFromEnd =
      scrollElement.scrollHeight - scrollElement.scrollTop - scrollElement.clientHeight;
    // Keep this strict: a generous threshold makes small upward scrolls get treated
    // as "still at bottom", so new events immediately pull the viewport back down.
    const nextScrollPosition = {
      isAtTop: scrollElement.scrollTop <= 4,
      isAtEnd: distanceFromEnd <= 4,
    };
    if (nextScrollPosition.isAtEnd) {
      setIsFollowingEnd((current) => (current ? current : true));
      setUnreadEventCount((current) => (current === 0 ? current : 0));
    }
    setScrollPosition((current) =>
      current.isAtTop === nextScrollPosition.isAtTop &&
      current.isAtEnd === nextScrollPosition.isAtEnd
        ? current
        : nextScrollPosition,
    );
  }, [events.length]);

  useLayoutEffect(() => {
    const appendedEventCount = Math.max(0, events.length - previousEventCount.current);
    previousEventCount.current = events.length;

    // The live-end follow mode is user intent. Virtualizer measurements can briefly
    // lag while many rows append, so button/unread state must not be driven by one
    // frame where the measured scroll position is temporarily behind the rendered rows.
    if (isFollowingEnd && events.length > 0) {
      virtualizer.scrollToEnd();
      setUnreadEventCount((current) => (current === 0 ? current : 0));
    } else if (appendedEventCount > 0) {
      setUnreadEventCount((current) => current + appendedEventCount);
    }
    syncScrollPosition();
  }, [events.length, isFollowingEnd, syncScrollPosition, virtualizer]);

  return (
    <section
      className="stream-page__stream"
      aria-label="Stream events"
      ref={parentRef}
      onScroll={syncScrollPosition}
      onTouchStart={() => {
        setIsFollowingEnd(false);
      }}
      onPointerDown={(event) => {
        if (event.target === event.currentTarget) setIsFollowingEnd(false);
      }}
      onWheel={(event) => {
        if (event.deltaY < 0) setIsFollowingEnd(false);
      }}
    >
      {events.length === 0 ? (
        <pre className="stream-page__empty">[]</pre>
      ) : (
        <>
          <div
            className="stream-page__virtual-content"
            style={{ height: virtualizer.getTotalSize() }}
          >
            {virtualItems.map((virtualItem) => {
              const event = events[virtualItem.index];
              if (event === undefined) return null;
              const isExpanded = expandedOffsets.has(event.offset);
              return (
                <div
                  className="stream-page__virtual-row"
                  data-index={virtualItem.index}
                  key={virtualItem.key}
                  ref={virtualizer.measureElement}
                  style={{ transform: `translateY(${virtualItem.start}px)` }}
                >
                  <article className="stream-page__event-row">
                    <button
                      aria-expanded={isExpanded}
                      className="stream-page__event-meta"
                      type="button"
                      onClick={() => {
                        setExpandedOffsets((current) => {
                          const next = new Set(current);
                          if (next.has(event.offset)) {
                            next.delete(event.offset);
                          } else {
                            next.add(event.offset);
                          }
                          return next;
                        });
                      }}
                    >
                      <span>{event.offset}</span>
                      <span>{event.type}</span>
                      <time dateTime={event.createdAt}>{event.createdAt}</time>
                    </button>
                    {isExpanded ? (
                      <pre className="stream-page__event-json">
                        {JSON.stringify(event, null, 2)}
                      </pre>
                    ) : null}
                  </article>
                </div>
              );
            })}
          </div>
          <div className="stream-page__scroll-affordances">
            {!scrollPosition.isAtTop ? (
              <button
                aria-label="Scroll to top"
                className="stream-page__scroll-button"
                type="button"
                onClick={() => {
                  setIsFollowingEnd(false);
                  virtualizer.scrollToIndex(0, { align: "start" });
                }}
              >
                ↑
              </button>
            ) : null}
            {!isFollowingEnd && !scrollPosition.isAtEnd ? (
              <div className="stream-page__bottom-affordance">
                {unreadEventCount > 0 ? (
                  <output className="stream-page__unread-badge" aria-live="polite">
                    {unreadEventCount} new
                  </output>
                ) : null}
                <button
                  aria-label="Scroll to bottom"
                  className="stream-page__scroll-button"
                  type="button"
                  onClick={() => {
                    // This means "follow the live end", not just "jump there once".
                    setIsFollowingEnd(true);
                    virtualizer.scrollToEnd();
                    setUnreadEventCount(0);
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

function StreamSidebar({
  streamPath,
  snapshot,
}: {
  streamPath: string;
  snapshot: StreamBrowserSnapshot;
}) {
  const [eventCount, setEventCount] = useState("10");
  const [periodSeconds, setPeriodSeconds] = useState("5");
  const [insertState, setInsertState] = useState<"idle" | "inserting" | "done" | "error">("idle");

  async function insertRandomEvents() {
    const count = Math.max(0, Math.floor(Number(eventCount)));
    const periodMs = Math.max(0, Number(periodSeconds) * 1_000);
    if (!Number.isFinite(count) || !Number.isFinite(periodMs) || count === 0) return;

    setInsertState("inserting");
    using stream = withStream({
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
              <output className="stream-page__state">{snapshot.events.length}</output>
            </dd>
          </div>
        </dl>
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
            value={eventCount}
            onChange={(event) => setEventCount(event.currentTarget.value)}
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
    using stream = withStream({
      url: new URL(`/stream/${encodeURIComponent(streamPath)}`, window.location.href),
    });

    try {
      await stream.rpc.append({
        event: JSON.parse(composerText),
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
