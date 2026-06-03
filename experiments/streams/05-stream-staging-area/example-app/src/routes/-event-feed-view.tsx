// The "event-feed" sibling view: grouped feed_items from the browser-event-feed processor.
// Consecutive events of the same type collapse into one row; specific-renderer types
// (created/woken) always get their own singleton row.

import { useLayoutEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import { acquireStreamRuntime } from "../../../src/browser/stream-browser-store.js";
import {
  BROWSER_EVENT_FEED_SCHEMA_VERSION,
  BROWSER_EVENT_FEED_TABLE,
  browserEventFeed,
  loadBrowserEventFeedCheckpoint,
} from "../../../src/processors/browser-event-feed/implementation.js";
import { useStreamQuery } from "../../../src/browser/hooks/use-stream-query.js";

type FeedItemRow = {
  local_index: number;
  component: string;
  first_offset: number;
  last_offset: number;
  event_count: number;
  data: Record<string, unknown>;
};

const SPECIFIC_RENDERER_TYPES: Record<string, string> = {
  "stream.created": "events.iterate.com/stream/created",
  "stream.woken": "events.iterate.com/stream/woken",
};

export function EventFeedView({ streamPath }: { streamPath: string }) {
  const store = useMemo(
    () =>
      acquireStreamRuntime({
        streamPath,
        processor: browserEventFeed,
        schemaVersion: BROWSER_EVENT_FEED_SCHEMA_VERSION,
        tables: [BROWSER_EVENT_FEED_TABLE],
        loadCheckpoint: loadBrowserEventFeedCheckpoint,
      }),
    [streamPath],
  );
  const snapshot = useSyncExternalStore(store.subscribe, store.getSnapshot, store.getServerSnapshot);
  const db = store.streamDatabase;

  const countResult = useStreamQuery(db, `SELECT COUNT(*) AS count FROM feed_items`);
  const itemCount = Number(countResult.data[0]?.count ?? 0);
  const rowsResult = useStreamQuery(
    db,
    `SELECT local_index, component, first_offset, last_offset, event_count, json(data) AS data
     FROM feed_items ORDER BY local_index ASC`,
  );
  const rows = rowsResult.data.flatMap((row) => {
    const parsed = parseFeedItem(row);
    return parsed === undefined ? [] : [parsed];
  });

  if (countResult.status !== "ok") {
    return (
      <section
        aria-label="Event feed"
        className="relative grid min-h-0 flex-1 place-items-center overflow-y-auto bg-white"
      >
        <div className="flex min-h-60 items-center justify-center gap-2.5 text-sm text-slate-500">
          <div className="size-4 animate-spin rounded-full border-2 border-slate-300 border-t-slate-600" aria-hidden="true" />
          <span>opening feed_items table</span>
        </div>
      </section>
    );
  }

  return (
    <div className="relative flex min-h-0 flex-1 flex-col">
      <section
        aria-label="Event feed"
        data-testid="event-feed"
        className="relative flex min-h-0 flex-1 flex-col overflow-x-hidden overflow-y-auto bg-white pr-4 [scrollbar-color:rgb(22_24_29_/_12%)_transparent] [scrollbar-gutter:stable_both-edges] [scrollbar-width:thin]"
      >
        <div
          className="sticky top-0 z-3 grid min-h-11 flex-none grid-cols-[minmax(0,1fr)_auto] items-center gap-2.5 border-b border-[#eef1f5] bg-white/95 pr-4 backdrop-blur-sm"
          data-testid="feed-summary-bar"
        >
          <span className="text-xs font-semibold text-[#667085]">Event feed</span>
          <output className="whitespace-nowrap font-mono text-xs text-[#667085]" data-testid="feed-item-count">
            {itemCount.toLocaleString()} feed {itemCount === 1 ? "item" : "items"}
          </output>
        </div>

        {rows.length === 0 ? (
          <div className="flex min-h-60 flex-1 items-center justify-center gap-2.5 text-sm text-slate-500">
            {snapshot.connectionStatus === "subscribed" ? null : (
              <div className="size-4 animate-spin rounded-full border-2 border-slate-300 border-t-slate-600" aria-hidden="true" />
            )}
            <span>
              {snapshot.connectionError === undefined
                ? snapshot.connectionStatus === "subscribed"
                  ? "No feed items yet"
                  : `Stream connection is ${snapshot.connectionStatus}`
                : `Stream connection is ${snapshot.connectionStatus}: ${snapshot.connectionError}`}
            </span>
          </div>
        ) : (
          <div className="flex-1 pb-2">
            {rows.map((row, index) => (
              <FeedItem key={row.local_index} isLast={index === rows.length - 1} row={row} />
            ))}
          </div>
        )}

        <div className="sticky bottom-0 z-[2] bg-white">
          <FeedComposer streamStore={store} />
        </div>
      </section>
    </div>
  );
}

function FeedItem({ row, isLast }: { row: FeedItemRow; isLast: boolean }) {
  const [expanded, setExpanded] = useState(false);
  const eventType = feedItemEventType(row);
  const offsetLabel =
    row.event_count === 1 ? String(row.first_offset) : `${row.first_offset}–${row.last_offset}`;
  const detailLabel =
    row.event_count === 1
      ? "1 event"
      : `${row.event_count.toLocaleString()} events`;

  return (
    <div
      className={
        isLast
          ? "pb-2"
          : "pb-2 after:absolute after:bottom-1 after:left-0 after:right-0 after:h-px after:bg-[#eef1f5] relative"
      }
    >
      <article
        data-testid="feed-item"
        data-component={row.component}
        data-event-type={eventType}
        data-first-offset={row.first_offset}
        data-last-offset={row.last_offset}
        data-event-count={row.event_count}
        className="min-w-0 overflow-hidden bg-white"
      >
        <button
          aria-expanded={expanded}
          className="grid w-full cursor-pointer grid-cols-[64px_minmax(0,1fr)_auto] items-center gap-3 border-0 bg-transparent px-2.5 py-2 text-left font-mono text-xs text-[#536073] hover:bg-[#f8fafc]"
          data-testid="feed-item-meta"
          type="button"
          onClick={() => setExpanded((current) => !current)}
        >
          <span>{offsetLabel}</span>
          <span className="truncate">{eventType}</span>
          <span className="whitespace-nowrap text-[#667085]">{detailLabel}</span>
        </button>
        {expanded ? (
          <pre className="m-0 overflow-auto whitespace-pre-wrap break-words p-2.5 font-mono text-[13px] leading-normal" data-testid="feed-item-json">
            {JSON.stringify(row.data, null, 2)}
          </pre>
        ) : null}
      </article>
    </div>
  );
}

function feedItemEventType(row: FeedItemRow) {
  if (typeof row.data.eventType === "string") return row.data.eventType;
  return SPECIFIC_RENDERER_TYPES[row.component] ?? row.component;
}

function parseFeedItem(row: Record<string, unknown>): FeedItemRow | undefined {
  if (
    typeof row.local_index !== "number" ||
    typeof row.component !== "string" ||
    typeof row.first_offset !== "number" ||
    typeof row.last_offset !== "number" ||
    typeof row.event_count !== "number"
  ) {
    return undefined;
  }
  let data: Record<string, unknown> = {};
  if (typeof row.data === "string") {
    try {
      const parsed: unknown = JSON.parse(row.data);
      if (parsed !== null && typeof parsed === "object") data = parsed as Record<string, unknown>;
    } catch {
      data = {};
    }
  }
  return {
    local_index: row.local_index,
    component: row.component,
    first_offset: row.first_offset,
    last_offset: row.last_offset,
    event_count: row.event_count,
    data,
  };
}

function FeedComposer({ streamStore }: { streamStore: ReturnType<typeof acquireStreamRuntime> }) {
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
    <section className="relative bg-white py-4" aria-label="Append event" data-testid="stream-composer">
      <div className="relative">
        <textarea
          aria-label="Event JSON"
          className="block min-h-[104px] w-full resize-y box-border rounded-md border border-[#bac2cf] px-2.5 pb-[38px] pt-2.5 font-mono text-[13px] leading-[1.5]"
          data-testid="composer-textarea"
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
                ? "pointer-events-none absolute bottom-[13px] left-3 font-mono text-[11px] uppercase text-[#b42318]"
                : "pointer-events-none absolute bottom-[13px] left-3 font-mono text-[11px] uppercase text-[#667085]"
            }
            data-testid="composer-state"
          >
            {appendState === "appending" ? "appending" : appendState === "done" ? "appended" : "error"}
          </output>
        )}
        <button
          aria-label={
            appendState === "error" ? "Append failed; retry append event" : "Append event"
          }
          className={
            appendState === "error"
              ? "absolute bottom-1.5 right-1.5 inline-flex size-7 cursor-pointer items-center justify-center rounded-full border-0 bg-transparent p-0 text-[#b42318] hover:bg-[#f2f4f7] disabled:cursor-default disabled:opacity-60"
              : appendState === "done"
                ? "absolute bottom-1.5 right-1.5 inline-flex size-7 cursor-pointer items-center justify-center rounded-full border-0 bg-transparent p-0 text-[#067647] hover:bg-[#f2f4f7] disabled:cursor-default disabled:opacity-60"
                : "absolute bottom-1.5 right-1.5 inline-flex size-7 cursor-pointer items-center justify-center rounded-full border-0 bg-transparent p-0 text-[#98a2b3] hover:bg-[#f2f4f7] hover:text-[#536073] disabled:cursor-default disabled:opacity-60"
          }
          disabled={appendState === "appending"}
          type="button"
          onClick={() => void appendComposerEvent()}
        >
          ↗
        </button>
      </div>
    </section>
  );
}
