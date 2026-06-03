import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { useVirtualizer } from "@tanstack/react-virtual";
import "./-stream-page.css";

type ReproFooter = "none" | "normal" | "overlay-measured";
type ReproMode = "single" | "microtask" | "raf" | "timeout";

export const Route = createFileRoute("/virtual-repro")({
  validateSearch: (search) => ({
    affordance: search.affordance === "1",
    batch: numberSearchParam(search.batch, 1500),
    chunk: numberSearchParam(search.chunk, 250),
    delayMs: numberSearchParam(search.delayMs, 0),
    directDomUpdates: search.directDomUpdates === "1",
    footer: footerSearchParam(search.footer),
    initial: numberSearchParam(search.initial, 2),
    indexKeys: search.indexKeys === "1",
    mode: modeSearchParam(search.mode),
    threshold: numberSearchParam(search.threshold, 80),
  }),
  component: VirtualReproRoute,
});

function VirtualReproRoute() {
  const search = Route.useSearch();
  const parentRef = useRef<HTMLDivElement>(null);
  const footerRef = useRef<HTMLDivElement>(null);
  const didInitialScroll = useRef(false);
  const nextMessageIndex = useRef(search.initial);
  const [footerHeight, setFooterHeight] = useState(0);
  const [hydrated, setHydrated] = useState(false);
  const [messages, setMessages] = useState(() => makeMessages(0, search.initial));
  const [runState, setRunState] = useState<"idle" | "running" | "done">("idle");

  const virtualizer = useVirtualizer({
    count: messages.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 40,
    getItemKey: (index) => search.indexKeys ? index : messages[index]?.id ?? index,
    anchorTo: "end",
    followOnAppend: true,
    paddingEnd: search.footer === "overlay-measured" ? footerHeight : 0,
    scrollEndThreshold: search.threshold,
    overscan: 6,
    directDomUpdates: search.directDomUpdates,
  });
  const virtualItems = virtualizer.getVirtualItems();
  const firstVirtualItem = virtualItems[0];
  const lastVirtualItem = virtualItems.at(-1);

  useEffect(() => {
    setHydrated(true);
  }, []);

  useLayoutEffect(() => {
    if (didInitialScroll.current) return;
    didInitialScroll.current = true;
    virtualizer.scrollToEnd();
  }, [virtualizer]);

  useLayoutEffect(() => {
    if (search.footer !== "overlay-measured") return;
    const footerElement = footerRef.current;
    if (footerElement === null) return;

    function updateFooterHeight() {
      if (footerElement === null) return;
      setFooterHeight(Math.ceil(footerElement.getBoundingClientRect().height));
    }

    updateFooterHeight();
    const resizeObserver = new ResizeObserver(updateFooterHeight);
    resizeObserver.observe(footerElement);
    return () => resizeObserver.disconnect();
  }, [search.footer]);

  async function appendBatch() {
    setRunState("running");
    const chunks = chunksFor({
      totalCount: search.batch,
      chunkSize: search.chunk,
      startIndex: nextMessageIndex.current,
    });
    nextMessageIndex.current += search.batch;

    for (const chunk of chunks) {
      await waitForMode(search.mode, search.delayMs);
      setMessages((current) => [...current, ...chunk]);
    }
    setRunState("done");
  }

  function reset() {
    didInitialScroll.current = false;
    nextMessageIndex.current = search.initial;
    setMessages(makeMessages(0, search.initial));
    setRunState("idle");
  }

  return (
    <main className="virtual-repro">
      <section className="virtual-repro__panel">
        <h1>TanStack Virtual chat repro</h1>
        <dl>
          <div><dt>Count</dt><dd data-testid="virtual-repro-count">{messages.length}</dd></div>
          <div><dt>At end</dt><dd>{String(virtualizer.isAtEnd(search.threshold))}</dd></div>
          <div><dt>Distance</dt><dd data-testid="virtual-repro-distance">{virtualizer.getDistanceFromEnd()}</dd></div>
          <div><dt>Visible</dt><dd>{firstVirtualItem?.index ?? "-"}..{lastVirtualItem?.index ?? "-"}</dd></div>
          <div><dt>Mode</dt><dd>{search.mode}</dd></div>
          <div><dt>Footer</dt><dd>{search.footer}</dd></div>
          <div><dt>Keys</dt><dd>{search.indexKeys ? "index" : "id"}</dd></div>
        </dl>
        <div className="virtual-repro__actions">
          <button type="button" onClick={() => void appendBatch()}>
            Append batch
          </button>
          <button type="button" onClick={reset}>
            Reset
          </button>
          <button type="button" onClick={() => virtualizer.scrollToEnd()}>
            Latest
          </button>
        </div>
        <output>{runState}</output>
        <output data-testid="virtual-repro-hydrated">{String(hydrated)}</output>
      </section>

      <section className="virtual-repro__feed">
        {search.affordance ? (
          <button
            className="virtual-repro__affordance virtual-repro__affordance--top"
            type="button"
            onClick={() => virtualizer.scrollToOffset(0)}
          >
            Top
          </button>
        ) : null}
        <div className="virtual-repro__scroller" data-testid="virtual-repro-scroller" ref={parentRef}>
          <div
            className="virtual-repro__content"
            style={{ height: virtualizer.getTotalSize() }}
          >
            {virtualItems.map((virtualItem) => {
              const message = messages[virtualItem.index];
              return (
                <div
                  className="virtual-repro__row"
                  data-index={virtualItem.index}
                  key={virtualItem.key}
                  ref={virtualizer.measureElement}
                  style={{ transform: `translateY(${virtualItem.start}px)` }}
                >
                  {message?.text ?? "..."}
                </div>
              );
            })}
          </div>
        </div>
        {search.affordance ? (
          <button
            className="virtual-repro__affordance virtual-repro__affordance--bottom"
            type="button"
            onClick={() => virtualizer.scrollToEnd()}
          >
            Latest
          </button>
        ) : null}
        {search.footer === "none" ? null : (
          <div
            className={
              search.footer === "overlay-measured"
                ? "virtual-repro__footer virtual-repro__footer--overlay"
                : "virtual-repro__footer"
            }
            ref={footerRef}
          >
            <textarea aria-label="Composer" defaultValue="composer height probe" />
          </div>
        )}
      </section>
    </main>
  );
}

function makeMessages(startIndex: number, count: number) {
  return Array.from({ length: count }, (_, offset) => {
    const index = startIndex + offset;
    return {
      id: `message-${index}`,
      text: `${index + 1} events.iterate.com/debug/virtual-repro ${new Date(0).toISOString()}`,
    };
  });
}

function chunksFor(args: { totalCount: number; chunkSize: number; startIndex: number }) {
  const chunks: Array<ReturnType<typeof makeMessages>> = [];
  for (let index = 0; index < args.totalCount; index += args.chunkSize) {
    chunks.push(makeMessages(args.startIndex + index, Math.min(args.chunkSize, args.totalCount - index)));
  }
  return chunks;
}

async function waitForMode(mode: ReproMode, delayMs: number) {
  if (mode === "single") return;
  if (mode === "microtask") {
    await Promise.resolve();
    return;
  }
  if (mode === "raf") {
    await new Promise<void>((resolve) => requestAnimationFrame(() => resolve()));
    return;
  }
  await new Promise<void>((resolve) => globalThis.setTimeout(resolve, delayMs));
}

function numberSearchParam(value: unknown, fallback: number) {
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? Math.floor(number) : fallback;
}

function footerSearchParam(value: unknown): ReproFooter {
  return value === "normal" || value === "overlay-measured" ? value : "none";
}

function modeSearchParam(value: unknown): ReproMode {
  return value === "microtask" || value === "raf" || value === "timeout" ? value : "single";
}
