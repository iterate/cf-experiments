import { useEffect, useState } from "react";
import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { StreamCompactView } from "./-stream-page.js";

export const Route = createFileRoute("/split-stream")({
  validateSearch: (search) => ({
    left: normalizeStreamPath(search.left),
    right: normalizeStreamPath(search.right),
  }),
  component: SplitStreamRoute,
});

function SplitStreamRoute() {
  const navigate = useNavigate();
  const search = Route.useSearch();
  const [leftDraft, setLeftDraft] = useState(search.left);
  const [rightDraft, setRightDraft] = useState(search.right);

  useEffect(() => {
    setLeftDraft(search.left);
    setRightDraft(search.right);
  }, [search.left, search.right]);

  function goToDrafts() {
    void navigate({
      to: "/split-stream",
      search: {
        left: normalizeStreamPath(leftDraft),
        right: normalizeStreamPath(rightDraft),
      },
    });
  }

  return (
    <main className="block h-dvh overflow-hidden bg-white font-sans text-slate-950 flex flex-col">
      <form
        className="grid grid-cols-[minmax(0,1fr)_minmax(0,1fr)] items-end gap-2.5 border-b border-slate-200 p-3"
        onSubmit={(event) => {
          event.preventDefault();
          goToDrafts();
        }}
      >
        <label className="grid gap-1.5 text-xs font-medium text-slate-600">
          <span>Left stream</span>
          <input
            className="min-w-0 rounded-md border border-slate-300 px-2.5 py-2 font-mono text-sm"
            value={leftDraft}
            onChange={(event) => setLeftDraft(event.currentTarget.value)}
          />
        </label>
        <label className="grid gap-1.5 text-xs font-medium text-slate-600">
          <span>Right stream</span>
          <input
            className="min-w-0 rounded-md border border-slate-300 px-2.5 py-2 font-mono text-sm"
            value={rightDraft}
            onChange={(event) => setRightDraft(event.currentTarget.value)}
          />
        </label>
        <button className="cursor-pointer whitespace-nowrap rounded-md bg-blue-600 px-3 py-2 text-sm font-semibold text-white no-underline disabled:cursor-not-allowed disabled:opacity-55" type="submit">
          Go to streams
        </button>
      </form>
      <div className="grid min-h-0 flex-1 grid-cols-[minmax(0,1fr)_minmax(0,1fr)] gap-px">
        <StreamCompactView streamPath={search.left} />
        <StreamCompactView streamPath={search.right} />
      </div>
    </main>
  );
}

function normalizeStreamPath(value: unknown) {
  if (typeof value !== "string") return "/";
  const trimmed = value.trim();
  if (trimmed === "") return "/";
  return trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
}
