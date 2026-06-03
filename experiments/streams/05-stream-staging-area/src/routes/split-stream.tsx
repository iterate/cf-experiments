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
    <main className="stream-page stream-page--split">
      <form
        className="stream-page__split-controls"
        onSubmit={(event) => {
          event.preventDefault();
          goToDrafts();
        }}
      >
        <label className="stream-page__field">
          <span>Left stream</span>
          <input
            className="stream-page__input"
            value={leftDraft}
            onChange={(event) => setLeftDraft(event.currentTarget.value)}
          />
        </label>
        <label className="stream-page__field">
          <span>Right stream</span>
          <input
            className="stream-page__input"
            value={rightDraft}
            onChange={(event) => setRightDraft(event.currentTarget.value)}
          />
        </label>
        <button className="stream-page__button" type="submit">
          Go to streams
        </button>
      </form>
      <div className="stream-page__split-grid">
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
