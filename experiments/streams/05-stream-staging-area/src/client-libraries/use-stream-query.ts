import { useSyncExternalStore } from "react";
import type {
  EventCountSnapshot,
  ReactiveQuerySnapshot,
  SqlValue,
  StreamBrowserDatabase,
  StreamQueryScope,
} from "./stream-browser-db.js";

// The live event count is special-cased in the db (advanced straight from the writer's
// change broadcast, no per-append SQL — see StreamBrowserDatabase.eventCount). It's all the
// virtualizer needs, so it gets its own tiny hook over the same useSyncExternalStore shape.
export function useStreamEventCount(db: StreamBrowserDatabase): EventCountSnapshot {
  const handle = db.eventCount();
  return useSyncExternalStore(handle.subscribe, handle.getSnapshot);
}

// React 19 reactive read for the local SQLite mirror.
//
// `useSyncExternalStore` is React's recommended primitive for subscribing a component to an
// external mutable store (https://react.dev/reference/react/useSyncExternalStore), and the
// right choice here over `use()`+Suspense — the React docs explicitly warn against
// suspending on a useSyncExternalStore value, since store mutations would flash the Suspense
// fallback over already-rendered rows. SQLocal's and TanStack DB's live-query hooks use this
// same shape.
//
// The two contracts it imposes are both satisfied by `db.reactiveQuery`:
//  - getSnapshot returns a referentially-stable value (the db caches each query's result and
//    only swaps it on a real re-run) — returning a fresh array here would infinite-loop.
//  - subscribe has a stable identity (bound once per query entry) — so React never
//    re-subscribes on re-render. getSnapshot is sync; the async worker re-run happens inside
//    the db on a change notification, which then calls the listener.
export function useStreamQuery<T extends Record<string, SqlValue>>(
  db: StreamBrowserDatabase,
  sql: string,
  params: SqlValue[],
  scope: StreamQueryScope,
): ReactiveQuerySnapshot<T> {
  // Pure: reactiveQuery only get-or-creates a cache entry and returns its stable handle;
  // the first actual query run is deferred to subscribe.
  const handle = db.reactiveQuery<T>(sql, params, scope);
  return useSyncExternalStore(handle.subscribe, handle.getSnapshot);
}
