# Stream Staging Area

This context defines the language for stream event storage and browser stream viewing in the staging experiment.

## Language

**Stream Offset**:
A one-based, continuous position assigned by a stream to each committed event.
_Avoid_: Virtual index, row number

**Local Index**:
A zero-based, continuous browser SQLite position used to render stored stream events as a list.
_Avoid_: Virtual index, stream offset

**Browser Mirror**:
The per-stream SQLite database in the browser that stores raw stream events for local viewing and resume.
_Avoid_: Cache, projection layer

**Stream View Runtime**:
The component-owned browser runtime for one mounted stream view, including its capnweb client, leadership election, SQLite worker connection, and change notifications.
_Avoid_: Global stream singleton, app-level stream client

**Events Table**:
The browser SQLite table written by the **events processor**. It mirrors delivered stream events, storing the payload once as `raw_jsonb`; scalar columns such as `offset`, `type`, `created_at`, and `idempotency_key` are generated from that JSONB value.
_Avoid_: derived projection (it is a raw mirror, just produced by a processor like any other table)

**Browser Stream Processor**:
A stream processor that runs **in the browser**, built on the same `createProcessorRunner` shape as the `StreamProcessorRunner` Durable Object (`processor + deps + storage + stream` ports; the runner is the subscription sink). Each one is independent: it owns its own capnweb connection, writes its own SQLite table, is selected by its own `view` query param, and is hosted/run by its own React sub-view component (mounting the sub-view starts the processor). Two to start — the **events processor** (→ `events` table) and the **UI elements processor** (→ its own table) — many later.
_Avoid_: projector-only mirror, shared connection (each processor has its own connection, even across tabs on the same stream)

## Relationships

- A **Stream Offset** starts at `1` for the first committed event in a stream.
- A **Stream Offset** is the durable resume cursor for browser subscriptions.
- A **Local Index** starts at `0` and maps to exactly one locally stored stream event.
- In the current experiment, a **Local Index** is always `Stream Offset - 1`; SQLite rejects any gap in **Stream Offset**.
- **Local Index** is separate from **Stream Offset** so the browser can eventually keep a dense local list even if the server later ages out events by TTL.
- A **Stream View Runtime** writes delivered server batches directly into the **Events Table** in one transaction.
- Replayed events with already-stored **Stream Offsets** are valid only when the JSON payload is identical; identical replays are ignored and keep the original `inserted_at`.
- Same-offset events with different JSON are storage errors.
- Any committed browser SQLite change wakes all mounted browser SQLite queries; this is intentionally blunt until measured otherwise.
- Browser change notifications may include append/clear details for observability, but SQLite remains the source of truth for counts and rows.
- `raw_jsonb` is stored only once as SQLite JSONB; inspect it with SQLite JSON functions such as `json(raw_jsonb)` or `json_pretty(raw_jsonb)`.
- A **Browser Mirror** row records `inserted_at`, the time the browser first stored that event locally.
- A **Stream View Runtime** is owned by the mounted React view; unmounting the view closes that runtime.
- Multiple **Stream View Runtimes** may exist in one browser tab. `/split-stream?left=...&right=...` renders two of them side by side.
- If two **Stream View Runtimes** mount the same stream path, they both participate independently in writer election; one becomes the subscriber/writer and the other follows the shared **Browser Mirror**.
- Each **Browser Stream Processor** is hosted by its React sub-view and has its own capnweb connection; mounting the sub-view starts the processor, unmounting stops it. A processor's table only advances while its sub-view is mounted, and catches up on remount by resuming from its own checkpoint.
- The events mirror is itself a **Browser Stream Processor** (the events processor), not a special generic copy path — symmetric with the UI elements processor and every later one.

## Example Dialogue

> **Dev:** "Can the browser viewer use the **Stream Offset** directly for replay?"
> **Domain expert:** "Yes. Subscribers resume after the highest **Stream Offset** fully stored in SQLite."
>
> **Dev:** "Can TanStack Virtual use the **Stream Offset** as its item index?"
> **Domain expert:** "No. It uses the zero-based **Local Index**, while the **Stream Offset** remains the one-based durable cursor."
>
> **Dev:** "If reconnect replays offsets 99 and 100 after the browser already stored them, is that a gap?"
> **Domain expert:** "No. Replay overlap is fine when the payload is identical, but the first new event after the overlap must be the next continuous **Stream Offset**."

## Flagged Ambiguities

- "virtual index" has been used both for TanStack Virtual's zero-based item index and a browser SQLite column; resolved: use **Local Index** for browser SQLite and avoid "virtual index" as a domain term.
- The browser-processor design was removed in a refactor, then deliberately reinstated: the goal is to practise writing real stream processors locally, one per view, each owning a table + connection + sub-view. Open: the debounce mechanism for SQLite writes, and whether per-processor leadership election is kept (vs. relying on insert idempotency when two tabs run the same processor).
