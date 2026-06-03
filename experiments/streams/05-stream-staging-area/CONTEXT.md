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
The per-stream SQLite database in the browser that stores stream events and browser processor state for local viewing and resume.
_Avoid_: Cache, replica

**Stream View Runtime**:
The component-owned browser runtime for one mounted stream view, including its stream client, SQLite worker connection, and change notifications.
_Avoid_: Global stream singleton, app-level stream client

**View**:
One of three content kinds a **Stream View Runtime** can render for a stream, selected by a `view` query param. Each View renders exactly one browser-hosted processor's output table:
- `events` — the raw event log (existing SQLite projector → `events` table). Composer shown.
- `elements` — the stream presented as a stream of UI components, grouped from the raw events (new reducing processor → UI elements table). Composer shown.
- `state` — the stream's own reduced core state, mirrored from the server `coreStreamProcessor` into the browser (→ `processor_state` row). Composer hidden — you read a projection, you do not append to it.
A View changes only the stream area and whether the composer is shown; it never changes which stream is connected.
_Avoid_: mode, tab, page (these collide with browser tabs and the writer-election "tab")

## Relationships

- A **Stream Offset** starts at `1` for the first committed event in a stream.
- A **Stream Offset** is the durable resume cursor for subscribers and processors.
- A **Local Index** starts at `0` and maps to exactly one locally stored stream event.
- In the current experiment, a **Local Index** is always `Stream Offset - 1`; any gap in **Stream Offset** is treated as a storage error.
- **Local Index** is separate from **Stream Offset** so the browser can eventually keep a dense local list even if the server later ages out events by TTL.
- Replayed events with already-stored **Stream Offsets** are valid; after dropping that prefix, any newly stored suffix must start at the next expected **Stream Offset** and remain continuous.
- Any committed change to a **Browser Mirror** wakes all mounted browser SQLite queries; this is intentionally blunt until measured otherwise.
- Browser change notifications may include append/clear details for observability, but SQLite remains the source of truth for counts and rows.
- A **Stream View Runtime** is owned by the mounted React view; unmounting the view closes that runtime.
- Multiple **Stream View Runtimes** may exist in one browser tab, including a future split view that renders two streams side by side.
- If two **Stream View Runtimes** mount the same stream path, they both participate independently in writer election; one becomes the subscriber/writer and the other follows the shared **Browser Mirror**.

## Example dialogue

> **Dev:** "Can the browser viewer use the **Stream Offset** directly for replay?"
> **Domain expert:** "Yes — subscribers resume after the highest **Stream Offset** they have fully stored or processed."
>
> **Dev:** "Can TanStack Virtual use the **Stream Offset** as its item index?"
> **Domain expert:** "No — it uses the zero-based **Local Index**, while the **Stream Offset** remains the one-based durable cursor."
>
> **Dev:** "If reconnect replays offsets 99 and 100 after the browser already stored them, is that a gap?"
> **Domain expert:** "No — replay overlap is fine, but the first new event after the overlap must be the next continuous **Stream Offset**."

## Flagged ambiguities

- "virtual index" has been used both for TanStack Virtual's zero-based item index and a browser SQLite column; resolved: use **Local Index** for browser SQLite and avoid "virtual index" as a domain term.
- "view" is overloaded: a **Stream View Runtime** is the per-mount runtime, while a **View** (`events`/`elements`/`state`) is the content kind it renders. A Runtime renders one View at a time. Open: confirm these two names stay distinct, or rename one.
- The grouped row in the `elements` View has been called "feed item", "UI element", "UI component", "grouped event row", and "component" interchangeably. UNRESOLVED — see open question on canonical name + column name.
