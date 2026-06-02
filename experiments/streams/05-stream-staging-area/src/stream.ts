import { newWebSocketRpcSession, newWorkersRpcResponse, type RpcStub } from "capnweb";
import { DurableObject } from "cloudflare:workers";
import {
  StreamEvent as StreamEventSchema,
  StreamEventInput as StreamEventInputSchema,
  type StreamEvent,
  type StreamEventInput,
} from "@cf-experiments/shared/event";
import { makeRpcTargetClass } from "@cf-experiments/shared/rpc-target";
import { coreStreamProcessorContract, type CoreStreamState } from "./core-stream-processor.js";
import type {
  StreamProcessorRunnerRpc,
  StreamRpc,
  SubscriptionSink,
} from "./stream-types.js";

const CORE_PROCESSOR_SLUG = coreStreamProcessorContract.slug;

export class Stream extends DurableObject<Env> implements StreamRpc {
  state: CoreStreamState;

  // Live delivery connections, keyed by subscriptionKey. Runtime-only: outbound
  // connections are recreated from reduced state, inbound from a fresh subscribe().
  #connections = new Map<string, Connection>();
  // subscriptionKeys with an outbound handshake in flight, so concurrent
  // reconciliation runs never dial the same runner twice.
  #connecting = new Set<string>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.#createTables();

    // Hydrate the built-in core processor's durable snapshot. This table is the
    // SQLite version of the StreamProcessorRunner DO's KV "snapshot" concept:
    // processor-owned state keyed by processor slug.
    this.state = this.#readCoreState();

    // When the durable object boots up the _first time_, we add a
    // events.iterate.com/stream/created event to the stream.
    //
    // And every time it's woken up for any reason (inbound fetch, rpc or alarm),
    // we append a "woken" event to the stream.
    if (this.state.eventCount === 0) {
      // stream durable objects have names like "namespace:/some/stream/path"
      if (!ctx.id.name) throw new Error("ctx.id.name is falsey - this should never happen");
      const [namespace, path] = ctx.id.name.split(":");
      this.append({
        event: {
          type: "events.iterate.com/stream/created",
          payload: { namespace, path },
        },
      });
    }
    // each time the durable object wakes up, we append this event
    this.append({
      event: {
        type: "events.iterate.com/stream/woken",
        payload: { incarnationId: crypto.randomUUID() },
      },
    });

    // Restore outbound connections this stream should have. Without this, a stream
    // that wakes with configured subscriptions but no new appends never reconnects.
    this.#reconcile();
  }

  #createTables(): void {
    this.ctx.storage.sql.exec(`
      -- Stream-owned append log. This is the same replay source that external
      -- StreamProcessorRunner DOs consume over subscribe().
      create table if not exists events (
        offset integer primary key autoincrement,
        type text not null,
        created_at text not null,
        idempotency_key text unique,
        raw_json text not null
      );

      -- Processor-owned durable snapshots, keyed by processor slug. The Stream
      -- DO stores events.iterate.com/stream/core here; runner DOs can use the
      -- same shape later for echo or multiple processors in one runner.
      create table if not exists processor_state (
        processor_slug text primary key,
        state text not null
      );
    `);
  }

  #initialCoreState(): CoreStreamState {
    const initialState = coreStreamProcessorContract.stateSchema.parse(
      coreStreamProcessorContract.initialState,
    );
    return coreStreamProcessorContract.stateSchema.parse({
      ...initialState,
      maxOffset: Math.max(0, initialState.maxOffset),
    });
  }

  #appendEventRows(events: StreamEvent[]): void {
    for (const event of events) {
      this.ctx.storage.sql.exec(
        `
          insert into events (offset, type, created_at, idempotency_key, raw_json)
          values (?, ?, ?, ?, ?)
        `,
        event.offset,
        event.type,
        event.createdAt,
        event.idempotencyKey ?? null,
        JSON.stringify(event),
      );
    }
  }

  #readEventByOffset(offset: number): StreamEvent | undefined {
    const row = this.ctx.storage.sql
      .exec<{ rawJson: string }>(
        `
          select raw_json as rawJson
          from events
          where offset = ?
          limit 1
        `,
        offset,
      )
      .toArray()[0];
    return row === undefined ? undefined : StreamEventSchema.parse(JSON.parse(row.rawJson));
  }

  #readEventByIdempotencyKey(idempotencyKey: string): StreamEvent | undefined {
    const row = this.ctx.storage.sql
      .exec<{ rawJson: string }>(
        `
          select raw_json as rawJson
          from events
          where idempotency_key = ?
          limit 1
        `,
        idempotencyKey,
      )
      .toArray()[0];
    return row === undefined ? undefined : StreamEventSchema.parse(JSON.parse(row.rawJson));
  }

  #readEventsInRange(args: {
    afterOffset: number;
    beforeOffset: number;
    limit: number;
  }): StreamEvent[] {
    return this.ctx.storage.sql
      .exec<{ rawJson: string }>(
        `
          select raw_json as rawJson
          from events
          where offset > ?
            and offset < ?
          order by offset asc
          limit ?
        `,
        args.afterOffset,
        args.beforeOffset,
        args.limit,
      )
      .toArray()
      .map((row) => StreamEventSchema.parse(JSON.parse(row.rawJson)));
  }

  #readProcessorStateJson(processorSlug: string): string | undefined {
    return this.ctx.storage.sql
      .exec<{ state: string }>(
        `
          select state
          from processor_state
          where processor_slug = ?
          limit 1
        `,
        processorSlug,
      )
      .toArray()[0]?.state;
  }

  #writeProcessorStateJson(processorSlug: string, state: unknown): void {
    this.ctx.storage.sql.exec(
      `
        insert into processor_state (processor_slug, state)
        values (?, ?)
        on conflict(processor_slug) do update set state = excluded.state
      `,
      processorSlug,
      JSON.stringify(state),
    );
  }

  #readCoreState(): CoreStreamState {
    const stateJson = this.#readProcessorStateJson(CORE_PROCESSOR_SLUG);
    if (stateJson === undefined) return this.#initialCoreState();
    return coreStreamProcessorContract.stateSchema.parse(JSON.parse(stateJson));
  }

  #writeCoreState(state: CoreStreamState): void {
    this.#writeProcessorStateJson(CORE_PROCESSOR_SLUG, state);
  }

  /** Opens the capnweb RPC API for this stream Durable Object. */
  async fetch(request: Request) {
    return newWorkersRpcResponse(request, new StreamRpcTarget(this));
  }

  /**
   * Convenience RPC for appending one event.
   *
   * Uses `appendBatch()`, so all append ordering and persistence stays in one place.
   */
  append(args: { event: StreamEventInput }): StreamEvent {
    return this.appendBatch({ events: [args.event] })[0]!;
  }

  /**
   * Synchronously assigns offsets, reduces, persists, then wakes delivery.
   *
   * What actually happens for `appendBatch({ events: [a, b] })` on a stream at
   * `maxOffset: 4`:
   * 1. `a` becomes offset 5, `b` becomes offset 6; each is folded into reduced state.
   *    An event whose `idempotencyKey` already exists is skipped and the existing
   *    event is returned in its place (so the returned array stays input-aligned).
   * 2. Both rows + the new reduced state are written in one await-free SQLite turn.
   *    After this line the append has succeeded.
   * 3. Post-commit fan-out: every live connection's `wake()` is called (its pump then
   *    reads offsets 5..6 from storage and delivers them); reconciliation runs only if
   *    one of the new events was a `subscription-configured`. Neither can fail the
   *    append.
   *
   * Returns the persisted events (including offsets + `createdAt`) in input order.
   */
  appendBatch(args: { events: StreamEventInput[] }): StreamEvent[] {
    let state = this.state;
    const events: StreamEvent[] = [];
    const newEvents: StreamEvent[] = [];
    const idempotencyHitsInBatch = new Map<string, StreamEvent>();

    // 1. Prepare events and reduced state.
    for (const eventInput of args.events) {
      const input = StreamEventInputSchema.strict().parse(eventInput);

      if (input.idempotencyKey !== undefined) {
        // Same-batch idempotency should behave like already-persisted idempotency.
        const existing =
          idempotencyHitsInBatch.get(input.idempotencyKey) ??
          this.getEvent({ idempotencyKey: input.idempotencyKey });
        if (existing !== undefined) {
          if (input.offset !== undefined && input.offset !== existing.offset) {
            throw new Error(`idempotency hit at offset ${existing.offset}, got ${input.offset}`);
          }
          events.push(existing);
          continue;
        }
      }

      const event = {
        ...input,
        offset: state.maxOffset + 1,
        createdAt: new Date().toISOString(),
      };
      if (input.offset !== undefined && input.offset !== event.offset) {
        throw new Error(`expected offset ${event.offset}, got ${input.offset}`);
      }

      state = this.reduce({ event, state });
      events.push(event);
      newEvents.push(event);
      if (event.idempotencyKey !== undefined) {
        idempotencyHitsInBatch.set(event.idempotencyKey, event);
      }
    }

    if (newEvents.length === 0) return events;

    // 2. Persist new event rows and reduced state.
    // Durable Object SQL storage runs synchronously in the object's thread. The
    // first-party docs say each sql.exec() call is atomic, cursors should be fully
    // consumed before awaits, and Output Gates hold responses until writes are durable:
    // https://developers.cloudflare.com/durable-objects/api/sql-storage/
    // https://blog.cloudflare.com/sqlite-in-durable-objects/
    //
    // Keep this section await-free: event rows + reduced state are the append boundary.
    this.#appendEventRows(newEvents);
    this.#writeCoreState(state);
    this.state = state;

    // 3. Wake live delivery; reconcile only when subscription topology changed.
    // Append success is already decided above — this is pure post-commit fan-out.
    for (const connection of this.#connections.values()) connection.wake();
    if (
      newEvents.some((event) => event.type === "events.iterate.com/stream/subscription-configured")
    ) {
      this.#reconcile();
    }

    return events;
  }

  getEvent(
    args: { offset: number; idempotencyKey?: never } | { idempotencyKey: string; offset?: never },
  ): StreamEvent | undefined {
    if (args.idempotencyKey !== undefined) {
      return this.#readEventByIdempotencyKey(args.idempotencyKey);
    }
    const event = this.#readEventByOffset(args.offset);
    if (event === undefined) throw new Error(`No stream event found at offset ${args.offset}.`);
    return event;
  }

  getEvents(
    args: {
      afterOffset?: number;
      beforeOffset?: number | null;
      limit?: number;
    } = {},
  ): StreamEvent[] {
    const limit = args.limit;
    if (limit !== undefined && (!Number.isInteger(limit) || limit <= 0)) {
      throw new Error("getEvents limit must be a positive integer.");
    }

    // Later this should accept event type filters for subscription catch-up and
    // operator views. Keep the first SQLite shape offset-only until that design is real.
    return this.#readEventsInRange({
      afterOffset: args.afterOffset ?? 0,
      beforeOffset: args.beforeOffset ?? Number.MAX_SAFE_INTEGER,
      limit: limit ?? Number.MAX_SAFE_INTEGER,
    });
  }

  reduce(args: { event: StreamEvent; state?: CoreStreamState }): CoreStreamState {
    return coreStreamProcessorContract.stateSchema.parse(
      (coreStreamProcessorContract.reduce as any)({
        contract: coreStreamProcessorContract,
        state: args.state ?? this.state,
        event: args.event,
      }),
    );
  }

  /**
   * Inbound subscribe: a subscriber hands the stream a sink and the stream delivers.
   *
   * `subscribe({ subscriptionKey: "s", sink })` on a stream with offsets 1..3 already
   * present: the subscriber immediately receives a replay batch `[1, 2, 3]`, then one
   * batch per subsequent append (`[4]`, `[5]`, ...). Passing `afterOffset: 3` skips the
   * replay and starts at offset 4. Re-subscribing with the same key replaces the old
   * connection. Call the returned `unsubscribe()` to stop delivery without closing the
   * underlying capnweb session.
   */
  subscribe(args: {
    subscriptionKey: string;
    sink: RpcStub<SubscriptionSink>;
    afterOffset?: number;
  }): { unsubscribe(): void } {
    // Type-filtered subscriptions belong here later. For now every subscription
    // observes the stream's full ordered event log after its offset boundary.
    return this.#openConnection({ ...args, direction: "inbound" });
  }

  runtimeState() {
    return {
      state: this.state,
      runtime: {
        connections: Object.fromEntries(
          [...this.#connections].map(([subscriptionKey, connection]) => [
            subscriptionKey,
            {
              direction: connection.direction,
              startedAt: connection.startedAt,
              cursor: connection.cursor,
              batchesSent: connection.batchesSent,
              eventsSent: connection.eventsSent,
              lastDeliveredAt: connection.lastDeliveredAt,
            },
          ]),
        ),
      },
    };
  }

  /** Kills the current Durable Object incarnation so experiments can observe restart behavior. */
  kill(): void {
    this.ctx.abort("kill requested");
  }

  /**
   * Opens (or replaces) a live delivery connection for one subscriptionKey.
   *
   * A connection is just a pump: it delivers everything after `cursor` in offset
   * order, then parks. There is no catch-up-vs-live distinction — replay and live
   * are the same `getEvents(afterOffset: cursor)` loop. `appendBatch` re-arms the
   * pump via `wake()`; the `draining` guard makes that idempotent and race-free.
   *
   * What actually happens (stream has offsets 1..3, subscribe with no afterOffset):
   * - open: `cursor = 0`, `wake()` → pump reads `(>0)` → delivers `[1, 2, 3]`,
   *   `cursor = 3` → reads `(>3)` → empty → parks.
   * - append offset 4 → `wake()` → pump reads `(>3)` → delivers `[4]`, `cursor = 4`
   *   → empty → parks. One batch per append while the subscriber keeps up.
   *
   * The `draining` guard is what removes the old catch-up/live race. If an append's
   * `wake()` lands while a slow pump is still mid-drain, it early-returns; the
   * in-flight loop's next `getEvents` sees the just-committed rows (commit happens
   * before `wake()`), so the event is delivered exactly once — never dropped, never
   * doubled, no matter the interleaving. A backlog (subscriber fell behind, or first
   * replay of 10k events) drains 100 at a time, yielding between batches so other
   * connections and incoming appends still make progress.
   */
  #openConnection(args: {
    direction: "inbound" | "outbound";
    subscriptionKey: string;
    sink: RpcStub<SubscriptionSink>;
    afterOffset?: number;
    onClose?: () => void;
  }): { unsubscribe(): void } {
    const subscriptionKey = args.subscriptionKey.trim();
    if (subscriptionKey.length === 0) throw new Error("subscriptionKey must not be blank.");

    // Replacing any existing connection for this key.
    this.#connections.get(subscriptionKey)?.close();

    const sink = args.sink.dup();
    let cursor = args.afterOffset ?? 0;
    let draining = false;
    let open = true;

    // The single delivery path: drain committed events to the sink, then park.
    //
    // FUTURE OPTIMIZATION (Proposal B): the live path currently pays one indexed
    // `getEvents` read per batch even when the subscriber is exactly at the head.
    // `appendBatch` already has the freshly-committed events array in memory, so when
    // `cursor === firstNewOffset - 1` it could hand that array straight to the sink and
    // skip the SQL round-trip — a pure fast path that can't desync because `cursor`
    // stays the source of truth (a behind/draining connection just falls back to this
    // loop). Not worth it until a benchmark shows the per-batch read in the hot path;
    // keeping one delivery path is the simpler default.
    const pump = async () => {
      if (draining) return;
      draining = true;
      try {
        while (open) {
          const events = this.getEvents({ afterOffset: cursor, limit: 100 }); // limit hardcoded for now
          const lastOffset = events.at(-1)?.offset;
          if (lastOffset === undefined) return; // caught up; the next append wakes us again
          cursor = lastOffset;
          connection.batchesSent += 1;
          connection.eventsSent += events.length;
          connection.lastDeliveredAt = new Date().toISOString();
          // Batch-first, fire-and-forget: never await the thenable, dispose the ignored result.
          // Awaiting it forces a return round-trip per batch (see design.md "capnweb API").
          sink.processEventBatch({ events })[Symbol.dispose]();
          await Promise.resolve();
        }
      } finally {
        draining = false;
      }
    };

    const connection: Connection = {
      direction: args.direction,
      startedAt: new Date().toISOString(),
      get cursor() {
        return cursor;
      },
      batchesSent: 0,
      eventsSent: 0,
      wake: () => void pump(),
      close: () => {
        if (!open) return;
        open = false;
        if (this.#connections.get(subscriptionKey) === connection) {
          this.#connections.delete(subscriptionKey);
        }
        sink[Symbol.dispose]();
        args.onClose?.();
      },
    };

    this.#connections.set(subscriptionKey, connection);
    sink.onRpcBroken(() => connection.close());
    connection.wake();

    return { unsubscribe: () => connection.close() };
  }

  /** Fire-and-forget outbound reconciliation; never blocks the append path. */
  #reconcile() {
    this.#reconcileOutboundConnections().then(undefined, (error: unknown) =>
      console.error("Stream outbound reconciliation failed", error),
    );
  }

  /**
   * Makes runtime outbound connections match the persisted subscription config:
   * closes connections whose config disappeared, dials a runner for each configured
   * subscription that has none. Triggered on boot, on subscription-configured
   * appends, and on outbound connection loss — never per-append.
   *
   * What actually happens after appending a `subscription-configured` for key "echo":
   * reduced state now has `subscriptionsByKey.echo`, no connection exists for it, so
   * this dials the `echo` runner DO over a websocket, handshakes via `runner.subscribe`,
   * and `#openConnection`s the resulting sink as an outbound connection. On the next
   * boot the constructor's `#reconcile()` re-establishes that same connection from
   * persisted state with no new append needed.
   */
  async #reconcileOutboundConnections() {
    for (const [subscriptionKey, connection] of this.#connections) {
      if (
        connection.direction === "outbound" &&
        this.state.subscriptionsByKey[subscriptionKey] === undefined
      ) {
        connection.close();
      }
    }

    for (const [subscriptionKey, configured] of Object.entries(this.state.subscriptionsByKey)) {
      if (this.#connections.has(subscriptionKey) || this.#connecting.has(subscriptionKey)) continue;

      // Reserve the key before any await so a concurrent reconcile can't dial twice.
      this.#connecting.add(subscriptionKey);
      try {
        const processor = this.env.STREAM_PROCESSOR_RUNNER.getByName(
          `${this.state.namespace}:${this.state.path}:${subscriptionKey}`,
        );
        const response = await processor.fetch(
          new Request("https://stream-processor.local/", { headers: { Upgrade: "websocket" } }),
        );
        const webSocket = response.webSocket;
        if (webSocket === null) throw new Error("expected stream processor websocket");

        webSocket.accept();
        const runner = newWebSocketRpcSession<StreamProcessorRunnerRpc>(webSocket);
        const request = await runner.subscribe({
          stream: new StreamRpcTarget(this),
          subscriptionConfiguredEvent: configured.latestConfiguredEvent,
          streamRuntimeState: this.runtimeState(),
        });

        this.#openConnection({
          ...request,
          direction: "outbound",
          subscriptionKey,
          onClose: () => runner[Symbol.dispose](),
        });
        runner.onRpcBroken(() => {
          // The connection's own onRpcBroken already closed it; reconnect if still configured.
          this.#reconcile();
        });
      } finally {
        this.#connecting.delete(subscriptionKey);
      }
    }
  }
}

// Wraps the Stream Durable Object in an RpcTarget that can be passed
// across workers rpc and capnweb rpc boundaries
export const StreamRpcTarget = makeRpcTargetClass(Stream);

/**
 * A live delivery connection from this stream to one subscriber sink. Not persisted;
 * the sink and pump state live in the `#openConnection` closure, so this is just the
 * metrics counters plus the two control verbs the stream calls.
 */
type Connection = {
  readonly direction: "inbound" | "outbound";
  readonly startedAt: string;
  /** Highest offset delivered to the sink; also the pump's resume cursor. */
  readonly cursor: number;
  batchesSent: number;
  eventsSent: number;
  lastDeliveredAt?: string;
  /** Re-arm the delivery pump after events are committed. Idempotent while draining. */
  wake(): void;
  /** Stop the pump, dispose the sink, run teardown, drop from the map. Idempotent. */
  close(): void;
};
