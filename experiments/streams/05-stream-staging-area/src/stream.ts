import { newWebSocketRpcSession, newWorkersRpcResponse, type RpcStub } from "capnweb";
import { DurableObject } from "cloudflare:workers";
import {
  StreamEvent as StreamEventSchema,
  StreamEventInput as StreamEventInputSchema,
  type StreamEvent,
  type StreamEventInput,
} from "@cf-experiments/shared/event";
import { makeRpcTargetClass } from "@cf-experiments/shared/rpc-target";
import { createDurableObjectClient, defineConfig, sql } from "sqlfu";
import { coreStreamProcessorContract, type CoreStreamState } from "./core-stream-processor.js";
import type {
  StreamCursor,
  StreamProcessorRunnerRpc,
  StreamRpc,
  Subscription,
  SubscriptionKey,
  SubscriptionSink,
} from "./stream-types.js";

export class Stream extends DurableObject<Env> implements StreamRpc {
  static db = defineConfig({
    definitions: sql`
      create table events (
        offset integer primary key autoincrement,
        type text not null,
        created_at text not null,
        idempotency_key text unique,
        raw_json blob not null
      );

      create index events_type_created_at on events (type, created_at);
    `,
    migrations: [
      {
        name: "2026-06-02T15.50.00.000Z_create_stream_events",
        content: sql`
          create table events (
            offset integer primary key autoincrement,
            type text not null,
            created_at text not null,
            idempotency_key text unique,
            raw_json blob not null
          );

          create index events_type_created_at on events (type, created_at);
        `,
      },
    ],
    queries: {
      appendEvents: sql.run<{ parameters: { events_json: any } }>`
        insert into events (offset, type, created_at, idempotency_key, raw_json)
        select
          json_extract(value, '$.offset') as offset,
          json_extract(value, '$.type') as type,
          json_extract(value, '$.createdAt') as created_at,
          json_extract(value, '$.idempotencyKey') as idempotency_key,
          value as raw_json
        from json_each(:events_json)
      `,
      eventByOffset: sql.nullableOne<{ parameters: { offset: number }; result: { raw_json: ArrayBuffer } }>`
        select raw_json
        from events
        where offset = :offset
        limit 1
      `,
      eventByIdempotencyKey: sql.nullableOne<{ parameters: { idempotency_key: string }; result: { raw_json: ArrayBuffer } }>`
        select raw_json
        from events
        where idempotency_key = :idempotency_key
        limit 1
      `,
      eventsInRange: sql.many<{ parameters: { after_offset: number; before_offset: number | null; limit: number }; result: { raw_json: ArrayBuffer } }>`
        select raw_json
        from events
        where offset > :after_offset
          and (:before_offset is null or offset < :before_offset)
        order by offset asc
        limit :limit
      `,
    },
  });

  db: ReturnType<typeof Stream.db<ReturnType<typeof createDurableObjectClient>>>;
  state: CoreStreamState;

  #subscriptions = new Map<string, LiveSubscription>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.db = Stream.db(createDurableObjectClient(ctx.storage));
    this.db.migrate();

    // Hydrate state from KV storage, but layer it over the current core initial state.
    // Local experiments often wake Durable Objects created before the latest reducer field existed.
    const initialState = coreStreamProcessorContract.stateSchema.parse(
      coreStreamProcessorContract.initialState,
    );
    const storedState = this.ctx.storage.kv.get<Partial<CoreStreamState>>("state");
    this.state = coreStreamProcessorContract.stateSchema.parse({
      ...initialState,
      ...storedState,
      maxOffset: Math.max(0, storedState?.maxOffset ?? initialState.maxOffset),
      config: {
        ...initialState.config,
        ...storedState?.config,
      },
      subscriptionsByKey: storedState?.subscriptionsByKey ?? initialState.subscriptionsByKey,
    });

    // When the durable object boots up the _first time_, we add a
    // events.iterate.com/stream/created event to the stream.
    //
    // And every time it's woken up for any reason (inbound fetch, rpc or alarm),
    // we append a "woken" event to the stream.
    const startupEvents: StreamEventInput[] = [];
    if (this.state.eventCount === 0) {
      // stream durable objects have names like "namespace:/some/stream/path"
      if (!ctx.id.name) throw new Error("ctx.id.name is falsey - this should never happen");
      const [namespace, path] = ctx.id.name.split(":");
      startupEvents.push({
        type: "events.iterate.com/stream/created",
        payload: { namespace, path },
      });
    }
    // each time the durable object wakes up, we append this event
    startupEvents.push({
      type: "events.iterate.com/stream/woken",
      payload: { incarnationId: crypto.randomUUID() },
    });

    try {
      const events = this.appendBatch({ events: startupEvents });
      console.log("Stream startup events appended", events);
    } catch (error: unknown) {
      console.error("Stream startup append failed", error);
    }
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

  /** Synchronously appends, reduces, persists, then kicks off live delivery. */
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

    // 2. Atomically persist new event rows and reduced state.
    // Durable Object sync KV is backed by the same SQLite storage system as sql.exec.
    // Keep this section await-free: event rows + reduced state are the atomic append boundary.
    this.db.appendEvents({ events_json: JSON.stringify(newEvents) });
    this.ctx.storage.kv.put("state", state);
    this.state = state;

    // 3. Kick off post-append delivery/reconciliation; append success is already decided.
    this.#deliverToLiveSubscriptions(newEvents);
    this.#reconcileOutboundSubscriptions().then(
      undefined,
      (error: unknown) => console.error("Stream post-append reconciliation failed", error),
    );

    return events;
  }

  getEvent(
    args: { offset: number; idempotencyKey?: never } | { idempotencyKey: string; offset?: never },
  ): StreamEvent | undefined {
    if (args.idempotencyKey !== undefined) {
      const row = this.db.eventByIdempotencyKey({ idempotency_key: args.idempotencyKey });
      return row === null ? undefined : streamEventFromRow(row);
    }
    const row = this.db.eventByOffset({ offset: args.offset });
    if (row === null) throw new Error(`No stream event found at offset ${args.offset}.`);
    return streamEventFromRow(row);
  }

  getEvents(
    args: {
      afterOffset?: StreamCursor;
      beforeOffset?: StreamCursor | null;
      limit?: number;
    } = {},
  ): StreamEvent[] {
    const limit = args.limit;
    if (limit !== undefined && (!Number.isInteger(limit) || limit <= 0)) {
      throw new Error("getEvents limit must be a positive integer.");
    }

    return this.db
      .eventsInRange({
        after_offset: args.afterOffset ?? 0,
        before_offset: args.beforeOffset ?? null,
        limit: limit ?? Number.MAX_SAFE_INTEGER,
      })
      .map(streamEventFromRow);
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

  subscribe(args: {
    subscriptionKey: string;
    sink: RpcStub<SubscriptionSink>;
    afterOffset?: StreamCursor;
  }): { unsubscribe(): void } {
    return this.#subscribe({
      ...args,
      direction: "inbound",
    });
  }

  runtimeState() {
    return {
      state: this.state,
      runtime: {
        liveSubscriptions: Object.fromEntries(
          [...this.#subscriptions].map(([subscriptionKey, subscription]) => [
            subscriptionKey,
            {
              direction: subscription.direction,
              phase: subscription.phase,
              startedAt: subscription.startedAt,
              afterOffset: subscription.afterOffset,
              lastDeliveredOffset: subscription.lastDeliveredOffset,
              batchesSent: subscription.batchesSent,
              eventsSent: subscription.eventsSent,
              lastDeliveredAt: subscription.lastDeliveredAt,
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

  #deliverToLiveSubscriptions(newEvents: StreamEvent[]): void {
    for (const subscription of this.#subscriptions.values()) {
      if (subscription.phase === "live") this.#deliverBatch(subscription, newEvents);
    }
  }

  async #reconcileOutboundSubscriptions() {
    for (const [subscriptionKey, subscription] of this.#subscriptions) {
      if (
        subscription.direction === "outbound" &&
        this.state.subscriptionsByKey[subscriptionKey] === undefined
      ) {
        this.#closeSubscription(subscription);
      }
    }

    for (const [subscriptionKey, configuredSubscription] of Object.entries(
      this.state.subscriptionsByKey,
    )) {
      if (this.#subscriptions.has(subscriptionKey)) continue;

      const processor = this.env.STREAM_PROCESSOR_RUNNER.getByName(
        `${this.state.namespace}:${this.state.path}:${subscriptionKey}`,
      );
      const response = await processor.fetch(
        new Request("https://stream-processor.local/", {
          headers: { Upgrade: "websocket" },
        }),
      );
      const webSocket = response.webSocket;
      if (webSocket === null) throw new Error("expected stream processor websocket");

      webSocket.accept();
      const runner = newWebSocketRpcSession<StreamProcessorRunnerRpc>(webSocket);
      const request = await runner.subscribe({
        stream: new StreamRpcTarget(this),
        subscriptionConfiguredEvent: configuredSubscription.latestConfiguredEvent,
        streamRuntimeState: this.runtimeState(),
      });

      this.#subscribe({
        ...request,
        direction: "outbound",
        subscriptionKey,
        onClose: () => runner[Symbol.dispose](),
      });
      runner.onRpcBroken(() => {
        const subscription = this.#subscriptions.get(subscriptionKey);
        if (subscription?.direction === "outbound") this.#closeSubscription(subscription);
      });
    }
  }

  #subscribe(args: {
    direction: "inbound" | "outbound";
    subscriptionKey: string;
    sink: RpcStub<SubscriptionSink>;
    afterOffset?: StreamCursor;
    onClose?: () => void;
  }): { unsubscribe(): void } {
    const subscriptionKey = args.subscriptionKey.trim();
    if (subscriptionKey.length === 0) throw new Error("subscriptionKey must not be blank.");

    const existing = this.#subscriptions.get(subscriptionKey);
    if (existing !== undefined) this.#closeSubscription(existing);

    const afterOffset = args.afterOffset ?? 0;
    const subscription: LiveSubscription = {
      direction: args.direction,
      subscriptionKey,
      phase: "catching-up",
      startedAt: new Date().toISOString(),
      afterOffset,
      lastDeliveredOffset: afterOffset,
      batchesSent: 0,
      eventsSent: 0,
      sink: args.sink.dup(),
      onClose: args.onClose,
    };
    this.#subscriptions.set(subscriptionKey, subscription);
    subscription.sink.onRpcBroken(() => this.#closeSubscription(subscription));
    void this.#catchUpSubscription(subscription);

    return {
      unsubscribe: () => {
        if (this.#subscriptions.get(subscriptionKey) === subscription) {
          this.#closeSubscription(subscription);
        }
      },
    };
  }

  async #catchUpSubscription(subscription: LiveSubscription) {
    while (this.#subscriptions.get(subscription.subscriptionKey) === subscription) {
      const events = this.getEvents({
        afterOffset: subscription.lastDeliveredOffset ?? subscription.afterOffset,
        limit: 100, // hardcoded for now
      });

      if (events.length === 0) {
        subscription.phase = "live";
        return;
      }

      this.#deliverBatch(subscription, events);
      await Promise.resolve();
    }
  }

  #closeSubscription(subscription: LiveSubscription) {
    if (this.#subscriptions.get(subscription.subscriptionKey) !== subscription) return;
    this.#subscriptions.delete(subscription.subscriptionKey);
    subscription.sink[Symbol.dispose]();
    subscription.onClose?.();
  }

  #deliverBatch(subscription: LiveSubscription, events: StreamEvent[]) {
    if (events.length === 0) return;

    subscription.batchesSent += 1;
    subscription.eventsSent += events.length;
    subscription.lastDeliveredOffset = events.at(-1)?.offset ?? subscription.lastDeliveredOffset;
    subscription.lastDeliveredAt = new Date().toISOString();

    const result = subscription.sink.processEventBatch({ events });
    result[Symbol.dispose]();
  }
}

// Wraps the Stream Durable Object in an RpcTarget that can be passed
// across workers rpc and capnweb rpc boundaries
export const StreamRpcTarget = makeRpcTargetClass(Stream);

/** A live capnweb subscription edge from this stream to a batch consumer. */
type LiveSubscription = Subscription & {
  subscriptionKey: SubscriptionKey;
  sink: RpcStub<SubscriptionSink>;
  onClose?: () => void;
};

function streamEventFromRow(row: { raw_json: string | ArrayBuffer }): StreamEvent {
  const rawJson =
    typeof row.raw_json === "string" ? row.raw_json : new TextDecoder().decode(row.raw_json);
  return StreamEventSchema.parse(JSON.parse(rawJson));
}
