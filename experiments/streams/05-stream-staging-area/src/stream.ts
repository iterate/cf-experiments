import { newWebSocketRpcSession, newWorkersRpcResponse, type RpcStub } from "capnweb";
import { DurableObject } from "cloudflare:workers";
import {
  StreamEventInput as StreamEventInputSchema,
  type StreamEvent,
  type StreamEventInput,
} from "@cf-experiments/shared/event";
import { makeRpcTargetClass } from "@cf-experiments/shared/rpc-target";
import { coreStreamProcessorContract, type CoreStreamState } from "./core-stream-processor.js";
import type { StreamProcessorRunnerRpc } from "./stream-processor-runner.js";
import type { SubscriptionRpcTarget } from "./stream-types.js";

export class Stream extends DurableObject<Env> {
  state: CoreStreamState;

  #subscriptions = new Set<LiveSubscription>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);

    // hydrate state from KV storage or use core processor initial state
    this.state =
      this.ctx.storage.kv.get<CoreStreamState>("state") ??
      coreStreamProcessorContract.stateSchema.parse(coreStreamProcessorContract.initialState);

    // If this stream has zero events, it means it was just created.
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

    // Startup should not wait for a persistent storage write before connecting outbound consumers.
    this.appendBatch({ events: startupEvents }).then(
      (events) => console.log("Stream startup events appended", events),
      (error: unknown) => console.error("Stream startup append failed", error),
    );
  }

  /** Opens the CaptainWeb RPC API for this stream Durable Object. */
  async fetch(request: Request) {
    // StreamRpcTarget is the capnweb "main object" for the stream's side of the connection.
    // The peer on the other side of the connection receives an RPC stub to it, on which it can call
    // any methods that StreamRpcTarget has.
    // And (for the moment), StreamRpcTarget is just a very thin wrapper around this durable object.
    // Think of this line as `return newWorkersRpcResponse(request, this);`
    return newWorkersRpcResponse(request, new StreamRpcTarget(this));
  }

  /**
   * Convenience RPC for appending one event. Production callers should prefer
   * `appendBatch()` when they naturally have more than one event.
   */
  async append(args: { event: StreamEventInput }) {
    const events = await this.appendBatch({ events: [args.event] });
    return events[0];
  }

  /**
   * Appends a batch of events in input order.
   * Storage uses unconfirmed writes so subscriber fan-out is not delayed by the output gate.
   * Cloudflare docs: https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#put
   */
  async appendBatch(args: { events: StreamEventInput[] }): Promise<StreamEvent[]> {
    const preparedAppend = this.#beforeAppend({ events: args.events });
    if (preparedAppend.newEvents.length === 0) return preparedAppend.events;

    const writes: Record<string, unknown> = { state: preparedAppend.newState };
    for (const event of preparedAppend.newEvents) writes[`event:${event.offset}`] = event;
    for (const event of preparedAppend.newEvents) {
      if (event.idempotencyKey !== undefined) {
        writes[`idempotency:${event.idempotencyKey}`] = event.offset;
      }
    }

    const storageWrite = this.ctx.storage.put(writes, {
      allowUnconfirmed: true,
      noCache: true,
    });
    this.state = preparedAppend.newState;
    void storageWrite.catch((error: unknown) => {
      console.error("Stream append storage write failed", error);
      this.ctx.abort("Stream append storage write failed");
    });

    await this.#afterAppend(preparedAppend);
    return preparedAppend.events;
  }

  /**
   * Prepares an append synchronously: idempotency reads, offset allocation, reducer, and return order.
   */
  #beforeAppend(args: { events: StreamEventInput[] }): PreparedAppend {
    const preparedAppend: PreparedAppend = {
      events: [],
      newEvents: [],
      newState: this.state,
    };

    for (const eventInput of args.events) {
      const input = StreamEventInputSchema.strict().parse(eventInput);

      if (input.idempotencyKey !== undefined) {
        const existingOffset = this.ctx.storage.kv.get<number>(
          `idempotency:${input.idempotencyKey}`,
        );
        if (existingOffset !== undefined) {
          const existing = this.ctx.storage.kv.get<StreamEvent>(`event:${existingOffset}`);
          if (existing === undefined) {
            throw new Error(`idempotency index points at missing event ${existingOffset}`);
          }
          preparedAppend.events.push(existing);
          continue;
        }
      }

      const offset = preparedAppend.newState.maxOffset + 1;
      if (input.offset !== undefined && input.offset !== offset) {
        throw new Error(`expected offset ${offset}, got ${input.offset}`);
      }

      const event = { ...input, offset, createdAt: new Date().toISOString() };
      // The core reducer intentionally sees every committed event. TypeScript
      // cannot model "*" as "any event except the named ones" without breaking
      // payload narrowing in the reducer's named switch cases.
      preparedAppend.newState = (coreStreamProcessorContract.reduce as any)({
        contract: coreStreamProcessorContract,
        state: preparedAppend.newState,
        event,
      });
      preparedAppend.events.push(event);
      preparedAppend.newEvents.push(event);
    }

    return preparedAppend;
  }

  /** Runs subscriber delivery and outbound reconciliation after events have entered stream memory. */
  async #afterAppend(preparedAppend: PreparedAppend): Promise<void> {
    for (const subscription of this.#subscriptions) {
      this.#deliverBatch(subscription, preparedAppend.newEvents);
    }
    for (const [subscriptionKey, configuredSubscription] of Object.entries(
      this.state.subscriptionsByKey,
    )) {
      if (
        [...this.#subscriptions].some(
          (subscription) =>
            subscription.direction === "outbound" &&
            subscription.subscriptionKey === subscriptionKey,
        )
      ) {
        continue;
      }

      const event = configuredSubscription.latestConfiguredEvent;

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
      const subscriptionRpcTarget = newWebSocketRpcSession<StreamProcessorRunnerRpc>(webSocket);
      const request = await subscriptionRpcTarget.initOutboundSubscription({
        streamRpcTarget: new StreamRpcTarget(this),
        subscriptionConfiguredEvent: event,
        streamSnapshot: this.state,
      });
      const subscription = {
        direction: "outbound" as const,
        subscriptionKey,
        subscriptionRpcTarget,
      };
      this.#subscriptions.add(subscription);
      void this.#streamStoredEventsToSubscription(subscription, request.afterOffset ?? -1);

      const disposeSubscription = () => {
        this.#subscriptions.delete(subscription);
        subscription.subscriptionRpcTarget[Symbol.dispose]();
      };
      subscriptionRpcTarget.onRpcBroken(disposeSubscription);
    }
  }

  /**
   * Attaches a caller-provided subscriber target to this stream and starts replaying events.
   * This is called by the subscriber on a StreamRpcTarget.
   * The subscriber passes in their subscriptionRpcTarget, which is an RpcTarget on which we can
   * call the consumeEvents method.
   **/
  initInboundSubscription(args: {
    subscriptionRpcTarget: RpcStub<SubscriptionRpcTarget>;
    afterOffset?: number;
  }): void {
    const subscription = {
      direction: "inbound" as const,
      subscriptionRpcTarget: args.subscriptionRpcTarget.dup(),
    };
    this.#subscriptions.add(subscription);
    void this.#streamStoredEventsToSubscription(subscription, args.afterOffset ?? -1);
  }

  /** Returns durable core state plus runtime-only connection state for experiments. */
  debug() {
    return {
      state: this.state,
      runtime: {
        subscriptions: [...this.#subscriptions].map((subscription) => ({
          direction: subscription.direction,
          subscriptionKey: subscription.subscriptionKey,
        })),
      },
    };
  }

  async #streamStoredEventsToSubscription(subscription: LiveSubscription, afterOffset: number) {
    const maxOffset = this.state.maxOffset;
    let batch: StreamEvent[] = [];

    for (let offset = afterOffset + 1; offset <= maxOffset; offset++) {
      const event = this.ctx.storage.kv.get<StreamEvent>(`event:${offset}`);
      if (event !== undefined) batch.push(event);

      if (batch.length === 100) {
        this.#deliverBatch(subscription, batch);
        batch = [];
        await Promise.resolve();
      }
    }

    if (batch.length > 0) this.#deliverBatch(subscription, batch);
  }

  #deliverBatch(subscription: LiveSubscription, events: StreamEvent[]) {
    const result = subscription.subscriptionRpcTarget.consumeEvents({ events });
    result[Symbol.dispose]();
  }
}

export const StreamRpcTarget = makeRpcTargetClass(Stream);

/** A live CaptainWeb subscription edge from this stream to a batch consumer. */
type LiveSubscription = {
  direction: "inbound" | "outbound";
  subscriptionKey?: string;
  subscriptionRpcTarget: RpcStub<SubscriptionRpcTarget>;
};

/** The result of validating a requested append batch before storage writes begin. */
type PreparedAppend = {
  /** One output event for each input event, including idempotency hits that will not be written again. */
  events: StreamEvent[];
  /** Only events that were newly assigned offsets and need persistence plus delivery. */
  newEvents: StreamEvent[];
  /** The reducer state after applying every event in `newEvents`. */
  newState: CoreStreamState;
};
