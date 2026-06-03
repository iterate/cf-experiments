import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import type { RpcStub, RpcTarget } from "capnweb";
import type { CoreStreamState, SubscriptionConfiguredEvent } from "./core-stream-processor.js";

type MaybePromise<T> = T | Promise<T>;

/**
 * The subscriber-side capnweb RPC target that receives stream event batches.
 * The stream piggybacks its current max offset so subscribers can compute lag
 * without an extra round trip.
 */
export type SubscriptionSink = RpcTarget & {
  processEventBatch(args: {
    events: StreamEvent[];
    streamMaxOffset: number;
  }): unknown;
};

export type StreamRpc = {
  append(args: { streamPath?: string; event: StreamEventInput }): MaybePromise<StreamEvent>;
  appendBatch(args: { streamPath?: string; events: StreamEventInput[] }): MaybePromise<StreamEvent[]>;
  getEvent(
    args:
      | { offset: number; idempotencyKey?: never }
      | { idempotencyKey: string; offset?: never },
  ): StreamEvent | undefined;
  /**
   * Reads events by numeric offset boundaries. Type filtering belongs here later,
   * but the first SQLite rewrite keeps the read API offset-only.
   */
  getEvents(args?: {
    afterOffset?: number;
    beforeOffset?: number | null;
    limit?: number;
  }): StreamEvent[];
  /**
   * Subscribes to catch-up then live event batches. Type-filtered subscriptions
   * are planned, but not part of this first simplified storage shape.
   */
  subscribe(args: {
    subscriptionKey?: SubscriptionKey;
    sink: RpcStub<SubscriptionSink>;
    replayAfterOffset?: number;
  }): { subscriptionKey: SubscriptionKey; streamMaxOffset: number; unsubscribe(): void };
  runtimeState(): {
    state: CoreStreamState;
    runtime: {
      connections: Record<SubscriptionKey, ConnectionInfo>;
    };
  };
  kill(): void;
  /** Clears all durable storage for this stream, then aborts the current incarnation. */
  reset(): Promise<void>;
  reduce(args: { event: StreamEvent; state?: CoreStreamState }): CoreStreamState;
};

export type SubscriptionKey = string;

/** Serializable debug view of a live delivery connection, returned by `runtimeState()`. */
export type ConnectionInfo = {
  direction: "inbound" | "outbound";
  startedAt: string;
  cursor: number;
  batchesSent: number;
  eventsSent: number;
  lastDeliveredAt?: string;
};

export type StreamProcessorSlug = "echo-test";

export type StreamProcessorState = { seen: number };

export type StreamProcessorRunnerSnapshot = { state: StreamProcessorState; offset: number };

export type StreamProcessorRunnerRuntimeState = {
  processorSlug: StreamProcessorSlug | undefined;
  snapshot: StreamProcessorRunnerSnapshot | undefined;
};

export type StreamProcessorRunnerRpc = {
  requestSubscription(args: {
    stream: RpcStub<StreamRpc>;
    subscriptionKey: SubscriptionKey;
    streamMaxOffset: number;
    subscriptionConfiguredEvent: SubscriptionConfiguredEvent;
    streamRuntimeState: { state: CoreStreamState };
  }): { sink: SubscriptionSink; replayAfterOffset?: number };
  runtimeState(): StreamProcessorRunnerRuntimeState;
};
