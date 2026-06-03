import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import type { RpcStub, RpcTarget } from "capnweb";
import type { CoreStreamState, SubscriptionConfiguredEvent } from "./core-stream-processor.js";

/**
 * The subscriber-side capnweb RPC target that receives stream event batches.
 * The stream piggybacks its high-water-mark (`headOffset`/`headCreatedAt`) so the
 * subscriber can compute lag without an extra round-trip. Both are optional; a
 * runner falls back to the batch's last event when absent.
 */
export type SubscriptionSink = RpcTarget & {
  processEventBatch(args: {
    events: StreamEvent[];
    headOffset?: number;
    headCreatedAt?: string;
  }): unknown;
};

export type StreamRpc = {
  append(args: { event: StreamEventInput }): StreamEvent;
  appendBatch(args: { events: StreamEventInput[] }): StreamEvent[];
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
    subscriptionKey: SubscriptionKey;
    sink: RpcStub<SubscriptionSink>;
    afterOffset?: number;
  }): { unsubscribe(): void };
  runtimeState(): {
    state: CoreStreamState;
    runtime: {
      connections: Record<SubscriptionKey, ConnectionInfo>;
    };
  };
  kill(): void;
  /** Clears all durable storage for this stream, then aborts the current incarnation. */
  reset(): void;
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

export type StreamProcessorSlug = "echo";

export type StreamProcessorState = { seen: number };

export type StreamProcessorRunnerSnapshot = { state: StreamProcessorState; offset: number };

export type StreamProcessorRunnerRuntimeState = {
  processorSlug: StreamProcessorSlug | undefined;
  snapshot: StreamProcessorRunnerSnapshot | undefined;
};

export type StreamProcessorRunnerRpc = SubscriptionSink & {
  subscribe(args: {
    stream: RpcStub<StreamRpc>;
    subscriptionConfiguredEvent: SubscriptionConfiguredEvent;
    streamRuntimeState: { state: CoreStreamState };
  }): { sink: SubscriptionSink; afterOffset?: number };
  runtimeState(): StreamProcessorRunnerRuntimeState;
};
