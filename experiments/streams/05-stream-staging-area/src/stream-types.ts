import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import type { RpcStub, RpcTarget } from "capnweb";
import type { CoreStreamState, SubscriptionConfiguredEvent } from "./core-stream-processor.js";

/** A stable event-log boundary. Offsets are 1-based, so `0` means before the first event. */
export type StreamCursor = number;

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
      | { offset: StreamCursor; idempotencyKey?: never }
      | { idempotencyKey: string; offset?: never },
  ): StreamEvent | undefined;
  /**
   * Reads events by numeric offset boundaries. Type filtering belongs here later,
   * but the first SQLite rewrite keeps the read API offset-only.
   */
  getEvents(args?: {
    afterOffset?: StreamCursor;
    beforeOffset?: StreamCursor | null;
    limit?: number;
  }): StreamEvent[];
  /**
   * Subscribes to catch-up then live event batches. Type-filtered subscriptions
   * are planned, but not part of this first simplified storage shape.
   */
  subscribe(args: {
    subscriptionKey: SubscriptionKey;
    sink: RpcStub<SubscriptionSink>;
    afterOffset?: StreamCursor;
  }): { unsubscribe(): void };
  runtimeState(): {
    state: CoreStreamState;
    runtime: {
      connections: Record<SubscriptionKey, ConnectionInfo>;
    };
  };
  kill(): void;
  reduce(args: { event: StreamEvent; state?: CoreStreamState }): CoreStreamState;
};

export type SubscriptionKey = string;

/** Serializable debug view of a live delivery connection, returned by `runtimeState()`. */
export type ConnectionInfo = {
  direction: "inbound" | "outbound";
  startedAt: string;
  cursor: StreamCursor;
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
  }): { sink: SubscriptionSink; afterOffset?: StreamCursor };
  runtimeState(): StreamProcessorRunnerRuntimeState;
};
