import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import type { SimpleStreamProcessorSnapshot } from "@cf-experiments/shared/simple-stream-processor";
import type { RpcStub, RpcTarget } from "capnweb";
import type { CoreStreamState, SubscriptionConfiguredEvent } from "./core-stream-processor.js";

/**
 * A stable boundary in a stream's ordered event log.
 *
 * Stream event offsets are zero-based integers. Passing a number names the
 * boundary at that event offset; stream methods decide whether that boundary is
 * used as an exclusive lower bound (`afterOffset`) or exclusive upper bound
 * (`beforeOffset`).
 *
 * `"start"` means the cursor before the first event in the stream. Use it to
 * replay from offset 0.
 *
 * `"end"` means the cursor after the latest event currently visible to the
 * Durable Object when the method resolves the cursor. Use it to skip history
 * and only observe future appends.
 */
export type StreamCursor = number | "start" | "end";

/** The subscriber-side capnweb RPC target that receives stream event batches. */
export type SubscriptionSink = RpcTarget & {
  processEventBatch(args: { events: StreamEvent[] }): unknown;
};

export type StreamRpc = {
  append(args: { event: StreamEventInput; durability?: AppendDurability }): Promise<StreamEvent>;
  appendBatch(args: {
    events: StreamEventInput[];
    durability?: AppendDurability;
  }): Promise<StreamEvent[]>;
  getEvent(
    args: { offset: Offset; idempotencyKey?: never } | { idempotencyKey: string; offset?: never },
  ): StreamEvent | undefined;
  getEvents(args?: {
    afterOffset?: StreamCursor;
    beforeOffset?: StreamCursor;
    limit?: number;
  }): StreamEvent[];
  subscribe(args: {
    subscriptionKey: SubscriptionKey;
    sink: RpcStub<SubscriptionSink>;
    afterOffset?: StreamCursor;
  }): { unsubscribe(): void };
  runtimeState(): {
    state: CoreStreamState;
    runtime: {
      liveSubscriptions: Record<SubscriptionKey, Subscription>;
    };
  };
};

export type SubscriptionKey = string;

export type AppendDurability = {
  closeOutputGate?: boolean;
  waitForStorageSync?: boolean;
};

export type Subscription = {
  direction: "inbound" | "outbound";
  phase: "catching-up" | "live";
  startedAt: string;
  afterOffset: StreamCursor;
  lastDeliveredOffset?: Offset;
  batchesSent: number;
  eventsSent: number;
  lastDeliveredAt?: string;
};

export type Offset = number;

export type StreamProcessorSlug = "echo";

export type StreamProcessorState = { seen: number };

export type StreamProcessorRunnerSnapshot = SimpleStreamProcessorSnapshot<StreamProcessorState>;

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
