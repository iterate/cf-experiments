import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import type { SimpleStreamProcessorSnapshot } from "@cf-experiments/shared/simple-stream-processor";
import type { RpcStub, RpcTarget } from "capnweb";
import type { CoreStreamState, SubscriptionConfiguredEvent } from "./core-stream-processor.js";

/** A stable event-log boundary. Offsets are 1-based, so `0` means before the first event. */
export type StreamCursor = number;

/** The subscriber-side capnweb RPC target that receives stream event batches. */
export type SubscriptionSink = RpcTarget & {
  processEventBatch(args: { events: StreamEvent[] }): unknown;
};

export type StreamRpc = {
  append(args: { event: StreamEventInput }): StreamEvent;
  appendBatch(args: { events: StreamEventInput[] }): StreamEvent[];
  getEvent(
    args: { offset: Offset; idempotencyKey?: never } | { idempotencyKey: string; offset?: never },
  ): StreamEvent | undefined;
  getEvents(args?: {
    afterOffset?: StreamCursor;
    beforeOffset?: StreamCursor | null;
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
  kill(): void;
  reduce(args: { event: StreamEvent; state?: CoreStreamState }): CoreStreamState;
};

export type SubscriptionKey = string;

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
