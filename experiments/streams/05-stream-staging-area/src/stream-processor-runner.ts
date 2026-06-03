import { newWorkersRpcResponse, type RpcStub } from "capnweb";
import { DurableObject } from "cloudflare:workers";
import type { StreamEvent } from "@cf-experiments/shared/event";
import { makeRpcTargetClass } from "@cf-experiments/shared/rpc-target";
import {
  createProcessorRunner,
  streamPortFromRpc,
  type ProcessorRunner,
} from "./stream-processor.js";
// The SAME processor the Node e2e (inbound) and the browser tab (inbound) run.
import { echo } from "./demo-processor.js";
import type { CoreStreamState, SubscriptionConfiguredEvent } from "./core-stream-processor.js";
import type {
  StreamProcessorRunnerRpc,
  StreamProcessorRunnerSnapshot,
  StreamProcessorSlug,
  StreamRpc,
  SubscriptionSink,
} from "./stream-types.js";

export class StreamProcessorRunner extends DurableObject {
  #stream: RpcStub<StreamRpc> | undefined;
  #runner: ProcessorRunner | undefined;

  // Stream durable object calls fetch on us to wake us up and establish a capnweb rpc connection
  // whenever an event is appended to a stream that this StreamProcessorRunner is subscribed to,
  // but there isn't an active capnweb rpc connection between Stream and StreamProcessorRunner.
  async fetch(request: Request) {
    return newWorkersRpcResponse(request, new StreamProcessorRunnerRpcTarget(this));
  }

  // The Stream Durable Object calls this method and we have to return an RpcTarget
  // that implements processEventBatch.
  // The Stream durable object helpfully shares the subscriptionConfiguredEvent with us,
  // so we can decide which stream processor implementation to use
  requestSubscription(args: {
    stream: RpcStub<StreamRpc>;
    subscriptionConfiguredEvent: SubscriptionConfiguredEvent;
    streamRuntimeState: { state: CoreStreamState };
  }): { sink: SubscriptionSink; afterOffset?: number } {
    const subscriber = args.subscriptionConfiguredEvent.payload.subscriber;
    if (subscriber.type !== "built-in") {
      throw new Error("StreamProcessorRunner only supports built-in subscribers");
    }
    if (subscriber.transport !== "capnweb-websocket") {
      throw new Error("StreamProcessorRunner only supports capnweb-websocket subscribers");
    }
    if (subscriber.processorSlug !== "echo") {
      throw new Error(`Unknown stream processor slug: ${subscriber.processorSlug}`);
    }

    this.#stream?.[Symbol.dispose]();
    this.#stream = args.stream.dup();
    this.ctx.storage.kv.put("processorSlug", subscriber.processorSlug);
    // Same runner as Node/browser. KV is the storage port; the stream stub is the
    // stream port. The DO stays the sink and just delegates processEventBatch.
    this.#runner = createProcessorRunner({
      processor: echo,
      deps: undefined,
      storage: {
        load: () => this.ctx.storage.kv.get<StreamProcessorRunnerSnapshot>("snapshot"),
        save: (snapshot) => void this.ctx.storage.kv.put("snapshot", snapshot),
      },
      stream: streamPortFromRpc(this.#stream),
    });
    return {
      sink: new StreamProcessorRunnerRpcTarget(this),
      afterOffset: this.ctx.storage.kv.get<StreamProcessorRunnerSnapshot>("snapshot")?.offset,
    };
  }

  /** Returns durable processor state for test fixtures and operator inspection. */
  runtimeState() {
    return {
      processorSlug: this.ctx.storage.kv.get<StreamProcessorSlug>("processorSlug"),
      snapshot: this.ctx.storage.kv.get<StreamProcessorRunnerSnapshot>("snapshot"),
    };
  }

  /** Consumes a batch delivered by a stream subscription sink. */
  async processEventBatch(args: { events: StreamEvent[]; headOffset?: number; headCreatedAt?: string }) {
    if (this.#runner === undefined) {
      throw new Error("StreamProcessorRunner has not been attached to a stream");
    }
    await this.#runner.processEventBatch(args);
  }
}

export const StreamProcessorRunnerRpcTarget = makeRpcTargetClass<
  StreamProcessorRunnerRpc,
  StreamProcessorRunner
>(StreamProcessorRunner);
