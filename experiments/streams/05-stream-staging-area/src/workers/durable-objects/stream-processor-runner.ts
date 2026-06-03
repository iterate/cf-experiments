import { newWorkersRpcResponse, type RpcStub } from "capnweb";
import { DurableObject } from "cloudflare:workers";
import { makeRpcTargetClass } from "@cf-experiments/shared/rpc-target";
import {
  createStreamSubscription,
  type StreamSubscription,
} from "../../subscription.js";
import { createProcessorRunner, type ProcessorRunner } from "../../processor-runner.js";
// The SAME processor the Node e2e (inbound) and the browser tab (inbound) run.
import { echoTestProcessor } from "../../processors/echo-test/implementation.js";
import type { CoreStreamState, SubscriptionConfiguredEvent } from "../../core-stream-processor.js";
import type {
  StreamProcessorRunnerRpc,
  StreamProcessorRunnerSnapshot,
  StreamProcessorSlug,
  StreamRpc,
  SubscriptionSink,
} from "../../types.js";

export class StreamProcessorRunner extends DurableObject {
  #stream: RpcStub<StreamRpc> | undefined;
  #runner: ProcessorRunner | undefined;
  #subscription: StreamSubscription | undefined;
  #processing: AsyncDisposable | undefined;

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
  async requestSubscription(args: {
    stream: RpcStub<StreamRpc>;
    subscriptionKey: string;
    streamMaxOffset: number;
    subscriptionConfiguredEvent: SubscriptionConfiguredEvent;
    streamRuntimeState: { state: CoreStreamState };
  }): Promise<{ sink: SubscriptionSink; replayAfterOffset?: number }> {
    const subscriber = args.subscriptionConfiguredEvent.payload.subscriber;
    if (subscriber.type !== "built-in") {
      throw new Error("StreamProcessorRunner only supports built-in subscribers");
    }
    if (subscriber.transport !== "capnweb-websocket") {
      throw new Error("StreamProcessorRunner only supports capnweb-websocket subscribers");
    }
    if (subscriber.processorSlug !== "echo-test") {
      throw new Error(`Unknown stream processor slug: ${subscriber.processorSlug}`);
    }

    await this.#processing?.[Symbol.asyncDispose]();
    await this.#subscription?.[Symbol.asyncDispose]();
    this.#stream?.[Symbol.dispose]();
    this.#stream = args.stream.dup();
    this.ctx.storage.kv.put("processorSlug", subscriber.processorSlug);
    // Same runner path as Node/browser. KV is the storage port; the stream stub is
    // the exact Stream RPC API passed to processor afterAppend.
    this.#runner = createProcessorRunner({
      processor: echoTestProcessor,
      deps: undefined,
      storage: {
        load: () => this.ctx.storage.kv.get<StreamProcessorRunnerSnapshot>("snapshot"),
        save: (snapshot) => void this.ctx.storage.kv.put("snapshot", snapshot),
      },
      stream: this.#stream,
    });
    const snapshot = await this.#runner.snapshot();
    this.#subscription = createStreamSubscription({
      subscriptionKey: args.subscriptionKey,
    });
    this.#processing = this.#runner.run({
      subscription: this.#subscription,
    });
    return {
      sink: this.#subscription.sink,
      replayAfterOffset: snapshot?.offset ?? 0,
    };
  }

  /** Returns durable processor state for test fixtures and operator inspection. */
  runtimeState() {
    return {
      processorSlug: this.ctx.storage.kv.get<StreamProcessorSlug>("processorSlug"),
      snapshot: this.ctx.storage.kv.get<StreamProcessorRunnerSnapshot>("snapshot"),
    };
  }

}

export const StreamProcessorRunnerRpcTarget = makeRpcTargetClass<
  StreamProcessorRunnerRpc,
  StreamProcessorRunner
>(StreamProcessorRunner);
