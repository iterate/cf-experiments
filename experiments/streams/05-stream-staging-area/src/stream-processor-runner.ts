import { newWorkersRpcResponse, type RpcStub } from "capnweb";
import { DurableObject } from "cloudflare:workers";
import { z } from "zod";
import type { StreamEvent } from "@cf-experiments/shared/event";
import { defineProcessorContract } from "@cf-experiments/shared/stream-processors";
import { makeRpcTargetClass } from "@cf-experiments/shared/rpc-target";
import {
  createProcessorRunner,
  implementProcessor,
  streamPortFromRpc,
  type ProcessorRunner,
} from "./stream-processor.js";
import type { CoreStreamState, SubscriptionConfiguredEvent } from "./core-stream-processor.js";
import type {
  StreamCursor,
  StreamProcessorRunnerRpc,
  StreamProcessorRunnerSnapshot,
  StreamProcessorSlug,
  StreamRpc,
  SubscriptionSink,
} from "./stream-types.js";

// The built-in "echo" processor, defined with the same model that runs in Node
// (stream-processor.test.ts) and the browser. The DO is just one host.
const echoContract = defineProcessorContract({
  slug: "echo",
  version: "0.1.0",
  description: "Echoes input events back as output events.",
  stateSchema: z.object({ seen: z.number().int().min(0).default(0) }),
  initialState: {},
  events: {
    "test.processor.input": { description: "in", payloadSchema: z.unknown() },
    "test.processor.output": { description: "out", payloadSchema: z.object({ seen: z.number() }) },
  },
  consumes: ["test.processor.input"],
  emits: ["test.processor.output"],
  reduce({ state, event }) {
    return event.type === "test.processor.input" ? { seen: state.seen + 1 } : state;
  },
});

const echo = implementProcessor(echoContract, () => ({
  afterAppend({ event, state, append }) {
    if (event.type !== "test.processor.input") return;
    append({ type: "test.processor.output", payload: { seen: state.seen } });
  },
}));

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
  subscribe(args: {
    stream: RpcStub<StreamRpc>;
    subscriptionConfiguredEvent: SubscriptionConfiguredEvent;
    streamRuntimeState: { state: CoreStreamState };
  }): { sink: SubscriptionSink; afterOffset?: StreamCursor } {
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
