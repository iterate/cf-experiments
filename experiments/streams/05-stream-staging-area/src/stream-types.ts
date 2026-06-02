import type { StreamEvent } from "@cf-experiments/shared/event";
import type { RpcTarget } from "capnweb";
import type { StreamRpcTarget } from "./stream.js";

export type StreamRpc = InstanceType<typeof StreamRpcTarget>;

/** The subscriber-side CaptainWeb RPC target that receives stream event batches. */
export type SubscriptionRpcTarget = RpcTarget & {
  consumeEvents(args: { events: StreamEvent[] }): unknown;
};
