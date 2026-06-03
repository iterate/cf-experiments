// Defines the built-in "stream" processor contract.
// This processor owns stream runtime state such as max offset, stream config,
// and outbound subscription configuration. The Stream Durable Object currently
// imports the original contract module directly while this folder becomes the
// package-shaped home for the processor slug.

export {
  coreStreamProcessorContract as streamProcessorContract,
  type CoreStreamState as StreamProcessorState,
  type SubscriptionConfiguredEvent,
} from "../../core-stream-processor.js";
