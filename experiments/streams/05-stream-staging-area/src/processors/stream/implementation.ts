// Implements the built-in "stream" processor.
// The Stream Durable Object runs this processor inline during append instead
// of through a subscription runner, because stream bookkeeping must be updated
// before committed events are delivered to subscribers.

import { implementBuiltinProcessor } from "../../processor.js";
import { streamProcessorContract } from "./contract.js";

export const streamProcessor = implementBuiltinProcessor(streamProcessorContract, () => ({}));
