import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";

export const CLEAN_STREAM_ORPC_SIGNING_KEY = "01-handwritten-stream-clean-transport-comparison";

export type CleanStreamEventSink = {
  event(event: StreamEvent): unknown;
};

export type CleanStreamRpc = {
  append(args: { event: StreamEventInput }): StreamEvent;
  subscribe(args?: unknown): ReadableStream<StreamEvent>;
  subscribeOneWay(sink: CleanStreamEventSink, args?: unknown): void;
  debug(): unknown;
};
