import {
  StreamEvent,
  StreamEventInput,
  type StreamEvent as StreamEventValue,
  type StreamEventInput as StreamEventInputValue,
} from "@cf-experiments/shared/event";
import type { RpcTarget } from "capnweb";
import type { StreamRpcTarget } from "./stream-do.js";

const StrictStreamEventInput = StreamEventInput.strict();

export type StreamRpc = InstanceType<typeof StreamRpcTarget>;

/** The subscription-side capability Stream calls whenever events are ready. */
export type SubscriptionRpcTarget = RpcTarget & {
  consumeEvents(args: { events: StreamEventValue[] }): unknown;
};

export type StreamSubscribeFrame = { op: "subscribe"; afterOffset?: number };
export type StreamAppendRequestAck = {
  key: string;
};
export type StreamAppendFrame = {
  op: "append";
  event: StreamEventInputValue;
  requestAck?: StreamAppendRequestAck;
};
export type StreamInboundFrame = StreamSubscribeFrame | StreamAppendFrame;
export type StreamEventsFrame = { op: "events"; events: StreamEventValue[] };
export type StreamAckFrame = {
  op: "append-ack";
  appendKey: string;
  event: StreamEventValue;
};
export type StreamAppendInput = StreamEventInputValue;

export const StreamSubscribeFrame = {
  parse(value: unknown): StreamSubscribeFrame {
    if (!isRecord(value) || value.op !== "subscribe") throw new Error("expected subscribe frame");
    if (value.afterOffset === undefined) return { op: "subscribe" };
    if (
      typeof value.afterOffset !== "number" ||
      !Number.isInteger(value.afterOffset) ||
      value.afterOffset < 0
    ) {
      throw new Error("subscribe afterOffset must be a non-negative integer");
    }
    return { op: "subscribe", afterOffset: value.afterOffset };
  },
};

export const StreamAppendFrame = {
  parse(value: unknown): StreamAppendFrame {
    if (!isRecord(value) || value.op !== "append") throw new Error("expected append frame");
    const requestAck = parseRequestAck(value.requestAck);
    return {
      op: "append",
      event: StrictStreamEventInput.parse(value.event),
      ...(requestAck === undefined ? {} : { requestAck }),
    };
  },
};

export const StreamInboundFrame = {
  parse(value: unknown): StreamInboundFrame {
    if (!isRecord(value)) throw new Error("expected WebSocket frame object");
    if (value.op === "subscribe") return StreamSubscribeFrame.parse(value);
    if (value.op === "append") return StreamAppendFrame.parse(value);
    throw new Error("expected subscribe or append frame");
  },
};

export const StreamEventsFrame = {
  parse(value: unknown): StreamEventsFrame {
    if (!isRecord(value) || value.op !== "events") throw new Error("expected events frame");
    if (!Array.isArray(value.events)) throw new Error("events must be an array");
    return { op: "events", events: value.events.map((event) => StreamEvent.parse(event)) };
  },
};

export const StreamAckFrame = {
  parse(value: unknown): StreamAckFrame {
    if (!isRecord(value) || value.op !== "append-ack") throw new Error("expected append-ack frame");
    if (typeof value.appendKey !== "string") throw new Error("appendKey must be a string");
    return {
      op: "append-ack",
      appendKey: value.appendKey,
      event: StreamEvent.parse(value.event),
    };
  },
};

function parseRequestAck(value: unknown): StreamAppendRequestAck | undefined {
  if (value === undefined) return undefined;
  return parseRequiredRequestAck(value);
}

function parseRequiredRequestAck(value: unknown): StreamAppendRequestAck {
  if (!isRecord(value)) throw new Error("requestAck must be an object");
  if (typeof value.key !== "string") throw new Error("requestAck key must be a string");
  return { key: value.key };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
