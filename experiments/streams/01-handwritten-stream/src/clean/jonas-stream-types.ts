import {
  StreamEvent,
  StreamEventInput,
  type StreamEvent as StreamEventValue,
  type StreamEventInput as StreamEventInputValue,
} from "@cf-experiments/shared/event";

const StrictStreamEventInput = StreamEventInput.strict();

export type JonasStreamSubscribeFrame = { op: "subscribe"; afterOffset?: number };
export type JonasStreamAppendRequestAck = {
  key: string;
};
export type JonasStreamAppendFrame = {
  op: "append";
  event: StreamEventInputValue;
  requestAck?: JonasStreamAppendRequestAck;
};
export type JonasStreamInboundFrame = JonasStreamSubscribeFrame | JonasStreamAppendFrame;
export type JonasStreamEventsFrame = { op: "events"; events: StreamEventValue[] };
export type JonasStreamAckFrame = {
  op: "append-ack";
  appendKey: string;
  event: StreamEventValue;
};
export type JonasStreamAppendInput = StreamEventInputValue;

export const JonasStreamSubscribeFrame = {
  parse(value: unknown): JonasStreamSubscribeFrame {
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

export const JonasStreamAppendFrame = {
  parse(value: unknown): JonasStreamAppendFrame {
    if (!isRecord(value) || value.op !== "append") throw new Error("expected append frame");
    const requestAck = parseRequestAck(value.requestAck);
    return {
      op: "append",
      event: StrictStreamEventInput.parse(value.event),
      ...(requestAck === undefined ? {} : { requestAck }),
    };
  },
};

export const JonasStreamInboundFrame = {
  parse(value: unknown): JonasStreamInboundFrame {
    if (!isRecord(value)) throw new Error("expected WebSocket frame object");
    if (value.op === "subscribe") return JonasStreamSubscribeFrame.parse(value);
    if (value.op === "append") return JonasStreamAppendFrame.parse(value);
    throw new Error("expected subscribe or append frame");
  },
};

export const JonasStreamEventsFrame = {
  parse(value: unknown): JonasStreamEventsFrame {
    if (!isRecord(value) || value.op !== "events") throw new Error("expected events frame");
    if (!Array.isArray(value.events)) throw new Error("events must be an array");
    return { op: "events", events: value.events.map((event) => StreamEvent.parse(event)) };
  },
};

export const JonasStreamAckFrame = {
  parse(value: unknown): JonasStreamAckFrame {
    if (!isRecord(value) || value.op !== "append-ack") throw new Error("expected append-ack frame");
    if (typeof value.appendKey !== "string") throw new Error("appendKey must be a string");
    return {
      op: "append-ack",
      appendKey: value.appendKey,
      event: StreamEvent.parse(value.event),
    };
  },
};

function parseRequestAck(value: unknown): JonasStreamAppendRequestAck | undefined {
  if (value === undefined) return undefined;
  return parseRequiredRequestAck(value);
}

function parseRequiredRequestAck(value: unknown): JonasStreamAppendRequestAck {
  if (!isRecord(value)) throw new Error("requestAck must be an object");
  if (typeof value.key !== "string") throw new Error("requestAck key must be a string");
  return { key: value.key };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
