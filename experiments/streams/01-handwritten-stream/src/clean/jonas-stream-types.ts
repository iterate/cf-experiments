import {
  StreamEvent,
  StreamEventInput,
  type StreamEvent as StreamEventValue,
  type StreamEventInput as StreamEventInputValue,
} from "@cf-experiments/shared/event";

const StrictStreamEventInput = StreamEventInput.strict();

export type JonasStreamStartFrame = { op: "start"; afterOffset?: number };
export type JonasStreamAppendRequestAck = {
  key: string;
};
export type JonasStreamAppendFrame = {
  op: "append";
  event: StreamEventInputValue;
  requestAck?: JonasStreamAppendRequestAck;
};
export type JonasStreamInboundFrame = JonasStreamStartFrame | JonasStreamAppendFrame;
export type JonasStreamEventFrame = { op: "event"; event: StreamEventValue };
export type JonasStreamAckFrame = {
  op: "append-ack";
  appendKey: string;
  event: StreamEventValue;
};
export type JonasStreamAppendInput = StreamEventInputValue;

export const JonasStreamStartFrame = {
  parse(value: unknown): JonasStreamStartFrame {
    if (!isRecord(value) || value.op !== "start") throw new Error("expected start frame");
    const afterOffset = value.afterOffset;
    if (afterOffset === undefined) return { op: "start" };
    if (typeof afterOffset !== "number" || !Number.isInteger(afterOffset) || afterOffset < 0) {
      throw new Error("start afterOffset must be a non-negative integer");
    }
    return { op: "start", afterOffset };
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
    if (value.op === "start") return JonasStreamStartFrame.parse(value);
    if (value.op === "append") return JonasStreamAppendFrame.parse(value);
    throw new Error("expected start or append frame");
  },
};

export const JonasStreamEventFrame = {
  parse(value: unknown): JonasStreamEventFrame {
    if (!isRecord(value) || value.op !== "event") throw new Error("expected event frame");
    return { op: "event", event: StreamEvent.parse(value.event) };
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
