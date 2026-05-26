import { describe, expect, it } from "vitest";
import { newWebSocketRpcSession } from "capnweb";

type StreamEventInput = {
  type: string;
  payload?: unknown;
  metadata?: Record<string, unknown>;
};

interface StreamApi {
  append(event: StreamEventInput): Promise<StreamEventInput & { offset: number; createdAt: string }>;
}

interface ProjectApi {
  streams: {
    get(path: string): StreamApi;
  };
}

type CapturedFrame = {
  direction: "out" | "in";
  data: string;
};

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";

describe("Cap'n Web project capability pipelining", () => {
  it(
    "pipelines project.streams.get(path).append(event) against the real Worker",
    async () => {
      const path = `/pipelined-${crypto.randomUUID()}`;
      const event: StreamEventInput = {
        type: "benchmark.project-pipelining",
        payload: { path },
        metadata: { path },
      };
      const frames: CapturedFrame[] = [];
      const webSocket = newCapturingWebSocket(toWebSocketUrl(`${workerUrl}/capnweb-project`), frames);

      using project = newWebSocketRpcSession<ProjectApi>(webSocket);
      const appendResult = await project.streams.get(path).append(event);
      expect(appendResult).toEqual({
        ...event,
        offset: expect.any(Number),
        createdAt: expect.any(String),
      });

      const parsedFrames = frames.map((frame) => ({
        direction: frame.direction,
        data: JSON.parse(frame.data) as unknown,
      }));

      expect(parsedFrames).toMatchObject([
        { direction: "out", data: ["push", ["pipeline", 0, ["streams", "get"], [path]]] },
        { direction: "out", data: ["push", ["pipeline", 1, ["append"], [event]]] },
        { direction: "out", data: ["pull", 2] },
        {
          direction: "in",
          data: [
            "resolve",
            2,
            {
              ...event,
              offset: expect.any(Number),
              createdAt: expect.any(String),
            },
          ],
        },
        { direction: "out", data: ["release", 2, expect.any(Number)] },
      ]);
    },
    30_000,
  );
});

function newCapturingWebSocket(url: string, frames: CapturedFrame[]): WebSocket {
  const webSocket = new WebSocket(url);
  const send = webSocket.send.bind(webSocket);

  webSocket.send = ((data: Parameters<WebSocket["send"]>[0]) => {
    frames.push({ direction: "out", data: describeWebSocketFrameData(data) });
    return send(data);
  }) as WebSocket["send"];

  webSocket.addEventListener("message", (event) => {
    frames.push({ direction: "in", data: describeWebSocketFrameData(event.data) });
  });

  return webSocket;
}

function describeWebSocketFrameData(data: unknown) {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (ArrayBuffer.isView(data)) {
    return new TextDecoder().decode(data);
  }
  throw new TypeError(`unexpected WebSocket frame data: ${String(data)}`);
}

function toWebSocketUrl(raw: string) {
  const url = new URL(raw);
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";
  return url.toString();
}
