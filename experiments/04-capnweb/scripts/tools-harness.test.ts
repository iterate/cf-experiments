import { newWebSocketRpcSession } from "capnweb";
import { describe, expect, it } from "vitest";
import type { ToolSessionApi } from "../src/capabilities";

type CapturedFrame = {
  direction: "out" | "in";
  data: string;
};

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";

describe("Cap'n Web generic provider harness", () => {
  it("forwards nested SDK-shaped calls through a Worker-hosted Cap'n Web session", async () => {
    const frames: CapturedFrame[] = [];
    const webSocket = newCapturingWebSocket(toWebSocketUrl(`${workerUrl}/tools`), frames);

    using api = newWebSocketRpcSession<ToolSessionApi>(webSocket);

    await expect(
      api.slack.chat.postMessage({ channel: "C1", text: "hi from capnweb" }),
    ).resolves.toMatchObject({
      ok: true,
      channel: "C1",
      ts: "1700000000.000100",
      message: { text: "hi from capnweb", user: "U_BOT" },
    });

    await expect(api.slack.users.profile.get({ user: "U1" })).resolves.toMatchObject({
      ok: true,
      profile: { real_name: "Ada Lovelace", email: "ada@example.com" },
    });

    await expect(
      api.github.repos.get({ owner: "anthropics", repo: "claude-code" }),
    ).resolves.toMatchObject({
      status: 200,
      data: { full_name: "anthropics/claude-code", private: false },
    });

    const parsedFrames = frames.map((frame) => ({
      direction: frame.direction,
      data: JSON.parse(frame.data) as unknown,
    }));

    expect(parsedFrames).toEqual(
      expect.arrayContaining([
        {
          direction: "out",
          data: [
            "push",
            ["pipeline", expect.any(Number), ["slack", "chat", "postMessage"], expect.any(Array)],
          ],
        },
        {
          direction: "out",
          data: [
            "push",
            [
              "pipeline",
              expect.any(Number),
              ["slack", "users", "profile", "get"],
              expect.any(Array),
            ],
          ],
        },
        {
          direction: "out",
          data: [
            "push",
            ["pipeline", expect.any(Number), ["github", "repos", "get"], expect.any(Array)],
          ],
        },
      ]),
    );
  }, 30_000);

  it("lets dynamic Worker code call the same SDK-shaped bindings through env", async () => {
    await expect(
      fetch(`${workerUrl}/dynamic-tools`).then((response) => response.json()),
    ).resolves.toMatchObject({
      post: {
        ok: true,
        channel: "C1",
        ts: "1700000000.000100",
        message: { text: "hi from a dynamic worker", user: "U_BOT" },
      },
      profile: {
        ok: true,
        profile: { real_name: "Ada Lovelace", email: "ada@example.com" },
      },
      repo: {
        status: 200,
        data: { full_name: "anthropics/claude-code", private: false },
      },
    });
  }, 30_000);
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
