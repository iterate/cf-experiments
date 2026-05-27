import { describe, expect, it } from "vitest";

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";
const deployedIt = workerUrl.includes("localhost") ? it.skip : it;
const oomIt = process.env.RUN_OOM_PROBE === "true" ? deployedIt : it.skip;
const hibernationWaitMs = Number(process.env.HIBERNATION_WAIT_MS ?? 15_000);
const oomBytes = Number(process.env.OOM_BYTES ?? 512 * 1024 * 1024);

type AutoPong = { op: "auto-pong"; incarnationId: string; expiresAt: number };
type AppPong = { op: "app-pong"; incarnationId: string };

describe("hibernatable WebSocket reset semantics", () => {
  deployedIt("normal hibernation keeps the socket and real messages wake a new DO", async () => {
    await using socket = await openSocket(`hibernate-${crypto.randomUUID()}`);

    const before = await socket.appPing();
    await delay(hibernationWaitMs);

    // Auto-response is cheap but stale: it was created by the old constructor.
    expect((await socket.autoPing()).expiresAt).toBeLessThan(Date.now());

    // A real message wakes the hibernated DO and reaches the reattached socket.
    const after = await socket.appPing();
    expect(after.incarnationId).not.toBe(before.incarnationId);
  }, 30_000);

  deployedIt("after ctx.abort the old socket can auto-pong but cannot reach the new DO", async () => {
    await proveResetLeavesGhostSocket({
      name: `abort-${crypto.randomUUID()}`,
      reset: kill,
    });
  });

  oomIt("after OOM the old socket can auto-pong but cannot reach the new DO", async () => {
    await proveResetLeavesGhostSocket({
      name: `oom-${crypto.randomUUID()}`,
      reset: (name) => allocate(name, oomBytes),
    });
  }, 60_000);
});

async function proveResetLeavesGhostSocket(args: {
  name: string;
  reset(name: string): Promise<void>;
}) {
  await using oldSocket = await openSocket(args.name);

  const before = await oldSocket.appPing();
  await expect(args.reset(args.name)).rejects.toThrow();

  const autoPong = await oldSocket.autoPing();
  expect(autoPong.incarnationId).toBe(before.incarnationId);

  await delayUntilExpired(autoPong);
  expect((await oldSocket.autoPing()).expiresAt).toBeLessThan(Date.now());

  await expect(oldSocket.appPing()).rejects.toThrow(/timed out/);

  await using freshSocket = await openSocket(args.name);
  const fresh = await freshSocket.appPing();
  expect(fresh.incarnationId).not.toBe(before.incarnationId);
}

async function openSocket(name: string) {
  const webSocket = new WebSocket(wsUrl(name).toString());
  await once(webSocket, "open");

  return {
    async autoPing() {
      webSocket.send("ping");
      const message = await readMessage(webSocket);
      if (!isAutoPong(message)) throw new Error("expected auto-pong");
      return message;
    },
    async appPing() {
      webSocket.send(JSON.stringify({ op: "app-ping" }));
      const message = await readMessage(webSocket);
      if (!isAppPong(message)) throw new Error("expected app-pong");
      return message;
    },
    async [Symbol.asyncDispose]() {
      webSocket.close();
      await delay(10);
    },
  };
}

async function kill(name: string) {
  const response = await fetch(httpUrl("/kill", name), { method: "POST" });
  if (!response.ok) throw new Error(`kill failed: ${response.status}`);
}

async function allocate(name: string, bytes: number) {
  const url = httpUrl("/allocate", name);
  url.searchParams.set("bytes", String(bytes));
  const response = await fetch(url, { method: "POST" });
  if (!response.ok) throw new Error(`allocate failed: ${response.status}`);
}

function httpUrl(path: string, name: string) {
  const url = new URL(path, workerUrl);
  url.searchParams.set("name", name);
  return url;
}

function wsUrl(name: string) {
  const url = httpUrl("/ws", name);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url;
}

function once(target: WebSocket, type: "open") {
  return new Promise<void>((resolve, reject) => {
    target.addEventListener(type, () => resolve(), { once: true });
    target.addEventListener("error", () => reject(new Error("WebSocket failed")), { once: true });
  });
}

function readMessage(webSocket: WebSocket) {
  return withTimeout(
    new Promise<unknown>((resolve) => {
      webSocket.addEventListener("message", (event) => resolve(JSON.parse(String(event.data))), {
        once: true,
      });
    }),
    1_000,
  );
}

async function delayUntilExpired(autoPong: AutoPong) {
  await delay(Math.max(0, autoPong.expiresAt - Date.now()) + 100);
}

function withTimeout<T>(promise: Promise<T>, ms: number) {
  return Promise.race([
    promise,
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error(`timed out after ${ms}ms`)), ms),
    ),
  ]);
}

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function isAutoPong(value: unknown): value is AutoPong {
  return (
    isRecord(value) &&
    value.op === "auto-pong" &&
    typeof value.incarnationId === "string" &&
    typeof value.expiresAt === "number"
  );
}

function isAppPong(value: unknown): value is AppPong {
  return isRecord(value) && value.op === "app-pong" && typeof value.incarnationId === "string";
}
