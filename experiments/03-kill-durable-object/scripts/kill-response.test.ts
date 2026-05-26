/**
 * Miniflare vs production: abort reason visibility.
 *
 *   WORKER_URL=http://localhost:8787 pnpm test:kill-response
 *   WORKER_URL=https://03-kill-durable-object.iterate-dev-preview.workers.dev pnpm test:kill-response
 */
import { describe, expect, it } from "vitest";

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";

describe(`kill response @ ${workerUrl}`, () => {
  it("POST /kill — compare body (local shows reason, prod shows error code 1101)", async () => {
    const name = `kill-${crypto.randomUUID()}`;
    const reason = `tweet-demo-${Date.now()}`;

    await fetch(`${workerUrl}/ping?name=${name}`);

    const res = await fetch(`${workerUrl}/kill?name=${name}&reason=${encodeURIComponent(reason)}`, {
      method: "POST",
    });
    const body = await res.text();
    const ray = res.headers.get("cf-ray") ?? "—";

    console.log([
      "",
      `kill response @ ${workerUrl}`,
      `  status: ${res.status}`,
      `  cf-ray: ${ray}`,
      `  body: ${JSON.stringify(body.slice(0, 200))}`,
      "",
    ].join("\n"));

    expect(res.status).toBe(500);

    const recovery = await fetch(`${workerUrl}/ping?name=${name}`);
    expect(recovery.status).toBe(200);
    const pong = (await recovery.json()) as { message: string };
    expect(pong.message).toBe("pong");
  });
});
