/**
 * Miniflare vs production: abort reason visibility.
 *
 *   pnpm dev   # in another terminal; use the "Ready on ..." URL
 *   WORKER_URL=http://localhost:8787 pnpm test:kill-response
 *   WORKER_URL=https://03-kill-durable-object.iterate-dev-preview.workers.dev pnpm test:kill-response
 */
import { describe, expect, it } from "vitest";

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";

describe(`kill response @ ${workerUrl}`, () => {
  it("POST /kill — compare body (local shows reason, prod shows Cloudflare error page)", async () => {
    const name = `kill-${crypto.randomUUID()}`;
    const reason = `tweet-demo-${Date.now()}`;

    await fetch(`${workerUrl}/ping?name=${name}`);

    const res = await fetch(`${workerUrl}/kill?name=${name}&reason=${encodeURIComponent(reason)}`, {
      method: "POST",
      headers: { Accept: "text/plain" },
    });
    const body = await res.text();
    const ray = res.headers.get("cf-ray") ?? "—";
    const cloudflareErrorCode = body.match(/cf-error-code">(\d+)</)?.[1] ?? "—";

    console.log([
      "",
      `kill response @ ${workerUrl}`,
      `  status: ${res.status}`,
      `  cf-ray: ${ray}`,
      `  body contains reason: ${String(body.includes(reason))}`,
      `  cloudflare error code: ${cloudflareErrorCode}`,
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
