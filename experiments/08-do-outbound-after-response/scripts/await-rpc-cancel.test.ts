/**
 * Parent Worker awaits a slow DO RPC; client aborts before the DO outbound fetch responds.
 *
 *   WORKER_URL=https://08-do-outbound-after-response.iterate-dev-preview.workers.dev \
 *     SLOW_MS=180000 ABORT_MS=30000 pnpm test:await-cancel
 */
import { describe, expect, it } from "vitest";
import {
  formatOutcome,
  pollBudgetMs,
  pollRun,
  postAwaitRpc,
  slowCaptun,
  slowUrl,
  workerUrl,
} from "./lib/outbound-probe";

const slowMs = Number(process.env.SLOW_MS ?? 180_000);
const abortMs = Number(process.env.ABORT_MS ?? 30_000);

describe(`awaited DO RPC client abort @ ${workerUrl}`, () => {
  it("records what happens to DO work after the parent request is aborted", async () => {
    using slowOrigin = await slowCaptun();
    const name = `await-cancel-${crypto.randomUUID()}`;
    const runId = crypto.randomUUID();
    const url = slowUrl(slowOrigin.url, slowMs);
    const controller = new AbortController();

    const startedAt = Date.now();
    const request = postAwaitRpc(name, runId, url, controller.signal);
    await delay(abortMs);
    controller.abort();

    await expect(request).rejects.toThrow();
    console.log(`client aborted after ${Date.now() - startedAt}ms`);

    const outcome = await pollRun({ name, runId, budgetMs: pollBudgetMs(slowMs) });
    console.log(formatOutcome(outcome));

    // This test is observational: a cancellation finding is represented as timeout/fetching
    // or error. A surprising "done" is also useful and should be logged.
    expect(["done", "error", "timeout"]).toContain(outcome.result);
  }, slowMs + abortMs + 60_000);
});

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
