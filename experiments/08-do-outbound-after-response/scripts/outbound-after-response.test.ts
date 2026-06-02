/**
 * Slow origin via captun; DO returns before fetch completes.
 *
 *   pnpm dev
 *   WORKER_URL=http://localhost:8787 SLOW_MS=8000 pnpm test
 *   WORKER_URL=https://08-do-outbound-after-response.iterate-dev-preview.workers.dev pnpm test
 */
import { describe, expect, it } from "vitest";
import {
  formatOutcome,
  pollBudgetMs,
  pollRun,
  postAlarm,
  postInline,
  ray,
  runAlarmProbe,
  slowCaptun,
  slowUrl,
  workerUrl,
} from "./lib/outbound-probe";

const slowMs = Number(process.env.SLOW_MS ?? 8_000);

describe(`DO outbound after response @ ${workerUrl}`, () => {
  it("inline fire-and-forget: slow fetch completes after RPC returns", async () => {
    using slowOrigin = await slowCaptun();
    const name = `inline-${crypto.randomUUID()}`;
    const runId = crypto.randomUUID();
    const url = slowUrl(slowOrigin.url, slowMs);

    const httpStart = Date.now();
    const start = await postInline(name, runId, url);
    const httpMs = Date.now() - httpStart;
    console.log(`inline-start cf-ray=${ray(start.response)} httpMs=${httpMs}`);

    expect(start.response.status).toBe(200);
    expect(httpMs).toBeLessThan(2_000);

    const outcome = await pollRun({ name, runId, budgetMs: pollBudgetMs(slowMs) });
    console.log(formatOutcome(outcome));
    expect(outcome.result).toBe("done");
    if (outcome.result === "done") {
      expect(outcome.record.via).toBe("rpc-inline");
      expect(outcome.record.status).toBe(200);
      expect(outcome.record.body).toContain(`slow-ok:${slowMs}`);
      expect(outcome.record.incarnationId).toBe(start.body.incarnationId);
    }
  }, slowMs + 30_000);

  it("alarm: slow fetch completes after arm returns (separate event)", async () => {
    using slowOrigin = await slowCaptun();
    const name = `alarm-${crypto.randomUUID()}`;
    const runId = crypto.randomUUID();
    const url = slowUrl(slowOrigin.url, slowMs);

    const { start, outcome } = await runAlarmProbe({ name, runId, url });
    console.log(`alarm-start cf-ray=${ray(start.response)} ${formatOutcome(outcome)}`);

    expect(start.response.status).toBe(200);
    expect(outcome.result).toBe("done");
    if (outcome.result === "done") {
      expect(outcome.record.via).toBe("alarm");
      expect(outcome.record.status).toBe(200);
      expect(outcome.record.body).toContain(`slow-ok:${slowMs}`);
      expect(outcome.record.incarnationId).toBe(start.body.incarnationId);
    }
  }, slowMs + 30_000);
});
