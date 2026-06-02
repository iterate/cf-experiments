/**
 * Sweep captun delay (ms query param) to find where inline vs alarm diverge.
 *
 *   WORKER_URL=https://08-do-outbound-after-response.iterate-dev-preview.workers.dev \
 *     SWEEP_MS=1000,5000,15000,30000,60000,120000 pnpm test:sweep
 */
import { describe, it } from "vitest";
import {
  formatOutcome,
  ray,
  runAlarmProbe,
  runInlineProbe,
  slowCaptun,
  slowUrl,
  workerUrl,
} from "./lib/outbound-probe";

const sweepMs = (process.env.SWEEP_MS ?? "1000,3000,8000,15000,30000,60000,90000,120000")
  .split(",")
  .map((part) => Number(part.trim()))
  .filter((ms) => Number.isInteger(ms) && ms >= 0);

describe(`delay sweep @ ${workerUrl}`, () => {
  it("inline vs alarm across slow-ms values", async () => {
    using slowOrigin = await slowCaptun();
    console.log(`captun ${slowOrigin.url}/slow?ms=<delay>`);
    console.log("delayMs | inline | alarm | match");
    console.log("--------|--------|-------|------");

    const rows: { delayMs: number; inline: string; alarm: string; match: boolean }[] = [];

    for (const delayMs of sweepMs) {
      const url = slowUrl(slowOrigin.url, delayMs);
      const inlineName = `sweep-inline-${delayMs}-${crypto.randomUUID()}`;
      const alarmName = `sweep-alarm-${delayMs}-${crypto.randomUUID()}`;

      const inline = await runInlineProbe({
        name: inlineName,
        runId: crypto.randomUUID(),
        url,
      });
      const alarm = await runAlarmProbe({
        name: alarmName,
        runId: crypto.randomUUID(),
        url,
      });

      const inlineSummary = formatOutcome(inline.outcome);
      const alarmSummary = formatOutcome(alarm.outcome);
      const match = inline.outcome.result === alarm.outcome.result;
      rows.push({ delayMs, inline: inlineSummary, alarm: alarmSummary, match });

      console.log(
        `${String(delayMs).padStart(7)} | ${inline.outcome.result.padEnd(6)} | ${alarm.outcome.result.padEnd(5)} | ${match ? "yes" : "NO"}`,
      );
      console.log(`  inline cf-ray=${ray(inline.start.response)} ${inlineSummary}`);
      console.log(`  alarm  cf-ray=${ray(alarm.start.response)} ${alarmSummary}`);
    }

    const diverged = rows.filter((row) => !row.match);
    console.log("");
    console.log(
      diverged.length === 0
        ? "Summary: inline and alarm matched on result for all delays"
        : `Summary: diverged at delayMs=${diverged.map((r) => r.delayMs).join(", ")}`,
    );
  }, 900_000);
});
