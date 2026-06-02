/**
 * Sweep different parent-context shapes at longer durations.
 *
 *   WORKER_URL=https://08-do-outbound-after-response.iterate-dev-preview.workers.dev \
 *     MODES=rpc-inline,do-fetch,await-rpc,root-fire-and-forget,root-wait-until,alarm \
 *     SWEEP_MS=180000,300000 pnpm test:modes
 */
import { describe, it } from "vitest";
import {
  type StartMode,
  formatOutcome,
  ray,
  runModeProbe,
  slowCaptun,
  slowUrl,
  workerUrl,
} from "./lib/outbound-probe";

const modes = (process.env.MODES ?? "rpc-inline,do-fetch,await-rpc,root-fire-and-forget,root-wait-until,alarm")
  .split(",")
  .map((part) => part.trim())
  .filter((part): part is StartMode => isStartMode(part));

const sweepMs = (process.env.SWEEP_MS ?? "180000,300000")
  .split(",")
  .map((part) => Number(part.trim()))
  .filter((ms) => Number.isInteger(ms) && ms >= 0);

describe(`parent context mode sweep @ ${workerUrl}`, () => {
  it("records each mode result without failing on divergence", async () => {
    using slowOrigin = await slowCaptun();
    console.log(`captun ${slowOrigin.url}/slow?ms=<delay>`);
    console.log(`modes=${modes.join(",")}`);

    for (const delayMs of sweepMs) {
      const url = slowUrl(slowOrigin.url, delayMs);
      console.log("");
      console.log(`delayMs=${delayMs}`);

      const runs = await Promise.all(
        modes.map(async (mode) => ({
          mode,
          run: await runModeProbe({
            mode,
            name: `${mode}-${delayMs}-${crypto.randomUUID()}`,
            runId: crypto.randomUUID(),
            url,
          }),
        })),
      );

      for (const { mode, run } of runs) {
        console.log(
          `  ${mode.padEnd(20)} cf-ray=${ray(run.start.response)} ${formatOutcome(run.outcome)}`,
        );
      }
    }
  }, 3_600_000);
});

function isStartMode(value: string): value is StartMode {
  return (
    value === "rpc-inline" ||
    value === "do-fetch" ||
    value === "await-rpc" ||
    value === "root-fire-and-forget" ||
    value === "root-wait-until" ||
    value === "alarm"
  );
}
