/**
 * OOM threshold sweep against a running worker.
 *
 *   pnpm dev   # in another terminal
 *   WORKER_URL=http://localhost:8787 pnpm test:oom
 *
 *   WORKER_URL=https://03-kill-durable-object.iterate-dev-preview.workers.dev pnpm test:oom
 */
import { describe, expect, it } from "vitest";

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";
const mib = (n: number) => n * 1024 * 1024;

// Key sizes from production sweeps (2026-05-22). Adjust if re-running elsewhere.
const sweepSizesMiB = [64, 128, 192, 208, 224, 256, 263, 264, 280];

describe(`OOM sweep @ ${workerUrl}`, () => {
  it("prints threshold table (alloc + follow-up ping)", async () => {
    const name = `oom-${crypto.randomUUID()}`;
    const rows: string[] = [];

    for (const sizeMiB of sweepSizesMiB) {
      const bytes = mib(sizeMiB);
      await fetch(`${workerUrl}/memory?name=${name}`, { method: "DELETE" }).catch(() => {});

      const alloc = await fetch(
        `${workerUrl}/memory?name=${name}&bytes=${bytes}&touch=fill`,
        { method: "POST" },
      );
      const allocBody = await alloc.text();
      let pingHeld = "—";
      if (alloc.ok) {
        const ping = await fetch(`${workerUrl}/ping?name=${name}`);
        if (ping.ok) {
          const json = (await ping.json()) as { heldBytes: number };
          pingHeld = String(json.heldBytes);
        } else {
          pingHeld = `ping ${ping.status}`;
        }
      }

      const ray = alloc.headers.get("cf-ray") ?? "—";
      rows.push(
        `${String(sizeMiB).padStart(4)} MiB | alloc ${alloc.status} | ping heldBytes ${pingHeld} | cf-ray ${ray}`,
      );
      if (!alloc.ok) {
        rows.push(`       body: ${allocBody.slice(0, 120)}`);
      }
    }

    const table = ["", "OOM sweep results:", ...rows, ""].join("\n");
    console.log(table);
    expect(rows.length).toBeGreaterThan(0);
  }, 600_000);
});
