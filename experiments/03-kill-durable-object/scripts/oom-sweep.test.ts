/**
 * OOM threshold sweep against a running worker.
 *
 *   pnpm dev   # in another terminal; use the "Ready on ..." URL
 *   WORKER_URL=http://localhost:8787 pnpm test:oom
 *
 *   WORKER_URL=https://03-kill-durable-object.iterate-dev-preview.workers.dev pnpm test:oom
 */
import { describe, expect, it } from "vitest";

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";
const mib = (n: number) => n * 1024 * 1024;

// Key sizes from production sweeps. Override with e.g. SWEEP_MIB=64,128,192,208,264,600.
const sweepSizesMiB = (process.env.SWEEP_MIB ?? "64,128,192,208,224,256,263,264,280")
  .split(",")
  .map((raw) => Number(raw.trim()));

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
      let pingIncarnation = "—";
      let incarnationChange = "—";
      if (alloc.ok) {
        const allocJson = JSON.parse(allocBody);
        const allocIncarnation =
          typeof allocJson.incarnationId === "string" ? allocJson.incarnationId.slice(0, 8) : "?";
        const ping = await fetch(`${workerUrl}/ping?name=${name}`);
        if (ping.ok) {
          const json = (await ping.json()) as { heldBytes?: unknown; incarnationId?: unknown };
          pingHeld = String(json.heldBytes);
          pingIncarnation = typeof json.incarnationId === "string" ? json.incarnationId.slice(0, 8) : "?";
          incarnationChange = allocIncarnation === pingIncarnation ? "same" : "changed";
        } else {
          pingHeld = `ping ${ping.status}`;
        }
      }

      const ray = alloc.headers.get("cf-ray") ?? "—";
      rows.push(
        `${String(sizeMiB).padStart(4)} MiB | alloc ${alloc.status} | ping heldBytes ${pingHeld} | incarnation ${incarnationChange} (${pingIncarnation}) | cf-ray ${ray}`,
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
