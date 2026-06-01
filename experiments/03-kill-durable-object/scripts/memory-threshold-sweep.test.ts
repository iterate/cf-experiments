/**
 * Sane memory threshold sweep: one fresh DO per size, classify outcome, summarize pivots.
 *
 *   WORKER_URL=https://03-kill-durable-object.iterate-dev-preview.workers.dev pnpm test:sweep
 *   WORKER_URL=http://localhost:8787 pnpm test:sweep
 *
 *   SWEEP_MIB=192,194,196,198,200,202,204,206,208 WORKER_URL=... pnpm test:sweep
 */
import { describe, expect, it } from "vitest";

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";
const mib = (n: number) => n * 1024 * 1024;

const defaultSweepMiB = [
  64, 128, 160, 180, 188,
  192, 194, 196, 198, 200, 202, 204, 206, 208, 210, 212, 216, 220,
  224, 240, 256, 263, 264, 280,
];

const sweepSizesMiB = (process.env.SWEEP_MIB ?? defaultSweepMiB.join(","))
  .split(",")
  .map((raw) => Number(raw.trim()));

type Outcome = "stable" | "replaced" | "alloc_failed" | "ping_failed";

interface Row {
  sizeMiB: number;
  allocStatus: number;
  allocIncarnation: string | null;
  pingStatus: number | null;
  pingHeldBytes: number | null;
  pingIncarnation: string | null;
  outcome: Outcome;
  ray: string | null;
}

async function probeSize(sizeMiB: number): Promise<Row> {
  const name = `sweep-${sizeMiB}m-${crypto.randomUUID()}`;
  const bytes = mib(sizeMiB);

  const alloc = await fetch(`${workerUrl}/memory?name=${name}&bytes=${bytes}`, {
    method: "POST",
    headers: { Accept: "application/json" },
  });
  const ray = alloc.headers.get("cf-ray");

  if (!alloc.ok) {
    return {
      sizeMiB,
      allocStatus: alloc.status,
      allocIncarnation: null,
      pingStatus: null,
      pingHeldBytes: null,
      pingIncarnation: null,
      outcome: "alloc_failed",
      ray,
    };
  }

  const allocJson = (await alloc.json()) as {
    incarnationId?: string;
    totalHeldBytes?: number;
  };
  const allocIncarnation = allocJson.incarnationId ?? null;

  const ping = await fetch(`${workerUrl}/ping?name=${name}`, {
    headers: { Accept: "application/json" },
  });

  if (!ping.ok) {
    return {
      sizeMiB,
      allocStatus: alloc.status,
      allocIncarnation,
      pingStatus: ping.status,
      pingHeldBytes: null,
      pingIncarnation: null,
      outcome: "ping_failed",
      ray,
    };
  }

  const pingJson = (await ping.json()) as {
    heldBytes?: number;
    incarnationId?: string;
  };
  const pingIncarnation = pingJson.incarnationId ?? null;
  const pingHeldBytes = pingJson.heldBytes ?? null;

  const sameIncarnation =
    allocIncarnation !== null &&
    pingIncarnation !== null &&
    allocIncarnation === pingIncarnation;
  const holdsBytes = pingHeldBytes === bytes;

  const outcome: Outcome =
    sameIncarnation && holdsBytes ? "stable" : "replaced";

  return {
    sizeMiB,
    allocStatus: alloc.status,
    allocIncarnation,
    pingStatus: ping.status,
    pingHeldBytes,
    pingIncarnation,
    outcome,
    ray,
  };
}

function summarize(rows: Row[]) {
  const stable = rows.filter((r) => r.outcome === "stable");
  const replaced = rows.filter((r) => r.outcome === "replaced");
  const failed = rows.filter((r) => r.outcome === "alloc_failed");

  const lastStable = stable.at(-1)?.sizeMiB ?? null;
  const firstReplaced = replaced.at(0)?.sizeMiB ?? null;
  const firstFail = failed.at(0)?.sizeMiB ?? null;

  return { lastStable, firstReplaced, firstFail, stable, replaced, failed };
}

describe(`memory threshold sweep @ ${workerUrl}`, () => {
  it("one fresh DO per size; fine steps around 192–208 MiB", async () => {
    const rows: Row[] = [];

    for (const sizeMiB of sweepSizesMiB) {
      rows.push(await probeSize(sizeMiB));
    }

    const summary = summarize(rows);

    const lines = rows.map((r) => {
      const allocInc = r.allocIncarnation?.slice(0, 8) ?? "—";
      const pingInc = r.pingIncarnation?.slice(0, 8) ?? "—";
      const held = r.pingHeldBytes ?? "—";
      return `${String(r.sizeMiB).padStart(4)} MiB | ${r.outcome.padEnd(13)} | alloc ${r.allocStatus} | ping ${r.pingStatus ?? "—"} | held ${held} | inc ${allocInc}→${pingInc} | ray ${r.ray ?? "—"}`;
    });

    console.log(
      [
        "",
        `Memory threshold sweep @ ${workerUrl}`,
        "Protocol: fresh DO name per size, single alloc then ping",
        "",
        ...lines,
        "",
        "Summary:",
        `  last stable (alloc ok + same DO + held bytes match): ${summary.lastStable ?? "none"} MiB`,
        `  first replaced (alloc ok but new DO or lost bytes):     ${summary.firstReplaced ?? "none"} MiB`,
        `  first alloc failure:                                  ${summary.firstFail ?? "none"} MiB`,
        summary.lastStable !== null &&
        summary.firstReplaced !== null &&
        summary.firstReplaced > summary.lastStable + 2
          ? `  gap between last stable and first replaced:           ${summary.lastStable + 2}–${summary.firstReplaced - 2} MiB untested`
          : "",
        "",
      ]
        .filter(Boolean)
        .join("\n"),
    );

    expect(rows.length).toBeGreaterThan(0);
  }, 1_800_000);
});
