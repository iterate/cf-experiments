/**
 * Prove ctx.id.name is set when the DO is addressed via getByName.
 *
 *   pnpm dev   # use the "Ready on ..." URL
 *   WORKER_URL=http://localhost:8787 pnpm test
 *   WORKER_URL=https://07-do-ctx-id-name.iterate-dev-preview.workers.dev pnpm test
 */
import { describe, expect, it } from "vitest";

const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";
const alarmPollMs = Number(process.env.ALARM_POLL_MS ?? 2_000);
const alarmDelayMs = Number(process.env.ALARM_DELAY_MS ?? 50);

describe(`ctx.id.name @ ${workerUrl}`, () => {
  it("RPC getName() returns the getByName name", async () => {
    const name = `rpc-${crypto.randomUUID()}`;
    const res = await fetch(`${workerUrl}/rpc?name=${encodeURIComponent(name)}`);
    const ray = res.headers.get("cf-ray") ?? "—";
    const body = (await res.json()) as { name?: string; error?: string };

    console.log(`rpc cf-ray=${ray} status=${res.status} body=${JSON.stringify(body)}`);

    expect(res.status).toBe(200);
    expect(body.name).toBe(name);
  });

  it("alarm() sees the same ctx.id.name", async () => {
    const name = `alarm-${crypto.randomUUID()}`;

    const arm = await fetch(
      `${workerUrl}/alarm?name=${encodeURIComponent(name)}&delayMs=${alarmDelayMs}`,
      { method: "POST" },
    );
    expect(arm.status).toBe(200);
    const armed = (await arm.json()) as { name: string };
    expect(armed.name).toBe(name);

    const deadline = Date.now() + alarmPollMs;
    let lastAlarm: { name: string; armedName: string | null } | null = null;
    while (Date.now() < deadline) {
      const poll = await fetch(`${workerUrl}/alarm?name=${encodeURIComponent(name)}`);
      expect(poll.status).toBe(200);
      const snapshot = (await poll.json()) as {
        lastAlarm: { name: string; armedName: string | null } | null;
      };
      if (snapshot.lastAlarm !== null) {
        lastAlarm = snapshot.lastAlarm;
        break;
      }
      await delay(25);
    }

    const ray = arm.headers.get("cf-ray") ?? "—";
    console.log(`alarm cf-ray=${ray} lastAlarm=${JSON.stringify(lastAlarm)}`);

    expect(lastAlarm).not.toBeNull();
    expect(lastAlarm!.name).toBe(name);
    expect(lastAlarm!.armedName).toBe(name);
  });
});

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
