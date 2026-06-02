import { createCaptunTunnel } from "captun";

export const workerUrl = process.env.WORKER_URL ?? "http://localhost:8787";
export const pollSlackMs = Number(process.env.POLL_SLACK_MS ?? 5_000);
export const alarmDelayMs = Number(process.env.ALARM_DELAY_MS ?? 0);

export type StartMode =
  | "rpc-inline"
  | "do-fetch"
  | "await-rpc"
  | "root-fire-and-forget"
  | "root-wait-until"
  | "alarm";

export type RunDone = {
  phase: "done";
  via: string;
  status: number;
  body: string;
  incarnationId: string;
  rootAbortedAt?: number;
};
export type RunError = {
  phase: "error";
  via: string;
  error: string;
  incarnationId: string;
  rootAbortedAt?: number;
};
export type RunFetching = {
  phase: "fetching";
  via: string;
  incarnationId: string;
  rootAbortedAt?: number;
};
export type RunMissing = { phase: "missing"; rootAbortedAt?: number };
export type RunSnapshot = RunDone | RunError | RunFetching | RunMissing;

export type ProbeOutcome =
  | { result: "done"; waitedMs: number; record: RunDone }
  | { result: "error"; waitedMs: number; record: RunError }
  | { result: "timeout"; waitedMs: number; last: RunSnapshot };

export async function slowCaptun() {
  const tunnel = await createCaptunTunnel({
    fetch: async (request) => {
      const url = new URL(request.url);
      if (url.pathname !== "/slow") return new Response("not found", { status: 404 });

      const ms = Number(url.searchParams.get("ms"));
      if (!Number.isInteger(ms) || ms < 0) {
        return new Response("ms query param must be a non-negative integer", { status: 400 });
      }

      await delay(ms);
      return new Response(`slow-ok:${ms}`, { headers: { "content-type": "text/plain" } });
    },
  });
  return tunnel;
}

export function slowUrl(base: string, delayMs: number) {
  const url = new URL("/slow", base.replace(/\/$/, "") + "/");
  url.searchParams.set("ms", String(delayMs));
  return url.toString();
}

export function pollBudgetMs(delayMs: number) {
  return delayMs + pollSlackMs;
}

export async function runInlineProbe(args: { name: string; runId: string; url: string }) {
  return runModeProbe({ mode: "rpc-inline", ...args });
}

export async function runAlarmProbe(args: { name: string; runId: string; url: string }) {
  return runModeProbe({ mode: "alarm", ...args });
}

export async function runModeProbe(args: {
  mode: StartMode;
  name: string;
  runId: string;
  url: string;
}) {
  const start = await startMode(args.mode, args.name, args.runId, args.url);
  const delayMs = delayFromSlowUrl(args.url);
  const outcome = await pollRun({
    name: args.name,
    runId: args.runId,
    budgetMs: pollBudgetMs(delayMs),
  });
  return { start, outcome, delayMs };
}

export async function startMode(mode: StartMode, name: string, runId: string, url: string) {
  if (mode === "rpc-inline") return postInline(name, runId, url);
  if (mode === "do-fetch") return postDoFetchInline(name, runId, url);
  if (mode === "await-rpc") return postAwaitRpc(name, runId, url);
  if (mode === "root-fire-and-forget") return postRootFireAndForget(name, runId, url);
  if (mode === "root-wait-until") return postRootWaitUntil(name, runId, url);
  return postAlarm(name, runId, url);
}

function delayFromSlowUrl(url: string) {
  const ms = Number(new URL(url).searchParams.get("ms"));
  if (!Number.isInteger(ms) || ms < 0) throw new Error(`slow url missing ms: ${url}`);
  return ms;
}

export async function postInline(name: string, runId: string, url: string) {
  const response = await fetch(`${workerUrl}/inline?name=${encodeURIComponent(name)}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ runId, url }),
  });
  const body = (await response.json()) as {
    via: string;
    runId: string;
    incarnationId: string;
    returnedAt: number;
  };
  return { response, body };
}

export async function postDoFetchInline(name: string, runId: string, url: string) {
  return postJson("/do-fetch-inline", name, { runId, url });
}

export async function postAwaitRpc(name: string, runId: string, url: string, signal?: AbortSignal) {
  return postJson("/await-rpc", name, { runId, url }, signal);
}

export async function postRootFireAndForget(name: string, runId: string, url: string) {
  return postJson("/root-fire-and-forget", name, { runId, url });
}

export async function postRootWaitUntil(name: string, runId: string, url: string) {
  return postJson("/root-wait-until", name, { runId, url });
}

export async function postAlarm(name: string, runId: string, url: string) {
  return postJson("/alarm", name, { runId, url, delayMs: alarmDelayMs });
}

async function postJson(
  path: string,
  name: string,
  body: Record<string, unknown>,
  signal?: AbortSignal,
) {
  const response = await fetch(`${workerUrl}${path}?name=${encodeURIComponent(name)}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
    signal,
  });
  const responseBody = (await response.json()) as {
    via: string;
    runId: string;
    incarnationId?: string;
    returnedAt: number;
  };
  return { response, body: responseBody };
}

export async function pollRun(args: { name: string; runId: string; budgetMs: number }): Promise<ProbeOutcome> {
  const started = Date.now();
  let lastSnapshot: RunSnapshot = { phase: "missing" };

  while (Date.now() - started < args.budgetMs) {
    const response = await fetch(
      `${workerUrl}/status?name=${encodeURIComponent(args.name)}&runId=${encodeURIComponent(args.runId)}`,
    );
    if (!response.ok) throw new Error(`status ${response.status}`);
    const snapshot = (await response.json()) as RunSnapshot;
    lastSnapshot = snapshot;
    if (snapshot.phase === "done") {
      return { result: "done", waitedMs: Date.now() - started, record: snapshot };
    }
    if (snapshot.phase === "error") {
      return { result: "error", waitedMs: Date.now() - started, record: snapshot };
    }
    await delay(200);
  }

  return { result: "timeout", waitedMs: Date.now() - started, last: lastSnapshot };
}

export function formatOutcome(outcome: ProbeOutcome) {
  const rootAbort = "record" in outcome
    ? formatRootAbort(outcome.record.rootAbortedAt)
    : formatRootAbort(outcome.last.rootAbortedAt);
  if (outcome.result === "done") {
    return `done status=${outcome.record.status} waited=${outcome.waitedMs}ms${rootAbort}`;
  }
  if (outcome.result === "error") {
    return `error ${outcome.record.error} waited=${outcome.waitedMs}ms${rootAbort}`;
  }
  return `timeout last=${JSON.stringify(outcome.last)} waited=${outcome.waitedMs}ms${rootAbort}`;
}

export function ray(response: Response) {
  return response.headers.get("cf-ray") ?? "—";
}

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function formatRootAbort(rootAbortedAt: number | undefined) {
  return rootAbortedAt === undefined ? "" : ` rootAbortedAt=${rootAbortedAt}`;
}
