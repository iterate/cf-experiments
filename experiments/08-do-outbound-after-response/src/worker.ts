import { DurableObject } from "cloudflare:workers";

type RunRecord =
  | { phase: "fetching"; via: "inline" | "alarm"; startedAt: number; incarnationId: string }
  | {
      phase: "done";
      via: "inline" | "alarm";
      status: number;
      body: string;
      finishedAt: number;
      incarnationId: string;
    }
  | {
      phase: "error";
      via: "inline" | "alarm";
      error: string;
      finishedAt: number;
      incarnationId: string;
    };

type PendingAlarmFetch = { runId: string; url: string; via: "alarm" };

/**
 * What happens if a DO starts a slow outbound fetch, returns to the caller immediately
 * (ending the inbound request), and the fetch is still in flight?
 */
export class OutboundAfterResponseProbe extends DurableObject<Env> {
  #incarnationId = crypto.randomUUID();

  debug() {
    return { incarnationId: this.#incarnationId };
  }

  /** Fire-and-forget fetch; RPC returns before fetch completes. */
  startInline(args: { runId: string; url: string }) {
    void this.#slowFetch({ runId: args.runId, url: args.url, via: "inline" });
    return {
      via: "inline" as const,
      runId: args.runId,
      incarnationId: this.#incarnationId,
      returnedAt: Date.now(),
    };
  }

  /** Schedule fetch on alarm; RPC returns before alarm runs. */
  async armAlarm(args: { runId: string; url: string; delayMs: number }) {
    const pending: PendingAlarmFetch = { runId: args.runId, url: args.url, via: "alarm" };
    await this.ctx.storage.put("pendingFetch", pending);
    await this.ctx.storage.setAlarm(Date.now() + args.delayMs);
    return {
      via: "alarm" as const,
      runId: args.runId,
      alarmDelayMs: args.delayMs,
      incarnationId: this.#incarnationId,
      returnedAt: Date.now(),
    };
  }

  async alarm() {
    const pending = await this.ctx.storage.get<PendingAlarmFetch>("pendingFetch");
    if (pending === undefined) return;
    await this.ctx.storage.delete("pendingFetch");
    await this.#slowFetch(pending);
  }

  async getRun(args: { runId: string }) {
    const record = await this.ctx.storage.get<RunRecord>(runKey(args.runId));
    if (record === undefined) return { phase: "missing" as const };
    return record;
  }

  async #slowFetch(args: { runId: string; url: string; via: "inline" | "alarm" }) {
    const key = runKey(args.runId);
    await this.ctx.storage.put(key, {
      phase: "fetching",
      via: args.via,
      startedAt: Date.now(),
      incarnationId: this.#incarnationId,
    } satisfies RunRecord);

    try {
      const response = await fetch(args.url);
      const body = await response.text();
      await this.ctx.storage.put(key, {
        phase: "done",
        via: args.via,
        status: response.status,
        body: body.slice(0, 200),
        finishedAt: Date.now(),
        incarnationId: this.#incarnationId,
      } satisfies RunRecord);
    } catch (error) {
      await this.ctx.storage.put(key, {
        phase: "error",
        via: args.via,
        error: error instanceof Error ? error.message : String(error),
        finishedAt: Date.now(),
        incarnationId: this.#incarnationId,
      } satisfies RunRecord);
    }
  }
}

function runKey(runId: string) {
  return `run:${runId}`;
}

export interface Env {
  PROBE: DurableObjectNamespace<OutboundAfterResponseProbe>;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const name = url.searchParams.get("name");
    if (name === null || name === "") {
      return new Response("name query param required", { status: 400 });
    }

    const stub = env.PROBE.getByName(name);

    if (url.pathname === "/debug") {
      return Response.json(await stub.debug());
    }

    if (url.pathname === "/inline" && request.method === "POST") {
      const body = (await request.json()) as { runId?: string; url?: string };
      if (typeof body.runId !== "string" || typeof body.url !== "string") {
        return new Response("body needs runId and url", { status: 400 });
      }
      return Response.json(await stub.startInline({ runId: body.runId, url: body.url }));
    }

    if (url.pathname === "/alarm" && request.method === "POST") {
      const body = (await request.json()) as { runId?: string; url?: string; delayMs?: number };
      if (typeof body.runId !== "string" || typeof body.url !== "string") {
        return new Response("body needs runId and url", { status: 400 });
      }
      const delayMs = body.delayMs ?? 0;
      if (!Number.isInteger(delayMs) || delayMs < 0) {
        return new Response("delayMs must be a non-negative integer", { status: 400 });
      }
      return Response.json(
        await stub.armAlarm({ runId: body.runId, url: body.url, delayMs }),
      );
    }

    if (url.pathname === "/status") {
      const runId = url.searchParams.get("runId");
      if (runId === null || runId === "") {
        return new Response("runId query param required", { status: 400 });
      }
      return Response.json(await stub.getRun({ runId }));
    }

    return new Response(
      [
        "DO outbound fetch after inbound response ends",
        "",
        "GET  /debug?name=...",
        "POST /inline?name=...   JSON { runId, url } — void slow fetch, return immediately",
        "POST /alarm?name=...    JSON { runId, url, delayMs? } — alarm does fetch",
        "GET  /status?name=...&runId=...",
      ].join("\n"),
      { headers: { "content-type": "text/plain; charset=utf-8" } },
    );
  },
} satisfies ExportedHandler<Env>;
