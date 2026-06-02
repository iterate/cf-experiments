import { DurableObject } from "cloudflare:workers";

type RunRecord =
  | { phase: "fetching"; via: RunVia; startedAt: number; incarnationId: string }
  | {
      phase: "done";
      via: RunVia;
      status: number;
      body: string;
      finishedAt: number;
      incarnationId: string;
    }
  | {
      phase: "error";
      via: RunVia;
      error: string;
      finishedAt: number;
      incarnationId: string;
    };

type RunVia = "rpc-inline" | "do-fetch" | "awaited-rpc" | "alarm";
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
    void this.#slowFetch({ runId: args.runId, url: args.url, via: "rpc-inline" });
    return {
      via: "rpc-inline" as const,
      runId: args.runId,
      incarnationId: this.#incarnationId,
      returnedAt: Date.now(),
    };
  }

  /** Awaited RPC: caller stays blocked until the outbound fetch finishes or is cancelled. */
  async doSlowStuff(args: { runId: string; url: string }) {
    await this.#slowFetch({ runId: args.runId, url: args.url, via: "awaited-rpc" });
    return {
      via: "awaited-rpc" as const,
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
    const rootAbortedAt = await this.ctx.storage.get<number>(rootAbortKey(args.runId));
    if (record === undefined) return { phase: "missing" as const, rootAbortedAt };
    return { ...record, rootAbortedAt };
  }

  async markRootAbort(args: { runId: string }) {
    await this.ctx.storage.put(rootAbortKey(args.runId), Date.now());
    return { marked: true };
  }

  async fetch(request: Request) {
    const url = new URL(request.url);
    if (url.pathname !== "/inline" || request.method !== "POST") {
      return new Response("POST /inline", { status: 404 });
    }

    const body = (await request.json()) as { runId?: string; url?: string };
    if (typeof body.runId !== "string" || typeof body.url !== "string") {
      return new Response("body needs runId and url", { status: 400 });
    }

    void this.#slowFetch({ runId: body.runId, url: body.url, via: "do-fetch" });
    return Response.json({
      via: "do-fetch" as const,
      runId: body.runId,
      incarnationId: this.#incarnationId,
      returnedAt: Date.now(),
    });
  }

  async #slowFetch(args: { runId: string; url: string; via: RunVia }) {
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

function rootAbortKey(runId: string) {
  return `root-abort:${runId}`;
}

export interface Env {
  PROBE: DurableObjectNamespace<OutboundAfterResponseProbe>;
}

export default {
  async fetch(request, env, ctx) {
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

    if (url.pathname === "/do-fetch-inline" && request.method === "POST") {
      return stub.fetch(new Request("https://durable-object/inline", request));
    }

    if (url.pathname === "/await-rpc" && request.method === "POST") {
      const body = (await request.json()) as { runId?: string; url?: string };
      if (typeof body.runId !== "string" || typeof body.url !== "string") {
        return new Response("body needs runId and url", { status: 400 });
      }
      request.signal.addEventListener("abort", () => {
        ctx.waitUntil(stub.markRootAbort({ runId: body.runId! }));
      });
      return Response.json(await stub.doSlowStuff({ runId: body.runId, url: body.url }));
    }

    if (url.pathname === "/root-fire-and-forget" && request.method === "POST") {
      const body = (await request.json()) as { runId?: string; url?: string };
      if (typeof body.runId !== "string" || typeof body.url !== "string") {
        return new Response("body needs runId and url", { status: 400 });
      }
      void stub.startInline({ runId: body.runId, url: body.url });
      return Response.json({ via: "root-fire-and-forget", runId: body.runId, returnedAt: Date.now() });
    }

    if (url.pathname === "/root-wait-until" && request.method === "POST") {
      const body = (await request.json()) as { runId?: string; url?: string };
      if (typeof body.runId !== "string" || typeof body.url !== "string") {
        return new Response("body needs runId and url", { status: 400 });
      }
      ctx.waitUntil(stub.startInline({ runId: body.runId, url: body.url }));
      return Response.json({ via: "root-wait-until", runId: body.runId, returnedAt: Date.now() });
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
        "POST /inline?name=...                RPC starts void slow fetch, then root awaits RPC return",
        "POST /do-fetch-inline?name=...       DO fetch starts void slow fetch and returns immediately",
        "POST /await-rpc?name=...             root awaits RPC; DO awaits slow outbound fetch",
        "POST /root-fire-and-forget?name=...  root voids RPC then returns immediately",
        "POST /root-wait-until?name=...       root ctx.waitUntil(RPC) then returns immediately",
        "POST /alarm?name=...                 JSON { runId, url, delayMs? } — alarm does fetch",
        "GET  /status?name=...&runId=...",
      ].join("\n"),
      { headers: { "content-type": "text/plain; charset=utf-8" } },
    );
  },
} satisfies ExportedHandler<Env>;
