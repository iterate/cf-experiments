import { DurableObject } from "cloudflare:workers";

function requireCtxIdName(ctx: DurableObjectState): string {
  const name = ctx.id.name;
  if (name === undefined) throw new Error("this should never happen: ctx.id.name is undefined");
  return name;
}

/** Minimal DO: assert `ctx.id.name` is set when addressed via `getByName`. */
export class NameProbe extends DurableObject {
  getName() {
    return { name: requireCtxIdName(this.ctx) };
  }

  async armAlarm(args: { delayMs: number }) {
    const name = requireCtxIdName(this.ctx);
    await this.ctx.storage.put("armedName", name);
    await this.ctx.storage.setAlarm(Date.now() + args.delayMs);
    return { armed: true, name, delayMs: args.delayMs };
  }

  async alarm() {
    const name = requireCtxIdName(this.ctx);
    const armedName = await this.ctx.storage.get<string>("armedName");
    await this.ctx.storage.put("lastAlarm", { name, armedName, at: Date.now() });
  }

  async lastAlarm() {
    return (await this.ctx.storage.get<{ name: string; armedName: string | null; at: number }>(
      "lastAlarm",
    )) ?? null;
  }
}

export interface Env {
  PROBE: DurableObjectNamespace<NameProbe>;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const name = url.searchParams.get("name");
    if (name === null || name === "") {
      return new Response("name query param required", { status: 400 });
    }

    const stub = env.PROBE.getByName(name);

    if (url.pathname === "/rpc") {
      const body = await stub.getName();
      if (body.name !== name) {
        return Response.json(
          { error: "this should never happen: ctx.id.name mismatch", expected: name, actual: body.name },
          { status: 500 },
        );
      }
      return Response.json(body);
    }

    if (url.pathname === "/alarm" && request.method === "POST") {
      const delayMs = Number(url.searchParams.get("delayMs") ?? 50);
      if (!Number.isInteger(delayMs) || delayMs < 1) {
        return new Response("delayMs must be a positive integer", { status: 400 });
      }
      return Response.json(await stub.armAlarm({ delayMs }));
    }

    if (url.pathname === "/alarm") {
      return Response.json({ lastAlarm: await stub.lastAlarm() });
    }

    return new Response(
      [
        "ctx.id.name probe",
        "",
        "GET  /rpc?name=...           RPC getName() — requires ctx.id.name",
        "POST /alarm?name=...         schedule alarm (delayMs query, default 50)",
        "GET  /alarm?name=...         read last alarm snapshot from storage",
      ].join("\n"),
      { headers: { "content-type": "text/plain; charset=utf-8" } },
    );
  },
} satisfies ExportedHandler<Env>;
