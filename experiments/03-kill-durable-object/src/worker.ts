import { DurableObject } from "cloudflare:workers";

/** Minimal DO: allocate filled buffers, ping to check survival, optional ctx.abort probe. */
export class DebugDurableObject extends DurableObject {
  private readonly incarnationId = crypto.randomUUID();
  private chunks: Uint8Array[] = [];

  ping() {
    let heldBytes = 0;
    for (const chunk of this.chunks) heldBytes += chunk.byteLength;
    return { message: "pong" as const, incarnationId: this.incarnationId, heldBytes };
  }

  /** 1 MiB chunks — single giant Uint8Array hits a lower failure mode in production. */
  allocate(bytes: number) {
    const chunkSize = 1024 * 1024;
    let allocated = 0;
    while (allocated < bytes) {
      const size = Math.min(chunkSize, bytes - allocated);
      const chunk = new Uint8Array(size);
      chunk.fill(0xa5);
      this.chunks.push(chunk);
      allocated += size;
    }
    let heldBytes = 0;
    for (const c of this.chunks) heldBytes += c.byteLength;
    return { incarnationId: this.incarnationId, heldBytes };
  }

  kill(reason?: string): never {
    this.ctx.abort(reason ?? "kill");
    throw new Error("unreachable");
  }
}

export interface Env {
  DEBUG_DO: DurableObjectNamespace<DebugDurableObject>;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const name = url.searchParams.get("name") ?? "default";
    const stub = env.DEBUG_DO.getByName(name);

    if (url.pathname === "/ping") {
      return Response.json(await stub.ping());
    }

    if (url.pathname === "/memory" && request.method === "POST") {
      const bytes = Number(url.searchParams.get("bytes"));
      if (!Number.isInteger(bytes) || bytes <= 0) {
        return new Response("bytes must be a positive integer", { status: 400 });
      }
      return Response.json(await stub.allocate(bytes));
    }

    if (url.pathname === "/kill" && request.method === "POST") {
      await stub.kill(url.searchParams.get("reason") ?? undefined);
      return new Response("kill returned without aborting", { status: 500 });
    }

    return new Response(
      [
        "DO memory limit probe",
        "",
        "GET  /ping?name=...",
        "POST /memory?name=...&bytes=N   allocate N bytes (filled Uint8Array, retained)",
        "POST /kill?name=...&reason=...   ctx.abort (Miniflare vs prod error body differs)",
      ].join("\n"),
      { headers: { "content-type": "text/plain; charset=utf-8" } },
    );
  },
} satisfies ExportedHandler<Env>;
