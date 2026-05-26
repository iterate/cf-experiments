import { DurableObject } from "cloudflare:workers";

// --- Memory probe (inlined; no shared deps) ---

type MemoryChunk = Uint8Array;
type MemoryTouchMode = "none" | "pages" | "fill" | "random";

interface MemoryAllocationResult {
  allocatedBytes: number;
  logicalAllocatedBytes: number;
  touchedBytes: number;
  estimatedCommittedBytes: number;
  chunkCount: number;
  totalHeldBytes: number;
  totalLogicalHeldBytes: number;
  touchMode: MemoryTouchMode;
}

const DEFAULT_CHUNK_BYTES = 1024 * 1024;
const DEFAULT_PAGE_BYTES = 4096;

function sumChunkBytes(chunks: readonly MemoryChunk[]): number {
  let total = 0;
  for (const chunk of chunks) total += chunk.byteLength;
  return total;
}

function accumulateMemory(args: {
  targetBytes: number;
  chunkBytes?: number;
  touchMode?: MemoryTouchMode;
  into?: MemoryChunk[];
}): MemoryAllocationResult {
  if (!Number.isFinite(args.targetBytes) || args.targetBytes < 0) {
    throw new RangeError("targetBytes must be a non-negative finite number");
  }

  const chunkBytes = args.chunkBytes ?? DEFAULT_CHUNK_BYTES;
  if (!Number.isFinite(chunkBytes) || chunkBytes <= 0) {
    throw new RangeError("chunkBytes must be a positive finite number");
  }

  const store = args.into ?? [];
  const touchMode = args.touchMode ?? "none";
  const startHeld = sumChunkBytes(store);
  let allocated = 0;
  let touched = 0;
  let estimatedCommitted = 0;

  while (allocated < args.targetBytes) {
    const size = Math.min(chunkBytes, args.targetBytes - allocated);
    const chunk = new Uint8Array(size);
    const touch = touchChunk(chunk, touchMode);
    touched += touch.touchedBytes;
    estimatedCommitted += touch.estimatedCommittedBytes;
    store.push(chunk);
    allocated += size;
  }

  return {
    allocatedBytes: allocated,
    logicalAllocatedBytes: allocated,
    touchedBytes: touched,
    estimatedCommittedBytes: estimatedCommitted,
    chunkCount: store.length,
    totalHeldBytes: startHeld + allocated,
    totalLogicalHeldBytes: startHeld + allocated,
    touchMode,
  };
}

function touchChunk(
  chunk: Uint8Array,
  touchMode: MemoryTouchMode,
): { touchedBytes: number; estimatedCommittedBytes: number } {
  if (touchMode === "none" || chunk.byteLength === 0) {
    return { touchedBytes: 0, estimatedCommittedBytes: 0 };
  }

  if (touchMode === "fill") {
    chunk.fill(0xa5);
    return { touchedBytes: chunk.byteLength, estimatedCommittedBytes: chunk.byteLength };
  }

  if (touchMode === "random") {
    const maxRandomBytesPerCall = 65_536;
    for (let offset = 0; offset < chunk.byteLength; offset += maxRandomBytesPerCall) {
      crypto.getRandomValues(chunk.subarray(offset, offset + maxRandomBytesPerCall));
    }
    return { touchedBytes: chunk.byteLength, estimatedCommittedBytes: chunk.byteLength };
  }

  if (touchMode === "pages") {
    let writes = 0;
    for (let offset = 0; offset < chunk.byteLength; offset += DEFAULT_PAGE_BYTES) {
      chunk[offset] = 0xa5;
      writes += 1;
    }
    return { touchedBytes: writes, estimatedCommittedBytes: chunk.byteLength };
  }

  throw new RangeError(`unknown touch mode: ${touchMode satisfies never}`);
}

// Experiment-only: retained by the top-level Worker isolate so we can compare
// stateless Worker memory behavior with Durable Object instance memory behavior.
let workerMemoryChunks: MemoryChunk[] = [];

/** Minimal DO for probing kill / crash behaviour (abort, OOM, etc.). */
export class DebugDurableObject extends DurableObject {
  private memoryChunks: MemoryChunk[] = [];

  async ping(args?: { timeoutMs?: number }): Promise<{
    message: "pong";
    at: string;
    heldBytes: number;
  }> {
    if (args?.timeoutMs !== undefined && args.timeoutMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, args.timeoutMs));
    }
    return {
      message: "pong",
      at: new Date().toISOString(),
      heldBytes: sumChunkBytes(this.memoryChunks),
    };
  }

  consumeMemory(args: { targetBytes: number; chunkBytes?: number; touchMode?: MemoryTouchMode }): MemoryAllocationResult {
    return accumulateMemory({ ...args, into: this.memoryChunks });
  }

  releaseMemory(): { freedBytes: number } {
    const freedBytes = sumChunkBytes(this.memoryChunks);
    this.memoryChunks = [];
    return { freedBytes };
  }

  /**
   * Alternating consume → release for `cycles` rounds. Peak heap stays at `bytesPerCycle`
   * (not cumulative). Throws if held bytes are wrong after any alloc or release.
   */
  cycleMemory(args: {
    bytesPerCycle: number;
    cycles: number;
    chunkBytes?: number;
    touchMode?: MemoryTouchMode;
  }): {
    cycles: number;
    bytesPerCycle: number;
    totalTouchedBytes: number;
    totalEstimatedCommittedBytes: number;
    finalHeldBytes: number;
    touchMode: MemoryTouchMode;
  } {
    if (!Number.isInteger(args.cycles) || args.cycles <= 0) {
      throw new RangeError("cycles must be a positive integer");
    }
    this.memoryChunks = [];
    let totalTouchedBytes = 0;
    let totalEstimatedCommittedBytes = 0;
    for (let i = 0; i < args.cycles; i++) {
      const { totalHeldBytes, touchedBytes, estimatedCommittedBytes } = accumulateMemory({
        targetBytes: args.bytesPerCycle,
        chunkBytes: args.chunkBytes,
        touchMode: args.touchMode,
        into: this.memoryChunks,
      });
      totalTouchedBytes += touchedBytes;
      totalEstimatedCommittedBytes += estimatedCommittedBytes;
      if (totalHeldBytes !== args.bytesPerCycle) {
        throw new Error(
          `cycle ${i + 1}/${args.cycles}: expected ${args.bytesPerCycle} held after alloc, got ${totalHeldBytes}`,
        );
      }
      this.memoryChunks = [];
      const afterRelease = sumChunkBytes(this.memoryChunks);
      if (afterRelease !== 0) {
        throw new Error(
          `cycle ${i + 1}/${args.cycles}: expected 0 held after release, got ${afterRelease}`,
        );
      }
    }
    return {
      cycles: args.cycles,
      bytesPerCycle: args.bytesPerCycle,
      totalTouchedBytes,
      totalEstimatedCommittedBytes,
      finalHeldBytes: sumChunkBytes(this.memoryChunks),
      touchMode: args.touchMode ?? "none",
    };
  }

  kill(args?: { reason?: string }): never {
    this.ctx.abort(args?.reason ?? "kill requested");
    throw new Error("unreachable");
  }

  throwError(args?: { message?: string }): never {
    throw new Error(args?.message ?? "deliberate uncaught error");
  }

  burnCpu(args?: { ms?: number }): { burnedMs: number } {
    const targetMs = args?.ms ?? 60_000;
    const start = Date.now();
    while (Date.now() - start < targetMs) {
      // spin
    }
    return { burnedMs: Date.now() - start };
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

    if (url.pathname === "/worker-ping") {
      return Response.json({
        message: "pong",
        at: new Date().toISOString(),
        heldBytes: sumChunkBytes(workerMemoryChunks),
        chunkCount: workerMemoryChunks.length,
      });
    }

    if (url.pathname === "/worker-memory" && request.method === "POST") {
      const parsed = parseMemoryParams(url);
      if (parsed instanceof Response) return parsed;
      if (url.searchParams.get("reset") === "1") {
        workerMemoryChunks = [];
      }
      const result = accumulateMemory({ ...parsed, into: workerMemoryChunks });
      return Response.json({ ...result, scope: "worker" });
    }

    if (url.pathname === "/worker-memory" && request.method === "DELETE") {
      const freedBytes = sumChunkBytes(workerMemoryChunks);
      const chunkCount = workerMemoryChunks.length;
      workerMemoryChunks = [];
      return Response.json({ scope: "worker", freedBytes, chunkCount });
    }

    if (url.pathname === "/ping") {
      const raw = url.searchParams.get("timeoutMs");
      const timeoutMs = raw === null ? undefined : Number(raw);
      const result = await stub.ping({ timeoutMs });
      return Response.json(result);
    }

    if (url.pathname === "/kill" && request.method === "POST") {
      const reason = url.searchParams.get("reason") ?? undefined;
      await stub.kill({ reason });
      return new Response("kill returned without aborting", { status: 500 });
    }

    if (url.pathname === "/memory" && request.method === "POST") {
      const parsed = parseMemoryParams(url);
      if (parsed instanceof Response) return parsed;
      const result = await stub.consumeMemory(parsed);
      return Response.json(result);
    }

    if (url.pathname === "/memory" && request.method === "DELETE") {
      const result = await stub.releaseMemory();
      return Response.json(result);
    }

    if (url.pathname === "/memory/cycle" && request.method === "POST") {
      const bytesParam = url.searchParams.get("bytes");
      const cyclesParam = url.searchParams.get("cycles");
      if (bytesParam === null) {
        return new Response("bytes query param is required", { status: 400 });
      }
      if (cyclesParam === null) {
        return new Response("cycles query param is required", { status: 400 });
      }
      const bytesPerCycle = Number(bytesParam);
      const cycles = Number(cyclesParam);
      if (!Number.isInteger(bytesPerCycle) || bytesPerCycle <= 0) {
        return new Response("bytes must be a positive integer", { status: 400 });
      }
      if (!Number.isInteger(cycles) || cycles <= 0) {
        return new Response("cycles must be a positive integer", { status: 400 });
      }
      const chunkParam = url.searchParams.get("chunkBytes");
      let chunkBytes: number | undefined;
      if (chunkParam !== null) {
        chunkBytes = Number(chunkParam);
        if (!Number.isInteger(chunkBytes) || chunkBytes <= 0) {
          return new Response("chunkBytes must be a positive integer", { status: 400 });
        }
      }
      const touchMode = parseTouchMode(url.searchParams.get("touch"));
      if (touchMode instanceof Response) return touchMode;
      const result = await stub.cycleMemory({ bytesPerCycle, cycles, chunkBytes, touchMode });
      return Response.json(result);
    }

    if (url.pathname === "/throw" && request.method === "POST") {
      const message = url.searchParams.get("message") ?? undefined;
      await stub.throwError({ message });
      return new Response("throw returned without error", { status: 500 });
    }

    if (url.pathname === "/burn-cpu" && request.method === "POST") {
      const msParam = url.searchParams.get("ms");
      let ms = 60_000;
      if (msParam !== null) {
        ms = Number(msParam);
        if (!Number.isInteger(ms) || ms <= 0) {
          return new Response("ms must be a positive integer", { status: 400 });
        }
      }
      const result = await stub.burnCpu({ ms });
      return Response.json(result);
    }

    return new Response(
      [
        "03-kill-durable-object",
        "",
        "GET    /ping?name=default&timeoutMs=1000",
        "GET    /worker-ping",
        "POST   /worker-memory?bytes=67108864&chunkBytes=1048576&touch=fill&reset=1",
        "DELETE /worker-memory",
        "POST   /kill?name=default&reason=experiment",
        "POST   /memory?name=default&bytes=67108864&chunkBytes=1048576&touch=fill",
        "DELETE /memory?name=default",
        "POST   /memory/cycle?name=default&bytes=67108864&cycles=10&touch=fill",
        "POST   /throw?name=default&message=deliberate",
        "POST   /burn-cpu?name=default&ms=60000",
      ].join("\n"),
      { headers: { "content-type": "text/plain; charset=utf-8" } },
    );
  },
} satisfies ExportedHandler<Env>;

function parseMemoryParams(
  url: URL,
): { targetBytes: number; chunkBytes?: number; touchMode: MemoryTouchMode } | Response {
  const bytesParam = url.searchParams.get("bytes");
  if (bytesParam === null) {
    return new Response("bytes query param is required", { status: 400 });
  }
  const targetBytes = Number(bytesParam);
  if (!Number.isInteger(targetBytes) || targetBytes < 0) {
    return new Response("bytes must be a non-negative integer", { status: 400 });
  }

  const chunkParam = url.searchParams.get("chunkBytes");
  let chunkBytes: number | undefined;
  if (chunkParam !== null) {
    chunkBytes = Number(chunkParam);
    if (!Number.isInteger(chunkBytes) || chunkBytes <= 0) {
      return new Response("chunkBytes must be a positive integer", { status: 400 });
    }
  }

  const touchMode = parseTouchMode(url.searchParams.get("touch"));
  if (touchMode instanceof Response) return touchMode;
  return { targetBytes, chunkBytes, touchMode };
}

function parseTouchMode(raw: string | null): MemoryTouchMode | Response {
  if (raw === null || raw === "none") return "none";
  if (raw === "pages" || raw === "fill" || raw === "random") return raw;
  return new Response("touch must be one of: none, pages, fill, random", { status: 400 });
}
