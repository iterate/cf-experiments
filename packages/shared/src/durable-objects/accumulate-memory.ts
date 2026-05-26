/** Backing store for retained allocations (pass to `accumulateMemory` to keep memory live). */
export type MemoryChunk = Uint8Array;

const DEFAULT_CHUNK_BYTES = 1024 * 1024;
const DEFAULT_PAGE_BYTES = 4096;

export type MemoryTouchMode = "none" | "pages" | "fill" | "random";

export interface MemoryAllocationResult {
  /** Backwards-compatible alias for `logicalAllocatedBytes`. */
  allocatedBytes: number;
  /** New Uint8Array byteLength allocated by this call. */
  logicalAllocatedBytes: number;
  /** Actual byte writes performed to force backing memory to materialize. */
  touchedBytes: number;
  /**
   * Best-effort estimate of bytes whose backing pages were forced to exist.
   * This is still not a Cloudflare/V8 heap measurement; it is derived from our touch mode.
   */
  estimatedCommittedBytes: number;
  chunkCount: number;
  /** Backwards-compatible alias for `totalLogicalHeldBytes`. */
  totalHeldBytes: number;
  /** Sum of byteLength for all retained chunks after this call. */
  totalLogicalHeldBytes: number;
  touchMode: MemoryTouchMode;
}

export function sumChunkBytes(chunks: readonly MemoryChunk[]): number {
  let total = 0;
  for (const chunk of chunks) total += chunk.byteLength;
  return total;
}

/**
 * Allocate up to `targetBytes` of heap memory in fixed-size chunks.
 * Chunks are appended to `into` so callers can retain them on `this` and avoid GC.
 */
export function accumulateMemory(args: {
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
    return {
      touchedBytes: chunk.byteLength,
      estimatedCommittedBytes: chunk.byteLength,
    };
  }

  if (touchMode === "random") {
    fillRandom(chunk);
    return {
      touchedBytes: chunk.byteLength,
      estimatedCommittedBytes: chunk.byteLength,
    };
  }

  if (touchMode === "pages") {
    let writes = 0;
    for (let offset = 0; offset < chunk.byteLength; offset += DEFAULT_PAGE_BYTES) {
      chunk[offset] = 0xa5;
      writes += 1;
    }

    return {
      touchedBytes: writes,
      estimatedCommittedBytes: chunk.byteLength,
    };
  }

  throw new RangeError(`unknown touch mode: ${touchMode satisfies never}`);
}

function fillRandom(chunk: Uint8Array): void {
  const maxRandomBytesPerCall = 65_536;
  for (let offset = 0; offset < chunk.byteLength; offset += maxRandomBytesPerCall) {
    crypto.getRandomValues(chunk.subarray(offset, offset + maxRandomBytesPerCall));
  }
}
