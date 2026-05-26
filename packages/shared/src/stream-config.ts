/**
 * Per-stream Durable Object settings persisted in sync KV (`ctx.storage.kv`).
 *
 * `ctx.storage.kv` is the synchronous KV API for SQLite-backed Durable Objects:
 * https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#synchronous-kv-api
 */
import { z } from "zod";

import type { StreamEventSyncKv } from "./event.js";

export const STREAM_DO_SETTINGS_KV_KEY = "stream:settings";

export const StreamDoSettings = z.object({
  /**
   * Maximum number of locally issued `allowUnconfirmed` appends to allow between explicit
   * durability checkpoints.
   *
   * - `null`: never explicitly call `ctx.storage.sync()` (maximum throughput / "yolo" mode).
   * - `0`: call `ctx.storage.sync()` after every append (no append RPC returns until previous
   *   pending writes are confirmed).
   * - `N`: call `ctx.storage.sync()` after N appends since the last explicit checkpoint.
   *
   * This is deliberately named as a bound on *our* unconfirmed-write window, not Cloudflare's
   * internal pending-write buffer. The platform may flush/coalesce writes independently. The
   * guarantee we get from `sync()` is that pending writes already submitted, including writes made
   * with `allowUnconfirmed`, have persisted when the promise resolves:
   * https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#sync
   */
  maxUnconfirmedWrites: z.number().int().nonnegative().nullable().default(null),
});

export type StreamDoSettings = z.infer<typeof StreamDoSettings>;

export const streamDoSettingsDefaults = (): StreamDoSettings => StreamDoSettings.parse({});

export function readStreamDoSettingsFromKv({ kv }: { kv: StreamEventSyncKv }): StreamDoSettings {
  const raw = kv.get(STREAM_DO_SETTINGS_KV_KEY);
  return StreamDoSettings.parse(raw ?? {});
}

export function writeStreamDoSettingsToKv(args: {
  kv: StreamEventSyncKv;
  settings: Partial<z.input<typeof StreamDoSettings>>;
}): StreamDoSettings {
  const next = StreamDoSettings.parse({
    ...readStreamDoSettingsFromKv({ kv: args.kv }),
    ...args.settings,
  });
  args.kv.put(STREAM_DO_SETTINGS_KV_KEY, next);
  return next;
}
