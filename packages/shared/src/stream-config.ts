/**
 * Per-stream Durable Object settings persisted in sync KV (`ctx.storage.kv`).
 *
 * `ctx.storage.kv` is the synchronous KV API for SQLite-backed Durable Objects:
 * https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#synchronous-kv-api
 */
import { z } from "zod";

import type { StreamEventSyncKv } from "./event.js";

export const STREAM_DO_SETTINGS_KV_KEY = "stream:settings";

export const AppendDurabilityMode = z.enum(["confirmed", "best-effort", "checkpointed"]);
export type AppendDurabilityMode = z.infer<typeof AppendDurabilityMode>;

export const StreamDoSettings = z.object({
  /**
   * Default acknowledgement/durability contract for `Stream.append()`.
   *
   * - `confirmed`: writes use normal Durable Object output-gate semantics. `append()` still returns
   *   synchronously to DO code, but RPC/WebSocket bytes that expose the offset can be held by the
   *   platform until the write is confirmed durable.
   * - `best-effort`: writes use `allowUnconfirmed: true`; `append()` exposes the offset as fast as
   *   possible, and durability is only eventual/best-effort until a later platform flush or explicit
   *   barrier.
   * - `checkpointed`: writes use `allowUnconfirmed: true`, but the DO periodically calls
   *   `storage.sync()` to bound the unconfirmed window. This does not mean the append that filled the
   *   window was already durable when its offset was observed.
   *
   * Cloudflare's relevant docs:
   * - output gates / `allowUnconfirmed`:
   *   https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#supported-options
   * - explicit barriers via `sync()`:
   *   https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#sync
   */
  defaultAppendDurabilityMode: AppendDurabilityMode.default("confirmed"),
  /**
   * `checkpointed` mode threshold. Once at least this many unconfirmed appends have been accepted,
   * the DO starts a `storage.sync()` checkpoint under `blockConcurrencyWhile()` so later externally
   * delivered events wait behind the barrier.
   */
  checkpointEveryUnconfirmedWrites: z.number().int().positive().default(100),
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
