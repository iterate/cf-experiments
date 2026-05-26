# 03-kv-unconfirmed-ceiling

How much faster is `writeEventFromKv` with **`allowUnconfirmedWrites: true`** vs SQL `writeEvent` vs gated KV?

Single DO (`KvWriteBench`) runs a tight in-DO loop, then `await storage.sync()` for KV modes before counting.

| Mode | Helper | Output gate |
|------|--------|-------------|
| `sql` | `writeEvent` | held per SQL write |
| `kv-gated` | `writeEventFromKv({ allowUnconfirmedWrites: false })` | held (sync `kv.put`) |
| `kv-unconfirmed` | `writeEventFromKv({ allowUnconfirmedWrites: true })` | **not held** on async `put` |

See `@cf-experiments/shared/event` for semantics.

## Run (production)

```bash
pnpm deploy
pnpm ceiling https://03-kv-unconfirmed-ceiling.iterate-dev-preview.workers.dev
```

## Routes

| Route | Purpose |
|-------|---------|
| `POST /write-loop?sync=0` | In-DO loop, no flush unless `sync=1` or `flush-every=N` |
| `POST /append` | More writes on same DO |
| `POST /flush` | Manual `storage.sync()` |
| `POST /pressure` | One-shot until max or error |

```bash
pnpm test:no-sync    # no end sync; separate /flush
pnpm test:flush      # flush-every sweep
pnpm test:pressure   # append batches until 1101
```

## Headline (production, kv-unconfirmed, no end sync)

| Scenario | Rate |
|----------|------|
| 100k × 4.8 KB, `sync=0` | **~5.2k/s** |
| 100k × 4.8 KB, `flush-every=10000` | **~7.9k/s** |
| Same DO, 50k batches without sync until crash | **~2.05M events** then 1101 |

See [log.md](./log.md).
