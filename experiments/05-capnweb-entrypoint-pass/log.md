# log

## Findings

**Direct stub pass (root / RpcTarget return / pass-back): yes for all three stub types** — confirmed Miniflare + `iterate-dev-preview`.

| Stub | Root | RpcTarget return | Pass-back | Nested from entrypoint method |
| --- | --- | --- | --- | --- |
| `ctx.exports` entrypoint | yes | yes | yes | no (needs `experimental`) |
| DO `getByName` stub | yes | yes | yes | no (`DurableObject` not serializable) |
| Service binding | yes | yes | yes | no (needs `experimental`) |

Nested return through `WorkerEntrypoint` methods (`getCapability`, `getDo`, `getUpstream`) fails on the default deployable config. RpcTarget-mediated return/pass-back works fine — the failure is specific to returning platform stubs from inside another entrypoint's RPC method.

`compatibility_flags: ["experimental"]` enables nested entrypoint + service binding locally; deploy rejected (API 10021).

## 2026-05-27 — first runs

Miniflare (`http://localhost:8788`) and deployed (`05-capnweb-entrypoint-pass.iterate-dev-preview.workers.dev`): all three vitest cases green.

## 2026-05-27 — DO + service binding stubs

Added `PingDurableObject`, upstream worker (`wrangler.upstream.jsonc` + `src/upstream-worker.ts`), service binding to `EchoEntrypoint`.

Local dev: `pnpm dev` runs `-c wrangler.jsonc -c wrangler.upstream.jsonc` (main **first** — owns HTTP port; upstream via service binding).

Production: all 12 vitest cases green on `05-capnweb-entrypoint-pass.iterate-dev-preview.workers.dev`.

Nested DO via `getDo()` fails with capnweb `Could not serialize object of type "DurableObject"` (distinct from workerd's `DurableObjectClass serialization requires...` message for namespace channels).

## 2026-05-27 — pipelining wire tests

Added `scripts/pipelining.test.ts` (9 cases). All RpcTarget-mediated chains pipeline in one round trip (`pipeline` 0 → `pipeline` 1 → `pull` 2 → `resolve`/`reject` 2). Pass-back embeds `[["pipeline", 1], ...]` inside the second call's args. Nested entrypoint methods still pipeline client-side but `reject` on stub return.

Added `GreeterEntrypoint.getCapability(name)` → `ctx.exports.CounterEntrypoint({ props: { name } })`.

- Default config: fails with `ServiceStub serialization requires the 'experimental' compat flag` (workerd `Fetcher::serialize`).
- With `compatibility_flags: ["experimental"]` in wrangler.jsonc: passes in Miniflare.
- Deploy with that flag: Cloudflare API 10021 — flag not allowed on deployed Workers yet.
