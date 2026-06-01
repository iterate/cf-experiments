# 05-capnweb-entrypoint-pass

Can we pass Workers RPC stubs (`WorkerEntrypoint`, Durable Object, service binding) across a Cap'n Web boundary?

Cap'n Web [documents interoperability](https://www.npmjs.com/package/capnweb#cloudflare-workers-rpc-interoperability) with Workers RPC. This experiment checks that for each stub type.

## Workers

| Worker | Config | Role |
| --- | --- | --- |
| `05-capnweb-entrypoint-pass` | `wrangler.jsonc` | Main — Cap'n Web sessions, DO, service binding relay |
| `05-capnweb-entrypoint-pass-upstream` | `wrangler.upstream.jsonc` | Upstream — `EchoEntrypoint` bound as `env.UPSTREAM` |

## Cases

### WorkerEntrypoint (`ctx.exports`)

| Case | Route |
| --- | --- |
| Session root | `POST /entrypoint?label=X` |
| RpcTarget return / pass-back | `POST /relay` |
| Nested from entrypoint method | `GreeterEntrypoint.getCapability()` |

### Durable Object stub (`env.PING_DO.getByName`)

| Case | Route |
| --- | --- |
| Session root | `POST /do-stub?name=X` |
| RpcTarget return / pass-back | `POST /do-relay` |
| Nested from entrypoint method | `GreeterEntrypoint.getDo()` |

### Service binding stub (`env.UPSTREAM`)

| Case | Route |
| --- | --- |
| Session root | `POST /service-stub` |
| RpcTarget return / pass-back | `POST /service-relay` |
| Nested from entrypoint method | `GreeterEntrypoint.getUpstream()` |

## Run

Terminal 1 (main worker must be **first** config — it owns the HTTP port):

```bash
pnpm dev
# wrangler dev -c wrangler.jsonc -c wrangler.upstream.jsonc
```

Terminal 2:

```bash
WORKER_URL=http://localhost:8787 pnpm test
```

Deployed (deploy upstream first):

```bash
pnpm deploy
WORKER_URL=https://05-capnweb-entrypoint-pass.iterate-dev-preview.workers.dev pnpm test
WORKER_URL=https://05-capnweb-entrypoint-pass.iterate-dev-preview.workers.dev pnpm test:pipelining
```

## Evaluate

**Result (2026-05-27), default deployable config (no `experimental` flag):**

| Stub type | Cap'n Web root | RpcTarget return | RpcTarget pass-back | Nested from entrypoint method |
| --- | --- | --- | --- | --- |
| `ctx.exports` entrypoint | yes | yes (pipelines) | yes (pipelines) | **no** — needs `experimental` |
| DO stub (`getByName`) | yes | yes (pipelines) | yes (pipelines) | **no** — `DurableObject` not serializable |
| Service binding | yes | yes (pipelines) | yes (pipelines) | **no** — needs `experimental` |

All three stub types work as Cap'n Web session roots and can be returned from or passed back through plain `RpcTarget` methods — same shape as the direct entrypoint cases.

Nested return through a `WorkerEntrypoint` method (the Dynamic Worker / `getCapability()` pattern) fails without `compatibility_flags: ["experimental"]`. That flag works in Miniflare but **cannot be deployed** to Cloudflare (API error 10021).

**Pipelining:** RpcTarget-mediated chains batch into one round trip (`scripts/pipelining.test.ts`). Nested entrypoint methods still emit `pipeline` frames client-side but `reject` when the inner stub can't serialize.

See [log.md](./log.md).
