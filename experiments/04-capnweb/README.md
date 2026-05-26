# 04-capnweb

Focused Cap'n Web / Workers RPC interop experiment.

The main shape under test is:

```ts
await project.streams.get("/some/path").append(event);
```

Goals:

- Use `RpcTarget` subclasses for the Cap'n Web object graph.
- Keep `WorkerEntrypoint` classes as thin adapters for Dynamic Worker-style bindings.
- Verify Cap'n Web promise pipelining over WebSocket by inspecting frames.

## Routes

| Route                                       | Meaning                                                                   |
| ------------------------------------------- | ------------------------------------------------------------------------- |
| `POST /capnweb-project` / WebSocket upgrade | Cap'n Web session with `ProjectRpcTarget` as main object                  |
| `POST /worker-project` / WebSocket upgrade  | Cap'n Web session with `ctx.exports.ProjectCapability({})` as main object |
| `POST /tools` / WebSocket upgrade           | Cap'n Web session with generic SDK-shaped tool providers                  |
| `GET /dynamic-tools`                        | Dynamic Worker calling the same SDK-shaped bindings through `env`         |
| `GET /count?path=/some/path`                | JSON count for one in-memory stream                                       |

`/worker-project` intentionally exercises the `ctx.exports` service-stub path. Returning nested
`ctx.exports` service stubs through Cap'n Web may require runtime support that is not enabled by
default; `/capnweb-project` avoids that by using plain `RpcTarget` instances.

## Run

```bash
pnpm dev
pnpm test:pipelining
pnpm test:tools
```

By default the pipelining test targets `http://localhost:8787`. Override with:

```bash
WORKER_URL=https://04-capnweb.iterate-dev-preview.workers.dev pnpm test:pipelining
```

# What we want to get to

We want to write identical code in multiple places

1. vitest e2e tests
2. codemode snippets
3.

These cases are all similar in that

- We're operating in the context of a project
- We have some data structure representing available tool providers

## Tool Provider Harness

`/tools` starts testing the capability-provider shape. The Worker builds a fresh Cap'n Web
session with a provider registry and exposes each provider as a lazily descended SDK proxy.
The server uses fake Slack and GitHub implementations so the Worker does not need Node.js
compatibility or SDK runtime dependencies. The client/test side imports `@slack/web-api`
and `@octokit/rest` types only, so calls are checked against the real SDK surfaces.

The important client-side shape is:

```ts
await api.slack.chat.postMessage({ channel: "C1", text: "hi from capnweb" });
await api.slack.users.profile.get({ user: "U1" });
await api.github.repos.get({ owner: "anthropics", repo: "claude-code" });
```

`pnpm test:tools` connects to `/tools`, makes those nested SDK calls, and asserts that the
outbound Cap'n Web frames contain the full provider path in one pipelined operation.

`/dynamic-tools` follows the Dynamic Workers binding model from Cloudflare's docs: the loader
Worker creates scoped stubs with `ctx.exports`, passes them into `env` via `env.LOADER.load`,
and the Dynamic Worker calls `env.slack.chat.postMessage(...)` and
`env.github.repos.get(...)`. Slack uses the generic proxy-backed provider shape; GitHub is an
explicit `RpcTarget`, which keeps the experiment honest that not every capability has to be an
SDK proxy.
