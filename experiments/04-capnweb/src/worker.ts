import { newWorkersRpcResponse } from "capnweb";
import { RpcTarget, WorkerEntrypoint } from "cloudflare:workers";

type StreamEventInput = {
  type: string;
  payload?: unknown;
  metadata?: Record<string, unknown>;
};

type StreamEvent = StreamEventInput & {
  offset: number;
  createdAt: string;
};

const streams = new Map<string, StreamEvent[]>();

function eventsFor(path: string) {
  let events = streams.get(path);
  if (!events) {
    events = [];
    streams.set(path, events);
  }
  return events;
}

class StreamRpcTarget extends RpcTarget {
  constructor(private readonly path: string) {
    super();
  }

  append(event: StreamEventInput): StreamEvent {
    const events = eventsFor(this.path);
    const committed = {
      ...event,
      offset: events.length + 1,
      createdAt: new Date().toISOString(),
    };
    events.push(committed);
    return committed;
  }

  count() {
    return { path: this.path, count: eventsFor(this.path).length };
  }
}

class StreamsRpcTarget extends RpcTarget {
  get(path: string) {
    return new StreamRpcTarget(path);
  }
}

class ProjectRpcTarget extends RpcTarget {
  get streams() {
    return new StreamsRpcTarget();
  }
}

interface Provider<Client> {
  client: Client;
  invoke: (path: readonly string[], args: readonly unknown[]) => unknown;
}

type Providers = Record<string, Provider<unknown>>;
type ProviderClients<P extends Providers> = { [K in keyof P]: P[K]["client"] };

function createFakeSlackClient() {
  return {
    chat: {
      postMessage(options: Record<string, unknown>) {
        return {
          ok: true,
          channel: options.channel,
          ts: "1700000000.000100",
          message: { text: options.text, user: "U_BOT" },
        };
      },
    },
    users: {
      profile: {
        get() {
          return {
            ok: true,
            profile: { real_name: "Ada Lovelace", email: "ada@example.com" },
          };
        },
      },
    },
  };
}

function fakeGithubReposGet(options: Record<string, unknown>) {
  return {
    status: 200,
    data: {
      full_name: `${options.owner}/${options.repo}`,
      private: false,
    },
  };
}

function makeProviderProxy(
  invoke: Provider<unknown>["invoke"],
  path: readonly string[] = [],
): unknown {
  const shim = function () {};
  Object.setPrototypeOf(shim, RpcTarget.prototype);

  return new Proxy(shim, {
    get(_target, key) {
      if (typeof key === "symbol") return undefined;
      return makeProviderProxy(invoke, [...path, key]);
    },
    apply(_target, _thisArg, args) {
      return invoke(path, args);
    },
  });
}

function forwardToSdk(client: object) {
  return (path: readonly string[], args: readonly unknown[]): unknown => {
    let parent: unknown = client;
    let target: unknown = client;

    for (const key of path) {
      parent = target;
      target = (target as Record<string, unknown> | null | undefined)?.[key];
    }

    if (typeof target !== "function") {
      throw new TypeError(`SDK path is not callable: ${path.join(".") || "<root>"}`);
    }

    return Reflect.apply(target as (...args: unknown[]) => unknown, parent, args);
  };
}

function createToolSession<P extends Providers>(providers: P) {
  class ToolSession extends RpcTarget {}

  for (const name of Object.keys(providers)) {
    Object.defineProperty(ToolSession.prototype, name, {
      get() {
        return makeProviderProxy(providers[name]!.invoke);
      },
      enumerable: true,
      configurable: true,
    });
  }

  return new ToolSession() as ToolSession & ProviderClients<P>;
}

function buildProviders() {
  const slack = createFakeSlackClient();

  const github = {
    repos: {
      get: fakeGithubReposGet,
    },
  };

  return {
    slack: { client: slack, invoke: forwardToSdk(slack) },
    github: { client: github, invoke: forwardToSdk(github) },
  } satisfies Providers;
}

export class SlackCapability extends WorkerEntrypoint<Env> {
  get chat() {
    return makeProviderProxy(forwardToSdk(createFakeSlackClient()), ["chat"]);
  }

  get users() {
    return makeProviderProxy(forwardToSdk(createFakeSlackClient()), ["users"]);
  }
}

class GithubReposRpcTarget extends RpcTarget {
  get(options: Record<string, unknown>) {
    return fakeGithubReposGet(options);
  }
}

export class GithubCapability extends WorkerEntrypoint<Env> {
  get repos() {
    return new GithubReposRpcTarget();
  }
}

type StreamCapabilityProps = {
  path: string;
};

export class StreamCapability extends WorkerEntrypoint<Env, StreamCapabilityProps> {
  append(event: StreamEventInput) {
    return new StreamRpcTarget(this.ctx.props.path).append(event);
  }

  count() {
    return new StreamRpcTarget(this.ctx.props.path).count();
  }
}

export class StreamsCapability extends WorkerEntrypoint<Env> {
  get(path: string) {
    return new StreamRpcTarget(path);
  }
}

export class ProjectCapability extends WorkerEntrypoint<Env> {
  get streams() {
    return new StreamsRpcTarget();
  }
}

export default {
  async fetch(request, _env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/capnweb-project") {
      return newWorkersRpcResponse(request, new ProjectRpcTarget());
    }

    if (url.pathname === "/worker-project") {
      return newWorkersRpcResponse(request, ctx.exports.ProjectCapability({}));
    }

    if (url.pathname === "/tools") {
      return newWorkersRpcResponse(request, createToolSession(buildProviders()));
    }

    if (url.pathname === "/dynamic-tools") {
      const worker = _env.LOADER.load({
        compatibilityDate: "2026-05-01",
        mainModule: "index.js",
        modules: {
          "index.js": `
            export default {
              async fetch(_request, env) {
                const post = await env.slack.chat.postMessage({
                  channel: "C1",
                  text: "hi from a dynamic worker",
                });
                const profile = await env.slack.users.profile.get({ user: "U1" });
                const repo = await env.github.repos.get({
                  owner: "anthropics",
                  repo: "claude-code",
                });

                return Response.json({ post, profile, repo });
              },
            };
          `,
        },
        env: {
          slack: ctx.exports.SlackCapability({}),
          github: ctx.exports.GithubCapability({}),
        },
        globalOutbound: null,
      });

      return worker.getEntrypoint().fetch(request);
    }

    if (url.pathname === "/count") {
      const path = url.searchParams.get("path") ?? "/default";
      return Response.json(new StreamRpcTarget(path).count());
    }

    return new Response(
      [
        "04-capnweb",
        "",
        "POST /capnweb-project        - Cap'n Web RPC with RpcTarget project main",
        "POST /worker-project         - Cap'n Web RPC with ctx.exports project main",
        "POST /tools                  - Cap'n Web RPC with generic SDK tool providers",
        "GET  /dynamic-tools          - Dynamic Worker calling env slack/github tool bindings",
        "GET  /count?path=/some/path  - Count in-memory stream events",
      ].join("\n"),
      { headers: { "content-type": "text/plain; charset=utf-8" } },
    );
  },
} satisfies ExportedHandler<Env>;
