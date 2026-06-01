import { newWorkersRpcResponse } from "capnweb";
import { DurableObject, RpcTarget, WorkerEntrypoint } from "cloudflare:workers";

type GreeterProps = {
  label: string;
};

type CounterProps = {
  name: string;
};

export class PingDurableObject extends DurableObject {
  private readonly incarnationId = crypto.randomUUID();

  ping(name?: string) {
    return {
      message: "pong" as const,
      ...(name === undefined ? {} : { name }),
      incarnationId: this.incarnationId,
    };
  }
}

export class CounterEntrypoint extends WorkerEntrypoint<Env, CounterProps> {
  bump() {
    return { name: this.ctx.props.name, count: 1 };
  }
}

type StreamEntrypointProps = {
  path: string;
};

export class StreamEntrypoint extends WorkerEntrypoint<Env, StreamEntrypointProps> {
  ping() {
    return { path: this.ctx.props.path, message: "pong" as const };
  }
}

/** get(path) → DO stub (DurableObject serialization error) */
export class StreamsDoEntrypoint extends WorkerEntrypoint<Env> {
  get(path: string) {
    return this.env.STREAMS.getByName(path);
  }
}

/** get(path) → ctx.exports.StreamEntrypoint (ServiceStub / experimental flag error) */
export class StreamsEntrypoint extends WorkerEntrypoint<Env> {
  get(path: string) {
    return this.ctx.exports.StreamEntrypoint({ props: { path } });
  }
}

export class GreeterEntrypoint extends WorkerEntrypoint<Env, GreeterProps> {
  ping() {
    return { label: this.ctx.props.label, message: "pong" };
  }

  getCapability(name: string) {
    return this.ctx.exports.CounterEntrypoint({ props: { name } });
  }

  getDo(name: string) {
    return this.env.PING_DO.getByName(name);
  }

  getUpstream() {
    return this.env.UPSTREAM;
  }
}

class EntrypointRelayRpcTarget extends RpcTarget {
  constructor(private readonly exports: ExecutionContext["exports"]) {
    super();
  }

  createGreeter(label: string) {
    return this.exports.GreeterEntrypoint({ props: { label } });
  }

  callPing(greeter: GreeterEntrypoint) {
    return greeter.ping();
  }
}

class DoRelayRpcTarget extends RpcTarget {
  constructor(private readonly pingDo: DurableObjectNamespace<PingDurableObject>) {
    super();
  }

  getDo(name: string) {
    return this.pingDo.getByName(name);
  }

  callPing(stub: DurableObjectStub<PingDurableObject>, name: string) {
    return stub.ping(name);
  }
}

type EchoService = {
  echo(label: string): { label: string; origin: string; message: string };
};

class ServiceRelayRpcTarget extends RpcTarget {
  constructor(private readonly upstream: EchoService) {
    super();
  }

  getUpstream() {
    return this.upstream;
  }

  callEcho(stub: EchoService, label: string) {
    return stub.echo(label);
  }
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/relay") {
      return newWorkersRpcResponse(request, new EntrypointRelayRpcTarget(ctx.exports));
    }

    if (url.pathname === "/do-relay") {
      return newWorkersRpcResponse(request, new DoRelayRpcTarget(env.PING_DO));
    }

    if (url.pathname === "/service-relay") {
      return newWorkersRpcResponse(request, new ServiceRelayRpcTarget(env.UPSTREAM as unknown as EchoService));
    }

    if (url.pathname === "/streams-do") {
      return newWorkersRpcResponse(request, ctx.exports.StreamsDoEntrypoint({}));
    }

    if (url.pathname === "/streams") {
      return newWorkersRpcResponse(request, ctx.exports.StreamsEntrypoint({}));
    }

    if (url.pathname === "/entrypoint") {
      const label = url.searchParams.get("label") ?? "default";
      return newWorkersRpcResponse(
        request,
        ctx.exports.GreeterEntrypoint({ props: { label } }),
      );
    }

    if (url.pathname === "/do-stub") {
      const name = url.searchParams.get("name") ?? "default";
      return newWorkersRpcResponse(request, env.PING_DO.getByName(name));
    }

    if (url.pathname === "/service-stub") {
      return newWorkersRpcResponse(request, env.UPSTREAM as unknown as EchoService);
    }

    return new Response(
      [
        "05-capnweb-entrypoint-pass",
        "",
        "POST /streams                  - streams.get(path) → ctx.exports.StreamEntrypoint (ServiceStub error)",
        "POST /streams-do               - streams.get(path) → env.STREAMS.getByName (DurableObject error)",
        "POST /entrypoint?label=X     - Cap'n Web root: ctx.exports.GreeterEntrypoint",
        "POST /relay                  - RpcTarget entrypoint return / pass-back",
        "POST /do-stub?name=X         - Cap'n Web root: PING_DO.getByName(name)",
        "POST /do-relay               - RpcTarget DO stub return / pass-back",
        "POST /service-stub           - Cap'n Web root: UPSTREAM service binding",
        "POST /service-relay          - RpcTarget service binding return / pass-back",
        "",
        "GreeterEntrypoint also exposes getCapability / getDo / getUpstream for nested-return probes",
      ].join("\n"),
      { headers: { "content-type": "text/plain; charset=utf-8" } },
    );
  },
} satisfies ExportedHandler<Env>;
