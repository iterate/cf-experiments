import { StreamEventInput } from "@cf-experiments/shared/event";
import { newWebSocketRpcSession, newWorkersRpcResponse, RpcTarget } from "capnweb";
import { BenchmarkRunner, type BenchmarkMode, type RunBenchmarkArgs } from "./benchmark-runner.js";
import { Stream, StreamRpcTarget } from "./stream.js";
import { env, WorkerEntrypoint } from "cloudflare:workers";

export { BenchmarkRunner, Stream, StreamRpcTarget, Stream as SuperSimpleStream };

type StreamsRpcTargetProps = {
  namespace: string;
};

class StreamsRpcTarget extends RpcTarget {
  private readonly env: Env;
  private readonly ctx: ExecutionContext;
  private readonly props: StreamsRpcTargetProps;

  constructor(args: { env: Env; ctx: ExecutionContext; props: StreamsRpcTargetProps }) {
    super();
    this.env = args.env;
    this.ctx = args.ctx;
    this.props = args.props;
  }

  get(path: string): StreamRpcTarget {
    return new StreamRpcTarget(this.env.STREAM.getByName(`${this.props.namespace}:${path}`));
  }
}

type ProjectCapabilityProps = {
  projectId: string;
};

export class ProjectCapability extends RpcTarget {
  private readonly env: Env;
  private readonly ctx: ExecutionContext;
  private readonly props: ProjectCapabilityProps;

  constructor(args: { env: Env; ctx: ExecutionContext; props: ProjectCapabilityProps }) {
    super();
    this.env = args.env;
    this.ctx = args.ctx;
    this.props = args.props;
  }

  get streams() {
    return new StreamsRpcTarget({
      env: this.env,
      ctx: this.ctx,
      props: { namespace: this.props.projectId },
    });
  }
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const projectId = url.searchParams.get("projectId") ?? "default";
    if (request.headers.get("Upgrade")?.toLowerCase() === "websocket") {
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      server.accept();
      const main = new ProjectCapability({ env, ctx, props: { projectId } });
      newWebSocketRpcSession(server, main);
      return new Response(null, { status: 101, webSocket: client });
    }
    return new Response("This endpoint only accepts WebSocket requests.", { status: 400 });
  },
} satisfies ExportedHandler<Env>;
