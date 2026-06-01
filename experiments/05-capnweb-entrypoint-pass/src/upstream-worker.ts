import { WorkerEntrypoint } from "cloudflare:workers";

type EchoProps = {
  origin: string;
};

export class EchoEntrypoint extends WorkerEntrypoint<Env, EchoProps> {
  echo(label: string) {
    return { label, origin: this.ctx.props.origin, message: "from-upstream" };
  }
}

export default {
  fetch() {
    return new Response("05-capnweb-entrypoint-pass-upstream", {
      headers: { "content-type": "text/plain; charset=utf-8" },
    });
  },
} satisfies ExportedHandler<Env>;
