import { BenchmarkRunner } from "./benchmark-runner.js";
import { Stream } from "./stream.js";

export { BenchmarkRunner, Stream };

export default {
  async fetch(request, env) {
    const name = new URL(request.url).pathname.slice(1) || "default";
    return env.STREAM.getByName(name).fetch(request);
  },
} satisfies ExportedHandler<Env>;
