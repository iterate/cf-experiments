import { Stream } from "./stream.js";
import { StreamProcessorRunner } from "./stream-processor-runner.js";

export { Stream, StreamProcessorRunner };

export default {
  fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname.startsWith("/stream-processor-runner/")) {
      const name = decodeURIComponent(url.pathname.slice("/stream-processor-runner/".length));
      return env.STREAM_PROCESSOR_RUNNER.getByName(name).fetch(request);
    }

    if (url.pathname.startsWith("/stream/")) {
      const path = decodeURIComponent(url.pathname.slice("/stream/".length));
      return env.STREAM.getByName(`stream:${path}`).fetch(request);
    }

    return new Response("Use /stream/:path or /stream-processor-runner/:name", { status: 404 });
  },
} satisfies ExportedHandler<Env>;
