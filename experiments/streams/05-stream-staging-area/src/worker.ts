import handler, { createServerEntry } from "@tanstack/react-start/server-entry";
import { env } from "cloudflare:workers";

export { Stream } from "./stream.js";
export { StreamProcessorRunner } from "./stream-processor-runner.js";

export default createServerEntry({
  fetch(request) {
    const url = new URL(request.url);

    if (url.pathname.startsWith("/stream-processor-runner/")) {
      const name = decodeURIComponent(url.pathname.slice("/stream-processor-runner/".length));
      return env.STREAM_PROCESSOR_RUNNER.getByName(name).fetch(request);
    }

    if (url.pathname.startsWith("/stream/")) {
      const path = decodeURIComponent(url.pathname.slice("/stream/".length));
      return env.STREAM.getByName(`stream:${path}`).fetch(request);
    }

    return handler.fetch(request);
  },
});
