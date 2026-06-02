import handler, { createServerEntry } from "@tanstack/react-start/server-entry";
import { env } from "cloudflare:workers";

export { Stream } from "./stream.js";
export { StreamProcessorRunner } from "./stream-processor-runner.js";

export default createServerEntry({
  async fetch(request) {
    const url = new URL(request.url);

    if (url.pathname.startsWith("/stream-processor-runner/")) {
      const name = decodeURIComponent(url.pathname.slice("/stream-processor-runner/".length));
      return env.STREAM_PROCESSOR_RUNNER.getByName(name).fetch(request);
    }

    if (url.pathname.startsWith("/stream/")) {
      const path = decodeURIComponent(url.pathname.slice("/stream/".length));
      return env.STREAM.getByName(`stream:${path}`).fetch(request);
    }

    const response = await handler.fetch(request);
    const headers = new Headers(response.headers);
    headers.set("Cross-Origin-Opener-Policy", "same-origin");
    headers.set("Cross-Origin-Embedder-Policy", "require-corp");
    return new Response(response.body, {
      headers,
      status: response.status,
      statusText: response.statusText,
    });
  },
});
