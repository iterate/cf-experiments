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

    // No COOP/COEP: cross-origin isolation enables SharedArrayBuffer, which makes
    // sqlite-wasm auto-install its async-proxy OPFS VFS during init — and that proxy
    // worker deadlocks in production builds (see log.md). The opfs-sahpool VFS we use
    // (patches/sqlocal@0.18.0.patch) needs neither SAB nor isolation.
    return handler.fetch(request);
  },
});
