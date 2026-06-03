import handler, { createServerEntry } from "@tanstack/react-start/server-entry";
import { env } from "cloudflare:workers";

export { Stream } from "../../src/workers/durable-objects/stream.js";
export { StreamProcessorRunner } from "../../src/workers/durable-objects/stream-processor-runner.js";

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

    // No COOP/COEP on purpose: the browser SQLite mirror uses wa-sqlite's OPFSCoopSyncVFS,
    // which needs no SharedArrayBuffer and no cross-origin isolation. (Isolation is what
    // made @sqlite.org/sqlite-wasm auto-install its async-proxy OPFS VFS and deadlock in
    // production builds — see log.md.) Leaving it off also keeps OPFS working the same way
    // across Chrome, Edge, Safari and mobile Safari.
    return handler.fetch(request);
  },
});
