import { cloudflare } from "@cloudflare/vite-plugin";
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import tailwindcss from "@tailwindcss/vite";
import viteReact from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  // wa-sqlite ships an Emscripten `.mjs` + `.wasm` pair that must NOT go through esbuild's
  // dep pre-bundling, or the glue/wasm pairing breaks. Exclude it; the dedicated worker
  // (stream-db.worker.ts) loads the `.wasm` as a hashed asset via a `?url` import, which
  // Vite resolves correctly in dev and in the production/Cloudflare build alike.
  //
  // Note there is deliberately NO COOP/COEP here: OPFSCoopSyncVFS needs no cross-origin
  // isolation. (Enabling it is what made @sqlite.org/sqlite-wasm auto-install its
  // async-proxy "opfs" VFS and deadlock in production builds — see log.md.)
  optimizeDeps: { exclude: ["@journeyapps/wa-sqlite"] },
  plugins: [
    tailwindcss(),
    cloudflare({ viteEnvironment: { name: "ssr" } }),
    tanstackStart(),
    viteReact(),
  ],
});
