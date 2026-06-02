import { cloudflare } from "@cloudflare/vite-plugin";
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import viteReact from "@vitejs/plugin-react";
import sqlocal from "sqlocal/vite";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [
    // coi:false → no COOP/COEP in dev either, so dev matches prod: no SharedArrayBuffer,
    // so sqlite-wasm never auto-installs its deadlocking async-proxy OPFS VFS. We use the
    // proxy-free opfs-sahpool VFS (patches/sqlocal@0.18.0.patch), which needs no isolation.
    sqlocal({ coi: false }),
    cloudflare({
      viteEnvironment: { name: "ssr" },
    }),
    tanstackStart(),
    viteReact(),
  ],
});
