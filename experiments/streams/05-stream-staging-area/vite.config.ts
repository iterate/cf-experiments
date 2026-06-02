import { cloudflare } from "@cloudflare/vite-plugin";
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import viteReact from "@vitejs/plugin-react";
import sqlocal from "sqlocal/vite";
import { defineConfig } from "vite";

export default defineConfig({
  server: {
    watch: {
      ignored: ["**/.wrangler/**"],
    },
  },
  plugins: [
    sqlocal(),
    cloudflare({
      inspectorPort: false,
      remoteBindings: false,
      tunnel: false,
      viteEnvironment: { name: "ssr" },
    }),
    tanstackStart(),
    viteReact(),
  ],
});
