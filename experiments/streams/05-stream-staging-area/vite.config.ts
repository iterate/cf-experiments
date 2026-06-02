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
    cloudflare({ viteEnvironment: { name: "ssr" } }),
    tanstackStart(),
    viteReact(),
  ],
});
