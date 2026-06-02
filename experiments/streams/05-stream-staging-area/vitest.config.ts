import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    exclude: ["scripts/**/*.test.ts", "**/node_modules/**"],
    testTimeout: 30_000,
  },
});
