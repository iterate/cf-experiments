import { createRouter } from "@tanstack/react-router";
import { routeTree } from "./routeTree.gen.js";

export function getRouter() {
  return createRouter({
    routeTree,
    scrollRestoration: true,
    trailingSlash: "preserve",
  });
}

declare module "@tanstack/react-router" {
  interface Register {
    router: ReturnType<typeof getRouter>;
  }
}
