import { createFileRoute } from "@tanstack/react-router";
import { StreamSplatRoute } from "./-stream-splat-route.js";

export const Route = createFileRoute("/streams/$")({
  component: StreamSplatRoute,
});
