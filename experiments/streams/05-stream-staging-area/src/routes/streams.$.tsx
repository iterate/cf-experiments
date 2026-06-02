import { createFileRoute } from "@tanstack/react-router";
import { StreamPage } from "./-stream-page.js";

export const Route = createFileRoute("/streams/$")({
  component: StreamRoute,
});

function StreamRoute() {
  const { _splat } = Route.useParams();
  return <StreamPage streamPath={`/${_splat}`} />;
}
