import { getRouteApi } from "@tanstack/react-router";
import { StreamPage } from "./-stream-page.js";

const streamSplatRoute = getRouteApi("/streams/$");

export function StreamSplatRoute() {
  const { _splat } = streamSplatRoute.useParams();
  const { view } = streamSplatRoute.useSearch();
  return <StreamPage streamPath={`/${_splat}`} viewSlug={view} />;
}
