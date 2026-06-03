import { createFileRoute } from "@tanstack/react-router";
import { StreamPage } from "../-stream-page.js";

export const Route = createFileRoute("/streams/")({
  component: () => <StreamPage streamPath="/" />,
});
