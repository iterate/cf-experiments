import { greet } from "@cf-experiments/utils/greet";

export default {
  fetch(request) {
    const { pathname } = new URL(request.url);
    return new Response(`${greet(pathname)} @ edge`);
  },
} satisfies ExportedHandler;
