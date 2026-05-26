import { greet } from "@cf-experiments/shared/greet";

export default {
  fetch() {
    return new Response(`${greet("sky")} — ${new Date().toISOString()}`);
  },
} satisfies ExportedHandler;
