import { greet } from "@cf-experiments/utils/greet";

export default {
  fetch() {
    return new Response(`${greet("sky")} — ${new Date().toISOString()}`);
  },
} satisfies ExportedHandler;
