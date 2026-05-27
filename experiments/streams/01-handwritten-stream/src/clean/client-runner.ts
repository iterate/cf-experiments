import { DurableObject } from "cloudflare:workers";
import { connectCleanStream, type CleanStreamTransport } from "./client.js";

export class CleanStreamClientRunner extends DurableObject {
  async runSmoke(args: { stream: string; transport: CleanStreamTransport }) {
    const endpoint = {
      fetch: (request: Request) => this.env.CLEAN_STREAM.getByName(args.stream).fetch(request),
    };
    await using subscriber = await connectCleanStream({ transport: args.transport, endpoint });
    await using publisher = await connectCleanStream({ transport: args.transport, endpoint });
    await using subscription = await subscriber.subscribe();

    const input = {
      type: "test.clean-stream.fetch-client",
      payload: {
        stream: args.stream,
        transport: args.transport,
      },
    };
    const appended = await publisher.append(input);
    const delivered = await subscription.read();

    return {
      transport: args.transport,
      appended,
      delivered,
      matched: JSON.stringify(appended) === JSON.stringify(delivered),
    };
  }
}
