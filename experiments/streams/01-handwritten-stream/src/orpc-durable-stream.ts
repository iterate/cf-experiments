import {
  DurableIteratorObject,
  type PublishEventOptions,
} from "@orpc/experimental-durable-iterator/durable-object";
import {
  StreamEventInput as StreamEventInputSchema,
  type StreamEvent,
} from "@cf-experiments/shared/event";

export const ORPC_DURABLE_ITERATOR_SIGNING_KEY =
  "01-handwritten-stream-orpc-durable-iterator-benchmark";

const APPEND_EVENT_INPUT_SCHEMA = StreamEventInputSchema.strict();

export class OrpcDurableStream extends DurableIteratorObject<StreamEvent, Env> {
  #offset = 0;
  #fanoutAttempts = 0;

  constructor(ctx: DurableObjectState<{}>, env: Env) {
    super(ctx, env, {
      signingKey: ORPC_DURABLE_ITERATOR_SIGNING_KEY,
      resumeRetentionSeconds: Number.NaN,
    });
  }

  /**
   * Diagnostic append path for `stream-kind=orpc-durable-iterator`.
   *
   * It is intentionally volatile: no replay, idempotency, durability, or
   * storage output-gate behavior. The benchmark uses this path to isolate ORPC
   * durable iterator subscriber delivery from Cap'n Web returned-stream pipes.
   */
  async append(args: unknown): Promise<StreamEvent> {
    if (args === null || typeof args !== "object" || !("event" in args)) {
      throw new Error("append args must be an object with event");
    }
    const parsedEvent = APPEND_EVENT_INPUT_SCHEMA.safeParse(args.event);
    if (!parsedEvent.success) {
      throw new Error("append event must be a valid StreamEventInput");
    }
    this.#offset += 1;
    const committed = {
      ...parsedEvent.data,
      offset: this.#offset,
      createdAt: new Date().toISOString(),
    };
    this.#fanoutAttempts += this.ctx.getWebSockets().length;
    this.publishEvent(committed);
    return committed;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/append" && request.method === "POST") {
      return Response.json(await this.append(await request.json()));
    }
    return super.fetch(request);
  }

  debug() {
    return {
      kind: "orpc-durable-iterator",
      maxOffset: this.#offset,
      subscribers: this.ctx.getWebSockets().length,
      fanoutAttempts: this.#fanoutAttempts,
    };
  }

  publishEvent(payload: StreamEvent, options?: PublishEventOptions): void {
    super.publishEvent(payload, options);
  }
}
