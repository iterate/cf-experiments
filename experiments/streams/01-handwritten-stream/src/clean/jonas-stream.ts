import { newWebSocketRpcSession } from "capnweb";
import { DurableObject } from "cloudflare:workers";
import type { StreamEvent, StreamEventInput } from "@cf-experiments/shared/event";
import { makeRpcTargetClass } from "@cf-experiments/shared/rpc-target";
import { JonasStreamInboundFrame } from "./jonas-stream-types.js";
import type { ProcessorSlug } from "./stream-processor.js";

const STORAGE_REPLAY_BATCH_SIZE = 100;

/**
 * Minimal durable event stream with a handwritten JSON-over-WebSocket protocol.
 *
 * **Primary interface (`transport=raw-ws`, the default):** clients and stream processors
 * subscribe, append, and receive live events over the raw WebSocket frames in
 * `JonasStreamInboundFrame` / `{ op: "events" }`. Stream processors connect back to the
 * stream this way; see `#connectStreamProcessor()`.
 *
 * **Cap'n Web interface (`transport=capnweb`):** exposes a small RPC surface for end-to-end
 * tests and experiment introspection — e.g. `kill()`, `ping()`, `simulateStorageSyncDelay()`,
 * `getMaxOffset()`. Tests use it so they can drive the DO without hand-rolling control-plane
 * WebSocket frames. Production-style append/subscribe traffic should stay on raw WebSocket.
 *
 * Storage always writes with `allowUnconfirmed: true` so unrelated DO egress is not blocked
 * behind the platform output gate. `append()` rebuilds the caller-facing durability contract
 * explicitly: wait for `storage.sync()`, then broadcast and return the committed event.
 */
export class JonasStream extends DurableObject<Env> {
  #incarnationId: string;
  #outboundWebSockets = new Set<{ metadata: { isSubscribed: boolean }; webSocket: WebSocket }>();
  #maxOffset: number | undefined;
  #streamName: string | undefined;
  #simulatedStorageSyncDelayMs: number | null = null;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.#incarnationId = crypto.randomUUID();
  }

  /**
   * WebSocket upgrade only. Select the wire protocol with `?transport=`:
   * - `raw-ws` (default): subscribe / append / events JSON frames — the production path.
   * - `capnweb`: Cap'n Web RPC for e2e tests and debug helpers.
   */
  async fetch(request: Request) {
    this.#streamName = new URL(request.url).pathname.slice("/jonas/".length) || "default";
    this.ctx.storage.kv.put("streamName", this.#streamName);

    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("WebSocket only", { status: 400 });
    }

    const transport = new URL(request.url).searchParams.get("transport") ?? "raw-ws";
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    if (transport === "raw-ws") {
      this.ctx.acceptWebSocket(server);
      server.serializeAttachment({
        isSubscribed: false,
      });
      return new Response(null, { status: 101, webSocket: client });
    }

    if (transport !== "capnweb") {
      return new Response("transport must be raw-ws or capnweb", { status: 400 });
    }

    server.accept();
    newWebSocketRpcSession(server, this.getCapability());
    return new Response(null, { status: 101, webSocket: client });
  }

  /** Test helper: open an outbound raw-ws connection to another JonasStream endpoint. */
  async connectOutboundWebSocket(url: string) {
    const response = await fetch(url, { headers: { Upgrade: "websocket" } });
    const webSocket = response.webSocket;
    if (webSocket === null) throw new Error("expected outbound websocket");

    webSocket.accept();

    const outboundWebSocket = {
      metadata: {
        isSubscribed: false,
      },
      webSocket,
    };
    this.#outboundWebSockets.add(outboundWebSocket);

    webSocket.addEventListener("message", (message) => {
      void this.#handleWebSocketMessage(webSocket, outboundWebSocket.metadata, message.data).catch(
        (error) => {
          console.error("JonasStream outbound WebSocket message failed", error);
          webSocket.close();
        },
      );
    });
    webSocket.addEventListener("close", () => this.#outboundWebSockets.delete(outboundWebSocket));
    webSocket.addEventListener("error", () => this.#outboundWebSockets.delete(outboundWebSocket));
  }

  /** Cloudflare callback for inbound messages on hibernatable raw-ws sockets. */
  async webSocketMessage(webSocket: WebSocket, message: string | ArrayBuffer) {
    const metadata = webSocket.deserializeAttachment();
    if (metadata === null) throw new Error("missing inbound WebSocket attachment");
    if (await this.#handleWebSocketMessage(webSocket, metadata, message)) {
      webSocket.serializeAttachment(metadata);
    }
  }

  /**
   * Durably append one event, then fan out to subscribed WebSocket clients.
   * Callable over Cap'n Web (tests) or indirectly via a raw-ws `{ op: "append" }` frame.
   */
  async append(args: { event: StreamEventInput }): Promise<StreamEvent> {
    const result = this.#writeAppend(args.event);
    if (this.#simulatedStorageSyncDelayMs !== null) {
      await new Promise((resolve) => setTimeout(resolve, this.#simulatedStorageSyncDelayMs ?? 0));
    }
    await this.ctx.storage.sync();
    if (result.appended) {
      await this.#handleBuiltinEvent(result.event);
      this.#broadcast(result.event);
    }
    return result.event;
  }

  /** E2e/debug helper: artificial delay before `storage.sync()` inside `append()`. */
  simulateStorageSyncDelay(delayMs: number | null): number | null {
    if (delayMs !== null && (!Number.isInteger(delayMs) || delayMs < 0)) {
      throw new Error("simulated storage sync delay must be null or a non-negative integer");
    }
    this.#simulatedStorageSyncDelayMs = delayMs;
    return delayMs;
  }

  /** E2e helper: abort the DO incarnation via `ctx.abort()`. */
  kill(args?: { reason?: string }): never {
    const reason = args?.reason ?? "kill requested";
    this.ctx.abort(reason);
    throw new Error("This point should never be reached; abort should kill the DO.");
  }

  /** E2e helper: return the current incarnation id (changes after hibernation restart). */
  ping() {
    return { incarnationId: this.#incarnationId };
  }

  /** E2e helper: read the highest committed offset without opening a raw-ws subscription. */
  getMaxOffset() {
    this.#maxOffset ??= this.ctx.storage.kv.get<number>("maxOffset") ?? 0;
    return this.#maxOffset;
  }

  /** Cap'n Web entrypoint; only used when `fetch()` selects `transport=capnweb`. */
  getCapability(_policy?: unknown) {
    return new JonasStreamRpcTarget(this);
  }

  /** Primary application protocol: raw-ws subscribe, append, and append-ack frames. */
  async #handleWebSocketMessage(
    webSocket: WebSocket,
    metadata: { isSubscribed: boolean },
    data: string | ArrayBuffer,
  ): Promise<boolean> {
    const text = typeof data === "string" ? data : new TextDecoder().decode(data);
    const frame = JonasStreamInboundFrame.parse(JSON.parse(text));
    if (frame.op === "subscribe") {
      // `subscribe` is the live-subscribe handshake. Inbound hibernatable WebSockets
      // cannot be tracked in an in-memory subscriber set, so this flag replaces
      // Set membership and survives hibernation via the socket attachment.
      metadata.isSubscribed = true;
      void this.#streamStoredEventsFromStorage(webSocket, frame.afterOffset ?? 0).catch((error) => {
        console.error("JonasStream storage replay failed", error);
        webSocket.close();
      });
      return true;
    }

    const event = await this.append({ event: frame.event });
    if (frame.requestAck !== undefined) {
      webSocket.send(
        JSON.stringify({
          op: "append-ack",
          appendKey: frame.requestAck.key,
          event,
        }),
      );
    }
    return false;
  }

  #writeAppend(input: StreamEventInput) {
    if (input.idempotencyKey !== undefined) {
      const existingOffset = this.ctx.storage.kv.get<number>(`idempotency:${input.idempotencyKey}`);
      if (existingOffset !== undefined) {
        const existing = this.ctx.storage.kv.get<StreamEvent>(`event:${existingOffset}`);
        if (existing !== undefined) return { event: existing, appended: false };
        throw new Error(`idempotency index points at missing event ${existingOffset}`);
      }
    }

    const offset = this.getMaxOffset() + 1;
    if (input.offset !== undefined && input.offset !== offset) {
      throw new Error(`expected offset ${offset}, got ${input.offset}`);
    }

    const event = { ...input, offset, createdAt: new Date().toISOString() };
    const writes = {
      [`event:${event.offset}`]: event,
      maxOffset: event.offset,
    };
    if (input.idempotencyKey !== undefined) {
      writes[`idempotency:${input.idempotencyKey}`] = event.offset;
    }
    this.#maxOffset = offset;
    void this.ctx.storage.put(writes, { allowUnconfirmed: true, noCache: true });
    return { event, appended: true };
  }

  /** Fan out one committed event to every subscribed raw-ws client (inbound and outbound). */
  #broadcast(event: StreamEvent) {
    const message = JSON.stringify({ op: "events", events: [event] });
    for (const connection of [
      ...this.#outboundWebSockets,
      ...this.ctx.getWebSockets().map((webSocket) => {
        const metadata = webSocket.deserializeAttachment();
        if (metadata === null) throw new Error("missing inbound WebSocket attachment");
        return { metadata, webSocket };
      }),
    ]) {
      if (!connection.metadata.isSubscribed) continue;
      try {
        connection.webSocket.send(message);
      } catch {
        connection.webSocket.close();
      }
    }
  }

  /** React to built-in stream events, e.g. lazily wire up a processor over raw-ws. */
  async #handleBuiltinEvent(event: StreamEvent) {
    if (event.type !== "events.iterate.com/stream/processor-subscribed") return;
    await this.#connectStreamProcessor(processorSlugFromPayload(event.payload));
  }

  /**
   * Open a raw-ws connection to a StreamProcessor DO and route its frames through
   * `#handleWebSocketMessage()`. This is how processors subscribe and append — not Cap'n Web.
   */
  async #connectStreamProcessor(processorSlug: ProcessorSlug) {
    const streamName = this.#readStreamName();
    const processor = this.env.STREAM_PROCESSOR.getByName(`${streamName}:${processorSlug}`);
    await processor.initialize({ processorSlug });

    const response = await processor.fetch(
      new Request("https://stream-processor.local/", {
        headers: { Upgrade: "websocket" },
      }),
    );
    const webSocket = response.webSocket;
    if (webSocket === null) throw new Error("expected stream processor websocket");

    webSocket.accept();
    const outboundWebSocket = {
      metadata: {
        isSubscribed: false,
      },
      webSocket,
    };
    this.#outboundWebSockets.add(outboundWebSocket);
    webSocket.addEventListener("message", (message) => {
      void this.#handleWebSocketMessage(webSocket, outboundWebSocket.metadata, message.data).catch(
        (error) => {
          console.error("JonasStream processor WebSocket message failed", error);
          webSocket.close();
        },
      );
    });
    webSocket.addEventListener("close", () => this.#outboundWebSockets.delete(outboundWebSocket));
    webSocket.addEventListener("error", () => this.#outboundWebSockets.delete(outboundWebSocket));
  }

  async #streamStoredEventsFromStorage(webSocket: WebSocket, afterOffset: number) {
    const maxOffset = this.getMaxOffset();
    let batch: StreamEvent[] = [];

    for (let offset = afterOffset + 1; offset <= maxOffset; offset++) {
      const event = this.ctx.storage.kv.get<StreamEvent>(`event:${offset}`);
      if (event !== undefined) batch.push(event);

      if (batch.length === STORAGE_REPLAY_BATCH_SIZE) {
        webSocket.send(JSON.stringify({ op: "events", events: batch }));
        batch = [];
        await Promise.resolve();
      }
    }

    if (batch.length > 0) {
      webSocket.send(JSON.stringify({ op: "events", events: batch }));
    }
  }

  #readStreamName() {
    this.#streamName ??= this.ctx.storage.kv.get<string>("streamName");
    if (this.#streamName === undefined) throw new Error("missing stream name");
    return this.#streamName;
  }

}

function processorSlugFromPayload(payload: unknown): ProcessorSlug {
  if (
    typeof payload === "object" &&
    payload !== null &&
    "processorSlug" in payload &&
    payload.processorSlug === "echo"
  ) {
    return payload.processorSlug;
  }
  throw new Error(
    'events.iterate.com/stream/processor-subscribed payload must include processorSlug: "echo"',
  );
}

/** Cap'n Web surface exposed to e2e tests via `transport=capnweb`. */
export type JonasStreamRpc = Pick<
  JonasStream,
  | "append"
  | "simulateStorageSyncDelay"
  | "kill"
  | "ping"
  | "getMaxOffset"
  | "connectOutboundWebSocket"
>;

export const JonasStreamRpcTarget = makeRpcTargetClass<JonasStreamRpc, JonasStream>(JonasStream, {
  exclude: ["fetch", "webSocketMessage", "getCapability"],
});
