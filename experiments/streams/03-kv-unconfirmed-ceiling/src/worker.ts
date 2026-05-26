import { KvWriteBench, parseBool, parseWriteMode } from "./kv-write-bench.js";

export { KvWriteBench };

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const name = url.searchParams.get("name") ?? `bench-${crypto.randomUUID().slice(0, 8)}`;
    const stub = env.KV_WRITE_BENCH.getByName(name);

    if (url.pathname === "/ping") {
      return Response.json(await stub.ping());
    }

    if (url.pathname === "/write-loop" && request.method === "POST") {
      const result = await stub.writeLoop({
        messages: parsePositiveInt(url.searchParams.get("messages"), 10_000),
        payloadBytes: parseNonNegativeInt(url.searchParams.get("payload-bytes"), 4800),
        mode: parseWriteMode(url.searchParams.get("mode")),
        sync: parseBool(url.searchParams.get("sync"), false),
        flushEvery: parseNonNegativeInt(url.searchParams.get("flush-every"), 0) || undefined,
      });
      return Response.json(result);
    }

    if (url.pathname === "/append" && request.method === "POST") {
      const result = await stub.appendBatch({
        messages: parsePositiveInt(url.searchParams.get("messages"), 10_000),
        payloadBytes: parseNonNegativeInt(url.searchParams.get("payload-bytes"), 4800),
        mode: parseWriteMode(url.searchParams.get("mode")),
        sync: parseBool(url.searchParams.get("sync"), false),
        flushEvery: parseNonNegativeInt(url.searchParams.get("flush-every"), 0) || undefined,
      });
      return Response.json(result);
    }

    if (url.pathname === "/flush" && request.method === "POST") {
      return Response.json(await stub.flush());
    }

    if (url.pathname === "/pressure" && request.method === "POST") {
      const result = await stub.writePressure({
        maxMessages: parsePositiveInt(url.searchParams.get("max-messages"), 1_000_000),
        payloadBytes: parseNonNegativeInt(url.searchParams.get("payload-bytes"), 4800),
        mode: parseWriteMode(url.searchParams.get("mode")),
        flushEvery: parseNonNegativeInt(url.searchParams.get("flush-every"), 0) || undefined,
      });
      return Response.json(result);
    }

    if (url.pathname === "/count") {
      return Response.json(await stub.count({ mode: parseWriteMode(url.searchParams.get("mode")) }));
    }

    return new Response(
      [
        "03-kv-unconfirmed-ceiling",
        "",
        "POST /write-loop?name=s&messages=N&payload-bytes=B&mode=kv-unconfirmed&sync=0",
        "POST /append?name=s&messages=N          — append to same DO, no sync by default",
        "POST /flush?name=s                      — manual storage.sync()",
        "POST /pressure?name=s&max-messages=N    — one shot until max or in-DO error",
        "  flush-every=N                         — sync every N appends during loop",
        "GET  /count?name=s&mode=kv-unconfirmed  — meta offset (includes unconfirmed)",
        "GET  /ping?name=s",
      ].join("\n"),
      { headers: { "content-type": "text/plain; charset=utf-8" } },
    );
  },
} satisfies ExportedHandler<Env>;

function parsePositiveInt(raw: string | null, fallback: number) {
  const n = Number(raw);
  return Number.isInteger(n) && n > 0 ? n : fallback;
}

function parseNonNegativeInt(raw: string | null, fallback: number) {
  const n = Number(raw);
  return Number.isInteger(n) && n >= 0 ? n : fallback;
}
