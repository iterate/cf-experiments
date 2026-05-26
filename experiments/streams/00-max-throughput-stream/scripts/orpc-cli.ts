#!/usr/bin/env node
import { createORPCClient } from "@orpc/client";
import { RPCLink } from "@orpc/client/fetch";
import { os } from "@orpc/server";
import { createCli } from "trpc-cli";
import type { AnyRouter, parseRouter } from "trpc-cli/dist/parse-router.js";
import type { StandardSchemaV1 } from "trpc-cli/dist/standard-schema/contract.js";

type ParsedRouter = ReturnType<typeof parseRouter>;

const DEFAULT_BASE_URL = "http://localhost:8787";

const baseUrl = normalizeBaseUrl(
  consumeStringFlag("--base-url") ?? process.env.STREAM_ORPC_BASE_URL ?? DEFAULT_BASE_URL,
);
const procedures = await loadRemoteProcedures(baseUrl);
const router = proxifyOrpc(procedures, () => {
  const client = createORPCClient(
    new RPCLink({
      url: joinUrl(baseUrl, "/orpc/"),
    }),
  );
  return orpcToTrpcStyleClient(client);
});

await createCli({
  router: router as AnyRouter,
  name: "00-max-throughput-stream",
  description: `Remote oRPC CLI for ${baseUrl}`,
}).run();

async function loadRemoteProcedures(baseUrl: string): Promise<ParsedRouter> {
  const url = joinUrl(baseUrl, "/api/__internal/trpc-cli-procedures");
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`${url} returned ${response.status}: ${await response.text()}`);
  }
  const body = (await response.json()) as { procedures?: unknown };
  if (!Array.isArray(body.procedures)) {
    throw new Error(`${url} returned invalid procedure metadata`);
  }
  return body.procedures as ParsedRouter;
}

function proxifyOrpc(router: ParsedRouter, getClient: (procedurePath: string) => unknown) {
  const outputRouterRecord = {};

  for (const [procedurePath, info] of router) {
    const parts = procedurePath.split(".");
    let currentRouter: Record<string, unknown> = outputRouterRecord;

    for (const part of parts.slice(0, -1)) {
      currentRouter = (currentRouter[part] ||= {}) as Record<string, unknown>;
    }

    const schemas = info.inputSchemas.success ? info.inputSchemas.value : [];
    const standardSchema: StandardSchemaV1 & { toJsonSchema: () => unknown } = {
      "~standard": {
        vendor: "trpc-cli",
        version: 1,
        validate: (value: unknown) => ({ value }),
      },
      toJsonSchema: () => {
        if (schemas.length === 0) return {};
        if (schemas.length === 1) return schemas[0];
        return { allOf: schemas };
      },
    };

    currentRouter[parts[parts.length - 1]!] = os
      .input(standardSchema)
      .handler(async ({ input }: { input: unknown }) => {
        const client = getClient(procedurePath) as Record<
          string,
          { query(input: unknown): unknown }
        >;
        return client[procedurePath]!.query(input);
      });
  }

  return outputRouterRecord;
}

function orpcToTrpcStyleClient(orpcClient: unknown) {
  return new Proxy(
    {},
    {
      get: (_target, prop: string | symbol) => {
        if (typeof prop !== "string") return undefined;
        const parts = prop.split(".");
        let current = orpcClient as Record<string, unknown>;
        for (const part of parts) {
          current = current[part] as Record<string, unknown>;
        }
        return {
          query: (input: unknown) => (current as (input: unknown) => unknown)(input),
          mutate: (input: unknown) => (current as (input: unknown) => unknown)(input),
        };
      },
    },
  );
}

function consumeStringFlag(flagName: string): string | undefined {
  const flagIndex = process.argv.indexOf(flagName);
  if (flagIndex === -1) return undefined;
  const value = process.argv[flagIndex + 1];
  if (!value || value.startsWith("-")) {
    throw new Error(`${flagName} requires a value`);
  }
  process.argv.splice(flagIndex, 2);
  return value;
}

function normalizeBaseUrl(value: string) {
  return value.replace(/\/+$/, "");
}

function joinUrl(baseUrl: string, path: string) {
  return `${baseUrl}${path.startsWith("/") ? path : `/${path}`}`;
}
