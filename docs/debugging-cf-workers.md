# Debugging Cloudflare Workers

All workers should have observability and tracing turned on.

Use Cloudflare MCP tools to get production logs and traces. Make test scripts print Cloudflare Ray IDs from response headers so runs are easy to find later.

Use Workers Analytics Engine (hosted ClickHouse with synchronous write API) for experiment-specific metrics.

## Useful `cloudflare-api` MCP Snippets

Use MCP server `user-cloudflare-api` / `cloudflare-api`. Always read the tool schemas first; in this workspace they are at:

- `.../mcps/user-cloudflare-api/tools/search.json`
- `.../mcps/user-cloudflare-api/tools/execute.json`

Most experiments in this repo deploy to **iterate (dev/preview)**:

```text
account_id = 376ef7ed81b0573f93524de763666c15
```

## Discover Workers Observability API

Use the `search` tool:

```js
async () => {
  const results = [];
  for (const [path, methods] of Object.entries(spec.paths)) {
    for (const [method, op] of Object.entries(methods)) {
      const hay = [path, op.summary, op.description, ...(op.tags || [])].join(" ").toLowerCase();
      if (
        hay.includes("workers observability") ||
        hay.includes("telemetry/query") ||
        hay.includes("log explorer") ||
        hay.includes("otel")
      ) {
        results.push({
          method: method.toUpperCase(),
          path,
          summary: op.summary,
          tags: op.tags,
          description: op.description?.slice(0, 300),
        });
      }
    }
  }
  return results;
}
```

The key endpoint for dashboard-style spans is:

```text
POST /accounts/{account_id}/workers/observability/telemetry/query
```

## Query Raw OTel Spans By Ray ID

Use the `execute` tool. This matches what the Cloudflare dashboard shows in Workers Observability.

```js
async () => {
  return cloudflare.request({
    method: "POST",
    path: `/accounts/${accountId}/workers/observability/telemetry/query`,
    body: {
      queryId: "adhoc-ray-9ffc19796913f668",
      view: "events",
      limit: 20,
      timeframe: {
        from: 1779455820000,
        to: 1779455840000,
      },
      parameters: {
        datasets: ["otel"],
        filterCombination: "and",
        filters: [
          {
            key: "cloudflare.ray_id",
            operation: "eq",
            type: "string",
            value: "9ffc19796913f668",
          },
        ],
      },
    },
  });
}
```

Useful fields in the result:

- `source.cloudflare.outcome` (`ok`, `exception`, `exceededMemory`, `exceededCpu`, ...)
- `source.cloudflare.execution_model` (`stateless`, Durable Object/stateful, etc.)
- `source.cloudflare.entrypoint`
- `source.cloudflare.ray_id`
- `source.faas.invocation_id`
- `source.traceId`, `source.spanId`
- `source.cpu_time_ms`, `source.wall_time_ms`, `source.durationMS`
- `$metadata.error`, `$metadata.message`, `$metadata.type`

## Query Trace Summary By Trace ID

Use `view: "traces"` with the same endpoint:

```js
async () => {
  return cloudflare.request({
    method: "POST",
    path: `/accounts/${accountId}/workers/observability/telemetry/query`,
    body: {
      queryId: "adhoc-trace-fdb7913a3c332af0c953da82dda7c568",
      view: "traces",
      limit: 20,
      timeframe: {
        from: 1779455820000,
        to: 1779455840000,
      },
      parameters: {
        datasets: ["otel"],
        filterCombination: "and",
        filters: [
          {
            key: "traceId",
            operation: "eq",
            type: "string",
            value: "fdb7913a3c332af0c953da82dda7c568",
          },
        ],
      },
    },
  });
}
```

This returns compact trace facts like `spans`, `errors`, `rootSpanName`, `rootTransactionName`, and `traceDurationMs`.

## Discover Available Telemetry Keys

Use this before guessing field names:

```js
async () => {
  return cloudflare.request({
    method: "POST",
    path: `/accounts/${accountId}/workers/observability/telemetry/keys`,
    body: {
      datasets: ["otel"],
      from: 1779455700000,
      to: 1779455900000,
      limit: 200,
      keyNeedle: { value: "cloudflare", matchCase: false },
      filters: [
        {
          key: "service.name",
          operation: "eq",
          type: "string",
          value: "03-kill-durable-object",
        },
      ],
    },
  });
}
```

Notes from experiment 03:

- No built-in isolate ID was exposed in OTel spans. To count isolates/incarnations, emit your own `workerIsolateId` / `doInstanceId` in structured logs and responses.
- `faas.invocation_id` is per invocation, not per isolate.
- `cloudflare.invocation.sequence.number` orders events within an invocation, not isolate lifetime.

## Query Aggregate Worker Invocation Data

This is useful as a fallback, but it is not a span query. It summarizes invocations and can confirm `status: "exceededMemory"` / `status: "success"` over time.

```js
async () => {
  const query = `query Workers($accountTag: string, $datetime_geq: Time, $datetime_leq: Time) {
    viewer {
      accounts(filter: { accountTag: $accountTag }) {
        workersInvocationsAdaptive(
          limit: 20,
          filter: {
            scriptName: "03-kill-durable-object",
            datetime_geq: $datetime_geq,
            datetime_leq: $datetime_leq
          },
          orderBy: [datetime_DESC]
        ) {
          dimensions { datetime scriptName status coloCode }
          sum { requests errors cpuTimeUs wallTime }
        }
      }
    }
  }`;

  return cloudflare.request({
    method: "POST",
    path: "/graphql",
    body: {
      query,
      variables: {
        accountTag: accountId,
        datetime_geq: "2026-05-22T13:00:00Z",
        datetime_leq: "2026-05-22T13:25:00Z",
      },
    },
  });
}
```

## `cf` CLI Note

`cf schema --list` may show `cf workers observability telemetry-query`, but the installed CLI build may not expose the generated command runner yet. If `cf workers observability telemetry-query ...` fails with `Unknown arguments`, use the MCP `execute` tool against `/workers/observability/telemetry/query` directly.
