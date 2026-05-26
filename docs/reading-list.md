### Durable Objects

- [Workers Durable Objects Beta: A New Approach to Stateful Serverless](https://blog.cloudflare.com/introducing-workers-durable-objects/) (2020) — original DO announcement
- [Durable Objects: Easy, Fast, Correct — Choose three](https://blog.cloudflare.com/durable-objects-easy-fast-correct-choose-three/) (2021) — input/output gates, in-memory caching
- [Zero-latency SQLite storage in every Durable Object](https://blog.cloudflare.com/sqlite-in-durable-objects/) (2024) — SQLite-in-thread, Storage Relay Service
- [Durable Objects in Dynamic Workers: Give each AI-generated app its own database](https://blog.cloudflare.com/durable-object-facets-dynamic-workers/) (2026) — DO facets / per-app SQLite

### Cloudflare Workers RPC & bindings

- [We've added JavaScript-native RPC to Cloudflare Workers](https://blog.cloudflare.com/javascript-native-rpc/) (2024) — `WorkerEntrypoint`, service bindings, `RpcTarget`, stubs/proxying
- [Why Workers environment variables contain live objects](https://blog.cloudflare.com/workers-environment-live-object-bindings/) (2024) — bindings as live handles, not config strings
- [Cap'n Web: a new RPC system for browsers and web servers](https://blog.cloudflare.com/capnweb-javascript-rpc-library/) (2025) — schema-less RPC; interoperates with Workers RPC; powers remote dev proxying
- [Connecting to production: the architecture of remote bindings](https://blog.cloudflare.com/connecting-to-production-the-architecture-of-remote-bindings/) (2025) — *not by Kenton*, but explains how `remote: true` bindings proxy via Cap'n Web over WebSockets to production JSRPC

### Cloudflare Workers (platform & runtime)

- [Introducing Cloudflare Workers: Run JavaScript Service Workers at the Edge](https://blog.cloudflare.com/introducing-cloudflare-workers/) (2017) — launch post, Service Worker API
- [Everyone can now run JavaScript on Cloudflare with Workers](https://blog.cloudflare.com/cloudflare-workers-unleashed/) (2018) — GA, pricing
- [WebAssembly on Cloudflare Workers](https://blog.cloudflare.com/webassembly-on-cloudflare-workers/) (2018)
- [Introducing workerd: the Open Source Workers runtime](https://blog.cloudflare.com/workerd-open-source-workers-runtime/) (2022) — open-sourcing the runtime
- [Backwards-compatibility in Cloudflare Workers](https://blog.cloudflare.com/backwards-compatibility-in-cloudflare-workers/) (2021)
- [A Workers optimization that reduces your bill](https://blog.cloudflare.com/workers-optimization-reduces-your-bill/) (2022)
- [Unpacking Cloudflare Workers CPU Performance Benchmarks](https://blog.cloudflare.com/unpacking-cloudflare-workers-cpu-performance-benchmarks/) (2025)
- [Sandboxing AI agents, 100x faster](https://blog.cloudflare.com/dynamic-workers/) (2026) — Dynamic Workers / isolates vs containers

### Security & isolation

- [Mitigating Spectre and Other Security Threats: The Cloudflare Workers Security Model](https://blog.cloudflare.com/mitigating-spectre-and-other-security-threats-the-cloudflare-workers-security-model/) (2020)
- [Dynamic Process Isolation: Research by Cloudflare and TU Graz](https://blog.cloudflare.com/spectre-research-with-tu-graz/) (2021)

### Other relevant posts

- [Containers at the edge: it's not what you think, or maybe it is](https://blog.cloudflare.com/containers-on-the-edge/) (2021) — early edge-containers exploration
- [Code Mode: the better way to use MCP](https://blog.cloudflare.com/code-mode/) (2025) — agents calling Workers via RPC instead of raw MCP tools
- [JAMstack podcast: originless code with Kenton Varda](https://blog.cloudflare.com/jamstack-podcast-with-kenton-varda) (2018) — originless / edge compute vision

