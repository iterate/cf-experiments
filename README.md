# cf-experiments

The goal of this repo is to systematically improve my understanding of Cloudflare's workers platform.

Each folder in `experiments/` contain reproducible code.


# Anatomy of an experiment

`README.md` (symlinked from `AGENTS.md/CLAUDE.md`) must show
- What we're trying to find out
- How to run the experiment (and how to parameterise it)
- How to evaluate the results

`log.md` contains
1. High level findings (at the top)
2. Detailed notes in dated/timestamped reverse chronological order

The log should contain 
- experiment runs
- design changes
- thoughts / comments 
- anything else relevant

# Coding guidelines

The most important thing in a file should be at the top.

Avoid single-use helpers. For the rare helper functions you do make:
- Check if other experiments or packages/shared have what you need first. If so, steal that.
- Stick in local `lib/` folder with comment explaining where it's used and why it was needed
- Don't add to packages/shared unless explicitly encouraged - experiments should be ~immutable

Prefer "back of properties" single-argument functions to lots of positional arguments (though fine to have `someFunction(requiredArg, optionalOptions)` and `someFunction(singleValue)`).

Use strict typescript. Do not declare types that can be inferred. Ask before using type assertions. Don't name types that are only used once and could be inlined.

This is not "production code". We do not need to catch and individually handle every error.

We never care about backwards compatibility unless explicitly stated.

Don't break stuff up into multiple files unless you have to. For example, most experiments can be a single worker.ts file.

We care MORE THAN ANYTHING about using the cloudflare platform properly. Read the workerd source code in ~/src/github.com/cloudflare/workerd if useful. Read and re-read the first-party cloudflare docs and in particular any blog posts by Kenton Varda - check out the reading list in docs/reading-list.md.

Don't add random scripts like `"cf:types"` to package.json. We can just run `pnpm wrangler types` etc

Durable Object / Workers RPC state: keep mutable internals in `#private` fields (or `#readFoo()` helpers for lazy/cached storage). Expose caller-facing reads as public RPC **methods** (`maxOffset()`, not a public field or getter mirroring a cache). Use `debug()` / `ping()` for experiment introspection (`incarnationId`, counters). Prototype getters are fine for cheap derived values used only inside the DO; don't expect class fields to appear on stubs automatically.


# Debugging cloudflare workers

All workers should have observability and tracing turned on.

Use your cloudflare MCP tools to get production logs and traces. See [Debugging Cloudflare Workers](./docs/debugging-cf-workers.md) for useful `cloudflare-api` MCP snippets.

Make sure your test scripts spit out cloudflare ray ids from headers to make stuff easy to find.

Use Workers Analytics Engine (hosted clickhouse with synchronous write API) to get metrics.

# High level findings
Important high level findings about the platform go into docs/findings.md. You should OFFER to your human to put stuff in there, but never do it automatically.

The kinds of things we're interested in:

- Cases where first party documentation is not correct
- Unexpected performance implications of small changes (e.g. compatibility mode, tracing, etc)
- Things we cannot explain based on public platform knowledge
- Differences between miniflare and deployed workers (v important!)
- Cases where documented constraints aren't enforced (e.g. isolates allowed to allocate more memory than documented 128mb)
- Specific reproducible cases of cloudflare errors (e.g. 1101 and 1102) and the exact circumstances where they occur are useful

VERY IMPORTANT: Any high level findings must be cited and refer back to an experiment that ought to be able to be run directly without modification.

Results need to be confirmed by repeated execution before making their way into docs/findings.md

# How-tos
- docs/making-a-new-experiment.md

