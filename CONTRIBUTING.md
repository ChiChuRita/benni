# Contributing to Beni

Thank you for helping make Redis safer and more predictable for TypeScript
applications.

## Development setup

Requirements:

- Node.js 20 or newer
- pnpm 11
- Docker for Redis-backed integration tests
- Bun for the Bun adapter contract

Install dependencies and run the local gate:

```sh
pnpm install
pnpm check
pnpm docs:build
```

Run the integration suite against Redis:

```sh
pnpm redis:run
BENI_REDIS_URL=redis://127.0.0.1:6379 pnpm test
BENI_REDIS_URL=redis://127.0.0.1:6379 pnpm test:bun
```

The cluster suites need a cluster-enabled node on 6381. Without
`BENI_REDIS_CLUSTER_URL` they skip, and a skipped suite looks exactly like a
passing one, so set it when touching keys, slots, or an adapter:

```sh
pnpm redis:cluster:build && pnpm redis:cluster:run
BENI_REDIS_CLUSTER_URL=redis://127.0.0.1:6381 pnpm test
```

The HTTP/edge suite is gated separately. Without these two variables the
Upstash integration tests silently skip, so set them when touching that
adapter — point them at [`serverless-redis-http`](https://github.com/hiett/serverless-redis-http)
in front of a local Redis (this is what CI does):

```sh
BENI_UPSTASH_URL=http://127.0.0.1:8079 BENI_UPSTASH_TOKEN=example_token pnpm test
```

CI sets all of these, so the only suite that skips there is one whose service
failed to start.

## Pull requests

- Keep the public API Redis-shaped. Prefer the Redis command name when a
  method maps to one command.
- Add unit tests for validation, encoding, decoding, and reply-shape failures.
- Add a live Redis test when behavior depends on command or reply semantics.
- Update the docs pages under `docs/src/content/docs/` when behavior changes.
- Add a changeset for user-visible changes with `pnpm changeset`.
- Keep transport-specific behavior in adapters and the typed data contract in
  the runtime-agnostic core.

Small, focused pull requests are easiest to review. For large API proposals,
open an issue first with representative Redis commands and TypeScript usage.
