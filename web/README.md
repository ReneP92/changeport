# web

The customer-facing React app of the Changeport console: a customer's
cluster overview, topic management, consumer lag in seconds, and the
operator console. It is built with Vite, React, TypeScript, Tailwind and
shadcn/ui, and everything around it (installing, scripts, tests, builds)
runs through Bun. The console service (`cmd/console`) serves it, embedded in
its binary. The work is tracked in the Phase 2 milestone (#46 to #58); see
docs/PROJECT_PLAN.md.

## Development

Bun runs everything here, so neither Node nor another package manager is
needed. The Makefile targets download the Bun that `hack/lib.sh` pins into
`bin/`, checksum-verified; `packageManager` in `package.json` names the same
version, and the scripts refuse to run when the two disagree. Run any other
Bun command in this directory through `hack/bun.sh` from the repository root.

```bash
make web-dev     # Vite with hot reload on http://localhost:5173; /api goes to the console on :8088
make web-check   # ESLint, tsc, Prettier, bun test and the production build
make web-e2e     # Playwright in Chromium against the production build
```

- **Unit tests** run under `bun test`, with happy-dom as the DOM
  (`src/test/setup.ts`) and Testing Library. They live next to the code as
  `*.test.tsx`.
- **End-to-end tests** live in `e2e/` and run under Playwright's own runner,
  which runs on Bun too, as do its workers and the preview server.
  `PLAYWRIGHT_BASE_URL` points them at a running console instead of a
  local build. Failing tests are fixed, never retried.
- **The API client** is generated from `api/openapi.yaml` into
  `src/api/schema.ts` by `make generate`, and calls the API with
  openapi-fetch and the Clerk session token. The app learns its Clerk
  publishable key from `GET /api/config`, so one build serves every
  environment, and says so when the console is not configured.
- **Components** come from [shadcn/ui](https://ui.shadcn.com) and are copied
  into `src/components/ui`, where they are ours to change. Add one with
  `hack/bun.sh x shadcn add <component>`.
- **Dependencies** are pinned to exact versions (`bunfig.toml` makes
  `bun add` do that) and `bun.lock` pins the rest; CI installs with
  `--frozen-lockfile`. TypeScript stays on 6.0 until typescript-eslint
  supports 7.
