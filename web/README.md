# web

The customer-facing React app of the Changeport console: a customer's
cluster overview, topic management, consumer lag in seconds, and the
operator console. It is built with Vite, React, TypeScript, Tailwind and
shadcn/ui, and everything around it (installing, scripts, tests, builds)
runs through Bun. The console service (`cmd/console`) serves it, embedded in
its binary. The work is tracked in the Phase 2 milestone (#46 to #58); see
docs/PROJECT_PLAN.md.
