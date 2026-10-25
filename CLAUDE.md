# Changeport

Managed Kafka service on AWS for teams new to Kafka, starting with a
dedicated cluster per customer and going multi-tenant at about 50 paying
customers (Phase 6); long-term goal is an Artie-style CDC product.

- Full project plan: docs/PROJECT_PLAN.md (read it before planning work)
- Stack: Go backend, React frontend, EKS Auto Mode, Strimzi (KRaft), Terraform, VictoriaMetrics
- Current phase: [Phase 0: Design partner and foundations](https://github.com/ReneP92/changeport/milestone/1), due Nov 6 2026; Phase 1 (single-tenant data plane) starts Nov 9 2026
- Milestones and issues: [GitHub milestones](https://github.com/ReneP92/changeport/milestones?direction=asc&sort=due_date), one per roadmap phase; record decisions in docs/PROJECT_PLAN.md and close the matching issue
- Every customer's Kubernetes resources come from the one template in internal/provisioning/templates; never hand-write per-customer manifests
- Local development: `just dev-up` (kind + Strimzi + demo customer), `just e2e`, `just lint test`; `just` lists every recipe
