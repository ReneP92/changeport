# Changeport

Managed Kafka service on AWS for teams new to Kafka, starting with a
dedicated cluster per customer and going multi-tenant at about 50 paying
customers (Phase 6); long-term goal is an Artie-style CDC product.

- Full project plan: docs/PROJECT_PLAN.md (read it before planning work)
- Stack: Go backend, React frontend, EKS, Strimzi (KRaft), Terraform, VictoriaMetrics
- Current phase: Phase 0 (design and user research)
- Milestones and issues: GitHub milestones on ReneP92/changeport, one per roadmap phase
- Local development: `make dev-up` (kind + Strimzi + demo customer), `make e2e`, `make lint test`; `make help` lists every target
