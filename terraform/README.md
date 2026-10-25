# terraform

Base infrastructure per environment: VPC, EKS, S3 and IAM. Each environment in
`environments/` lives in its own AWS account and composes the modules in
`modules/`. The modules are empty until Phase 1, and nothing has been applied.

Per-customer resources are not managed here; the Go provisioning service
creates them through the Kubernetes API.

```bash
just tf-fmt tf-validate
```
