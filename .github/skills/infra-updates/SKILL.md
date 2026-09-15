---
name: infra-updates
description: Safely change AWS CDK, Docker, deployment workflows, IAM, networking, ECS, RDS, S3, SQS, or migration delivery for this repository.
disable-model-invocation: true
---

# Infrastructure Updates

1. Read `AGENTS.md`, the relevant sections of `docs/ARCHITECTURE.md` and `docs/DEVELOPMENT.md`, the affected CDK stacks, and `.github/workflows/deploy.yml`.
2. Identify the trust boundary, resource ownership, data path, deployment order, permissions, failure mode, and rollback or recovery path before editing.
3. Preserve least-privilege IAM, private application and database networking, encrypted storage, secret injection, health checks, and explicit migration execution.
4. Keep application, container, CDK, workflow, configuration, and documentation changes consistent. Treat schema changes and deployment ordering as one change.
5. Validate CDK synthesis or the smallest existing infrastructure check, inspect the generated diff or template, and run affected application tests.
6. Call out irreversible actions, data migration requirements, cost changes, and production assumptions in the PR description.

An infrastructure change is complete when the resource graph, permissions, deployment path, failure handling, documentation, and validation evidence agree.
