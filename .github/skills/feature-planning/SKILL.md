---
name: feature-planning
description: Plan repository features before implementation using the project architecture, data flows, acceptance criteria, risks, and validation strategy.
disable-model-invocation: true
---

# Feature Planning

Use this skill before implementing a non-trivial feature.

1. Read `AGENTS.md`, the relevant sections of `docs/ARCHITECTURE.md` and `docs/DEVELOPMENT.md`, and the affected code and tests.
2. Define the user-visible outcome, scope, constraints, assumptions, and acceptance criteria.
3. Trace the affected API, database, S3, SQS, worker, and infrastructure boundaries end to end.
4. Identify schema, migration, compatibility, retry, idempotency, security, observability, and deployment concerns.
5. Choose the smallest coherent design that fits existing patterns. Name alternatives only when they materially change risk or complexity.
6. Produce an ordered implementation plan with affected files, tests, documentation, and rollback or recovery considerations.

The plan is complete when every acceptance criterion maps to an implementation step and a validation step, and no affected boundary is left implicit.
