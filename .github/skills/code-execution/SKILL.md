---
name: repo-code-execution
description: Implement approved changes in this FastAPI, async SQLAlchemy, S3/SQS worker, and AWS CDK repository using its established patterns and validation commands.
disable-model-invocation: true
---

# Repository Code Execution

Use this skill for implementation work after the scope and acceptance criteria are clear.

## Workflow

1. Read `AGENTS.md`, `.github/instructions/project-shape.md`, `.github/instructions/coding-practices.md`, and, for schema work, `.github/instructions/database-migrations.md`. Then read the relevant sections of `docs/ARCHITECTURE.md` and `docs/DEVELOPMENT.md`, and inspect the affected code and nearby tests.
2. Trace the change across every affected boundary: FastAPI route, service, repository, database model/migration, S3/SQS client, worker job, Docker image, CDK stack, and deployment workflow.
3. Reuse existing modules and seams. Keep route handlers thin, business rules in services, persistence in repositories, and AWS access behind the existing storage and queue helpers.
4. Implement typed, explicit async code. Match the surrounding `AsyncSession`, `async with` client, Pydantic response-model, and `Annotated` dependency patterns.
5. Preserve distributed-system behavior: S3/SQS delivery is at-least-once, messages must be deleted only after successful processing, retries must be idempotent, and partial job completion must remain resumable.
6. For model changes, create an Alembic migration and manually review generated SQL. Treat enum check constraints and data fixes explicitly.
7. Add focused regression tests for public behavior and important failure paths. Reuse the in-memory async SQLite fixture and AWS client fakes unless the documented Docker end-to-end path is required.
8. Update architecture or development documentation when behavior, operations, deployment order, or infrastructure changes.
9. Run the smallest relevant checks, then the shared CI commands when the change spans multiple areas:

```bash
bash scripts/ci-check.sh lint
bash scripts/ci-check.sh test
```

## Python and FastAPI standards

- Target Python 3.12 and follow the Ruff configuration in `pyproject.toml`.
- Prefer precise types, `collections.abc` interfaces, explicit return types, and narrow exception handling.
- Use UTC-aware datetimes and preserve the project’s UUID, enum, and response-model conventions.
- Validate at the API boundary with Pydantic and map expected domain failures to the established HTTP responses.
- Keep transactions explicit. Commit only after dependent AWS operations succeed when a failed external call would leave unusable database state.
- Avoid blocking subprocesses, synchronous database calls, and synchronous AWS clients in async request or worker paths.
- Keep comments focused on non-obvious invariants, failure semantics, or deliberate tradeoffs.

## Delivery rules

- Keep the diff surgical and preserve unrelated work.
- Use a branch beginning with `michael/`.
- Keep commit authorship personal and accurate. Never add a `Co-authored-by: Copilot` trailer.
- Do not add credentials, `.env` values, generated artifacts, or local machine state.

Execution is complete when the requested behavior is wired through all affected layers, focused validation passes, documentation and migrations agree with the implementation, and the diff is ready for review.
