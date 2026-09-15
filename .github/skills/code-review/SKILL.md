---
name: repo-code-review
description: Review changes in this FastAPI, async SQLAlchemy, S3/SQS worker, and AWS CDK repository for concrete correctness, security, reliability, and maintainability issues.
disable-model-invocation: true
---

# Repository Code Review

Review the complete change set against `AGENTS.md`, `.github/instructions/coding-practices.md`, `.github/instructions/database-migrations.md` when relevant, the originating request, and the relevant architecture and development documentation. Report findings only; do not edit files unless explicitly asked to fix them.

## Review pass

1. Inspect the branch diff against `main`, including workflow, migration, documentation, and generated-file changes.
2. Trace changed behavior across route, service, repository, model, migration, AWS client, worker, infrastructure, and deployment boundaries.
3. Check public behavior and failure paths before style. A finding needs an observable impact, a file and line reference, and a concrete fix.
4. Verify focused tests cover the changed behavior and important negative cases. Check that required CI jobs and status contexts still match branch governance.
5. Separate correctness, security, reliability, and maintainability findings from preferences. Do not report issues that cannot affect behavior or violate a repository rule.

## Python and FastAPI checks

- Routes should validate input, call services, map expected domain errors, and return declared response models without owning business logic.
- Async request paths must not call blocking database, AWS, filesystem, or subprocess APIs.
- Pydantic schemas should constrain trust-boundary inputs and preserve the established response shape.
- Exceptions should be narrow and surfaced through existing HTTP, logging, and retry patterns. Flag broad catches and silent fallbacks.
- Types should be explicit and compatible with Python 3.12 and the configured Ruff rules.

## Database and distributed-system checks

- Model changes require a safe Alembic migration with reviewed upgrade and downgrade behavior.
- Enum changes must account for string-backed check constraints and existing data.
- Transactions must not commit state that claims an AWS operation succeeded when that operation failed.
- S3/SQS processing must remain idempotent under duplicate delivery, retry, reordering, worker failure, and partial job completion.
- Message deletion must happen only after successful processing, and failed work must remain observable and retryable.
- Check concurrency, connection-pool limits, timeouts, backpressure, and resource cleanup in async code.

## Infrastructure and security checks

- Check IAM for least privilege, private networking, secret handling, encryption, and trust-boundary changes.
- Check deployment ordering, migration execution, health checks, rollback or recovery behavior, and workflow permissions.
- Workflow actions must use immutable commit SHAs, required checks must match actual check-run names, and jobs need bounded execution where appropriate.
- Treat changes to S3, SQS, RDS, ECS, CDK, Docker, and deployment workflows as production-impacting.
- Confirm no credentials, `.env` values, generated artifacts, or Copilot attribution trailers were added.

## Report format

Group findings by severity:

- **Must fix**: bugs, security issues, data-loss risks, broken CI, or violated repository requirements.
- **Should fix**: concrete reliability, maintainability, test, documentation, or operational gaps.
- **Nit**: minor style or clarity issues only.

Use this format:

```text
- `path/to/file.py:42` - Finding and why it matters. Suggested fix.
```

If no actionable findings remain, say `No issues found`.

A review is complete when every changed file and affected boundary has been examined and each finding is tied to observable behavior, security, operations, or a repository rule.
