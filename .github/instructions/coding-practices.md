# Coding Practices

These practices apply to application, worker, and infrastructure code in this repository.

## Boundaries

- Keep FastAPI routes thin: validate at the boundary, call a service, and return the established response model.
- Keep business rules in services and worker jobs.
- Keep persistence behind the existing repository and async session patterns.
- Keep AWS access behind the existing S3 and SQS helpers.
- Trace changes across API, database, storage, queue, worker, and infrastructure boundaries before editing.

## Python and async code

- Target Python 3.12 and follow the Ruff rules in `pyproject.toml`.
- Prefer precise types, explicit return types, and narrow exception handling.
- Use async database and AWS APIs consistently in request and worker paths.
- Avoid blocking subprocess, filesystem, database, or AWS calls in async code.
- Use UTC-aware datetimes and preserve existing UUID, enum, and response-model conventions.
- Reuse established helpers before introducing abstractions or dependencies.
- Add comments only for non-obvious decisions, invariants, or failure semantics.

## Reliability and security

- Treat S3 and SQS as at-least-once and failure-prone.
- Preserve idempotency, retry behavior, transaction boundaries, and message acknowledgement semantics.
- Commit database state only when the related external operation has succeeded.
- Keep least-privilege IAM, private networking, encrypted storage, and secret injection intact.
- Do not add credentials, `.env` values, generated artifacts, or local machine state.

## Validation

Add focused regression coverage for behavior changes and important failure paths. Use the shared CI entry point when the change spans multiple layers:

```bash
bash scripts/ci-check.sh lint
bash scripts/ci-check.sh test
```
