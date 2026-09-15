---
name: unit-tests
description: Add focused regression tests for the Python API, repositories, worker jobs, S3 event handling, and transcode behavior in this repository.
disable-model-invocation: true
---

# Unit Tests

1. Read `AGENTS.md`, the target implementation, nearby tests, and `tests/conftest.py`.
2. Test the public behavior at the narrowest useful boundary. Prefer existing fixtures, async patterns, factories, and mocked AWS clients.
3. Cover the happy path plus the relevant failure, retry, duplicate-delivery, transaction, validation, or authorization path.
4. Assert outcomes and side effects that matter to callers: response shape, persisted state, emitted work, asset keys, message deletion, and error behavior.
5. Keep tests deterministic and independent of AWS, Docker, credentials, and wall-clock timing unless the test explicitly exercises the documented end-to-end path.
6. Run the focused selector locally, then use `uv run ruff check .` and `uv run pytest` when fast feedback is useful. CI remains the authoritative required check.

Testing is complete when the regression would fail against the old behavior, passes against the new behavior, and does not rely on implementation details that the project treats as private.
