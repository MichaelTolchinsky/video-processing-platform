# Repository Instructions

These instructions apply to all work in this repository. Keep changes small, explicit, and easy to review. Treat the repository configuration and the linked documentation as the source of truth.

## Before changing code

1. Read the relevant code and tests before editing.
2. For API, worker, database, or infrastructure changes, read the applicable sections of [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
3. For local commands, migrations, Docker, and end-to-end behavior, use [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md).
4. Trace the complete data flow across API, database, S3, SQS, worker, and infrastructure when a change crosses a service boundary.
5. Preserve unrelated work already present in the working tree.

## Project shape

Use this map to find the right boundary before changing code:

- Python package code lives under `src/video_processing/`.
- Tests live under `tests/` and mirror API, common, and worker responsibilities.
- AWS CDK infrastructure lives under `infra/`.
- Alembic migrations live under `migrations/versions/`.
- Local integration dependencies are defined in `docker-compose.yaml`, with Floci providing S3 and SQS.
- Dependencies and tool configuration are defined in `pyproject.toml` and locked in `uv.lock`.

Runtime boundaries are intentional: routes validate and delegate, services coordinate domain behavior, repositories own database access, S3/SQS helpers own AWS interactions, worker jobs process work idempotently, and CDK stacks define production resources.

## Referenced practices

For application, worker, and infrastructure coding conventions, read
[`coding-practices.md`](.github/instructions/coding-practices.md).

For SQLAlchemy, Alembic, schema compatibility, and deployment ordering, read
[`database-migrations.md`](.github/instructions/database-migrations.md).

## Validation

CI is the authoritative merge gate for linting and tests. The pull-request workflow runs Ruff and pytest, and the repository ruleset requires both checks. Run the smallest relevant checks locally when practical for faster feedback. The standard Python checks are:

```bash
uv run ruff check .
uv run pytest
```

Use the Docker-based checks in [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md) when the change affects containers, Floci, ffmpeg/ffprobe, migrations, or the full worker flow. Check the infrastructure separately when changing `infra/`.

Every behavior change should have focused regression coverage unless the behavior is only wiring or configuration. Tests should verify the public behavior and important failure paths, not implementation details.

## Git and pull requests

- Work on a branch whose name starts with `michael/`.
- Changes reach `main` through a pull request. Keep commits focused and describe the behavior or decision they introduce.
- Before opening a pull request, inspect the diff, run the relevant validation, and mention migration or deployment implications.
- Never put credentials, tokens, `.env` values, generated artifacts, or local machine state into commits.
- Keep commit authorship personal and accurate. Never add a `Co-authored-by: Copilot` trailer or attribute Copilot as an author.
- Treat deployment workflow and infrastructure changes as production-impacting. Review IAM, network exposure, data migration, rollback, and secret handling explicitly.

## Completion criteria

A change is complete when the implementation, tests, documentation, migrations, and infrastructure wiring affected by the change are all updated consistently; the relevant checks pass; and the resulting diff contains no unrelated edits.
