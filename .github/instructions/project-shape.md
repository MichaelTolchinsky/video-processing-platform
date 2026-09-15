# Project Shape

Use this map to find the right boundary before changing code:

- Python package code lives under `src/video_processing/`.
- Tests live under `tests/` and mirror API, common, and worker responsibilities.
- AWS CDK infrastructure lives under `infra/`.
- Alembic migrations live under `migrations/versions/`.
- Local integration dependencies are defined in `docker-compose.yaml`, with Floci providing S3 and SQS.
- Dependencies and tool configuration are defined in `pyproject.toml` and locked in `uv.lock`.

The main runtime boundaries are:

- FastAPI routes validate requests and delegate to services.
- Services coordinate domain behavior and transaction boundaries.
- Repositories own database queries through the async session.
- S3 and SQS helpers own AWS client interactions.
- Worker jobs claim and process work idempotently.
- CDK stacks define production resources and deployment wiring.
