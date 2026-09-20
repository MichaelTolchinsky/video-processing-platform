# Local Development

Full local setup and commands for this project. See [`readme.md`](../readme.md) for the project overview and [`docs/ARCHITECTURE.md`](ARCHITECTURE.md) for design details.

Everything runs in Docker Compose - API, `sqs-shim`, `temporal-worker`, a self-hosted Temporal server + UI, PostgreSQL, and a Floci container standing in for S3 + SQS. No AWS account or credentials are needed for local development.

## Prerequisites

- Docker and Docker Compose
- `curl` (or Postman) for exercising the API
- A small local `.mp4` file to upload for testing

## Start the stack

**First time only** - create Temporal's schema and the `default` namespace (Temporal's server refuses to start without the former, and `start_workflow` fails without the latter):

```bash
docker compose up -d temporal-db
docker compose run --rm temporal-admin-tools temporal-sql-tool create-database
docker compose run --rm temporal-admin-tools temporal-sql-tool setup-schema -v 0.0
docker compose run --rm temporal-admin-tools temporal-sql-tool update-schema -d /etc/temporal/schema/postgresql/v12/temporal/versioned
docker compose run --rm temporal-admin-tools temporal-sql-tool --db temporal_visibility create-database
docker compose run --rm temporal-admin-tools temporal-sql-tool --db temporal_visibility setup-schema -v 0.0
docker compose run --rm temporal-admin-tools temporal-sql-tool --db temporal_visibility update-schema -d /etc/temporal/schema/postgresql/v12/visibility/versioned
docker compose up -d temporal
docker compose run --rm temporal-admin-tools temporal operator namespace create -n default --address temporal:7233
```

Then, every time:

```bash
docker compose up --build
```

This starts:

- `api` — FastAPI on `http://localhost:8000`
- `sqs-shim` — reads the processing queue, starts a workflow per upload (no exposed port)
- `temporal-worker` — executes the workflow's activities: ffprobe, ffmpeg thumbnail, ffmpeg transcode (no exposed port)
- `temporal` — self-hosted Temporal server, gRPC on `localhost:7233`
- `temporal-ui` — Temporal Web UI on `http://localhost:8080`, useful for inspecting a workflow's execution history
- `db` — PostgreSQL 16 on `localhost:5432`
- `temporal-db` — PostgreSQL 16 backing the Temporal server, not exposed
- `floci` - S3 + SQS on `http://localhost:4566`, with the `video-processing-local` bucket and queue created automatically (see `floci/init-s3.sh` and `floci/init-sqs.sh`), including the bucket's `uploads/` -> queue notification wiring that mirrors production.

## Run the test suite

Unit and integration tests run against an in-memory SQLite database and mocked S3 calls — no Docker/AWS needed:

```bash
uv run pytest
```

Covers repositories (`common/db/repositories/`), worker job idempotency/retry logic (`worker/jobs.py`), the workflow and activities (`worker/workflows.py`, `worker/activities.py`), the API service layer, and HTTP-level route behavior (via FastAPI's `TestClient`). It does not replace the [end-to-end test](#end-to-end-test) below, which is the only check that exercises real ffmpeg/ffprobe and a real Temporal server.

## Apply database migrations

The API image doesn't run migrations on startup by design (so a bad migration can't block every container from starting). Run them explicitly:

```bash
docker compose run --rm -v "$PWD:/workspace" -w /workspace api alembic upgrade head
```

The `-v`/`-w` flags mount the project root, since the built image's default `/app` only contains the installed package, not the Alembic project files.

Create a new migration after changing a model:

```bash
docker compose run --rm -v "$PWD:/workspace" -w /workspace api alembic revision --autogenerate -m "describe the change"
```

Always review the generated migration file before applying it -- autogenerate is known to be unreliable specifically for the enum columns in this project (`common/models/enum_type.py` uses `create_constraint=True`, an anonymous CHECK constraint, not a native Postgres enum). When an enum gains/loses values, autogenerate tends to emit a same-named `ADD CONSTRAINT` without dropping the old one first, which fails outright; it also won't add a data-fix for rows still holding a removed value, which a straight `ADD CONSTRAINT` would otherwise reject. Expect to hand-write explicit `op.drop_constraint(...)` / `op.create_check_constraint(...)` (and an `op.execute("UPDATE ...")` data-fix if a value was renamed/removed) rather than accepting the generated file as-is.

## End-to-end test

**1. Create the upload record**

```bash
curl -X POST http://localhost:8000/videos -H 'Content-Type: application/json' -d '{"filename":"salmon.mp4","content_type":"video/mp4"}'
```

Copy `id` and `upload_url` from the response.

**2. Upload the file directly to S3**

```bash
curl -X PUT --upload-file ./salmon.mp4 -H 'Content-Type: video/mp4' 'PASTE_UPLOAD_URL_HERE'
```

This is what triggers the S3 → SQS event `sqs-shim` consumes and turns into a workflow start.

**3. Watch it process**

```bash
docker compose logs -f sqs-shim temporal-worker
```

Expect `sqs-shim` to log the workflow start, then `temporal-worker` to log each activity completing, within a few seconds. Optionally, open `http://localhost:8080` and find the workflow execution by its ID (the video's `id`) to inspect its activity history directly.

**4. Check the result**

```bash
curl http://localhost:8000/videos/PASTE_VIDEO_ID
```

Expect `status: "completed"`, populated `metadata`, and generated assets: `thumbnail`, plus a `preview_{height}p` rendition for each standard resolution (1080p/720p/480p) strictly below the source's height -- e.g. a 1080p source produces `preview_720p` and `preview_480p` but not a redundant `preview_1080p`. Each asset has a working `download_url`.

**5. Retry a failed video (optional)**

If a video's `status` ends up `"failed"` (e.g. after killing `temporal-worker` mid-job), re-drive it without re-uploading:

```bash
curl -X POST http://localhost:8000/videos/PASTE_VIDEO_ID/retry
```

Returns 409 if the video isn't currently `"failed"`. Re-publishes the original upload's S3 event to the queue; `sqs-shim` starts a fresh workflow execution under the same workflow ID, so only the jobs that didn't complete are re-run.

**6. Inspect Floci directly (optional)**

```bash
# List uploaded originals
docker compose exec floci aws s3 ls s3://video-processing-local/uploads/ --recursive

# List generated assets
docker compose exec floci aws s3 ls s3://video-processing-local/assets/ --recursive

# Peek at a queue message without consuming it (useful when debugging the worker)
docker compose exec floci aws sqs receive-message \
  --queue-url http://localhost:4566/000000000000/video-processing-local \
  --max-number-of-messages 1 --visibility-timeout 3600
```

## Load testing

`k6` (or any HTTP load tool) against the running compose stack, e.g.:

```bash
k6 run --vus 25 --duration 30s - <<'EOF'
import http from "k6/http";
export default function () {
  http.get("http://localhost:8000/videos/PASTE_VIDEO_ID");
}
EOF
```

**Findings against this local setup** (single uvicorn process, `db.t4g.micro`-equivalent pool sizing): comfortable up to ~80 concurrent users (0% errors, p95 ~120ms, ~550 req/s). The real ceiling is the API's DB connection pool (`common/db/session.py`, `DB_POOL_SIZE`/`DB_MAX_OVERFLOW` env vars — 20 max), not CPU or memory. The Temporal migration doesn't touch the API or its pool, so this ceiling is unchanged by it. Past that ceiling, the pool's short `pool_timeout` (`DB_POOL_TIMEOUT`, 5s on the API) fails requests fast with a `503` instead of every request queuing for the default 30s.

At extreme, unrealistic overload (10x+ the pool's capacity, e.g. 300+ concurrent against a 20-connection pool) some requests can hang indefinitely rather than fail cleanly — a Starlette sync-threadpool + disconnected-client interaction, not something worth solving at the app level for this project's scope. In production this self-heals via the ALB's `/health/ready` check cycling an unresponsive ECS task; locally it needs a manual `docker compose restart api`. Chasing a fully graceful app-level fix for a load level far beyond this project's realistic traffic wasn't worth the added complexity (an attempted DB-side `idle_in_transaction_session_timeout` fix was tried and reverted — it traded a clear failure mode for a more confusing one without actually solving it).

**Against the deployed AWS stack** (single small Fargate task + `db.t4g.micro`-class RDS), the ceiling is much lower: 0% errors up to ~60 concurrent (p95 ~700ms), climbing to ~29% timeout failures by 150-250 concurrent. Same root cause as local -- DB pool exhaustion, just reached sooner with less real capacity behind it. The fix there is more pool/RDS capacity or ECS desired-count, not application code.

**Bumped to "small startup" pool sizes** (API `DB_POOL_SIZE=30`/`DB_MAX_OVERFLOW=20`, worker `WORKER_CONCURRENCY=4`) against the same local stack: 0% errors up to ~700 concurrent (~1480 req/s, p95 ~756ms), degrading to ~14% failures by 1500 concurrent, with throughput plateauing around ~1450 req/s. That plateau is a different, higher-order bottleneck now (single uvicorn process/CPU core), not the DB pool -- confirming async paid off here by letting one process hold far more concurrent in-flight requests against a bigger pool, not by making any single request faster.

**Retry/failure behavior**, verified with two real scenarios against the Temporal path: a permanently-broken upload correctly exhausts the activity retry policy (3 attempts, ~5s/~10s backoff), `attempts` incrementing on the `ProcessingJob` row (1->2->3) as the same activity is retried, and the workflow execution ends **Failed** in the Temporal UI; `POST /videos/{id}/retry` then starts a fresh execution under the same workflow ID and `attempts` continues climbing (4->5->6) from where it left off. A `temporal-worker` killed mid-transcode correctly left `metadata`/`thumbnail` completed and only `transcode` incomplete (its `ProcessingJob` row stuck `processing`). Recovery was not prompt, though: `transcode_activity` sets no heartbeat (see the `ponytail:` comment in `activities.py`), so Temporal has no way to notice the worker died until the activity's `startToCloseTimeout` (30 minutes) elapses -- only then did it retry the activity (attempt 2) onto the restarted worker. It completed cleanly at that point, with exactly one row per asset type in `generated_assets` (no duplicate renditions), confirming the idempotent-resume design holds under a real crash, but recovery time for a stuck transcode is bounded by that 30-minute timeout, not by how fast the worker comes back up. `activity.heartbeat()` is the upgrade path if faster crash detection ever matters here.

## Estimated scale

Rough, honest numbers for planning purposes -- not a guarantee, and the worker side in particular is a methodology, not a measured throughput, since video length/resolution varies too much for one number to mean anything.

**API (read/status endpoints):**

| Setup | Concurrent users @ 0% errors | Throughput | Bottleneck |
|---|---|---|---|
| Local, default pool (`DB_POOL_SIZE=10`/`+10` overflow) | ~80 | ~550 req/s | DB pool |
| Local, "small startup" pool (`30`/`+20`) | ~700 | ~1480 req/s | Single uvicorn process/CPU core |
| Deployed AWS, 1 Fargate task + `db.t4g.micro` | ~60 | -- | DB pool |

Extrapolating to a realistic small-startup deployment (2-3 Fargate API tasks behind the ALB, `db.t4g.small`/`medium` RDS, pool sized like the "small startup" row per task): comfortably **several hundred concurrent users / low thousands of req/s** for status/read traffic, scaling roughly linearly with task count until Postgres itself (not the app) becomes the limit -- which needs its own headroom check (`db.t4g.small` tops out around 100-200 native connections, so pool size per task x task count must stay under that with margin).

**Worker (video processing):**

No single "videos/hour" number is honest here -- a 10s clip and a 45-minute 4K upload cost wildly different ffmpeg time. The model instead: each `temporal-worker` task processes up to `WORKER_CONCURRENCY` activities in parallel (`Worker(max_concurrent_activities=...)`, default 4 locally), bounded by the task's vCPUs (`cpu=1024` = 1 vCPU currently -- true parallel encoding needs `cpu` raised alongside `WORKER_CONCURRENCY`, or concurrency mostly just overlaps I/O with another activity's CPU-bound encode instead of running both at once). Note this now counts activity slots, not videos: one video in flight can occupy up to 3 slots at once (metadata + thumbnail concurrently, then transcode), so the same `WORKER_CONCURRENCY` value processes fewer videos in parallel than it did under the old per-video worker loop. Real throughput = `WORKER_CONCURRENCY x (task vCPUs / 1) x (3600 / avg_seconds_per_video)`, and scales further by adding more `temporal-worker` tasks (fully independent -- Temporal's task queue handles distribution with no coordination needed). Sizing this for real needs a measured `avg_seconds_per_video` against representative uploads -- worth doing before trusting any specific videos/hour figure.

## Async API (DB + S3/SQS)

The API service (routes -> `video_service` -> repositories -> DB) is fully async: SQLAlchemy's `AsyncSession` over `asyncpg`, and `aioboto3` for the API's S3 presigning/SQS calls. This was a deliberate learning/production-realism exercise (matching how bigger async FastAPI services are typically built), **not a fix for the load-test ceiling above** -- the bottleneck is the DB connection pool, and async doesn't create more pool connections or make Postgres answer faster. Confirmed by design, not just asserted: the numbers above were unaffected by this migration.

`temporal-worker` is fully async too: `ffmpeg`/`ffprobe` run via `asyncio.create_subprocess_exec` (not blocking `subprocess.run`), S3 downloads/uploads use the async `get_async_s3_client`, and the Temporal `Worker` dispatches up to `max_concurrent_activities` (`WORKER_CONCURRENCY`, default 4 locally) activities concurrently -- each is a separate ffmpeg/ffprobe OS process, so this is genuine parallelism, not just interleaved I/O waits. Bump `WORKER_CONCURRENCY` alongside the Fargate task's `cpu` (currently 1024 = 1 vCPU) and `DB_POOL_SIZE` (must cover one connection per concurrently-running activity, since every running activity holds a connection for its full duration) together -- raising one without the others either starves the pool or just adds context-switching with no real throughput gain. `sqs-shim` is a thin async consumer of its own: `receive_message` (long poll, batch up to 10) then one database read and one `start_workflow` call per message, processed concurrently via `asyncio.gather`.

Notable friction hit along the way: `aioboto3` pins `boto3` to a specific compatible range and lags official `boto3` releases, forcing a `boto3` downgrade (`1.43.51` -> `1.40.61`) when it was added -- a recurring real-world cost of this dependency, not a one-time fix.

## Everyday commands

```bash
# Rebuild after a dependency change (pyproject.toml / uv.lock)
docker compose up --build

# Follow logs for one service
docker compose logs -f api
docker compose logs -f sqs-shim temporal-worker

# Restart just one service after a code change (api/sqs-shim/temporal-worker source
# is bind-mounted, so most changes don't need a rebuild — api runs with --reload;
# the other two need a restart)
docker compose restart temporal-worker

# Stop everything (keeps the postgres_data volume)
docker compose down

# Stop and wipe the database too (fresh start)
docker compose down -v
```

## Managing Python dependencies

Dependencies are managed with `uv` and locked in `uv.lock`:

```bash
uv add <package>       # add a runtime dependency
uv sync                # install/update the local .venv from the lockfile
```

The root `.venv` is for editor/IDE support only (import resolution, linting) — the application itself always runs inside Docker. `infra/.venv` is separate and only used for CDK commands.
