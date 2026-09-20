"""Temporal worker entrypoint: runs the workflow and its activities.

The `asyncio.run` lives here rather than in `workflows.py` on purpose. The
workflow sandbox re-imports the workflow's module to validate it, so a
module-level `asyncio.run()` alongside the `@workflow.defn` fails worker
startup with "RuntimeError: Failed validating workflow
VideoProcessingWorkflow".

Unlike `Client.connect`, `Worker.run()` validates the namespace up front, so
a misconfigured namespace fails this process immediately and loudly instead
of once per message.
"""

import asyncio
import logging
import signal
from datetime import timedelta

from temporalio.worker import Worker

from video_processing.common.config.settings import settings
from video_processing.common.temporal.client import get_temporal_client
from video_processing.worker.activities import (
    extract_metadata_activity,
    generate_thumbnail_activity,
    transcode_activity,
)
from video_processing.worker.workflows import VideoProcessingWorkflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Long enough for an in-flight S3 upload and its database write to land,
# short enough that `docker compose down` doesn't look hung. Activities still
# running when it expires are cancelled, and Temporal re-dispatches them.
_GRACEFUL_SHUTDOWN_TIMEOUT = timedelta(seconds=30)


async def run() -> None:
    client = await get_temporal_client()

    # `docker compose restart` and `docker compose down` both send SIGTERM,
    # so it has to reach the worker's graceful shutdown rather than killing
    # the process out from under an in-flight encode.
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for received in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(received, shutdown.set)

    async with Worker(
        client,
        task_queue=settings.temporal_task_queue,
        workflows=[VideoProcessingWorkflow],
        activities=[
            extract_metadata_activity,
            generate_thumbnail_activity,
            transcode_activity,
        ],
        max_concurrent_activities=settings.worker_concurrency,
        graceful_shutdown_timeout=_GRACEFUL_SHUTDOWN_TIMEOUT,
    ):
        logger.info(
            "Temporal worker started on task queue %s (max_concurrent_activities=%d)",
            settings.temporal_task_queue,
            settings.worker_concurrency,
        )
        await shutdown.wait()

    logger.info("Temporal worker stopped")


if __name__ == "__main__":
    asyncio.run(run())
