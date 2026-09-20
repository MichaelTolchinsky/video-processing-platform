"""SQS shim: turns upload notifications into workflow executions.

Replaces the body of `worker/main.py`'s poll loop, keeping its receive /
gather / delete-on-success shape. This process runs no ffmpeg, moves no video
bytes, and makes one short database read per message, so it can be sized
small and restarted freely. It is the only component that talks to both SQS
and Temporal.
"""

import asyncio
import logging
import uuid
from typing import Any

from aiobotocore.client import AioBaseClient
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from video_processing.common.config.settings import settings
from video_processing.common.db.session import SessionFactory
from video_processing.common.models.video import Video
from video_processing.common.queue.s3_events import (
    parse_object_created_events,
    parse_video_id_from_key,
)
from video_processing.common.queue.sqs import get_async_sqs_client
from video_processing.common.temporal.client import get_temporal_client
from video_processing.worker.workflows import VideoProcessingWorkflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def _start_processing(client: Client, video_id: uuid.UUID) -> None:
    """Start this video's workflow, tolerating a duplicate delivery.

    The workflow ID is the video ID, which is the entire dedup mechanism.
    Both policies are set explicitly because they govern different
    situations, and only the pair covers every case.
    """
    try:
        await client.start_workflow(
            VideoProcessingWorkflow.run,
            video_id,
            id=str(video_id),
            task_queue=settings.temporal_task_queue,
            # Closed executions: only a Failed/Cancelled/TimedOut one may be
            # reused. That is what lets POST /retry work through this same
            # call with no special-casing.
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
            # Open executions: reject rather than join, so both duplicate
            # cases raise the same error and need one handler. FAIL is what
            # UNSPECIFIED maps to server-side, but the whole dedup story
            # should not rest on a default that can change on an upgrade.
            id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
        )
    except WorkflowAlreadyStartedError:
        # Already running, or already completed successfully. Either way this
        # delivery is a duplicate and the message should be acknowledged.
        logger.info("Workflow already exists for video %s; treating as duplicate", video_id)


async def _resolve_video_id(object_key: str) -> uuid.UUID | None:
    """Recover the video this object key belongs to, and verify it.

    Returns None when the event can never become valid, so the caller
    acknowledges the message rather than having SQS redeliver it forever.
    """
    video_id = parse_video_id_from_key(object_key)
    if video_id is None:
        logger.warning("Ignoring unrecognized object key: %s", object_key)
        return None

    async with SessionFactory() as db:
        video = await db.get(Video, video_id)
        # Compare against the artifact-free key (see s3_events.py) so a
        # legitimate match is still recognized locally.
        if video is None or object_key.removesuffix("\\") != video.original_object_key:
            # Defends against a stale or unexpected event that doesn't match
            # a known video record and its upload key.
            logger.warning("No matching video for key: %s", object_key)
            return None

    return video_id


async def process_message(client: Client, message: dict[str, Any]) -> None:
    for event in parse_object_created_events(message["Body"]):
        video_id = await _resolve_video_id(event.key)
        if video_id is None:
            continue
        await _start_processing(client, video_id)


async def _handle_message(
    sqs: AioBaseClient, client: Client, message: dict[str, Any]
) -> None:
    """Start the workflow for one message and delete it on success.

    Isolated per-message (own try/except) so `asyncio.gather` over a batch
    lets the rest of the batch through even if this one fails.

    Anything other than a duplicate leaves the message undeleted, so SQS
    makes it visible again after the visibility timeout. That rule is what
    makes `Client.connect`'s fail-late behavior safe: it succeeds instantly
    against a namespace that does not exist, and the resulting per-message
    `RPCError` then delays processing rather than dropping it.
    """
    try:
        await process_message(client, message)
    except Exception:
        logger.exception(
            "Failed to start processing for message %s", message.get("MessageId")
        )
        return

    await sqs.delete_message(
        QueueUrl=settings.sqs_queue_url,
        ReceiptHandle=message["ReceiptHandle"],
    )


async def run() -> None:
    client = await get_temporal_client()
    logger.info("SQS shim started; polling %s", settings.sqs_queue_url)

    async with get_async_sqs_client() as sqs:
        while True:
            response = await sqs.receive_message(
                QueueUrl=settings.sqs_queue_url,
                # Fixed at SQS's own per-call cap. Unlike the old poll loop
                # this is not gated by worker_concurrency: starting a
                # workflow holds no ffmpeg or CPU budget, and that setting
                # now sizes the Temporal worker instead.
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,  # long polling, matches the queue's configuration
            )
            messages = response.get("Messages", [])
            if messages:
                await asyncio.gather(
                    *(_handle_message(sqs, client, message) for message in messages)
                )


if __name__ == "__main__":
    asyncio.run(run())
