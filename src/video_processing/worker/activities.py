"""Temporal activities: every piece of I/O the video pipeline performs.

The workflow only schedules; the database sessions, S3 transfers, and
ffmpeg/ffprobe subprocesses all live here. `processing.py`, `transcode.py`,
and `jobs.py` are reused unchanged -- this module is only the
Temporal-shaped boundary around them.

Each activity owns its own session, S3 download, and temporary directory,
because Temporal may run two activities of the same workflow in different
worker processes with no shared filesystem.

The failure handlers below catch `Exception`, deliberately not
`BaseException`: a worker shutdown cancels in-flight activities with
`asyncio.CancelledError`, and Temporal will re-dispatch those, so marking
the job `failed` on the way out would be a lie about a job still due to run.
"""

import tempfile
import uuid
from pathlib import Path

from aiobotocore.client import AioBaseClient
from sqlalchemy.ext.asyncio import AsyncSession
from temporalio import activity
from temporalio.exceptions import ApplicationError

from video_processing.common.config.settings import settings
from video_processing.common.db.session import SessionFactory
from video_processing.common.models.asset_type import AssetType
from video_processing.common.models.job_type import JobType
from video_processing.common.models.video import Video
from video_processing.common.storage.s3 import get_async_s3_client
from video_processing.worker.jobs import (
    claim_job,
    complete_metadata_job,
    complete_thumbnail_job,
    complete_transcode_job,
    fail_job,
)
from video_processing.worker.processing import (
    VideoMetadata,
    extract_metadata,
    generate_thumbnail,
)
from video_processing.worker.transcode import renditions_for_source_height, transcode

# Generated assets live outside "uploads/" so they never re-trigger the
# bucket's ObjectCreated notification (which only watches "uploads/").
_ASSETS_PREFIX = "assets"


async def _load_video(db: AsyncSession, video_id: uuid.UUID) -> Video:
    """Load the video row, or fail this activity permanently.

    A missing row cannot become present by waiting, so retrying it only
    burns the retry budget and delays the Failed execution an operator
    needs to see.
    """
    video = await db.get(Video, video_id)
    if video is None:
        raise ApplicationError(f"No video row for {video_id}", non_retryable=True)
    return video


async def _download_original(s3: AioBaseClient, video: Video, work_dir: Path) -> Path:
    """Fetch the uploaded original into this activity's temporary directory.

    The key comes from the stored row, not from the S3 event, because the
    only payload an activity receives is the video ID.
    """
    # ponytail: each activity downloads its own copy, up to 3x the S3 GETs of
    # worker/main.py's single shared download. Activities may run in
    # different processes, so there is no safe shared temp file. Upgrade path:
    # Temporal Sessions or sticky task-queue affinity, if egress becomes a
    # measured cost.
    original_path = work_dir / "original"
    await s3.download_file(
        settings.s3_bucket_name, video.original_object_key, str(original_path)
    )
    return original_path


def _stored_metadata(video: Video) -> VideoMetadata:
    """Rebuild metadata from the row when the metadata job is already done.

    Transcoding needs `.height`, so metadata is the one activity that still
    has to return a value on a partial retry rather than no-op'ing. A
    completed metadata job with nothing stored is a contradiction that
    waiting cannot resolve, hence non-retryable.
    """
    if video.duration_ms is None or video.width is None or video.height is None:
        raise ApplicationError(
            f"Video {video.id} has a completed metadata job but no stored metadata",
            non_retryable=True,
        )
    return VideoMetadata(
        duration_ms=video.duration_ms, width=video.width, height=video.height
    )


@activity.defn
async def extract_metadata_activity(video_id: uuid.UUID) -> VideoMetadata:
    async with SessionFactory() as db:
        video = await _load_video(db, video_id)
        job = await claim_job(db, video, JobType.METADATA)
        if job is None:
            return _stored_metadata(video)

        try:
            with tempfile.TemporaryDirectory() as work_dir_str:
                async with get_async_s3_client() as s3:
                    original_path = await _download_original(s3, video, Path(work_dir_str))
                    metadata = await extract_metadata(original_path)
            await complete_metadata_job(db, job, video, metadata)
        except Exception:
            # Only this job. A sibling activity's row is not this activity's
            # to touch -- that conflation is what worker/main.py:179 got wrong.
            await fail_job(db, job, video)
            raise

        activity.logger.info("Extracted metadata for video %s", video_id)
        return metadata


@activity.defn
async def generate_thumbnail_activity(video_id: uuid.UUID) -> None:
    async with SessionFactory() as db:
        video = await _load_video(db, video_id)
        job = await claim_job(db, video, JobType.THUMBNAIL)
        if job is None:
            return

        thumbnail_key = f"{_ASSETS_PREFIX}/{video_id}/thumbnail.jpg"
        try:
            with tempfile.TemporaryDirectory() as work_dir_str:
                work_dir = Path(work_dir_str)
                thumbnail_path = work_dir / "thumbnail.jpg"
                async with get_async_s3_client() as s3:
                    original_path = await _download_original(s3, video, work_dir)
                    await generate_thumbnail(original_path, thumbnail_path)
                    await s3.upload_file(
                        str(thumbnail_path), settings.s3_bucket_name, thumbnail_key
                    )
            await complete_thumbnail_job(db, job, video, thumbnail_key)
        except Exception:
            await fail_job(db, job, video)
            raise

        activity.logger.info("Generated thumbnail for video %s", video_id)


@activity.defn
async def transcode_activity(video_id: uuid.UUID, source_height: int) -> None:
    async with SessionFactory() as db:
        video = await _load_video(db, video_id)
        job = await claim_job(db, video, JobType.TRANSCODE)
        if job is None:
            return

        # Validated after claiming, not before: claiming first costs one
        # attempt, which is correct bookkeeping for a permanently broken
        # video, and it gives fail_job a row to mark so `video.status` reaches
        # `failed`. Raising before the claim left no row at all, so the
        # workflow closed Failed while GET /videos/{id} still read
        # "processing" and a polling client never learned to call retry.
        # Not removable either: renditions_for_source_height(0) returns [],
        # so an unchecked zero height would complete the job with no output.
        if source_height <= 0:
            await fail_job(db, job, video)
            raise ApplicationError(
                f"Video {video_id} has no usable source height ({source_height})",
                non_retryable=True,
            )

        try:
            renditions: list[tuple[AssetType, str]] = []
            with tempfile.TemporaryDirectory() as work_dir_str:
                work_dir = Path(work_dir_str)
                async with get_async_s3_client() as s3:
                    original_path = await _download_original(s3, video, work_dir)
                    # ponytail: no heartbeating, so a crashed worker mid-encode
                    # is only detected when start_to_close_timeout (30 min)
                    # expires. Upgrade path: activity.heartbeat() in this loop
                    # plus a heartbeat_timeout, if faster crash detection
                    # matters.
                    for rendition in renditions_for_source_height(source_height):
                        rendition_path = work_dir / f"{rendition.asset_type.value}.mp4"
                        await transcode(original_path, rendition_path, rendition)

                        rendition_key = (
                            f"{_ASSETS_PREFIX}/{video_id}/{rendition.asset_type.value}.mp4"
                        )
                        await s3.upload_file(
                            str(rendition_path), settings.s3_bucket_name, rendition_key
                        )
                        renditions.append((rendition.asset_type, rendition_key))
            await complete_transcode_job(db, job, video, renditions)
        except Exception:
            await fail_job(db, job, video)
            raise

        activity.logger.info("Transcoded video %s", video_id)
