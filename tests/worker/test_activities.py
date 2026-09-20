import uuid
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from tests.conftest import fake_async_client
from video_processing.common.db.repositories import video_repository
from video_processing.common.models.asset_type import AssetType
from video_processing.common.models.generated_asset import GeneratedAsset
from video_processing.common.models.job_status import JobStatus
from video_processing.common.models.job_type import JobType
from video_processing.common.models.processing_job import ProcessingJob
from video_processing.common.models.video import Video
from video_processing.common.models.video_status import VideoStatus
from video_processing.worker import activities
from video_processing.worker.jobs import claim_job, complete_metadata_job
from video_processing.worker.processing import VideoMetadata


@pytest.fixture
async def video(db) -> Video:
    video_id = uuid.uuid4()
    video = Video(
        id=video_id,
        filename="clip.mp4",
        # The key carries the video ID, which is what lets an S3 event be
        # correlated back to its row.
        original_object_key=f"uploads/{video_id}/original.mp4",
        status=VideoStatus.PENDING_UPLOAD,
    )
    video_repository.create(db, video)
    await db.commit()
    return video


@pytest.fixture
def fake_s3_client(monkeypatch) -> AsyncMock:
    """Replaces the S3 client at the seam the activities call through."""
    client = AsyncMock()
    monkeypatch.setattr(activities, "get_async_s3_client", lambda: fake_async_client(client))
    return client


@pytest.fixture(autouse=True)
def session_factory(db, monkeypatch) -> None:
    """Hands every activity the test's in-memory session.

    `fake_async_client` is reused because it yields the session without
    closing it on exit -- the `db` fixture owns that lifecycle, and the tests
    keep querying through the same session after the activity returns.
    """
    monkeypatch.setattr(activities, "SessionFactory", lambda: fake_async_client(db))


@pytest.fixture
def fake_ffmpeg(monkeypatch) -> None:
    """Stubs out ffprobe/ffmpeg so these tests need no media files or binaries."""
    monkeypatch.setattr(
        activities,
        "extract_metadata",
        AsyncMock(return_value=VideoMetadata(duration_ms=5000, width=1920, height=1080)),
    )
    monkeypatch.setattr(activities, "generate_thumbnail", AsyncMock())
    monkeypatch.setattr(activities, "transcode", AsyncMock())


async def _job(db, video: Video, job_type: JobType) -> ProcessingJob | None:
    return (
        await db.execute(select(ProcessingJob).filter_by(video_id=video.id, job_type=job_type))
    ).scalar_one_or_none()


class TestExtractMetadataActivity:
    async def test_persists_metadata_and_completes_the_job(
        self, db, video, fake_s3_client, fake_ffmpeg
    ):
        metadata = await ActivityEnvironment().run(
            activities.extract_metadata_activity, video.id
        )

        assert metadata == VideoMetadata(duration_ms=5000, width=1920, height=1080)
        assert video.height == 1080
        job = await _job(db, video, JobType.METADATA)
        assert job.status == JobStatus.COMPLETED

    async def test_downloads_the_original_from_the_stored_object_key(
        self, db, video, fake_s3_client, fake_ffmpeg
    ):
        await ActivityEnvironment().run(activities.extract_metadata_activity, video.id)

        bucket, key, destination = fake_s3_client.download_file.call_args[0]
        assert key == video.original_object_key
        assert Path(destination).name == "original"

    async def test_returns_stored_metadata_without_reprobing_when_already_completed(
        self, db, video, fake_s3_client, fake_ffmpeg
    ):
        # Transcode needs the height, so this activity is the one that still
        # has to return a value when its job is already done.
        job = await claim_job(db, video, JobType.METADATA)
        await complete_metadata_job(
            db, job, video, VideoMetadata(duration_ms=1234, width=640, height=480)
        )

        metadata = await ActivityEnvironment().run(
            activities.extract_metadata_activity, video.id
        )

        assert metadata == VideoMetadata(duration_ms=1234, width=640, height=480)
        activities.extract_metadata.assert_not_awaited()
        fake_s3_client.download_file.assert_not_awaited()

    async def test_non_retryable_when_the_video_row_is_missing(self, fake_s3_client):
        with pytest.raises(ApplicationError) as excinfo:
            await ActivityEnvironment().run(
                activities.extract_metadata_activity, uuid.uuid4()
            )

        assert excinfo.value.non_retryable

    async def test_non_retryable_when_a_completed_job_stored_no_metadata(
        self, db, video, fake_s3_client
    ):
        job = await claim_job(db, video, JobType.METADATA)
        job.status = JobStatus.COMPLETED
        await db.commit()

        with pytest.raises(ApplicationError) as excinfo:
            await ActivityEnvironment().run(
                activities.extract_metadata_activity, video.id
            )

        assert excinfo.value.non_retryable

    async def test_fails_only_its_own_job_and_reraises(
        self, db, video, fake_s3_client, fake_ffmpeg, monkeypatch
    ):
        monkeypatch.setattr(
            activities, "extract_metadata", AsyncMock(side_effect=RuntimeError("ffprobe blew up"))
        )

        with pytest.raises(RuntimeError):
            await ActivityEnvironment().run(
                activities.extract_metadata_activity, video.id
            )

        assert (await _job(db, video, JobType.METADATA)).status == JobStatus.FAILED
        assert video.status == VideoStatus.FAILED
        # The activity that never ran leaves no row at all, rather than a
        # "failed" row it never earned.
        assert await _job(db, video, JobType.THUMBNAIL) is None


class TestGenerateThumbnailActivity:
    async def test_uploads_the_thumbnail_and_completes_the_job(
        self, db, video, fake_s3_client, fake_ffmpeg
    ):
        await ActivityEnvironment().run(activities.generate_thumbnail_activity, video.id)

        source, bucket, key = fake_s3_client.upload_file.call_args[0]
        assert key == f"assets/{video.id}/thumbnail.jpg"
        assert Path(source).name == "thumbnail.jpg"
        assert (await _job(db, video, JobType.THUMBNAIL)).status == JobStatus.COMPLETED
        asset = (
            await db.execute(
                select(GeneratedAsset).filter_by(video_id=video.id, asset_type=AssetType.THUMBNAIL)
            )
        ).scalar_one()
        assert asset.object_key == key

    async def test_returns_immediately_when_already_completed(
        self, db, video, fake_s3_client, fake_ffmpeg
    ):
        job = await claim_job(db, video, JobType.THUMBNAIL)
        job.status = JobStatus.COMPLETED
        await db.commit()

        await ActivityEnvironment().run(activities.generate_thumbnail_activity, video.id)

        fake_s3_client.download_file.assert_not_awaited()
        activities.generate_thumbnail.assert_not_awaited()

    async def test_non_retryable_when_the_video_row_is_missing(self, fake_s3_client):
        with pytest.raises(ApplicationError) as excinfo:
            await ActivityEnvironment().run(
                activities.generate_thumbnail_activity, uuid.uuid4()
            )

        assert excinfo.value.non_retryable

    async def test_fails_only_its_own_job_and_reraises(
        self, db, video, fake_s3_client, fake_ffmpeg, monkeypatch
    ):
        monkeypatch.setattr(
            activities, "generate_thumbnail", AsyncMock(side_effect=RuntimeError("ffmpeg died"))
        )

        with pytest.raises(RuntimeError):
            await ActivityEnvironment().run(
                activities.generate_thumbnail_activity, video.id
            )

        assert (await _job(db, video, JobType.THUMBNAIL)).status == JobStatus.FAILED
        assert await _job(db, video, JobType.METADATA) is None


class TestTranscodeActivity:
    async def test_uploads_one_rendition_per_resolution_below_the_source(
        self, db, video, fake_s3_client, fake_ffmpeg
    ):
        await ActivityEnvironment().run(activities.transcode_activity, video.id, 1080)

        uploaded_keys = [call[0][2] for call in fake_s3_client.upload_file.call_args_list]
        assert uploaded_keys == [
            f"assets/{video.id}/preview_720p.mp4",
            f"assets/{video.id}/preview_480p.mp4",
        ]
        assert (await _job(db, video, JobType.TRANSCODE)).status == JobStatus.COMPLETED

    async def test_completes_with_no_renditions_when_the_source_is_smallest(
        self, db, video, fake_s3_client, fake_ffmpeg
    ):
        await ActivityEnvironment().run(activities.transcode_activity, video.id, 480)

        fake_s3_client.upload_file.assert_not_awaited()
        assert (await _job(db, video, JobType.TRANSCODE)).status == JobStatus.COMPLETED

    async def test_returns_immediately_when_already_completed(
        self, db, video, fake_s3_client, fake_ffmpeg
    ):
        job = await claim_job(db, video, JobType.TRANSCODE)
        job.status = JobStatus.COMPLETED
        await db.commit()

        await ActivityEnvironment().run(activities.transcode_activity, video.id, 1080)

        fake_s3_client.download_file.assert_not_awaited()
        activities.transcode.assert_not_awaited()

    async def test_non_retryable_when_the_video_row_is_missing(self, fake_s3_client):
        with pytest.raises(ApplicationError) as excinfo:
            await ActivityEnvironment().run(
                activities.transcode_activity, uuid.uuid4(), 1080
            )

        assert excinfo.value.non_retryable

    async def test_marks_the_video_failed_when_the_source_height_is_unusable(
        self, db, video, fake_s3_client
    ):
        with pytest.raises(ApplicationError) as excinfo:
            await ActivityEnvironment().run(activities.transcode_activity, video.id, 0)

        assert excinfo.value.non_retryable
        # Claimed first, so fail_job has a row to mark and the video reaches
        # "failed". Raising before the claim left no row, which stranded the
        # video reading "processing" while the workflow closed Failed.
        job = await _job(db, video, JobType.TRANSCODE)
        assert job is not None
        assert job.status == JobStatus.FAILED
        await db.refresh(video)
        assert video.status == VideoStatus.FAILED

    async def test_fails_only_its_own_job_and_reraises(
        self, db, video, fake_s3_client, fake_ffmpeg, monkeypatch
    ):
        monkeypatch.setattr(
            activities, "transcode", AsyncMock(side_effect=RuntimeError("encode died"))
        )

        with pytest.raises(RuntimeError):
            await ActivityEnvironment().run(activities.transcode_activity, video.id, 1080)

        assert (await _job(db, video, JobType.TRANSCODE)).status == JobStatus.FAILED
        assert await _job(db, video, JobType.METADATA) is None
