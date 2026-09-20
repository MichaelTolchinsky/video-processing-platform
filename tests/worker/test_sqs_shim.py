import asyncio
import uuid
from unittest.mock import AsyncMock

import pytest
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from tests.conftest import fake_async_client, fake_temporal_client
from video_processing.common.db.repositories import video_repository
from video_processing.common.models.video import Video
from video_processing.common.models.video_status import VideoStatus
from video_processing.common.queue.s3_events import build_object_created_message
from video_processing.worker import sqs_shim
from video_processing.worker.workflows import VideoProcessingWorkflow


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


@pytest.fixture(autouse=True)
def session_factory(db, monkeypatch) -> None:
    """Hands the shim's key check the test's in-memory session."""
    monkeypatch.setattr(sqs_shim, "SessionFactory", lambda: fake_async_client(db))


@pytest.fixture
def fake_sqs_client() -> AsyncMock:
    return AsyncMock()


def _message(object_key: str) -> dict[str, str]:
    return {
        "MessageId": "message-1",
        "ReceiptHandle": "receipt-1",
        "Body": build_object_created_message("test-bucket", object_key),
    }


async def test_starts_the_workflow_and_deletes_the_message(video, fake_sqs_client):
    client = fake_temporal_client()

    await sqs_shim._handle_message(
        fake_sqs_client, client, _message(video.original_object_key)
    )

    args, kwargs = client.start_workflow.call_args
    assert args == (VideoProcessingWorkflow.run, video.id)
    assert kwargs["id"] == str(video.id)
    assert kwargs["id_reuse_policy"] == WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY
    assert kwargs["id_conflict_policy"] == WorkflowIDConflictPolicy.FAIL
    fake_sqs_client.delete_message.assert_awaited_once()
    assert fake_sqs_client.delete_message.call_args.kwargs["ReceiptHandle"] == "receipt-1"


async def test_unrecognized_key_is_acknowledged_without_starting_a_workflow(fake_sqs_client):
    client = fake_temporal_client()

    # An object key that can never map to a video ID will not become valid by
    # being redelivered, so the message is acknowledged.
    await sqs_shim._handle_message(fake_sqs_client, client, _message("uploads/nonsense"))

    client.start_workflow.assert_not_awaited()
    fake_sqs_client.delete_message.assert_awaited_once()


async def test_key_that_no_video_row_matches_is_acknowledged(fake_sqs_client):
    client = fake_temporal_client()

    await sqs_shim._handle_message(
        fake_sqs_client, client, _message(f"uploads/{uuid.uuid4()}/original.mp4")
    )

    client.start_workflow.assert_not_awaited()
    fake_sqs_client.delete_message.assert_awaited_once()


async def test_key_that_disagrees_with_the_stored_key_is_acknowledged(
    db, video, fake_sqs_client
):
    client = fake_temporal_client()
    # Same video ID, different upload key: a stale or unexpected event.
    video.original_object_key = f"uploads/{video.id}/original.mov"
    await db.commit()

    await sqs_shim._handle_message(
        fake_sqs_client, client, _message(f"uploads/{video.id}/original.mp4")
    )

    client.start_workflow.assert_not_awaited()
    fake_sqs_client.delete_message.assert_awaited_once()


async def test_duplicate_workflow_is_treated_as_benign_and_acknowledged(
    video, fake_sqs_client
):
    client = fake_temporal_client()
    client.start_workflow.side_effect = WorkflowAlreadyStartedError(
        str(video.id), "VideoProcessingWorkflow"
    )

    await sqs_shim._handle_message(
        fake_sqs_client, client, _message(video.original_object_key)
    )

    fake_sqs_client.delete_message.assert_awaited_once()


async def test_any_other_error_leaves_the_message_in_the_queue(video, fake_sqs_client):
    client = fake_temporal_client()
    # e.g. Temporal unreachable, or the namespace does not exist yet -- both
    # resolve on their own, so the delivery must survive for redelivery.
    client.start_workflow.side_effect = RuntimeError("Temporal unreachable")

    await sqs_shim._handle_message(
        fake_sqs_client, client, _message(video.original_object_key)
    )

    fake_sqs_client.delete_message.assert_not_awaited()


async def test_one_failing_message_does_not_acknowledge_the_others(
    db, video, fake_sqs_client
):
    """`asyncio.gather` over a batch must not let one bad message block the rest."""
    other_id = uuid.uuid4()
    other = Video(
        id=other_id,
        filename="other.mp4",
        original_object_key=f"uploads/{other_id}/original.mp4",
        status=VideoStatus.PENDING_UPLOAD,
    )
    video_repository.create(db, other)
    await db.commit()

    client = fake_temporal_client()

    async def start_workflow(_run, started_video_id, **_kwargs) -> None:
        # Keyed on the video rather than call order, so the assertion does not
        # depend on how gather happens to interleave the two.
        if started_video_id == video.id:
            raise RuntimeError("Temporal unreachable")

    client.start_workflow.side_effect = start_workflow

    failing = _message(video.original_object_key) | {"ReceiptHandle": "receipt-failing"}
    succeeding = _message(other.original_object_key) | {"ReceiptHandle": "receipt-ok"}
    await asyncio.gather(
        sqs_shim._handle_message(fake_sqs_client, client, failing),
        sqs_shim._handle_message(fake_sqs_client, client, succeeding),
    )

    acknowledged = [
        call.kwargs["ReceiptHandle"] for call in fake_sqs_client.delete_message.call_args_list
    ]
    assert acknowledged == ["receipt-ok"]
