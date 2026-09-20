import asyncio
import inspect
import uuid
from collections.abc import Callable
from typing import Any

import pytest
from temporalio import activity
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from video_processing.worker import activities
from video_processing.worker.processing import VideoMetadata
from video_processing.worker.workflows import VideoProcessingWorkflow

_TASK_QUEUE = "test-video-processing"


async def _execute(
    stub_activities: list[Callable[..., Any]], video_id: uuid.UUID
) -> None:
    """Run the real workflow against stub activities, with time skipping.

    Time skipping is what makes the retry assertions run in milliseconds
    rather than waiting out the policy's 5s/10s backoff for real.
    """
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue=_TASK_QUEUE,
            workflows=[VideoProcessingWorkflow],
            activities=stub_activities,
        ):
            await env.client.execute_workflow(
                VideoProcessingWorkflow.run,
                video_id,
                id=str(video_id),
                task_queue=_TASK_QUEUE,
            )


async def test_metadata_and_thumbnail_run_concurrently_then_transcode():
    events: list[str] = []
    # A barrier rather than a sleep: if the workflow scheduled these serially
    # the first one would never be released, so this asserts real concurrency
    # instead of observing a lucky interleaving.
    both_started = asyncio.Barrier(2)

    @activity.defn(name="extract_metadata_activity")
    async def stub_metadata(video_id: uuid.UUID) -> VideoMetadata:
        events.append("metadata:start")
        await asyncio.wait_for(both_started.wait(), timeout=10)
        events.append("metadata:end")
        return VideoMetadata(duration_ms=5000, width=1920, height=1080)

    @activity.defn(name="generate_thumbnail_activity")
    async def stub_thumbnail(video_id: uuid.UUID) -> None:
        events.append("thumbnail:start")
        await asyncio.wait_for(both_started.wait(), timeout=10)
        events.append("thumbnail:end")

    @activity.defn(name="transcode_activity")
    async def stub_transcode(video_id: uuid.UUID, source_height: int) -> None:
        events.append(f"transcode:start(h={source_height})")

    await _execute([stub_metadata, stub_thumbnail, stub_transcode], uuid.uuid4())

    assert events[:2] == ["metadata:start", "thumbnail:start"] or events[:2] == [
        "thumbnail:start",
        "metadata:start",
    ]
    # Transcode is the only step that depends on another's output, so it must
    # start only after both siblings have settled.
    assert events[-1] == "transcode:start(h=1080)"


async def test_video_id_crosses_the_boundary_as_a_uuid():
    # The receiving parameter's annotation is what drives payload decoding --
    # an unannotated uuid.UUID silently arrives as a str. The stub declares
    # its own, so this covers the workflow's side of the round trip; the real
    # activities' annotations are covered by the signature test below.
    received: list[object] = []
    video_id = uuid.uuid4()

    @activity.defn(name="extract_metadata_activity")
    async def stub_metadata(video_id: uuid.UUID) -> VideoMetadata:
        received.append(video_id)
        return VideoMetadata(duration_ms=1, width=1, height=1)

    @activity.defn(name="generate_thumbnail_activity")
    async def stub_thumbnail(video_id: uuid.UUID) -> None:
        return None

    @activity.defn(name="transcode_activity")
    async def stub_transcode(video_id: uuid.UUID, source_height: int) -> None:
        return None

    await _execute([stub_metadata, stub_thumbnail, stub_transcode], video_id)

    assert received == [video_id]
    assert isinstance(received[0], uuid.UUID)


async def test_workflow_fails_but_thumbnail_still_completes_when_metadata_fails():
    """The single most important behavior in this migration.

    The workflow must end Failed -- swallowing the error would close it as
    Completed, and ALLOW_DUPLICATE_FAILED_ONLY would then refuse to start a
    fresh execution, leaving the video permanently un-retryable with no error
    visible anywhere. The sibling must still finish, because a job that
    succeeded is not condemned by its partner's failure.
    """
    completed: list[str] = []

    @activity.defn(name="extract_metadata_activity")
    async def stub_metadata(video_id: uuid.UUID) -> VideoMetadata:
        raise ApplicationError("no video row", non_retryable=True)

    @activity.defn(name="generate_thumbnail_activity")
    async def stub_thumbnail(video_id: uuid.UUID) -> None:
        completed.append("thumbnail")

    @activity.defn(name="transcode_activity")
    async def stub_transcode(video_id: uuid.UUID, source_height: int) -> None:
        completed.append("transcode")

    with pytest.raises(WorkflowFailureError):
        await _execute([stub_metadata, stub_thumbnail, stub_transcode], uuid.uuid4())

    assert completed == ["thumbnail"]


async def test_retryable_activity_error_is_attempted_three_times_then_fails():
    attempts: list[int] = []

    @activity.defn(name="extract_metadata_activity")
    async def stub_metadata(video_id: uuid.UUID) -> VideoMetadata:
        attempts.append(activity.info().attempt)
        raise RuntimeError("transient ffprobe failure")

    @activity.defn(name="generate_thumbnail_activity")
    async def stub_thumbnail(video_id: uuid.UUID) -> None:
        return None

    @activity.defn(name="transcode_activity")
    async def stub_transcode(video_id: uuid.UUID, source_height: int) -> None:
        return None

    with pytest.raises(WorkflowFailureError):
        await _execute([stub_metadata, stub_thumbnail, stub_transcode], uuid.uuid4())

    # maximum_attempts=3, the analog of the production queue's maxReceiveCount.
    assert attempts == [1, 2, 3]


async def test_non_retryable_activity_error_is_attempted_once():
    attempts: list[int] = []

    @activity.defn(name="extract_metadata_activity")
    async def stub_metadata(video_id: uuid.UUID) -> VideoMetadata:
        attempts.append(activity.info().attempt)
        raise ApplicationError("no video row", non_retryable=True)

    @activity.defn(name="generate_thumbnail_activity")
    async def stub_thumbnail(video_id: uuid.UUID) -> None:
        return None

    @activity.defn(name="transcode_activity")
    async def stub_transcode(video_id: uuid.UUID, source_height: int) -> None:
        return None

    with pytest.raises(WorkflowFailureError):
        await _execute([stub_metadata, stub_thumbnail, stub_transcode], uuid.uuid4())

    assert attempts == [1]


def test_every_payload_boundary_parameter_is_annotated():
    """The real activities' annotations, which no other test can reach.

    `ActivityEnvironment.run` calls the activity directly, and the stubs above
    declare their own hints, so dropping `: uuid.UUID` from `activities.py`
    would pass every other test here and hand the activity a `str` -- failing
    far away, inside `db.get(Video, "0ee7...")`.
    """
    for fn in (
        activities.extract_metadata_activity,
        activities.generate_thumbnail_activity,
        activities.transcode_activity,
        VideoProcessingWorkflow.run,
    ):
        parameters = [
            parameter
            for parameter in inspect.signature(fn).parameters.values()
            if parameter.name != "self"
        ]
        assert parameters, f"{fn.__qualname__} takes no payload"
        for parameter in parameters:
            assert parameter.annotation is not inspect.Parameter.empty, (
                f"{fn.__qualname__}.{parameter.name} is unannotated"
            )
