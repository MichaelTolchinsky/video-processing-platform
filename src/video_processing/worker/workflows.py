"""The video processing workflow: scheduling only, no I/O.

This module is re-imported by the workflow sandbox when the worker
validates the workflow, so it must be import-safe: no module-level side
effects, no `asyncio.run()`, no `Settings()` construction. A module-level
`asyncio.run()` here fails worker startup outright with
"RuntimeError: Failed validating workflow VideoProcessingWorkflow".

Workflow code also re-executes from history on every workflow task, so it
must stay deterministic: no `datetime.now()`, no `random`, no `uuid.uuid4()`,
no `asyncio.sleep`. `asyncio.gather` is safe -- Temporal runs the workflow on
its own deterministic event loop.
"""

import asyncio
import uuid
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

# Passed through rather than re-imported by the sandbox: activities.py
# transitively pulls in SQLAlchemy, aioboto3, and settings.py's import-time
# Settings() construction, none of which the workflow needs -- it only needs
# the activity names.
with workflow.unsafe.imports_passed_through():
    from video_processing.worker.activities import (
        extract_metadata_activity,
        generate_thumbnail_activity,
        transcode_activity,
    )
    from video_processing.worker.processing import VideoMetadata

# Analog of the production queue's maxReceiveCount=3
# (infra/infra/platform_stack.py:64): three attempts, then permanent failure.
# The SDK default is unlimited attempts, which is not what this project wants.
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    maximum_attempts=3,
)


@workflow.defn
class VideoProcessingWorkflow:
    @workflow.run
    async def run(self, video_id: uuid.UUID) -> None:
        # return_exceptions so a thumbnail that succeeds still records its
        # completed row when metadata fails -- both settle before either is
        # raised. This is the concrete fix for the per-job read model that
        # worker/main.py's single shared `except` made lie.
        results = await asyncio.gather(
            workflow.execute_activity(
                extract_metadata_activity,
                video_id,
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=_RETRY_POLICY,
            ),
            workflow.execute_activity(
                generate_thumbnail_activity,
                video_id,
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=_RETRY_POLICY,
            ),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                # Must propagate. A workflow that swallowed this and closed
                # as Completed would make the video permanently
                # un-retryable: ALLOW_DUPLICATE_FAILED_ONLY rejects reuse
                # against a Completed execution, so POST /retry would
                # republish, the shim would read WorkflowAlreadyStartedError
                # as a benign duplicate, and the error would vanish.
                raise result

        metadata: VideoMetadata = results[0]
        await workflow.execute_activity(
            transcode_activity,
            args=[video_id, metadata.height],
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=_RETRY_POLICY,
        )
