"""Temporal client seam, shared by the SQS shim and the Temporal worker.

Unlike the S3 and SQS helpers this returns an awaited `Client` rather than an
async context manager, because `Client.connect` is itself a coroutine and the
client needs no teardown -- a real API difference, not an inconsistency.

Never construct a client at import time: `Client.connect` does not validate
the namespace and succeeds instantly against one that does not exist, so the
failure has to be allowed to surface on the first real RPC inside a running
process rather than at module import.
"""

from temporalio.client import Client

from video_processing.common.config.settings import settings


async def get_temporal_client() -> Client:
    # The field is optional because Settings is shared with processes that
    # never build a client, so the address is validated here, where it is
    # actually needed, and fails loudly in the two processes that need it.
    if settings.temporal_address is None:
        raise ValueError("Set TEMPORAL_ADDRESS to the Temporal server's host:port")

    return await Client.connect(
        settings.temporal_address, namespace=settings.temporal_namespace
    )
