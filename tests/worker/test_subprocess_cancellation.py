"""Both ffmpeg/ffprobe subprocess sites kill their child when cancelled.

A worker shutdown cancels in-flight activities, and asyncio does not reap a
subprocess whose awaiting coroutine was cancelled -- verified: the child
keeps running. Orphaning a 30-minute encode holds a CPU core and its temp
directory after the container is gone, so both call sites guard against it.

These tests need no real ffmpeg: a fake, never-exiting `ffmpeg`/`ffprobe` is
put at the front of PATH instead, which is also what keeps them honest about
the child actually being gone rather than just unreferenced.
"""

import asyncio
import os
from pathlib import Path

import pytest

from video_processing.common.models.asset_type import AssetType
from video_processing.worker.processing import _run
from video_processing.worker.transcode import Rendition, transcode


@pytest.fixture
def hanging_binaries(tmp_path, monkeypatch) -> Path:
    """Shadows ffmpeg/ffprobe with a script that records its PID and hangs.

    `exec` keeps the shell's PID, so the recorded PID is the one asyncio
    spawned and the one the guard has to kill.
    """
    pid_file = tmp_path / "child.pid"
    for name in ("ffmpeg", "ffprobe"):
        script = tmp_path / name
        script.write_text(f'#!/bin/sh\necho $$ > "{pid_file}"\nexec sleep 300\n')
        script.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    return pid_file


async def _cancel_once_started(task: asyncio.Task, pid_file: Path) -> int:
    """Wait for the child to record its PID, then cancel the task."""
    for _ in range(100):
        if pid_file.exists() and pid_file.read_text().strip():
            break
        await asyncio.sleep(0.02)
    else:
        pytest.fail("the fake binary never started")

    pid = int(pid_file.read_text())
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    return pid


def _assert_reaped(pid: int) -> None:
    # The guard's `await process.wait()` reaps the child, so the PID is gone
    # rather than lingering as a zombie.
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


async def test_run_kills_its_child_when_cancelled(hanging_binaries):
    task = asyncio.create_task(_run("ffprobe", "-version"))

    pid = await _cancel_once_started(task, hanging_binaries)

    _assert_reaped(pid)


async def test_transcode_kills_ffmpeg_when_cancelled(tmp_path, hanging_binaries):
    rendition = Rendition(AssetType.PREVIEW_720P, height=720, bitrate_kbps=2500)
    task = asyncio.create_task(
        transcode(tmp_path / "original", tmp_path / "out.mp4", rendition)
    )

    pid = await _cancel_once_started(task, hanging_binaries)

    _assert_reaped(pid)
