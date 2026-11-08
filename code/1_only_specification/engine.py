"""単一プロセス内でジョブを非同期に実行するエンジン。"""

import asyncio
from dataclasses import dataclass, field
from typing import Awaitable, Callable
from uuid import uuid4

from interface import (
    JobCancelledError,
    JobExecutionError,
    JobNotFinishedError,
    JobNotFoundError,
    JobStatus,
)


@dataclass
class _Job:
    status: JobStatus = JobStatus.PENDING
    result: object = None
    cause: BaseException | None = None
    finished: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task[None] | None = None
    cancel_requested: bool = False


class AsyncJobEngine:
    def __init__(self) -> None:
        self._jobs: dict[str, _Job] = {}

    async def submit(
        self,
        func: Callable[..., Awaitable[object]],
        *args: object,
        **kwargs: object,
    ) -> str:
        if not callable(func):
            raise TypeError("The job must be callable")

        job_id = uuid4().hex
        while job_id in self._jobs:
            job_id = uuid4().hex
        job = _Job()
        self._jobs[job_id] = job
        # Keep a strong reference to the task for the job's entire lifetime.
        job.task = asyncio.create_task(self._run(job, func, args, kwargs))
        return job_id

    async def get_status(self, job_id: str) -> JobStatus:
        return self._get_job(job_id).status

    async def cancel(self, job_id: str) -> bool:
        job = self._get_job(job_id)
        if job.cancel_requested or job.status not in (
            JobStatus.PENDING,
            JobStatus.RUNNING,
        ):
            return False

        # No suspension between checking and recording the request: concurrent
        # callers on the event loop can only notify this task once.
        job.cancel_requested = True
        assert job.task is not None
        job.task.cancel()
        if job.status == JobStatus.PENDING:
            # A task cancelled before its first step never enters _run(),
            # including its finally block. Publish completion here instead.
            job.status = JobStatus.CANCELLED
            job.finished.set()
        return True

    async def wait(self, job_id: str) -> None:
        # Waiting on an event lets each caller cancel its own wait independently.
        await self._get_job(job_id).finished.wait()

    async def get_result(self, job_id: str) -> object:
        job = self._get_job(job_id)
        if job.status == JobStatus.CANCELLED:
            raise JobCancelledError(job_id)
        if job.status == JobStatus.FAILED:
            assert job.cause is not None
            raise JobExecutionError(job.cause) from job.cause
        if job.status != JobStatus.SUCCEEDED:
            raise JobNotFinishedError(job_id)
        return job.result

    def _get_job(self, job_id: str) -> _Job:
        try:
            return self._jobs[job_id]
        except KeyError:
            raise JobNotFoundError(job_id) from None

    async def _run(
        self,
        job: _Job,
        func: Callable[..., Awaitable[object]],
        args: tuple[object, ...],
        kwargs: dict[str, object],
    ) -> None:
        job.status = JobStatus.RUNNING
        try:
            job.result = await func(*args, **kwargs)
        except asyncio.CancelledError:
            job.status = JobStatus.CANCELLED
        except BaseException as cause:
            # Other BaseExceptions also finish the job without escaping into
            # the event loop or affecting other jobs.
            job.cause = cause
            job.status = JobStatus.FAILED
        else:
            job.status = JobStatus.SUCCEEDED
        finally:
            job.finished.set()
