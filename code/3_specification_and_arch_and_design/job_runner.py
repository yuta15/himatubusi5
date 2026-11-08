"""一度の試行と、ジョブ単位の再試行・中断要求を管理する。"""

import asyncio
from typing import Callable

from interface import JobStatus
from job import Job


class JobRunner:
    def __init__(self) -> None:
        self._task: asyncio.Task[None] | None = None
        self._cancel_requested = False
        self._interruption: str | None = None
        self._retries = 0
        self._next_delay: float | None = None
        self.retry_delay = 0.0

    def schedule(self, job: Job, finished: Callable[[Job], None]) -> None:
        self._interruption = None
        # PENDING時のキャンセルでもTaskを起動し、finallyで予約済みの枠を返す。
        self._task = asyncio.create_task(self._run(job, finished))

    def cancel(self, job: Job) -> bool:
        if (
            self._cancel_requested
            or job.status not in (JobStatus.PENDING, JobStatus.RUNNING)
            or self._interruption == "timeout"
        ):
            return False
        self._cancel_requested = True
        if job.status is JobStatus.PENDING:
            job.cancelled()
        else:
            self._interrupt(job, "cancel")
        return True

    def _interrupt(self, job: Job, reason: str) -> None:
        if self._interruption is not None or job.status is not JobStatus.RUNNING:
            return
        assert self._task is not None
        self._interruption = reason
        self._task.cancel()

    async def _run(self, job: Job, finished: Callable[[Job], None]) -> None:
        timer = None
        try:
            if not job.start():
                return
            if job.settings.timeout is not None:
                timer = asyncio.get_running_loop().call_later(
                    job.settings.timeout, self._interrupt, job, "timeout"
                )
            try:
                result = await job.settings.func(*job.settings.args, **job.settings.kwargs)
            except asyncio.CancelledError:
                if self._interruption == "timeout":
                    self._failed(job, TimeoutError())
                else:
                    job.cancelled()
            except BaseException as cause:
                self._failed(job, cause)
            else:
                job.succeed(result)
        finally:
            if timer is not None:
                timer.cancel()
            self._interruption = None
            finished(job)

    def _failed(self, job: Job, cause: BaseException) -> None:
        policy = job.settings.retry
        if (
            not self._cancel_requested
            and self._retries < policy.max_retries
            and isinstance(cause, policy.retry_on)
        ):
            delay = policy.initial_delay if self._next_delay is None else self._next_delay
            if policy.max_delay is not None:
                delay = min(delay, policy.max_delay)
            self.retry_delay = delay
            self._next_delay = delay * policy.multiplier
            self._retries += 1
            job.retry()
        else:
            job.fail(cause)
