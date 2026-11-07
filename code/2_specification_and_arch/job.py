"""ジョブの状態、結果、完了通知を管理する。"""

import asyncio

from interface import JobCancelledError, JobExecutionError, JobNotFinishedError, JobStatus


class Job:
    def __init__(self) -> None:
        self._status = JobStatus.PENDING
        self._result: object = None
        self._cause: BaseException | None = None
        self._finished = asyncio.Event()
        self._cancel_requested = False

    @property
    def status(self) -> JobStatus:
        return self._status

    def start(self) -> None:
        self._require_status(JobStatus.PENDING)
        self._status = JobStatus.RUNNING

    def succeed(self, result: object) -> None:
        self._require_status(JobStatus.RUNNING)
        self._result = result
        self._status = JobStatus.SUCCEEDED
        self._finished.set()

    def fail(self, cause: BaseException) -> None:
        self._require_status(JobStatus.RUNNING)
        self._cause = cause
        self._status = JobStatus.FAILED
        self._finished.set()

    def request_cancel(self) -> bool:
        if self._finished.is_set() or self._cancel_requested:
            return False
        self._cancel_requested = True
        return True

    def finish_cancelled(self) -> None:
        if self._status not in (JobStatus.PENDING, JobStatus.RUNNING):
            raise RuntimeError(f"Cannot cancel a {self._status.value} job")
        self._status = JobStatus.CANCELLED
        self._finished.set()

    async def wait(self) -> None:
        await self._finished.wait()

    def get_result(self) -> object:
        if self._status in (JobStatus.PENDING, JobStatus.RUNNING):
            raise JobNotFinishedError("Job has not finished")
        if self._status is JobStatus.CANCELLED:
            raise JobCancelledError("Job was cancelled")
        if self._status is JobStatus.FAILED:
            assert self._cause is not None
            raise JobExecutionError(self._cause) from self._cause
        return self._result

    def _require_status(self, expected: JobStatus) -> None:
        if self._status is not expected:
            raise RuntimeError(f"Expected {expected.value}, got {self._status.value}")
