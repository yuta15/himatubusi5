"""ジョブの実行内容と実行状態。"""

import asyncio
from dataclasses import dataclass, field
from typing import Awaitable, Callable

from interface import JobCancelledError, JobExecutionError, JobNotFinishedError, JobStatus


@dataclass(frozen=True)
class JobSettings:
    func: Callable[..., Awaitable[object]]
    args: tuple[object, ...]
    kwargs: dict[str, object]


@dataclass
class Job:
    job_id: str
    settings: JobSettings
    _status: JobStatus = field(default=JobStatus.PENDING, init=False)
    result: object = field(default=None, init=False)
    cause: BaseException | None = field(default=None, init=False)
    _completed: asyncio.Event = field(default_factory=asyncio.Event, init=False)

    @property
    def status(self) -> JobStatus:
        return self._status

    def start(self) -> bool:
        """未実行のジョブだけを開始する。"""
        if self._status is not JobStatus.PENDING:
            return False
        self._status = JobStatus.RUNNING
        return True

    def succeed(self, result: object) -> None:
        if self._status is not JobStatus.RUNNING:
            return
        self.result = result
        self._status = JobStatus.SUCCEEDED
        self._completed.set()

    def fail(self, cause: BaseException) -> None:
        if self._status is not JobStatus.RUNNING:
            return
        self.cause = cause
        self._status = JobStatus.FAILED
        self._completed.set()

    def cancelled(self) -> None:
        """処理が未開始、またはCancelledErrorで終了した場合に確定する。"""
        if self._status not in (JobStatus.PENDING, JobStatus.RUNNING):
            return
        self._status = JobStatus.CANCELLED
        self._completed.set()

    async def wait(self) -> None:
        await self._completed.wait()

    def get_result(self) -> object:
        """状態に応じて結果を返すか、未完了・失敗の例外を送出する。"""
        if self._status is JobStatus.FAILED:
            assert self.cause is not None
            raise JobExecutionError(self.cause) from self.cause
        if self._status is JobStatus.CANCELLED:
            raise JobCancelledError(self.job_id)
        if self._status is not JobStatus.SUCCEEDED:
            raise JobNotFinishedError(self.job_id)
        return self.result
