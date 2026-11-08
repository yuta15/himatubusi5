"""単一プロセス内の非同期ジョブ実行エンジン。"""

import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable
from uuid import uuid4

from interface import JobNotFoundError, JobStatus
from job import Job, JobSettings
from job_runner import JobRunner


@dataclass(frozen=True)
class _Registration:
    job: Job
    runner: JobRunner


class Engine:
    def __init__(self, max_concurrency: int | None = None) -> None:
        if max_concurrency is not None:
            if isinstance(max_concurrency, bool) or not isinstance(max_concurrency, int):
                raise TypeError("最大同時実行数には正の整数またはNoneを指定してください")
            if max_concurrency <= 0:
                raise ValueError("最大同時実行数には正の整数を指定してください")
        self._execution_slots = (
            asyncio.Semaphore(max_concurrency) if max_concurrency is not None else None
        )
        self._jobs: dict[str, _Registration] = {}

    async def submit(
        self,
        func: Callable[..., Awaitable[object]],
        *args: object,
        **kwargs: object,
    ) -> str:
        if not callable(func):
            raise TypeError("ジョブの処理にはCallableを指定してください")

        job_id = uuid4().hex
        while job_id in self._jobs:
            job_id = uuid4().hex
        settings = JobSettings(func, args, kwargs)
        job = Job(job_id, settings)
        runner = JobRunner(self._execution_slots)
        self._jobs[job_id] = _Registration(job, runner)
        runner.schedule(job)
        return job_id

    async def get_status(self, job_id: str) -> JobStatus:
        return self._get_job(job_id).status

    async def wait(self, job_id: str) -> None:
        await self._get_job(job_id).wait()

    async def get_result(self, job_id: str) -> object:
        return self._get_job(job_id).get_result()

    async def cancel(self, job_id: str) -> bool:
        registration = self._get_registration(job_id)
        return registration.runner.cancel(registration.job)

    def _get_job(self, job_id: str) -> Job:
        return self._get_registration(job_id).job

    def _get_registration(self, job_id: str) -> _Registration:
        try:
            return self._jobs[job_id]
        except KeyError:
            raise JobNotFoundError(job_id) from None
