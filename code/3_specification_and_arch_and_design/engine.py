"""単一プロセス内の非同期ジョブ実行エンジン。"""

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
    def __init__(self) -> None:
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
        runner = JobRunner()
        self._jobs[job_id] = _Registration(job, runner)
        runner.schedule(job)
        return job_id

    async def get_status(self, job_id: str) -> JobStatus:
        return self._get_job(job_id).status

    async def wait(self, job_id: str) -> None:
        await self._get_job(job_id).wait()

    async def get_result(self, job_id: str) -> object:
        return self._get_job(job_id).get_result()

    def _get_job(self, job_id: str) -> Job:
        try:
            return self._jobs[job_id].job
        except KeyError:
            raise JobNotFoundError(job_id) from None
