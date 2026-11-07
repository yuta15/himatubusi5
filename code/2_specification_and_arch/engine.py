"""公開 API とジョブの登録・検索を担当する。"""

from typing import Awaitable, Callable
from uuid import uuid4

from interface import JobNotFoundError, JobStatus
from job import Job
from job_runner import JobRunner


class Engine:
    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._runner = JobRunner()

    async def submit(
        self,
        func: Callable[..., Awaitable[object]],
        *args: object,
        **kwargs: object,
    ) -> str:
        if not callable(func):
            raise TypeError("Job must be callable")
        job_id = uuid4().hex
        job = Job()
        self._jobs[job_id] = job
        self._runner.schedule(job, func, *args, **kwargs)
        return job_id

    async def get_status(self, job_id: str) -> JobStatus:
        return self._get_job(job_id).status

    async def wait(self, job_id: str) -> None:
        await self._get_job(job_id).wait()

    async def get_result(self, job_id: str) -> object:
        return self._get_job(job_id).get_result()

    def _get_job(self, job_id: str) -> Job:
        try:
            return self._jobs[job_id]
        except KeyError:
            raise JobNotFoundError(job_id) from None
