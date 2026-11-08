"""公開 API とジョブの登録・検索を担当する。"""

from typing import Awaitable, Callable
from uuid import uuid4

from interface import JobNotFoundError, JobStatus
from job import Job
from job_runner import JobRunner


class Engine:
    def __init__(self, max_concurrency: int | None = None) -> None:
        if max_concurrency is not None:
            if isinstance(max_concurrency, bool) or not isinstance(max_concurrency, int):
                raise TypeError("max_concurrency must be a positive integer or None")
            if max_concurrency <= 0:
                raise ValueError("max_concurrency must be positive")
        self._jobs: dict[str, Job] = {}
        self._runner = JobRunner(max_concurrency)

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

    async def cancel(self, job_id: str) -> bool:
        return self._runner.cancel(self._get_job(job_id))

    def _get_job(self, job_id: str) -> Job:
        try:
            return self._jobs[job_id]
        except KeyError:
            raise JobNotFoundError(job_id) from None
