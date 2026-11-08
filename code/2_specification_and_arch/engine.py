"""公開 API とジョブの登録・検索を担当する。"""

from dataclasses import replace
import math
from typing import Awaitable, Callable
from uuid import uuid4

from interface import JobNotFoundError, JobSpec, JobStatus, RetryPolicy
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
        return await self.submit_job(JobSpec(func, args, kwargs))

    async def submit_job(self, spec: JobSpec) -> str:
        self._validate_spec(spec)
        # 設定コンテナをコピーし、処理に渡す値の同一性は維持する。
        spec = replace(
            spec,
            args=tuple(spec.args),
            kwargs=dict(spec.kwargs),
            retry=replace(spec.retry),
            depends_on=tuple(spec.depends_on),
        )
        dependencies = {key: self._get_job(key) for key in spec.depends_on}
        job_id = uuid4().hex
        job = Job()
        self._runner.schedule(job, spec, dependencies)
        self._jobs[job_id] = job
        return job_id

    @staticmethod
    def _validate_spec(spec: JobSpec) -> None:
        if not isinstance(spec, JobSpec):
            raise TypeError("spec must be a JobSpec")
        if not callable(spec.func):
            raise TypeError("Job must be callable")
        if not isinstance(spec.args, tuple) or not isinstance(spec.kwargs, dict):
            raise TypeError("args must be a tuple and kwargs must be a dict")
        if any(not isinstance(key, str) for key in spec.kwargs):
            raise TypeError("kwargs keys must be strings")
        if not isinstance(spec.retry, RetryPolicy):
            raise TypeError("retry must be a RetryPolicy")
        for name, value in (
            ("max_retries", spec.retry.max_retries),
            ("priority", spec.priority),
        ):
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{name} must be an integer")
        if spec.retry.max_retries < 0:
            raise ValueError("max_retries must be nonnegative")
        for name, value, minimum, strict in (
            ("initial_delay", spec.retry.initial_delay, 0, False),
            ("multiplier", spec.retry.multiplier, 1, False),
            ("max_delay", spec.retry.max_delay, 0, False),
            ("timeout", spec.timeout, 0, True),
        ):
            if value is None and name in ("max_delay", "timeout"):
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise TypeError(f"{name} must be a finite number")
            if (not math.isfinite(value) or value < minimum
                    or (strict and value == minimum)):
                raise ValueError(f"Invalid {name}")
        retry_on = spec.retry.retry_on
        if (not isinstance(retry_on, tuple)
                or any(not isinstance(cls, type) or not issubclass(cls, Exception)
                       for cls in retry_on)):
            raise TypeError("retry_on must be a tuple of Exception types")
        if not retry_on:
            raise ValueError("retry_on must not be empty")
        if (not isinstance(spec.depends_on, tuple)
                or any(not isinstance(key, str) for key in spec.depends_on)):
            raise TypeError("depends_on must be a tuple of job IDs")
        if len(set(spec.depends_on)) != len(spec.depends_on):
            raise ValueError("Duplicate dependency IDs")

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
