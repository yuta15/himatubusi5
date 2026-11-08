"""単一プロセス内の非同期ジョブ実行エンジン。"""

import asyncio
import heapq
import math
from collections import deque
from dataclasses import dataclass, field
from itertools import count
from typing import Awaitable, Callable
from uuid import uuid4

from interface import JobDependencyError, JobNotFoundError, JobSpec, JobStatus, RetryPolicy
from job import Job
from job_runner import JobRunner


@dataclass
class _Registration:
    job: Job
    runner: JobRunner = field(default_factory=JobRunner)
    waiting_for: set[str] = field(default_factory=set)
    backoff: asyncio.TimerHandle | None = None


def _finite_number(name: str, value: object, minimum: float, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name}には有限数値を指定してください")
    try:
        number = float(value)
    except OverflowError:
        raise ValueError(f"{name}には有限数値を指定してください") from None
    if not math.isfinite(number) or (number <= minimum if positive else number < minimum):
        raise ValueError(f"{name}の値が範囲外です")
    return number


def _snapshot(spec: JobSpec) -> JobSpec:
    """登録前に検証し、呼び出し元の設定コンテナから切り離す。"""
    if not isinstance(spec, JobSpec):
        raise TypeError("JobSpecを指定してください")
    if not callable(spec.func):
        raise TypeError("ジョブの処理にはCallableを指定してください")
    if not isinstance(spec.args, tuple):
        raise TypeError("argsにはtupleを指定してください")
    if not isinstance(spec.kwargs, dict) or any(not isinstance(k, str) for k in spec.kwargs):
        raise TypeError("kwargsには文字列キーのdictを指定してください")
    policy = spec.retry
    if not isinstance(policy, RetryPolicy):
        raise TypeError("retryにはRetryPolicyを指定してください")
    if isinstance(policy.max_retries, bool) or not isinstance(policy.max_retries, int):
        raise TypeError("max_retriesには0以上の整数を指定してください")
    if policy.max_retries < 0:
        raise ValueError("max_retriesには0以上の整数を指定してください")
    if not isinstance(policy.retry_on, tuple) or not policy.retry_on or any(
        not isinstance(exc, type) or not issubclass(exc, Exception)
        for exc in policy.retry_on
    ):
        raise TypeError("retry_onにはExceptionのサブクラスからなる空でないtupleを指定してください")
    retry = RetryPolicy(
        max_retries=policy.max_retries,
        initial_delay=_finite_number("initial_delay", policy.initial_delay, 0),
        multiplier=_finite_number("multiplier", policy.multiplier, 1),
        max_delay=(
            None if policy.max_delay is None
            else _finite_number("max_delay", policy.max_delay, 0)
        ),
        retry_on=tuple(policy.retry_on),
    )
    timeout = (
        None if spec.timeout is None
        else _finite_number("timeout", spec.timeout, 0, positive=True)
    )
    if isinstance(spec.priority, bool) or not isinstance(spec.priority, int):
        raise TypeError("priorityには整数を指定してください")
    if not isinstance(spec.depends_on, tuple) or any(
        not isinstance(job_id, str) for job_id in spec.depends_on
    ):
        raise TypeError("depends_onにはジョブIDのtupleを指定してください")
    if len(set(spec.depends_on)) != len(spec.depends_on):
        raise ValueError("依存先IDが重複しています")
    return JobSpec(
        spec.func, tuple(spec.args), dict(spec.kwargs), retry,
        timeout, spec.priority, tuple(spec.depends_on),
    )


class Engine:
    def __init__(self, max_concurrency: int | None = None) -> None:
        if max_concurrency is not None:
            if isinstance(max_concurrency, bool) or not isinstance(max_concurrency, int):
                raise TypeError("最大同時実行数には正の整数またはNoneを指定してください")
            if max_concurrency <= 0:
                raise ValueError("最大同時実行数には正の整数を指定してください")
        self._max_concurrency = max_concurrency
        self._active = 0
        self._jobs: dict[str, _Registration] = {}
        self._dependents: dict[str, set[str]] = {}
        self._ready: list[tuple[int, int, str]] = []
        self._order = count()
        self._dispatch_handle: asyncio.Handle | None = None

    async def submit(
        self,
        func: Callable[..., Awaitable[object]],
        *args: object,
        **kwargs: object,
    ) -> str:
        return await self.submit_job(JobSpec(func, args, kwargs))

    async def submit_job(self, spec: JobSpec) -> str:
        settings = _snapshot(spec)
        dependencies = [self._get_job(job_id) for job_id in settings.depends_on]
        job_id = uuid4().hex
        while job_id in self._jobs:
            job_id = uuid4().hex
        job = Job(job_id, settings)
        registration = _Registration(job)
        self._jobs[job_id] = registration

        failed = next((dep for dep in dependencies if dep.status in (
            JobStatus.FAILED, JobStatus.CANCELLED
        )), None)
        if failed is not None:
            job.fail(JobDependencyError(failed.job_id))
        else:
            for dep in dependencies:
                if dep.status is not JobStatus.SUCCEEDED:
                    registration.waiting_for.add(dep.job_id)
                    self._dependents.setdefault(dep.job_id, set()).add(job_id)
            if not registration.waiting_for:
                self._enqueue(job)
        return job_id

    async def get_status(self, job_id: str) -> JobStatus:
        return self._get_job(job_id).status

    async def wait(self, job_id: str) -> None:
        await self._get_job(job_id).wait()

    async def get_result(self, job_id: str) -> object:
        return self._get_job(job_id).get_result()

    async def cancel(self, job_id: str) -> bool:
        registration = self._get_registration(job_id)
        accepted = registration.runner.cancel(registration.job)
        if accepted and registration.job.status is JobStatus.CANCELLED:
            if registration.backoff is not None:
                registration.backoff.cancel()
                registration.backoff = None
            self._notify_terminal(registration.job)
        return accepted

    def _enqueue(self, job: Job) -> None:
        if job.status is not JobStatus.PENDING:
            return
        self._jobs[job.job_id].backoff = None
        heapq.heappush(self._ready, (-job.settings.priority, next(self._order), job.job_id))
        self._request_dispatch()

    def _request_dispatch(self) -> None:
        if self._dispatch_handle is None:
            self._dispatch_handle = asyncio.get_running_loop().call_soon(self._dispatch)

    def _dispatch(self) -> None:
        self._dispatch_handle = None
        while self._ready and (
            self._max_concurrency is None or self._active < self._max_concurrency
        ):
            _, _, job_id = heapq.heappop(self._ready)
            registration = self._jobs[job_id]
            if registration.job.status is not JobStatus.PENDING:
                continue
            self._active += 1
            registration.runner.schedule(registration.job, self._attempt_finished)

    def _attempt_finished(self, job: Job) -> None:
        self._active -= 1
        if job.status is JobStatus.PENDING:
            registration = self._jobs[job.job_id]
            registration.backoff = asyncio.get_running_loop().call_later(
                registration.runner.retry_delay, self._enqueue, job
            )
        else:
            self._notify_terminal(job)
        self._request_dispatch()

    def _notify_terminal(self, job: Job) -> None:
        # 長い依存チェーンの失敗も再帰せず伝播する。
        completed = deque([job])
        while completed:
            dependency = completed.popleft()
            registration = self._jobs[dependency.job_id]
            for parent in registration.waiting_for:
                self._dependents.get(parent, set()).discard(dependency.job_id)
            registration.waiting_for.clear()
            for job_id in self._dependents.pop(dependency.job_id, ()):
                dependent = self._jobs[job_id]
                if dependent.job.status is not JobStatus.PENDING:
                    continue
                if dependency.status is not JobStatus.SUCCEEDED:
                    dependent.job.fail(JobDependencyError(dependency.job_id))
                    completed.append(dependent.job)
                else:
                    dependent.waiting_for.discard(dependency.job_id)
                    if not dependent.waiting_for:
                        self._enqueue(dependent.job)

    def _get_job(self, job_id: str) -> Job:
        return self._get_registration(job_id).job

    def _get_registration(self, job_id: str) -> _Registration:
        try:
            return self._jobs[job_id]
        except KeyError:
            raise JobNotFoundError(job_id) from None
