"""単一プロセス内でジョブを非同期に実行するエンジン。"""

import asyncio
import heapq
import math
from collections import deque
from dataclasses import dataclass, field, replace
from itertools import count
from typing import Awaitable, Callable
from uuid import uuid4

from interface import (
    JobCancelledError,
    JobDependencyError,
    JobExecutionError,
    JobNotFinishedError,
    JobNotFoundError,
    JobSpec,
    JobStatus,
    RetryPolicy,
)


@dataclass
class _Job:
    spec: JobSpec
    status: JobStatus = JobStatus.PENDING
    result: object = None
    cause: BaseException | None = None
    finished: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task[None] | None = None
    timer: asyncio.TimerHandle | None = None
    cancel_requested: bool = False
    interruption: str | None = None
    retries: int = 0
    delay: float = 0.0
    ready: bool = False
    remaining: set[str] = field(default_factory=set)
    dependents: list[str] = field(default_factory=list)


class AsyncJobEngine:
    def __init__(self, max_concurrency: int | None = None) -> None:
        if max_concurrency is not None:
            if isinstance(max_concurrency, bool) or not isinstance(max_concurrency, int):
                raise TypeError("max_concurrency must be a positive integer or None")
            if max_concurrency <= 0:
                raise ValueError("max_concurrency must be positive")
        self._jobs: dict[str, _Job] = {}
        self._limit = max_concurrency
        self._active = 0
        self._ready: list[tuple[int, int, str]] = []
        self._sequence = count()
        self._scheduled = False

    async def submit(
        self,
        func: Callable[..., Awaitable[object]],
        *args: object,
        **kwargs: object,
    ) -> str:
        return await self.submit_job(JobSpec(func, args=args, kwargs=kwargs))

    async def submit_job(self, spec: JobSpec) -> str:
        spec = self._validate_spec(spec)
        job_id = uuid4().hex
        while job_id in self._jobs:
            job_id = uuid4().hex
        job = _Job(spec=spec, delay=spec.retry.initial_delay)
        self._jobs[job_id] = job
        for dependency_id in spec.depends_on:
            dependency = self._jobs[dependency_id]
            if dependency.status in (JobStatus.FAILED, JobStatus.CANCELLED):
                self._finish(job_id, JobStatus.FAILED, JobDependencyError(dependency_id))
                return job_id
        for dependency_id in spec.depends_on:
            dependency = self._jobs[dependency_id]
            if dependency.status != JobStatus.SUCCEEDED:
                job.remaining.add(dependency_id)
                dependency.dependents.append(job_id)
        if not job.remaining:
            self._enqueue(job_id)
        return job_id

    def _validate_spec(self, spec: JobSpec) -> JobSpec:
        if not isinstance(spec, JobSpec):
            raise TypeError("spec must be a JobSpec")
        if not callable(spec.func):
            raise TypeError("The job must be callable")
        if not isinstance(spec.args, tuple) or not isinstance(spec.kwargs, dict):
            raise TypeError("args must be a tuple and kwargs must be a dict")
        if not all(isinstance(key, str) for key in spec.kwargs):
            raise TypeError("kwargs keys must be strings")
        policy = spec.retry
        if not isinstance(policy, RetryPolicy):
            raise TypeError("retry must be a RetryPolicy")
        self._integer(policy.max_retries, "max_retries")
        if policy.max_retries < 0:
            raise ValueError("max_retries must be nonnegative")
        self._number(policy.initial_delay, "initial_delay", minimum=0)
        self._number(policy.multiplier, "multiplier", minimum=1)
        if policy.max_delay is not None:
            self._number(policy.max_delay, "max_delay", minimum=0)
        if not isinstance(policy.retry_on, tuple):
            raise TypeError("retry_on must be a tuple")
        if not policy.retry_on:
            raise ValueError("retry_on must not be empty")
        if not all(isinstance(t, type) and issubclass(t, Exception) for t in policy.retry_on):
            raise TypeError("retry_on must contain Exception subclasses")
        if spec.timeout is not None:
            self._number(spec.timeout, "timeout", minimum=0)
            if spec.timeout == 0:
                raise ValueError("timeout must be positive")
        self._integer(spec.priority, "priority")
        if not isinstance(spec.depends_on, tuple):
            raise TypeError("depends_on must be a tuple")
        if not all(isinstance(i, str) for i in spec.depends_on):
            raise TypeError("dependency IDs must be strings")
        if len(set(spec.depends_on)) != len(spec.depends_on):
            raise ValueError("duplicate dependency IDs")
        for dependency_id in spec.depends_on:
            self._get_job(dependency_id)
        # Copy configuration containers; argument values retain their identity,
        # just as they do for ordinary Python calls and the existing submit API.
        return replace(
            spec, args=tuple(spec.args), kwargs=dict(spec.kwargs),
            retry=replace(policy, retry_on=tuple(policy.retry_on)),
            depends_on=tuple(spec.depends_on),
        )

    @staticmethod
    def _integer(value: object, name: str) -> None:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{name} must be an integer")

    @staticmethod
    def _number(value: object, name: str, *, minimum: float) -> None:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"{name} must be a finite number")
        try:
            valid = math.isfinite(value) and value >= minimum
        except OverflowError:
            valid = False
        if not valid:
            raise ValueError(f"{name} must be finite and at least {minimum}")

    async def get_status(self, job_id: str) -> JobStatus:
        return self._get_job(job_id).status

    async def cancel(self, job_id: str) -> bool:
        job = self._get_job(job_id)
        if job.cancel_requested or job.finished.is_set():
            return False
        # A timeout already owns the interruption for this running attempt.
        # Reject without recording an explicit cancellation of future retries.
        if job.status == JobStatus.RUNNING and job.interruption == "timeout":
            return False
        job.cancel_requested = True
        if job.status == JobStatus.PENDING:
            self._finish(job_id, JobStatus.CANCELLED)
        else:
            self._interrupt(job, "cancel")
        return True

    async def wait(self, job_id: str) -> None:
        # Each waiter can be cancelled without affecting the job or other waiters.
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

    def _schedule(self) -> None:
        if not self._scheduled:
            self._scheduled = True
            asyncio.get_running_loop().call_soon(self._dispatch)

    def _enqueue(self, job_id: str) -> None:
        job = self._jobs[job_id]
        if job.finished.is_set():
            return
        job.timer = None
        job.ready = True
        heapq.heappush(self._ready, (-job.spec.priority, next(self._sequence), job_id))
        self._schedule()

    def _dispatch(self) -> None:
        self._scheduled = False
        while self._ready and (self._limit is None or self._active < self._limit):
            _, _, job_id = heapq.heappop(self._ready)
            job = self._jobs[job_id]
            if not job.ready or job.finished.is_set():
                continue
            job.ready = False
            self._active += 1
            # The reservation is released by _run even if cancelled before its
            # first step. Pending cancellation therefore never cancels this task.
            job.task = asyncio.create_task(self._run(job_id))

    @staticmethod
    def _interrupt(job: _Job, reason: str) -> None:
        if job.status == JobStatus.RUNNING and job.interruption is None:
            job.interruption = reason
            assert job.task is not None
            job.task.cancel()

    async def _run(self, job_id: str) -> None:
        job = self._jobs[job_id]
        try:
            if job.finished.is_set():
                return
            job.status = JobStatus.RUNNING
            job.interruption = None
            if job.spec.timeout is not None:
                job.timer = asyncio.get_running_loop().call_later(
                    job.spec.timeout, self._interrupt, job, "timeout"
                )
            try:
                job.result = await job.spec.func(*job.spec.args, **job.spec.kwargs)
            except asyncio.CancelledError:
                if job.interruption == "timeout":
                    self._failed_attempt(job_id, TimeoutError("Job attempt timed out"))
                else:
                    self._finish(job_id, JobStatus.CANCELLED)
            except BaseException as cause:
                self._failed_attempt(job_id, cause)
            else:
                self._finish(job_id, JobStatus.SUCCEEDED)
        finally:
            # Retry timers are installed only after cancelling the attempt timer.
            # Cleanup inside the callable has finished before releasing the slot.
            self._active -= 1
            self._schedule()

    def _failed_attempt(self, job_id: str, cause: BaseException) -> None:
        job = self._jobs[job_id]
        policy = job.spec.retry
        if (job.retries >= policy.max_retries
                or not isinstance(cause, policy.retry_on)
                or job.interruption == "cancel"):
            self._finish(job_id, JobStatus.FAILED, cause)
            return
        if job.timer is not None:
            job.timer.cancel()
        delay = job.delay
        if policy.max_delay is not None:
            delay = min(delay, policy.max_delay)
        job.retries += 1
        job.status = JobStatus.PENDING
        job.timer = asyncio.get_running_loop().call_later(delay, self._enqueue, job_id)
        # Multiplication avoids overflow from computing multiplier ** retries,
        # and a zero initial delay stays zero even for very large multipliers.
        try:
            job.delay = delay * policy.multiplier
        except OverflowError:
            job.delay = math.inf

    def _finish(
        self, job_id: str, status: JobStatus, cause: BaseException | None = None,
    ) -> None:
        # Iteration also supports long dependency chains without recursion.
        completed = deque([(job_id, status, cause)])
        while completed:
            current_id, current_status, current_cause = completed.popleft()
            job = self._jobs[current_id]
            if job.finished.is_set():
                continue
            if job.timer is not None:
                job.timer.cancel()
                job.timer = None
            job.status = current_status
            job.cause = current_cause
            job.ready = False
            job.finished.set()
            for dependent_id in job.dependents:
                dependent = self._jobs[dependent_id]
                if dependent.finished.is_set():
                    continue
                if current_status != JobStatus.SUCCEEDED:
                    completed.append((dependent_id, JobStatus.FAILED,
                                      JobDependencyError(current_id)))
                else:
                    dependent.remaining.discard(current_id)
                    if not dependent.remaining:
                        self._enqueue(dependent_id)
            job.dependents.clear()
