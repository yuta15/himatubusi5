"""実行可能キュー、試行、中断要求と依存関係の進行を管理する。"""

import asyncio
from dataclasses import dataclass, field
import heapq
from itertools import count

from interface import JobDependencyError, JobSpec, JobStatus
from job import Job


@dataclass(eq=False)
class _Execution:
    job: Job
    spec: JobSpec
    dependencies: dict[str, Job]
    task: asyncio.Task[None] | None = None
    timer: asyncio.TimerHandle | None = None
    interruption: str | None = None
    retries: int = 0
    delay: float = 0.0
    remaining: set[Job] = field(default_factory=set)


class JobRunner:
    def __init__(self, max_concurrency: int | None = None) -> None:
        self._limit = max_concurrency
        self._active = 0
        self._executions: dict[Job, _Execution] = {}
        self._dependents: dict[Job, set[_Execution]] = {}
        self._ready: list[tuple[int, int, _Execution]] = []
        self._sequence = count()
        self._dispatch_handle: asyncio.Handle | None = None

    def schedule(self, job: Job, spec: JobSpec, dependencies: dict[str, Job]) -> None:
        execution = _Execution(job, spec, dependencies)
        self._executions[job] = execution
        for key, dependency in dependencies.items():
            if dependency.status in (JobStatus.FAILED, JobStatus.CANCELLED):
                job.fail(JobDependencyError(key))
                self._finish(job)
                return
        execution.remaining = {
            dependency for dependency in dependencies.values()
            if dependency.status is not JobStatus.SUCCEEDED
        }
        for dependency in execution.remaining:
            self._dependents.setdefault(dependency, set()).add(execution)
        if not execution.remaining:
            self._enqueue(execution)

    def cancel(self, job: Job) -> bool:
        execution = self._executions.get(job)
        # タイムアウトの終了処理を明示的キャンセルで再び中断しない。
        if execution is None or execution.interruption is not None:
            return False
        if not job.request_cancel():
            return False
        execution.interruption = "cancel"
        if execution.task is not None:
            execution.task.cancel()
        else:
            job.finish_cancelled()
            self._finish(job)
        return True

    def _enqueue(self, execution: _Execution) -> None:
        execution.timer = None
        heapq.heappush(self._ready, (-execution.spec.priority, next(self._sequence), execution))
        self._request_dispatch()

    def _request_dispatch(self) -> None:
        if self._dispatch_handle is None:
            self._dispatch_handle = asyncio.get_running_loop().call_soon(self._dispatch)

    def _dispatch(self) -> None:
        self._dispatch_handle = None
        while self._ready and (self._limit is None or self._active < self._limit):
            _, _, execution = heapq.heappop(self._ready)
            if execution.job not in self._executions:
                continue
            self._active += 1
            execution.task = asyncio.create_task(self._run(execution))
            execution.task.add_done_callback(
                lambda task, entry=execution: self._on_done(entry, task)
            )

    def _timeout(self, execution: _Execution) -> None:
        if (execution.interruption is None and execution.task is not None
                and not execution.task.done()):
            execution.interruption = "timeout"
            execution.task.cancel()

    async def _run(self, execution: _Execution) -> None:
        job, spec = execution.job, execution.spec
        job.start()
        if spec.timeout is not None:
            execution.timer = asyncio.get_running_loop().call_later(
                spec.timeout, self._timeout, execution
            )
        try:
            result = await spec.func(*spec.args, **spec.kwargs)
        except asyncio.CancelledError:
            if execution.interruption == "timeout":
                self._failed_attempt(execution, TimeoutError())
            else:
                job.finish_cancelled()
        except BaseException as cause:
            self._failed_attempt(execution, cause)
        else:
            job.succeed(result)
        finally:
            if execution.timer is not None:
                execution.timer.cancel()
                execution.timer = None
            # 完了待機者が再開する前に後続を実行可能にし、FIFO の順序を保つ。
            if job.status is not JobStatus.PENDING:
                self._finish(job)

    def _failed_attempt(self, execution: _Execution, cause: BaseException) -> None:
        policy = execution.spec.retry
        if (execution.interruption != "cancel"
                and execution.retries < policy.max_retries
                and isinstance(cause, policy.retry_on)):
            # 逐次乗算で巨大な指数計算のオーバーフローを避ける。
            if execution.retries == 0:
                execution.delay = policy.initial_delay
            else:
                try:
                    execution.delay *= policy.multiplier
                except OverflowError:
                    execution.delay = float("inf")
            if policy.max_delay is not None:
                execution.delay = min(execution.delay, policy.max_delay)
            execution.retries += 1
            execution.interruption = None
            execution.job.retry()
        else:
            execution.job.fail(cause)

    def _on_done(self, execution: _Execution, task: asyncio.Task[None]) -> None:
        self._active -= 1
        execution.task = None
        if task.cancelled() or (execution.job.status is JobStatus.PENDING
                                and execution.interruption == "cancel"):
            # 初回実行前、または失敗直後のコールバック待ちでのキャンセル。
            execution.job.finish_cancelled()
            self._finish(execution.job)
        if execution.job.status is JobStatus.PENDING:
            execution.interruption = None
            execution.timer = asyncio.get_running_loop().call_later(
                execution.delay, self._enqueue, execution
            )
        self._request_dispatch()

    def _finish(self, job: Job) -> None:
        # 依存失敗の長い連鎖でも Python の再帰上限に依存しない。
        finished = [job]
        while finished:
            completed = finished.pop()
            execution = self._executions.pop(completed)
            if execution.timer is not None:
                execution.timer.cancel()
            for dependency in execution.remaining:
                followers = self._dependents.get(dependency)
                if followers is not None:
                    followers.discard(execution)
                    if not followers:
                        del self._dependents[dependency]
            for follower in self._dependents.pop(completed, ()):
                follower.remaining.discard(completed)
                if follower.job.status is not JobStatus.PENDING:
                    continue
                if completed.status is not JobStatus.SUCCEEDED:
                    key = next(key for key, value in follower.dependencies.items()
                               if value is completed)
                    follower.job.fail(JobDependencyError(key))
                    finished.append(follower.job)
                elif not follower.remaining:
                    self._enqueue(follower)
