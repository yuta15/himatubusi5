"""テスト所有の試行・中断処理と、公開APIによるv2テストの同期・後処理。"""

import asyncio

import pytest

from run_count_support import RunCountScope
from support import bounded, checkpoint


RETRY_DELAY = 0.15
JOB_TIMEOUT = 0.15
EARLY_TOLERANCE = 0.03
LATE_TOLERANCE = 0.30


async def elapsed(duration):
    """時間要件の観測期間だけを確保する。ジョブの同期はEventで行う。"""
    event = asyncio.Event()
    timer = asyncio.get_running_loop().call_later(duration, event.set)
    try:
        await bounded(event.wait())
    finally:
        timer.cancel()


async def status_is(engine, job_id, expected):
    async def observe():
        while await engine.get_status(job_id) is not expected:
            await elapsed(0.002)
    await bounded(observe())


class AttemptJob:
    """各呼び出しで新しいコルーチンを返し、各試行を独立したEventで制御する。"""

    def __init__(self, record, outcomes, blocked=()):
        self.record = record
        self.outcomes = outcomes
        self.calls = 0
        self.invocations = 0
        self.active = 0
        self.peak = 0
        self.starts = [asyncio.Event() for _ in outcomes]
        self.finishes = [asyncio.Event() for _ in outcomes]
        self.gates = [asyncio.Event() for _ in outcomes]
        self.started_at = []
        self.finished_at = []
        for index, gate in enumerate(self.gates):
            if index not in blocked:
                gate.set()

    def __call__(self):
        self.invocations += 1
        return self.run()

    async def run(self):
        index = min(self.calls, len(self.outcomes) - 1)
        self.record.enter(self)
        self.active += 1
        self.peak = max(self.peak, self.active)
        self.started_at.append(asyncio.get_running_loop().time())
        self.starts[index].set()
        try:
            await self.gates[index].wait()
            result = self.outcomes[index]
            if isinstance(result, BaseException):
                raise result
            return result
        finally:
            self.finished_at.append(asyncio.get_running_loop().time())
            self.finishes[index].set()
            self.active -= 1
            self.record.leave()


class ContinuingJob:
    """中断を抑制し、exceptを抜けた後の本体継続をEventで観測する。"""

    def __init__(self, record):
        self.record = record
        self.calls = 0
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.continued = asyncio.Event()
        self.continue_release = asyncio.Event()
        self.cancel_notifications = 0
        self.result = "continued"

    async def __call__(self):
        self.record.enter(self)
        self.started.set()
        try:
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancel_notifications += 1
            self.continued.set()
            while not self.continue_release.is_set():
                try:
                    await self.continue_release.wait()
                except asyncio.CancelledError:
                    self.cancel_notifications += 1
            return self.result
        finally:
            self.record.leave()


class V2Scope(RunCountScope):
    def __init__(self):
        super().__init__()
        self.attempts = []
        self.continuations = []

    def attempt(self, outcomes, blocked=()):
        job = AttemptJob(self.record, outcomes, blocked)
        self.attempts.append(job)
        return job

    def continuing_job(self):
        job = ContinuingJob(self.record)
        self.continuations.append(job)
        return job

    async def submit_spec(self, engine, spec):
        async def register():
            job_id = await engine.submit_job(spec)
            self.submitted.append((engine, job_id))
            return job_id
        task = self.task(register())
        return await bounded(asyncio.shield(task))

    async def finish(self):
        for job in self.attempts:
            for gate in job.gates:
                gate.set()
        for job in self.continuations:
            job.release.set()
            job.continue_release.set()
        for job in self.controlled:
            job.release.set()
            if hasattr(job, "cleanup_release"):
                job.cleanup_release.set()
        # 長いバックオフも公開cancelで回収する。内部Taskは列挙・操作しない。
        errors = []
        for engine, job_id in self.submitted:
            try:
                await bounded(engine.cancel(job_id))
            except Exception as error:
                errors.append(error)
        try:
            await super().finish()
        except Exception as error:
            errors.append(error)
        if errors:
            raise ExceptionGroup("v2ジョブの後処理に失敗しました", errors)


async def failure_cause(engine, api, job_id, cause_type):
    assert await bounded(engine.wait(job_id)) is None
    assert await bounded(engine.get_status(job_id)) is api.JobStatus.FAILED
    with pytest.raises(api.JobExecutionError) as caught:
        await bounded(engine.get_result(job_id))
    assert isinstance(caught.value.cause, cause_type)
    return caught.value.cause


async def assert_pending(engine, api, job_id, waiters=()):
    assert await bounded(engine.get_status(job_id)) is api.JobStatus.PENDING
    with pytest.raises(api.JobNotFinishedError):
        await bounded(engine.get_result(job_id))
    await checkpoint()
    assert all(not waiter.done() for waiter in waiters)


async def dependency_failed(engine, api, job_id, direct_ids):
    cause = await failure_cause(engine, api, job_id, api.JobDependencyError)
    assert cause.dependency_id in direct_ids
    return cause
