"""テスト所有のジョブ本体だけを計測する実行数制限テスト用ヘルパー。"""

import asyncio

import pytest

from cancel_support import CancellationScope
from support import ControlledJob, bounded, checkpoint


class ExecutionRecord:
    def __init__(self):
        self.active = 0
        self.peak = 0
        self.started = asyncio.Queue()

    def enter(self, job):
        # 同じイベントループ内で、計測値更新の途中にはawaitしない。
        self.active += 1
        self.peak = max(self.peak, self.active)
        job.calls += 1
        self.started.put_nowait(job)

    def leave(self):
        self.active -= 1


class RunCountJob(ControlledJob):
    def __init__(self, record, outcome="succeeded", cleanup=False,
                 suppress=False, spontaneous=False, release=None):
        super().__init__()
        if release is not None:
            self.release = release
        self.record = record
        self.outcome = outcome
        self.cleanup = cleanup
        self.suppress = suppress
        self.spontaneous = spontaneous
        self.cause = ValueError("run count job failure")
        self.cleanup_started = asyncio.Event()
        self.cleanup_release = asyncio.Event()
        self.cleanup_finished = asyncio.Event()
        self.cancel_notifications = 0
        self.calls = 0

    async def __call__(self):
        self.record.enter(self)
        self.started.set()
        cancelled = False
        try:
            try:
                try:
                    await self.release.wait()
                    if self.spontaneous:
                        raise asyncio.CancelledError("spontaneous cancellation")
                except asyncio.CancelledError:
                    cancelled = True
                    self.cancel_notifications += 1
                    if not self.suppress:
                        raise
                if self.outcome == "failed":
                    raise self.cause
                return self.result
            finally:
                # R10/R12: 最初のCancelledErrorを捕捉した後、finally内で停止する。
                if cancelled and self.cleanup:
                    self.cleanup_started.set()
                    while not self.cleanup_release.is_set():
                        try:
                            await self.cleanup_release.wait()
                        except asyncio.CancelledError:
                            # 再通知を記録し、不正実装でもゲート解放で回収可能にする。
                            self.cancel_notifications += 1
                    self.cleanup_finished.set()
        finally:
            # 終了処理中も本体実行数に含める。
            self.record.leave()


class RunCountScope(CancellationScope):
    def __init__(self):
        super().__init__()
        self.record = ExecutionRecord()

    def run_job(self, record=None, **kwargs):
        job = RunCountJob(self.record if record is None else record, **kwargs)
        job.result = {"job": len(self.controlled)}
        self.controlled.append(job)
        return job

    async def fill(self, engine, count, **kwargs):
        pairs = []
        for _ in range(count):
            job = self.run_job(**kwargs)
            job_id = await self.submit(engine, job)
            pairs.append((job_id, job))
            await bounded(job.started.wait())
        return pairs

    async def finish(self):
        for job in self.controlled:
            if isinstance(job, RunCountJob):
                job.cleanup_release.set()
        await super().finish()


async def assert_pending(engine, api, job_id, job, waiters=()):
    assert await bounded(engine.get_status(job_id)) is api.JobStatus.PENDING
    with pytest.raises(api.JobNotFinishedError):
        await bounded(engine.get_result(job_id))
    await checkpoint()
    assert job.calls == 0
    assert all(not waiter.done() for waiter in waiters)


async def next_started(record, excluded=()):
    # FIFOを要求せず、実際の開始通知から次に解放するジョブを選ぶ。
    while True:
        job = await bounded(record.started.get())
        if job not in excluded:
            return job
