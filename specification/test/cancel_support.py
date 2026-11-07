"""キャンセル契約テスト用のジョブと、公開APIによる後処理。"""

import asyncio

import pytest

from support import JobScope, bounded, checkpoint


class CancellationJob:
    def __init__(self, outcome="cancelled", spontaneous=False):
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.cancel_received = asyncio.Event()
        self.cleanup_release = asyncio.Event()
        self.cleanup_finished = asyncio.Event()
        self.outcome = outcome
        self.spontaneous = spontaneous
        self.result = {"finished": True}
        self.cause = ValueError("failure after cancellation")
        self.cancel_notifications = 0
        self.normal_path_finished = False

    async def __call__(self):
        self.started.set()
        try:
            await self.release.wait()
            if self.spontaneous:
                raise asyncio.CancelledError("job-originated cancellation")
        except asyncio.CancelledError:
            self.cancel_notifications += 1
            self.cancel_received.set()
            # 二度目の通知も記録して回収可能にし、テスト本文で不合格にする。
            while not self.cleanup_release.is_set():
                try:
                    await self.cleanup_release.wait()
                except asyncio.CancelledError:
                    self.cancel_notifications += 1
            self.cleanup_finished.set()
            if self.outcome == "cancelled":
                raise
            if self.outcome == "failed":
                raise self.cause
            return self.result
        self.normal_path_finished = True
        return self.result


class CancellationScope(JobScope):
    def cancellation_job(self, outcome="cancelled", spontaneous=False):
        job = CancellationJob(outcome, spontaneous)
        self.controlled.append(job)
        return job

    async def submit_direct(self, engine, func):
        # C02では登録を別Taskに移さず、開始前キャンセルの機会を保つ。
        job_id = await engine.submit(func)
        self.submitted.append((engine, job_id))
        return job_id

    async def waiter(self, engine, job_id):
        entered = asyncio.Event()

        async def wait_for_job():
            entered.set()
            return await engine.wait(job_id)

        task = self.task(wait_for_job())
        await bounded(entered.wait())
        await checkpoint()
        return task

    async def finish(self):
        # fixtureのfinallyから実行され、失敗・skip時にも全てのゲートを開く。
        for job in self.controlled:
            if isinstance(job, CancellationJob):
                job.cleanup_release.set()
        await super().finish()


async def assert_unfinished(engine, api, job_id, waiters=()):
    assert await bounded(engine.get_status(job_id)) is api.JobStatus.RUNNING
    with pytest.raises(api.JobNotFinishedError):
        await bounded(engine.get_result(job_id))
    await checkpoint()
    assert all(not waiter.done() for waiter in waiters)


async def assert_terminal(engine, api, job_id, status, result=None, cause=None):
    assert await bounded(engine.wait(job_id)) is None
    assert await bounded(engine.get_status(job_id)) is status
    if status is api.JobStatus.SUCCEEDED:
        assert await bounded(engine.get_result(job_id)) == result
    elif status is api.JobStatus.FAILED:
        with pytest.raises(api.JobExecutionError) as caught:
            await bounded(engine.get_result(job_id))
        assert caught.value.cause is cause
    else:
        assert status is api.JobStatus.CANCELLED
        with pytest.raises(api.JobCancelledError):
            await bounded(engine.get_result(job_id))
