"""公開 API の受け入れ条件を外部サービスなしで検証する。"""

import asyncio
import unittest

from interface import (
    JobExecutionError,
    JobNotFinishedError,
    JobNotFoundError,
    JobStatus,
)
from main import create_engine


class EngineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.engine = create_engine()
        # デッドロックはテスト失敗にし、経過時間による状態推測はしない。
        self.deadline = asyncio.timeout(5)
        await self.deadline.__aenter__()

    async def asyncTearDown(self) -> None:
        await self.deadline.__aexit__(None, None, None)

    async def test_arguments_unique_ids_and_retained_results(self) -> None:
        async def calculate(value: int, *, factor: int, job: str) -> int:
            self.assertEqual(job, "user argument")
            return value * factor

        ids = [
            await self.engine.submit(calculate, n, factor=3, job="user argument")
            for n in range(3)
        ]
        self.assertEqual(len(set(ids)), 3)
        for n, job_id in enumerate(ids):
            self.assertIsNone(await self.engine.wait(job_id))
            for _ in range(2):
                self.assertEqual(await self.engine.get_status(job_id), JobStatus.SUCCEEDED)
                self.assertEqual(await self.engine.get_result(job_id), n * 3)
                self.assertIsNone(await self.engine.wait(job_id))

    async def test_nonblocking_submit_state_transitions_and_none_result(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()

        async def work() -> None:
            started.set()
            await release.wait()

        job_id = await self.engine.submit(work)
        # デフォルトの asyncio スケジューリングでは次の yield まで PENDING。
        self.assertEqual(await self.engine.get_status(job_id), JobStatus.PENDING)
        with self.assertRaises(JobNotFinishedError):
            await self.engine.get_result(job_id)
        await started.wait()
        self.assertEqual(await self.engine.get_status(job_id), JobStatus.RUNNING)
        with self.assertRaises(JobNotFinishedError):
            await self.engine.get_result(job_id)
        waiter = asyncio.create_task(self.engine.wait(job_id))
        await asyncio.sleep(0)
        self.assertFalse(waiter.done())
        release.set()
        self.assertIsNone(await waiter)
        self.assertEqual(await self.engine.get_status(job_id), JobStatus.SUCCEEDED)
        self.assertIsNone(await self.engine.get_result(job_id))

    async def test_concurrent_jobs_and_failure_isolation(self) -> None:
        started = [asyncio.Event(), asyncio.Event()]
        release = asyncio.Event()
        cause = ValueError("job failed")

        async def work(index: int) -> int:
            started[index].set()
            await release.wait()
            return index

        async def fail() -> None:
            raise cause

        ids = [await self.engine.submit(work, index) for index in range(2)]
        failed_id = await self.engine.submit(fail)
        await asyncio.gather(*(event.wait() for event in started))
        self.assertIsNone(await self.engine.wait(failed_id))
        for job_id in ids:
            self.assertEqual(await self.engine.get_status(job_id), JobStatus.RUNNING)
        for _ in range(2):
            self.assertEqual(await self.engine.get_status(failed_id), JobStatus.FAILED)
            with self.assertRaises(JobExecutionError) as error:
                await self.engine.get_result(failed_id)
            self.assertIs(error.exception.cause, cause)
        release.set()
        await asyncio.gather(*(self.engine.wait(job_id) for job_id in ids))
        self.assertEqual([await self.engine.get_result(job_id) for job_id in ids], [0, 1])

    async def test_unknown_ids_and_engine_isolation(self) -> None:
        async def work() -> str:
            return "done"

        other_engine = create_engine()
        self.assertIsNot(self.engine, other_engine)
        job_id = await self.engine.submit(work)
        for engine, unknown_id in ((self.engine, "missing"), (other_engine, job_id)):
            for method in (engine.get_status, engine.wait, engine.get_result):
                with self.subTest(method=method.__name__, job_id=unknown_id):
                    with self.assertRaises(JobNotFoundError):
                        await method(unknown_id)
        await self.engine.wait(job_id)
        self.assertEqual(await self.engine.get_result(job_id), "done")

    async def test_non_callable_is_rejected_without_registration(self) -> None:
        with self.assertRaises(TypeError):
            await self.engine.submit(None)
        self.assertEqual(self.engine._jobs, {})

    async def test_callable_awaitable_and_synchronous_invocation_failure(self) -> None:
        future = asyncio.get_running_loop().create_future()

        class Work:
            def __call__(self) -> asyncio.Future:
                return future

        job_id = await self.engine.submit(Work())
        future.set_result("future result")
        await self.engine.wait(job_id)
        self.assertEqual(await self.engine.get_result(job_id), "future result")

        cause = RuntimeError("raised before returning an awaitable")

        def fail():
            raise cause

        failed_id = await self.engine.submit(fail)
        await self.engine.wait(failed_id)
        with self.assertRaises(JobExecutionError) as error:
            await self.engine.get_result(failed_id)
        self.assertIs(error.exception.cause, cause)

    async def test_cancelling_waiter_does_not_cancel_job_or_other_waiters(self) -> None:
        release = asyncio.Event()
        job_id = await self.engine.submit(release.wait)
        cancelled_waiter = asyncio.create_task(self.engine.wait(job_id))
        other_waiter = asyncio.create_task(self.engine.wait(job_id))
        await asyncio.sleep(0)
        cancelled_waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await cancelled_waiter
        self.assertEqual(await self.engine.get_status(job_id), JobStatus.RUNNING)
        self.assertFalse(other_waiter.done())
        release.set()
        await other_waiter
        self.assertEqual(await self.engine.get_status(job_id), JobStatus.SUCCEEDED)


if __name__ == "__main__":
    unittest.main()
