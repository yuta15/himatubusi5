"""v1.1 のキャンセル契約を公開 API とイベント同期で検証する。"""

import asyncio
import unittest

from interface import (
    JobCancelledError,
    JobError,
    JobExecutionError,
    JobNotFinishedError,
    JobNotFoundError,
    JobStatus,
)
from main import create_engine


class CancellationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.engine = create_engine()
        self.deadline = asyncio.timeout(5)
        await self.deadline.__aenter__()

    async def asyncTearDown(self) -> None:
        await self.deadline.__aexit__(None, None, None)

    async def assert_cancelled(self, job_id: str) -> None:
        # 終端状態・結果・待機が後続の操作でも一貫することを確認する。
        for _ in range(2):
            self.assertIsNone(await self.engine.wait(job_id))
            self.assertEqual(await self.engine.get_status(job_id), JobStatus.CANCELLED)
            with self.assertRaises(JobCancelledError) as error:
                await self.engine.get_result(job_id)
            self.assertIsInstance(error.exception, JobError)
            self.assertFalse(await self.engine.cancel(job_id))

    async def test_pending_cancel_never_invokes_callable(self) -> None:
        invoked = False

        def work():
            nonlocal invoked
            invoked = True
            raise AssertionError("Pending job must not be invoked")

        job_id = await self.engine.submit(work)
        self.assertEqual(await self.engine.get_status(job_id), JobStatus.PENDING)
        self.assertTrue(await self.engine.cancel(job_id))
        self.assertFalse(await self.engine.cancel(job_id))
        await self.assert_cancelled(job_id)
        self.assertFalse(invoked)

    async def test_running_cancel_notifies_all_waiters(self) -> None:
        started = asyncio.Event()

        async def work() -> None:
            started.set()
            await asyncio.Event().wait()

        job_id = await self.engine.submit(work)
        await started.wait()
        waiters = [asyncio.create_task(self.engine.wait(job_id)) for _ in range(3)]
        await asyncio.sleep(0)
        waiters[0].cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiters[0]
        self.assertEqual(await self.engine.get_status(job_id), JobStatus.RUNNING)
        self.assertTrue(await self.engine.cancel(job_id))
        self.assertEqual(await asyncio.gather(*waiters[1:]), [None, None])
        await self.assert_cancelled(job_id)

    async def test_cleanup_is_not_finished_or_interrupted_by_duplicate_requests(self) -> None:
        for outcome in ("cancelled", "succeeded", "failed"):
            with self.subTest(outcome=outcome):
                started = asyncio.Event()
                cleaning = asyncio.Event()
                release = asyncio.Event()
                cause = ValueError("cleanup failed")
                notifications = 0

                async def work() -> str:
                    nonlocal notifications
                    started.set()
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        notifications += 1
                        cleaning.set()
                        await release.wait()
                        if outcome == "cancelled":
                            raise
                        if outcome == "failed":
                            raise cause
                        return "recovered"

                job_id = await self.engine.submit(work)
                await started.wait()
                # cancel() が終了待機してしまうと、このテストはタイムアウトする。
                self.assertTrue(await self.engine.cancel(job_id))
                await cleaning.wait()
                waiter = asyncio.create_task(self.engine.wait(job_id))
                results = await asyncio.gather(*(self.engine.cancel(job_id) for _ in range(5)))
                self.assertEqual(results, [False] * 5)
                self.assertEqual(notifications, 1)
                self.assertFalse(waiter.done())
                self.assertEqual(await self.engine.get_status(job_id), JobStatus.RUNNING)
                with self.assertRaises(JobNotFinishedError):
                    await self.engine.get_result(job_id)
                release.set()
                self.assertIsNone(await waiter)
                if outcome == "cancelled":
                    await self.assert_cancelled(job_id)
                elif outcome == "succeeded":
                    self.assertEqual(await self.engine.get_status(job_id), JobStatus.SUCCEEDED)
                    self.assertEqual(await self.engine.get_result(job_id), "recovered")
                else:
                    self.assertEqual(await self.engine.get_status(job_id), JobStatus.FAILED)
                    with self.assertRaises(JobExecutionError) as error:
                        await self.engine.get_result(job_id)
                    self.assertIs(error.exception.cause, cause)
                self.assertFalse(await self.engine.cancel(job_id))

    async def test_cancelled_error_without_request_uses_actual_outcome(self) -> None:
        for outcome in ("cancelled", "succeeded", "failed"):
            with self.subTest(outcome=outcome):
                cause = RuntimeError("replacement error")

                async def work() -> None:
                    try:
                        raise asyncio.CancelledError()
                    except asyncio.CancelledError:
                        if outcome == "cancelled":
                            raise
                        if outcome == "failed":
                            raise cause

                job_id = await self.engine.submit(work)
                await self.engine.wait(job_id)
                if outcome == "cancelled":
                    await self.assert_cancelled(job_id)
                elif outcome == "succeeded":
                    self.assertEqual(await self.engine.get_status(job_id), JobStatus.SUCCEEDED)
                    self.assertIsNone(await self.engine.get_result(job_id))
                else:
                    self.assertEqual(await self.engine.get_status(job_id), JobStatus.FAILED)
                    with self.assertRaises(JobExecutionError) as error:
                        await self.engine.get_result(job_id)
                    self.assertIs(error.exception.cause, cause)

    async def test_unknown_id_and_engine_isolation(self) -> None:
        job_id = await self.engine.submit(asyncio.sleep, 0)
        other_engine = create_engine()
        for engine, unknown_id in ((self.engine, "missing"), (other_engine, job_id)):
            with self.assertRaises(JobNotFoundError):
                await engine.cancel(unknown_id)
        await self.engine.wait(job_id)
        self.assertEqual(await self.engine.get_status(job_id), JobStatus.SUCCEEDED)

    async def test_independent_cancellations_and_unaffected_job(self) -> None:
        started = [asyncio.Event() for _ in range(3)]
        release = asyncio.Event()

        async def work(index: int) -> int:
            started[index].set()
            await release.wait()
            return index

        ids = [await self.engine.submit(work, index) for index in range(3)]
        await asyncio.gather(*(event.wait() for event in started))
        results = await asyncio.gather(*(self.engine.cancel(ids[0]) for _ in range(5)))
        self.assertEqual(results.count(True), 1)
        self.assertEqual(results.count(False), 4)
        self.assertTrue(await self.engine.cancel(ids[1]))
        await asyncio.gather(*(self.assert_cancelled(job_id) for job_id in ids[:2]))
        self.assertEqual(await self.engine.get_status(ids[2]), JobStatus.RUNNING)
        release.set()
        await self.engine.wait(ids[2])
        self.assertEqual(await self.engine.get_result(ids[2]), 2)
        for job_id in ids[:2]:
            await self.assert_cancelled(job_id)

    async def test_completion_racing_with_cancellation_preserves_terminal_state(self) -> None:
        for fails in (False, True):
            for cancel_first in (False, True):
                with self.subTest(fails=fails, cancel_first=cancel_first):
                    started = asyncio.Event()
                    release = asyncio.Event()
                    cause = ValueError("work failed")

                    async def work() -> str:
                        started.set()
                        await release.wait()
                        if fails:
                            raise cause
                        return "done"

                    job_id = await self.engine.submit(work)
                    await started.wait()
                    release.set()
                    if cancel_first:
                        # 正常/異常終了の再開がキューにあっても、先に通知できる。
                        self.assertTrue(await self.engine.cancel(job_id))
                        await self.assert_cancelled(job_id)
                    else:
                        # _run が終端状態を確定し、完了コールバックはまだ未実行。
                        await asyncio.sleep(0)
                        self.assertFalse(await self.engine.cancel(job_id))
                        expected = JobStatus.FAILED if fails else JobStatus.SUCCEEDED
                        for _ in range(2):
                            self.assertEqual(await self.engine.get_status(job_id), expected)
                            self.assertIsNone(await self.engine.wait(job_id))
                            self.assertFalse(await self.engine.cancel(job_id))
                            if fails:
                                with self.assertRaises(JobExecutionError) as error:
                                    await self.engine.get_result(job_id)
                                self.assertIs(error.exception.cause, cause)
                            else:
                                self.assertEqual(await self.engine.get_result(job_id), "done")


if __name__ == "__main__":
    unittest.main()
