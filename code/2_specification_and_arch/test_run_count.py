"""v1.2 の同時実行数制限を公開 API とイベント同期で検証する。"""

import asyncio
import unittest

from interface import (
    JobCancelledError,
    JobExecutionError,
    JobNotFinishedError,
    JobStatus,
)
from main import create_engine


class ConcurrencyValidationTests(unittest.TestCase):
    def test_invalid_limits(self) -> None:
        """正でない整数、整数以外、bool を生成時に拒否する。"""
        for limit in (0, -1, -100, True, False, 1.0, "2", [], object()):
            with self.subTest(limit=limit):
                with self.assertRaises((ValueError, TypeError)):
                    create_engine(limit)


class ConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.deadline = asyncio.timeout(5)
        await self.deadline.__aenter__()

    async def asyncTearDown(self) -> None:
        await self.deadline.__aexit__(None, None, None)

    async def test_limit_pending_queries_waiters_and_automatic_progress(self) -> None:
        """上限を守り、空き枠ごとに待機ジョブを一度だけ実行する。"""
        engine = create_engine(max_concurrency=3)
        started = asyncio.Queue()
        releases = [asyncio.Event() for _ in range(12)]
        invocations = [0] * len(releases)
        active = 0
        peak = 0

        async def work(index: int) -> int:
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            invocations[index] += 1
            started.put_nowait(index)
            try:
                await releases[index].wait()
                return index
            finally:
                active -= 1

        ids = [await engine.submit(work, index) for index in range(len(releases))]
        self.assertEqual(len(set(ids)), len(ids))
        running = [await started.get() for _ in range(3)]
        pending = set(range(len(ids))) - set(running)
        for index in pending:
            self.assertEqual(await engine.get_status(ids[index]), JobStatus.PENDING)
            with self.assertRaises(JobNotFinishedError):
                await engine.get_result(ids[index])
        waiters = [asyncio.create_task(engine.wait(job_id)) for job_id in ids]
        await asyncio.sleep(0)
        self.assertTrue(all(not waiter.done() for waiter in waiters))
        for _ in range(len(pending)):
            releases[running.pop()].set()
            running.append(await started.get())
            statuses = [await engine.get_status(job_id) for job_id in ids]
            self.assertEqual(statuses.count(JobStatus.RUNNING), 3)
        for index in running:
            releases[index].set()
        await asyncio.gather(*waiters)
        self.assertEqual(peak, 3)
        self.assertEqual(invocations, [1] * len(ids))
        self.assertEqual([await engine.get_result(job_id) for job_id in ids], list(range(12)))

    async def test_pending_cancellation_does_not_block_other_jobs(self) -> None:
        """実行前と実行枠待ちのキャンセル後も、他のジョブが進行する。"""
        engine = create_engine(1)
        running = asyncio.Event()
        release = asyncio.Event()
        invoked = []

        async def block() -> None:
            running.set()
            await release.wait()

        async def work(index: int) -> int:
            invoked.append(index)
            return index

        first = await engine.submit(block)
        await running.wait()
        cancelled = [await engine.submit(work, index) for index in range(2)]
        self.assertTrue(await engine.cancel(cancelled[0]))
        await asyncio.sleep(0)
        self.assertEqual(await engine.get_status(cancelled[1]), JobStatus.PENDING)
        self.assertTrue(await engine.cancel(cancelled[1]))
        for job_id in cancelled:
            await engine.wait(job_id)
            self.assertEqual(await engine.get_status(job_id), JobStatus.CANCELLED)
            self.assertFalse(await engine.cancel(job_id))
            with self.assertRaises(JobCancelledError):
                await engine.get_result(job_id)
        next_id = await engine.submit(work, 2)
        release.set()
        await asyncio.gather(engine.wait(first), engine.wait(next_id))
        self.assertEqual(invoked, [2])
        self.assertEqual(await engine.get_result(next_id), 2)

    async def test_failure_and_self_cancellation_release_slots(self) -> None:
        """呼び出し時の例外と処理中の例外・キャンセルでも枠を解放する。"""
        for outcome in ("invocation_failure", "failure", "cancelled"):
            with self.subTest(outcome=outcome):
                engine = create_engine(1)
                cause = ValueError("failed")

                async def fail() -> None:
                    if outcome == "cancelled":
                        raise asyncio.CancelledError()
                    raise cause

                def invocation_failure():
                    raise cause

                first = await engine.submit(
                    invocation_failure if outcome == "invocation_failure" else fail
                )
                next_id = await engine.submit(asyncio.sleep, 0, result="done")
                await asyncio.gather(engine.wait(first), engine.wait(next_id))
                expected = JobCancelledError if outcome == "cancelled" else JobExecutionError
                with self.assertRaises(expected):
                    await engine.get_result(first)
                self.assertEqual(await engine.get_result(next_id), "done")

    async def test_running_cancellation_holds_slot_until_actual_completion(self) -> None:
        """キャンセル後の後処理や抑制中も、実際の終了まで枠を保持する。"""
        for outcome in ("cancelled", "succeeded", "failed"):
            with self.subTest(outcome=outcome):
                engine = create_engine(1)
                started = asyncio.Event()
                cleaning = asyncio.Event()
                release = asyncio.Event()
                next_started = asyncio.Event()
                cause = ValueError("cleanup failed")

                async def work() -> str:
                    started.set()
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        cleaning.set()
                        await release.wait()
                        if outcome == "cancelled":
                            raise
                        if outcome == "failed":
                            raise cause
                        return "recovered"

                async def next_work() -> None:
                    next_started.set()

                first = await engine.submit(work)
                await started.wait()
                next_id = await engine.submit(next_work)
                self.assertTrue(await engine.cancel(first))
                await cleaning.wait()
                self.assertFalse(await engine.cancel(first))
                self.assertEqual(await engine.get_status(first), JobStatus.RUNNING)
                self.assertEqual(await engine.get_status(next_id), JobStatus.PENDING)
                self.assertFalse(next_started.is_set())
                release.set()
                await next_started.wait()
                await asyncio.gather(engine.wait(first), engine.wait(next_id))
                if outcome == "succeeded":
                    self.assertEqual(await engine.get_result(first), "recovered")
                else:
                    expected = JobCancelledError if outcome == "cancelled" else JobExecutionError
                    with self.assertRaises(expected):
                        await engine.get_result(first)

    async def test_engine_limits_are_independent(self) -> None:
        """一方のエンジンが満杯でも、他方は自身の上限まで実行できる。"""
        engines = [create_engine(1), create_engine(2)]
        release = asyncio.Event()
        started = [[asyncio.Event() for _ in range(3)] for _ in engines]

        async def work(event: asyncio.Event) -> None:
            event.set()
            await release.wait()

        ids = [
            [await engine.submit(work, event) for event in events]
            for engine, events in zip(engines, started)
        ]
        for events, limit in zip(started, (1, 2)):
            await asyncio.gather(*(event.wait() for event in events[:limit]))
            self.assertFalse(events[limit].is_set())
        release.set()
        await asyncio.gather(*(
            engine.wait(job_id) for engine, jobs in zip(engines, ids) for job_id in jobs
        ))

    async def test_default_and_explicit_none_are_unlimited(self) -> None:
        """省略時と None 指定時は、すべてのジョブを並行実行できる。"""
        for engine in (create_engine(), create_engine(None)):
            release = asyncio.Event()
            started = [asyncio.Event() for _ in range(8)]

            async def work(event: asyncio.Event) -> None:
                event.set()
                await release.wait()

            ids = [await engine.submit(work, event) for event in started]
            await asyncio.gather(*(event.wait() for event in started))
            for job_id in ids:
                self.assertEqual(await engine.get_status(job_id), JobStatus.RUNNING)
            release.set()
            await asyncio.gather(*(engine.wait(job_id) for job_id in ids))


if __name__ == "__main__":
    unittest.main()
