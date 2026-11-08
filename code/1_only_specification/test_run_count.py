import asyncio
import unittest

from interface import JobCancelledError, JobExecutionError, JobNotFinishedError, JobStatus
from main import create_engine


class ConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def finish(self, engine, *ids):
        await asyncio.wait_for(asyncio.gather(*(engine.wait(i) for i in ids)), 2)

    def test_invalid_limits(self):
        for value in (0, -1, -100, True, False, 1.0, "2", [], object()):
            with self.subTest(value=value):
                with self.assertRaises((TypeError, ValueError)):
                    create_engine(value)

    async def test_limit_pending_queries_and_automatic_progress(self):
        for limit in (1, 3):
            with self.subTest(limit=limit):
                engine = create_engine(limit)
                release = asyncio.Event()
                saturated = asyncio.Event()
                active = peak = 0
                calls = []

                async def job(index):
                    nonlocal active, peak
                    calls.append(index)
                    active += 1
                    peak = max(peak, active)
                    if active == limit:
                        saturated.set()
                    try:
                        await release.wait()
                        return index
                    finally:
                        active -= 1

                running = [await engine.submit(job, i) for i in range(limit)]
                await asyncio.wait_for(saturated.wait(), 2)
                queued = await asyncio.wait_for(
                    asyncio.gather(*(engine.submit(job, i) for i in range(limit, 20))), 2
                )
                self.assertEqual(len(set(running + queued)), 20)
                for job_id in queued:
                    self.assertEqual(await engine.get_status(job_id), JobStatus.PENDING)
                    with self.assertRaises(JobNotFinishedError):
                        await engine.get_result(job_id)
                waiter = asyncio.create_task(engine.wait(queued[-1]))
                await asyncio.sleep(0)
                self.assertFalse(waiter.done())
                release.set()
                await self.finish(engine, *running, *queued)
                await asyncio.wait_for(waiter, 2)
                self.assertEqual(peak, limit)
                self.assertEqual(active, 0)
                self.assertEqual(sorted(calls), list(range(20)))
                for index, job_id in enumerate(running + queued):
                    self.assertEqual(await engine.get_status(job_id), JobStatus.SUCCEEDED)
                    self.assertEqual(await engine.get_result(job_id), index)

    async def test_pending_cancellation_does_not_consume_slots(self):
        engine = create_engine(1)
        started = asyncio.Event()
        release = asyncio.Event()
        invoked = []

        async def blocker():
            started.set()
            await release.wait()

        async def job(index):
            invoked.append(index)
            return index

        first = await engine.submit(blocker)
        await asyncio.wait_for(started.wait(), 2)
        early = await engine.submit(job, 0)
        self.assertTrue(await engine.cancel(early))
        queued = [await engine.submit(job, i) for i in range(1, 4)]
        # Let wrappers reach the slot wait before cancelling queued jobs.
        await asyncio.sleep(0)
        for job_id in queued[:2]:
            self.assertTrue(await engine.cancel(job_id))
        cancelled = [early, *queued[:2]]
        await self.finish(engine, *cancelled)
        self.assertEqual(invoked, [])
        self.assertEqual(await engine.get_status(first), JobStatus.RUNNING)
        self.assertEqual(await engine.get_status(queued[-1]), JobStatus.PENDING)
        release.set()
        await self.finish(engine, first, *queued)
        self.assertEqual(invoked, [3])
        for job_id in cancelled:
            self.assertEqual(await engine.get_status(job_id), JobStatus.CANCELLED)
            self.assertFalse(await engine.cancel(job_id))
            with self.assertRaises(JobCancelledError):
                await engine.get_result(job_id)

    async def test_each_terminal_outcome_releases_slot(self):
        for outcome in ("success", "failure", "cancelled", "self_cancelled"):
            with self.subTest(outcome=outcome):
                engine = create_engine(1)
                started = asyncio.Event()
                release = asyncio.Event()
                cause = ValueError("failed")

                async def first():
                    started.set()
                    await release.wait()
                    if outcome == "failure":
                        raise cause
                    if outcome == "self_cancelled":
                        raise asyncio.CancelledError
                    return 42

                async def next_job():
                    return "next"

                first_id = await engine.submit(first)
                await asyncio.wait_for(started.wait(), 2)
                next_id = await engine.submit(next_job)
                if outcome == "cancelled":
                    self.assertTrue(await engine.cancel(first_id))
                else:
                    release.set()
                await self.finish(engine, first_id, next_id)
                self.assertEqual(await engine.get_result(next_id), "next")
                if outcome == "success":
                    self.assertEqual(await engine.get_result(first_id), 42)
                elif outcome == "failure":
                    with self.assertRaises(JobExecutionError) as error:
                        await engine.get_result(first_id)
                    self.assertIs(error.exception.cause, cause)
                else:
                    with self.assertRaises(JobCancelledError):
                        await engine.get_result(first_id)

    async def test_cancellation_retains_slot_until_actual_exit(self):
        for outcome in ("cancelled", "success", "failure"):
            with self.subTest(outcome=outcome):
                engine = create_engine(1)
                started = asyncio.Event()
                cleaning = asyncio.Event()
                release = asyncio.Event()
                next_started = asyncio.Event()
                cause = RuntimeError("cleanup failed")

                async def first():
                    started.set()
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        cleaning.set()
                        await release.wait()
                        if outcome == "success":
                            return "recovered"
                        if outcome == "failure":
                            raise cause
                        raise

                async def next_job():
                    next_started.set()

                first_id = await engine.submit(first)
                await asyncio.wait_for(started.wait(), 2)
                next_id = await engine.submit(next_job)
                self.assertTrue(await engine.cancel(first_id))
                await asyncio.wait_for(cleaning.wait(), 2)
                self.assertFalse(await engine.cancel(first_id))
                self.assertEqual(await engine.get_status(first_id), JobStatus.RUNNING)
                self.assertEqual(await engine.get_status(next_id), JobStatus.PENDING)
                self.assertFalse(next_started.is_set())
                with self.assertRaises(JobNotFinishedError):
                    await engine.get_result(first_id)
                release.set()
                await self.finish(engine, first_id, next_id)
                self.assertTrue(next_started.is_set())
                if outcome == "success":
                    self.assertEqual(await engine.get_result(first_id), "recovered")
                elif outcome == "failure":
                    with self.assertRaises(JobExecutionError) as error:
                        await engine.get_result(first_id)
                    self.assertIs(error.exception.cause, cause)
                else:
                    with self.assertRaises(JobCancelledError):
                        await engine.get_result(first_id)

    async def test_engines_have_independent_limits_and_none_is_unlimited(self):
        limited = create_engine(1)
        other = create_engine(2)
        unlimited = create_engine(None)
        default = create_engine()
        release = asyncio.Event()

        async def job(started):
            started.set()
            await release.wait()

        ids = []
        for engine, count in ((limited, 1), (other, 2), (unlimited, 5), (default, 5)):
            started = [asyncio.Event() for _ in range(count)]
            job_ids = [await engine.submit(job, event) for event in started]
            ids.append((engine, job_ids))
            await asyncio.wait_for(asyncio.gather(*(e.wait() for e in started)), 2)
        pending_started = asyncio.Event()
        pending = await limited.submit(job, pending_started)
        await asyncio.sleep(0)
        self.assertFalse(pending_started.is_set())
        self.assertEqual(await limited.get_status(pending), JobStatus.PENDING)
        release.set()
        await asyncio.gather(*(self.finish(engine, *jobs) for engine, jobs in ids))
        await self.finish(limited, pending)


if __name__ == "__main__":
    unittest.main()
