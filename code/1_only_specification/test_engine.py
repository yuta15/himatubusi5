import asyncio
import unittest

from interface import (
    JobExecutionError,
    JobNotFinishedError,
    JobNotFoundError,
    JobStatus,
)
from main import create_engine


class JobEngineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_engine()

    async def finish(self, job_id):
        return await asyncio.wait_for(self.engine.wait(job_id), timeout=2)

    async def test_arguments_unique_ids_and_retained_results(self):
        async def calculate(a, b, *, scale):
            return (a + b) * scale

        first = await self.engine.submit(calculate, 2, 3, scale=4)
        second = await self.engine.submit(calculate, 5, 6, scale=2)
        self.assertIsInstance(first, str)
        self.assertNotEqual(first, second)
        self.assertIsNone(await self.finish(first))
        await self.finish(second)
        for _ in range(2):
            self.assertEqual(await self.engine.get_result(first), 20)
            self.assertEqual(await self.engine.get_result(second), 22)
            self.assertEqual(await self.engine.get_status(first), JobStatus.SUCCEEDED)
            self.assertIsNone(await self.finish(first))

    async def test_submission_states_and_none_result(self):
        started = asyncio.Event()
        release = asyncio.Event()

        async def job():
            started.set()
            await release.wait()

        job_id = await asyncio.wait_for(self.engine.submit(job), timeout=2)
        self.assertIn(
            await self.engine.get_status(job_id),
            (JobStatus.PENDING, JobStatus.RUNNING),
        )
        with self.assertRaises(JobNotFinishedError):
            await self.engine.get_result(job_id)
        await asyncio.wait_for(started.wait(), timeout=2)
        self.assertEqual(await self.engine.get_status(job_id), JobStatus.RUNNING)
        with self.assertRaises(JobNotFinishedError):
            await self.engine.get_result(job_id)
        release.set()
        await self.finish(job_id)
        self.assertEqual(await self.engine.get_status(job_id), JobStatus.SUCCEEDED)
        self.assertIsNone(await self.engine.get_result(job_id))

    async def test_jobs_make_progress_concurrently(self):
        first_started = asyncio.Event()
        second_started = asyncio.Event()

        async def first():
            first_started.set()
            await second_started.wait()
            return "first"

        async def second():
            second_started.set()
            await first_started.wait()
            return "second"

        first_id = await self.engine.submit(first)
        second_id = await self.engine.submit(second)
        await asyncio.gather(self.finish(first_id), self.finish(second_id))
        self.assertEqual(await self.engine.get_result(first_id), "first")
        self.assertEqual(await self.engine.get_result(second_id), "second")

    async def test_failure_retains_original_cause_and_other_jobs_continue(self):
        cause = ValueError("job failed")
        release = asyncio.Event()

        async def fail():
            raise cause

        async def succeed():
            await release.wait()
            return 42

        other_id = await self.engine.submit(succeed)
        failed_id = await self.engine.submit(fail)
        self.assertIsNone(await self.finish(failed_id))
        for _ in range(2):
            self.assertEqual(await self.engine.get_status(failed_id), JobStatus.FAILED)
            with self.assertRaises(JobExecutionError) as error:
                await self.engine.get_result(failed_id)
            self.assertIs(error.exception.cause, cause)
        self.assertEqual(await self.engine.get_status(other_id), JobStatus.RUNNING)
        release.set()
        await self.finish(other_id)
        self.assertEqual(await self.engine.get_result(other_id), 42)

    async def test_callable_objects_and_functions_returning_awaitables(self):
        loop = asyncio.get_running_loop()

        class Job:
            def __call__(self, value):
                future = loop.create_future()
                future.set_result(value)
                return future

        job_id = await self.engine.submit(Job(), "value")
        await self.finish(job_id)
        self.assertEqual(await self.engine.get_result(job_id), "value")

    async def test_errors_when_invoking_or_awaiting_callable_fail_job(self):
        cause = RuntimeError("invocation failed")

        def raises():
            raise cause

        def not_awaitable():
            return 1

        for func, expected in ((raises, RuntimeError), (not_awaitable, TypeError)):
            with self.subTest(func=func):
                job_id = await self.engine.submit(func)
                await self.finish(job_id)
                self.assertEqual(await self.engine.get_status(job_id), JobStatus.FAILED)
                with self.assertRaises(JobExecutionError) as error:
                    await self.engine.get_result(job_id)
                self.assertIsInstance(error.exception.cause, expected)

    async def test_base_exceptions_finish_job(self):
        for cause in (asyncio.CancelledError("cancelled"), BaseException("failed")):
            with self.subTest(cause=cause):
                async def fail():
                    raise cause

                job_id = await self.engine.submit(fail)
                await self.finish(job_id)
                self.assertEqual(await self.engine.get_status(job_id), JobStatus.FAILED)
                with self.assertRaises(JobExecutionError) as error:
                    await self.engine.get_result(job_id)
                self.assertIs(error.exception.cause, cause)

    async def test_missing_ids_and_independent_engines(self):
        other = create_engine()
        self.assertIsNot(self.engine, other)

        async def job():
            return 1

        job_id = await self.engine.submit(job)
        await self.finish(job_id)
        for engine, missing in ((self.engine, "missing"), (other, job_id)):
            for method in (engine.get_status, engine.get_result, engine.wait):
                with self.subTest(method=method, missing=missing):
                    with self.assertRaises(JobNotFoundError):
                        await method(missing)

    async def test_non_callable_is_rejected(self):
        for value in (None, 123, "job"):
            with self.subTest(value=value):
                with self.assertRaises(TypeError):
                    await self.engine.submit(value)

    async def test_multiple_waiters_and_waiter_cancellation(self):
        release = asyncio.Event()
        waiter_started = asyncio.Event()

        async def job():
            await release.wait()
            return "done"

        job_id = await self.engine.submit(job)

        async def wait():
            waiter_started.set()
            await self.engine.wait(job_id)

        cancelled_waiter = asyncio.create_task(wait())
        await asyncio.wait_for(waiter_started.wait(), timeout=2)
        cancelled_waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await cancelled_waiter
        self.assertEqual(await self.engine.get_status(job_id), JobStatus.RUNNING)
        waiters = [asyncio.create_task(self.finish(job_id)) for _ in range(3)]
        release.set()
        self.assertEqual(await asyncio.gather(*waiters), [None, None, None])
        self.assertEqual(await self.engine.get_result(job_id), "done")


if __name__ == "__main__":
    unittest.main()
