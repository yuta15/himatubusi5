import asyncio
import unittest

from interface import (
    JobCancelledError,
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
        cause = BaseException("failed")

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
            for method in (engine.get_status, engine.get_result, engine.wait, engine.cancel):
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

    async def test_pending_cancellation_never_invokes_callable(self):
        invoked = False

        def job():
            nonlocal invoked
            invoked = True
            raise AssertionError("cancelled pending job was invoked")

        job_id = await self.engine.submit(job)
        self.assertEqual(await self.engine.get_status(job_id), JobStatus.PENDING)
        self.assertTrue(await self.engine.cancel(job_id))
        self.assertFalse(await self.engine.cancel(job_id))
        for _ in range(2):
            self.assertIsNone(await self.finish(job_id))
            self.assertEqual(await self.engine.get_status(job_id), JobStatus.CANCELLED)
            with self.assertRaises(JobCancelledError):
                await self.engine.get_result(job_id)
        self.assertFalse(invoked)

    async def test_running_cancellation_waits_for_cleanup_and_notifies_once(self):
        started = asyncio.Event()
        cleaning = asyncio.Event()
        release = asyncio.Event()
        notifications = 0

        async def job():
            nonlocal notifications
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                notifications += 1
                cleaning.set()
                await release.wait()
                raise

        job_id = await self.engine.submit(job)
        await asyncio.wait_for(started.wait(), timeout=2)
        waiters = [asyncio.create_task(self.engine.wait(job_id)) for _ in range(3)]
        # wait_for also verifies cancel returns without waiting for cleanup.
        self.assertTrue(await asyncio.wait_for(self.engine.cancel(job_id), timeout=2))
        await asyncio.wait_for(cleaning.wait(), timeout=2)
        self.assertEqual(await self.engine.get_status(job_id), JobStatus.RUNNING)
        with self.assertRaises(JobNotFinishedError):
            await self.engine.get_result(job_id)
        self.assertEqual(
            await asyncio.gather(*(self.engine.cancel(job_id) for _ in range(5))),
            [False] * 5,
        )
        waiters[0].cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiters[0]
        self.assertTrue(all(not waiter.done() for waiter in waiters[1:]))
        release.set()
        self.assertEqual(await asyncio.wait_for(asyncio.gather(*waiters[1:]), 2), [None, None])
        self.assertEqual(notifications, 1)
        self.assertEqual(await self.engine.get_status(job_id), JobStatus.CANCELLED)
        with self.assertRaises(JobCancelledError):
            await self.engine.get_result(job_id)
        self.assertFalse(await self.engine.cancel(job_id))

    async def test_cancellation_handler_outcome_determines_terminal_state(self):
        for outcome in ("success", "failure"):
            with self.subTest(outcome=outcome):
                started = asyncio.Event()
                cause = ValueError("cleanup failed")

                async def job():
                    started.set()
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        if outcome == "failure":
                            raise cause
                        return "recovered"

                job_id = await self.engine.submit(job)
                await asyncio.wait_for(started.wait(), 2)
                self.assertTrue(await self.engine.cancel(job_id))
                await self.finish(job_id)
                if outcome == "success":
                    self.assertEqual(await self.engine.get_status(job_id), JobStatus.SUCCEEDED)
                    self.assertEqual(await self.engine.get_result(job_id), "recovered")
                else:
                    self.assertEqual(await self.engine.get_status(job_id), JobStatus.FAILED)
                    with self.assertRaises(JobExecutionError) as error:
                        await self.engine.get_result(job_id)
                    self.assertIs(error.exception.cause, cause)
                self.assertFalse(await self.engine.cancel(job_id))

    async def test_cancelled_error_without_request(self):
        for outcome in ("cancelled", "success", "failure"):
            with self.subTest(outcome=outcome):
                cause = RuntimeError("replacement")

                async def job():
                    try:
                        raise asyncio.CancelledError("from job")
                    except asyncio.CancelledError:
                        if outcome == "success":
                            return 42
                        if outcome == "failure":
                            raise cause
                        raise

                job_id = await self.engine.submit(job)
                await self.finish(job_id)
                expected = {
                    "cancelled": JobStatus.CANCELLED,
                    "success": JobStatus.SUCCEEDED,
                    "failure": JobStatus.FAILED,
                }[outcome]
                self.assertEqual(await self.engine.get_status(job_id), expected)
                if outcome == "cancelled":
                    with self.assertRaises(JobCancelledError):
                        await self.engine.get_result(job_id)
                elif outcome == "success":
                    self.assertEqual(await self.engine.get_result(job_id), 42)
                else:
                    with self.assertRaises(JobExecutionError) as error:
                        await self.engine.get_result(job_id)
                    self.assertIs(error.exception.cause, cause)
                self.assertFalse(await self.engine.cancel(job_id))

    async def test_multiple_jobs_cancel_independently(self):
        started = [asyncio.Event() for _ in range(3)]
        release = asyncio.Event()

        async def job(index):
            started[index].set()
            await release.wait()
            return index

        ids = [await self.engine.submit(job, i) for i in range(3)]
        await asyncio.wait_for(asyncio.gather(*(event.wait() for event in started)), 2)
        results = await asyncio.gather(
            self.engine.cancel(ids[0]), self.engine.cancel(ids[1]),
            self.engine.cancel(ids[0]), self.engine.cancel(ids[1]),
        )
        self.assertEqual(results, [True, True, False, False])
        await asyncio.gather(*(self.finish(job_id) for job_id in ids[:2]))
        self.assertEqual(await self.engine.get_status(ids[2]), JobStatus.RUNNING)
        release.set()
        await self.finish(ids[2])
        self.assertEqual(await self.engine.get_result(ids[2]), 2)
        for job_id in ids[:2]:
            self.assertEqual(await self.engine.get_status(job_id), JobStatus.CANCELLED)
            with self.assertRaises(JobCancelledError):
                await self.engine.get_result(job_id)

    async def test_completion_and_cancellation_race_preserves_outcome(self):
        for fails in (False, True):
            for cancel_first in (False, True):
                with self.subTest(fails=fails, cancel_first=cancel_first):
                    started = asyncio.Event()
                    release = asyncio.Event()
                    cause = ValueError("failed")

                    async def job():
                        started.set()
                        await release.wait()
                        if fails:
                            raise cause
                        return 42

                    job_id = await self.engine.submit(job)
                    await asyncio.wait_for(started.wait(), 2)
                    if cancel_first:
                        self.assertTrue(await self.engine.cancel(job_id))
                        release.set()
                    else:
                        release.set()
                        # Queue cancel after the job's continuation, before
                        # awaiting either, to exercise the completion race.
                        self.assertFalse(await asyncio.create_task(self.engine.cancel(job_id)))
                    await self.finish(job_id)
                    expected = (JobStatus.CANCELLED if cancel_first else
                                JobStatus.FAILED if fails else JobStatus.SUCCEEDED)
                    for _ in range(2):
                        self.assertEqual(await self.engine.get_status(job_id), expected)
                        self.assertFalse(await self.engine.cancel(job_id))
                        if cancel_first:
                            with self.assertRaises(JobCancelledError):
                                await self.engine.get_result(job_id)
                        elif fails:
                            with self.assertRaises(JobExecutionError) as error:
                                await self.engine.get_result(job_id)
                            self.assertIs(error.exception.cause, cause)
                        else:
                            self.assertEqual(await self.engine.get_result(job_id), 42)


if __name__ == "__main__":
    unittest.main()
