import asyncio
import math
import unittest
from dataclasses import replace

from interface import (
    JobCancelledError,
    JobDependencyError,
    JobExecutionError,
    JobNotFinishedError,
    JobNotFoundError,
    JobSpec,
    JobStatus,
    RetryPolicy,
)
from main import create_engine


class V2Tests(unittest.IsolatedAsyncioTestCase):
    async def finish(self, engine, *ids):
        await asyncio.wait_for(asyncio.gather(*(engine.wait(i) for i in ids)), 2)

    async def failure(self, engine, job_id):
        await self.finish(engine, job_id)
        self.assertEqual(await engine.get_status(job_id), JobStatus.FAILED)
        with self.assertRaises(JobExecutionError) as error:
            await engine.get_result(job_id)
        return error.exception.cause

    async def test_invalid_specs_are_rejected_before_registration(self):
        invoked = []

        async def job():
            invoked.append(True)

        base = JobSpec(job)
        invalid = [None, replace(base, func=None), replace(base, args=[]),
                   replace(base, kwargs=[]), replace(base, kwargs={1: 2}),
                   replace(base, retry=None), replace(base, depends_on=[]),
                   replace(base, depends_on=(1,))]
        for value in (True, False, 1.0, "1", None):
            invalid.append(replace(base, priority=value))
            invalid.append(replace(base, retry=RetryPolicy(max_retries=value)))
        invalid.append(replace(base, retry=RetryPolicy(max_retries=-1)))
        for name, bad_values in (
            ("initial_delay", (-1, math.inf, -math.inf, math.nan, True, "0")),
            ("multiplier", (0, .5, math.inf, math.nan, False, None)),
            ("max_delay", (-1, math.inf, math.nan, True, "1")),
        ):
            for value in bad_values:
                invalid.append(replace(base, retry=RetryPolicy(**{name: value})))
        for value in (0, -1, math.inf, math.nan, True, "1"):
            invalid.append(replace(base, timeout=value))
        for value in ([], (), (BaseException,), (asyncio.CancelledError,), (1,)):
            invalid.append(replace(base, retry=RetryPolicy(retry_on=value)))
        engine = create_engine()
        for spec in invalid:
            with self.subTest(spec=spec):
                with self.assertRaises((TypeError, ValueError)):
                    await engine.submit_job(spec)
        with self.assertRaises(JobNotFoundError):
            await engine.submit_job(replace(base, depends_on=("missing",)))
        first = await engine.submit(job)
        with self.assertRaises(ValueError):
            await engine.submit_job(replace(base, depends_on=(first, first)))
        await self.finish(engine, first)
        self.assertEqual(invoked, [True])

    async def test_registration_snapshots_keyword_configuration(self):
        engine = create_engine(1)
        started, release = asyncio.Event(), asyncio.Event()

        async def blocker():
            started.set()
            await release.wait()

        first = await engine.submit(blocker)
        await started.wait()

        async def job(value, *, increment):
            return value + increment

        kwargs = {"increment": 2}
        spec = JobSpec(job, args=(3,), kwargs=kwargs)
        job_id = await engine.submit_job(spec)
        kwargs["increment"] = 100
        release.set()
        await self.finish(engine, first, job_id)
        self.assertEqual(await engine.get_result(job_id), 5)

    async def test_retries_success_exhaustion_and_filtering(self):
        for retry_on, failures, expected_attempts in (
            ((Exception,), 2, 3), ((ValueError,), 2, 3),
            ((RuntimeError,), 2, 1), ((Exception,), 5, 3),
        ):
            with self.subTest(retry_on=retry_on, failures=failures):
                engine = create_engine()
                causes = []

                async def job():
                    if len(causes) < failures:
                        cause = ValueError(str(len(causes)))
                        causes.append(cause)
                        raise cause
                    return 42

                job_id = await engine.submit_job(JobSpec(
                    job, retry=RetryPolicy(max_retries=2, retry_on=retry_on)))
                await self.finish(engine, job_id)
                if failures < expected_attempts:
                    self.assertEqual(await engine.get_result(job_id), 42)
                    self.assertEqual(len(causes), failures)
                else:
                    self.assertIs(await self.failure(engine, job_id), causes[-1])
                    self.assertEqual(len(causes), expected_attempts)

    async def test_synchronous_invocation_errors_retry_and_self_cancel_does_not(self):
        engine = create_engine()
        attempts = 0

        def job():
            nonlocal attempts
            attempts += 1
            raise ValueError("invocation")

        job_id = await engine.submit_job(JobSpec(job, retry=RetryPolicy(max_retries=2)))
        self.assertIsInstance(await self.failure(engine, job_id), ValueError)
        self.assertEqual(attempts, 3)

        async def cancelled():
            nonlocal attempts
            attempts += 1
            raise asyncio.CancelledError

        job_id = await engine.submit_job(JobSpec(cancelled, retry=RetryPolicy(max_retries=2)))
        await self.finish(engine, job_id)
        self.assertEqual(await engine.get_status(job_id), JobStatus.CANCELLED)
        self.assertEqual(attempts, 4)

    async def test_backoff_delays_and_cap(self):
        engine = create_engine()
        times = []

        async def job():
            times.append(asyncio.get_running_loop().time())
            if len(times) < 4:
                raise ValueError()
            return "done"

        job_id = await engine.submit_job(JobSpec(
            job, retry=RetryPolicy(max_retries=3, initial_delay=.02,
                                   multiplier=2, max_delay=.05)))
        await self.finish(engine, job_id)
        for before, after, delay in zip(times, times[1:], (.02, .04, .05)):
            self.assertGreaterEqual(after - before, delay - .001)
        self.assertEqual(await engine.get_result(job_id), "done")

    async def test_backoff_is_pending_releases_slot_and_can_be_cancelled(self):
        engine = create_engine(1)
        failed = asyncio.Event()
        attempts = 0

        async def job():
            nonlocal attempts
            attempts += 1
            failed.set()
            raise ValueError()

        job_id = await engine.submit_job(JobSpec(
            job, retry=RetryPolicy(max_retries=5, initial_delay=10)))
        await failed.wait()
        self.assertEqual(await engine.get_status(job_id), JobStatus.PENDING)
        with self.assertRaises(JobNotFinishedError):
            await engine.get_result(job_id)
        waiter = asyncio.create_task(engine.wait(job_id))
        other = await engine.submit(asyncio.sleep, 0, result=42)
        await self.finish(engine, other)
        self.assertFalse(waiter.done())
        self.assertEqual(await engine.get_result(other), 42)
        self.assertTrue(await engine.cancel(job_id))
        self.assertFalse(await engine.cancel(job_id))
        await asyncio.wait_for(waiter, 2)
        self.assertEqual(attempts, 1)
        with self.assertRaises(JobCancelledError):
            await engine.get_result(job_id)

    async def test_timeout_is_per_attempt_and_can_retry(self):
        for retry_on, attempts_expected in (((TimeoutError,), 3), ((ValueError,), 1)):
            with self.subTest(retry_on=retry_on):
                engine = create_engine()
                attempts = active = peak = 0

                async def job():
                    nonlocal attempts, active, peak
                    attempts += 1
                    active += 1
                    peak = max(peak, active)
                    try:
                        await asyncio.Event().wait()
                    finally:
                        active -= 1

                job_id = await engine.submit_job(JobSpec(
                    job, timeout=.01,
                    retry=RetryPolicy(max_retries=2, retry_on=retry_on)))
                self.assertIsInstance(await self.failure(engine, job_id), TimeoutError)
                self.assertEqual(attempts, attempts_expected)
                self.assertEqual(peak, 1)
                self.assertEqual(active, 0)

    async def test_timeout_cleanup_actual_outcome_and_first_interruption_wins(self):
        for first in ("timeout", "cancel"):
            for outcome in ("cancelled", "success", "failure"):
                with self.subTest(first=first, outcome=outcome):
                    engine = create_engine(1)
                    started, cleaning, release = (asyncio.Event() for _ in range(3))
                    cause = ValueError("cleanup failure")
                    interruptions = 0

                    async def job():
                        nonlocal interruptions
                        started.set()
                        try:
                            await asyncio.Event().wait()
                        except asyncio.CancelledError:
                            interruptions += 1
                            cleaning.set()
                            try:
                                await release.wait()
                            except asyncio.CancelledError:
                                interruptions += 1
                                raise
                            if outcome == "success":
                                return 42
                            if outcome == "failure":
                                raise cause
                            raise

                    job_id = await engine.submit_job(JobSpec(job, timeout=.01))
                    await started.wait()
                    other = await engine.submit(asyncio.sleep, 0, result="next")
                    if first == "cancel":
                        self.assertTrue(await engine.cancel(job_id))
                    await asyncio.wait_for(cleaning.wait(), 2)
                    if first == "timeout":
                        self.assertFalse(await engine.cancel(job_id))
                        self.assertFalse(await engine.cancel(job_id))
                    await asyncio.sleep(.02)
                    self.assertEqual(await engine.get_status(job_id), JobStatus.RUNNING)
                    self.assertEqual(await engine.get_status(other), JobStatus.PENDING)
                    self.assertEqual(interruptions, 1)
                    release.set()
                    await self.finish(engine, job_id, other)
                    self.assertEqual(await engine.get_result(other), "next")
                    if outcome == "success":
                        self.assertEqual(await engine.get_result(job_id), 42)
                    elif outcome == "failure":
                        self.assertIs(await self.failure(engine, job_id), cause)
                    elif first == "timeout":
                        self.assertIsInstance(await self.failure(engine, job_id), TimeoutError)
                    else:
                        self.assertEqual(await engine.get_status(job_id), JobStatus.CANCELLED)

    async def test_rejected_cancel_after_timeout_does_not_prevent_later_cancel(self):
        for cancel_during in ("backoff", "next_attempt"):
            with self.subTest(cancel_during=cancel_during):
                engine = create_engine()
                cleaning, release, retry_started = (asyncio.Event() for _ in range(3))
                attempts = 0

                async def job():
                    nonlocal attempts
                    attempts += 1
                    if attempts > 1:
                        retry_started.set()
                        await asyncio.Event().wait()
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        cleaning.set()
                        await release.wait()
                        raise

                job_id = await engine.submit_job(JobSpec(
                    job, timeout=.05,
                    retry=RetryPolicy(max_retries=2, initial_delay=(
                        10 if cancel_during == "backoff" else 0)),
                ))
                await asyncio.wait_for(cleaning.wait(), 2)
                self.assertFalse(await engine.cancel(job_id))
                release.set()
                if cancel_during == "backoff":
                    # A queued loop callback runs after the first attempt exits.
                    await asyncio.sleep(0)
                    self.assertEqual(await engine.get_status(job_id), JobStatus.PENDING)
                else:
                    await asyncio.wait_for(retry_started.wait(), 2)
                    self.assertEqual(await engine.get_status(job_id), JobStatus.RUNNING)
                self.assertTrue(await engine.cancel(job_id))
                self.assertFalse(await engine.cancel(job_id))
                await self.finish(engine, job_id)
                self.assertEqual(await engine.get_status(job_id), JobStatus.CANCELLED)
                self.assertEqual(attempts, 1 if cancel_during == "backoff" else 2)

    async def test_dependencies_wait_for_all_and_for_final_retry_result(self):
        engine = create_engine(1)
        release = asyncio.Event()
        attempted = asyncio.Event()
        calls = []

        async def parent():
            calls.append("parent")
            if calls.count("parent") == 1:
                attempted.set()
                raise ValueError()
            await release.wait()
            return "parent result"

        async def child(label):
            calls.append(label)
            return label

        first = await engine.submit_job(JobSpec(
            parent, retry=RetryPolicy(max_retries=1, initial_delay=.02)))
        second = await engine.submit(child, "independent")
        joined = await engine.submit_job(JobSpec(
            child, args=("joined",), depends_on=(first, second), priority=100))
        await attempted.wait()
        await self.finish(engine, second)
        self.assertEqual(await engine.get_status(joined), JobStatus.PENDING)
        release.set()
        await self.finish(engine, first, joined)
        self.assertEqual(calls, ["parent", "independent", "parent", "joined"])

    async def test_dependency_failure_propagates_with_direct_cause(self):
        for outcome in ("failed", "cancelled"):
            with self.subTest(outcome=outcome):
                engine = create_engine()
                invoked = []

                async def parent():
                    if outcome == "cancelled":
                        raise asyncio.CancelledError
                    raise ValueError()

                async def child():
                    invoked.append(True)

                root = await engine.submit(parent)
                middle = await engine.submit_job(JobSpec(child, depends_on=(root,)))
                leaf = await engine.submit_job(JobSpec(child, depends_on=(middle,)))
                await self.finish(engine, root, middle, leaf)
                for job_id, dependency_id in ((middle, root), (leaf, middle)):
                    cause = await self.failure(engine, job_id)
                    self.assertIsInstance(cause, JobDependencyError)
                    self.assertEqual(cause.dependency_id, dependency_id)
                late = await engine.submit_job(JobSpec(child, depends_on=(root,)))
                self.assertIsInstance(await self.failure(engine, late), JobDependencyError)
                self.assertEqual(invoked, [])

    async def test_cancel_dependency_waiter_does_not_affect_parent_or_siblings(self):
        engine = create_engine(1)
        release = asyncio.Event()
        first = await engine.submit(release.wait)
        cancelled = await engine.submit_job(JobSpec(asyncio.sleep, args=(0,), depends_on=(first,)))
        sibling = await engine.submit_job(JobSpec(asyncio.sleep, args=(0,), depends_on=(first,)))
        self.assertTrue(await engine.cancel(cancelled))
        release.set()
        await self.finish(engine, first, cancelled, sibling)
        self.assertEqual(await engine.get_status(cancelled), JobStatus.CANCELLED)
        self.assertEqual(await engine.get_status(first), JobStatus.SUCCEEDED)
        self.assertEqual(await engine.get_status(sibling), JobStatus.SUCCEEDED)

    async def test_priority_and_fifo_among_ready_jobs(self):
        engine = create_engine(1)
        started, release = asyncio.Event(), asyncio.Event()
        order = []

        async def blocker():
            started.set()
            await release.wait()

        async def job(label):
            order.append(label)

        first = await engine.submit(blocker)
        await started.wait()
        ids = []
        for label, priority in (("low", -2), ("normal1", 0), ("high", 10), ("normal2", 0)):
            ids.append(await engine.submit_job(JobSpec(job, args=(label,), priority=priority)))
        release.set()
        await self.finish(engine, first, *ids)
        self.assertEqual(order, ["high", "normal1", "normal2", "low"])

    async def test_retries_reenter_priority_queue(self):
        engine = create_engine(1)
        order = []
        started, release = asyncio.Event(), asyncio.Event()

        async def retrying():
            order.append("retry")
            if order.count("retry") == 1:
                raise ValueError()

        async def blocker():
            started.set()
            await release.wait()

        async def low_priority():
            order.append("low")

        high = await engine.submit_job(JobSpec(
            retrying, priority=10, retry=RetryPolicy(max_retries=1, initial_delay=.01)))
        blocked = await engine.submit(blocker)
        await started.wait()
        low = await engine.submit_job(JobSpec(low_priority, priority=-1))
        await asyncio.sleep(.02)
        release.set()
        await self.finish(engine, high, blocked, low)
        self.assertEqual(order, ["retry", "retry", "low"])
        self.assertEqual(await engine.get_status(low), JobStatus.SUCCEEDED)

    async def test_queue_and_dependency_wait_are_excluded_from_timeout(self):
        engine = create_engine(1)
        started, release = asyncio.Event(), asyncio.Event()

        async def parent():
            started.set()
            await release.wait()

        first = await engine.submit(parent)
        await started.wait()
        queued = await engine.submit_job(JobSpec(asyncio.sleep, args=(0,), timeout=.01))
        dependent = await engine.submit_job(JobSpec(
            asyncio.sleep, args=(0,), timeout=.01, depends_on=(first,)))
        await asyncio.sleep(.03)
        release.set()
        await self.finish(engine, first, queued, dependent)
        for job_id in (queued, dependent):
            self.assertEqual(await engine.get_status(job_id), JobStatus.SUCCEEDED)


if __name__ == "__main__":
    unittest.main()
