"""v2 の公開契約をイベント同期と制御可能なタイマーで検証する。"""

import asyncio
from dataclasses import replace
import unittest
from unittest.mock import patch

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
    async def asyncSetUp(self) -> None:
        self.engine = create_engine(1)
        self.deadline = asyncio.timeout(5)
        await self.deadline.__aenter__()

    async def asyncTearDown(self) -> None:
        await self.deadline.__aexit__(None, None, None)

    async def result(self, job_id):
        await self.engine.wait(job_id)
        return await self.engine.get_result(job_id)

    async def failure(self, job_id):
        await self.engine.wait(job_id)
        with self.assertRaises(JobExecutionError) as error:
            await self.engine.get_result(job_id)
        return error.exception.cause

    async def test_retry_success_and_fresh_invocation(self):
        """例外のサブクラスも再試行し、成功後は試行を追加しない。"""
        calls = []

        async def work(value, *, increment):
            calls.append(value)
            if len(calls) < 3:
                raise LookupError("retry")
            return value + increment

        job = await self.engine.submit_job(JobSpec(
            work, (4,), {"increment": 2}, RetryPolicy(5, retry_on=(LookupError,))
        ))
        self.assertEqual(await self.result(job), 6)
        self.assertEqual(calls, [4, 4, 4])

    async def test_retry_exhaustion_and_filter_preserve_last_exception(self):
        """上限と対象例外を守り、最後の例外オブジェクトを保持する。"""
        for retries, retry_on, expected in (
            (0, (Exception,), 1), (2, (Exception,), 3), (2, (TypeError,), 1),
        ):
            with self.subTest(retries=retries, retry_on=retry_on):
                causes = []

                async def work():
                    cause = ValueError(str(len(causes)))
                    causes.append(cause)
                    raise cause

                job = await self.engine.submit_job(JobSpec(
                    work, retry=RetryPolicy(retries, retry_on=retry_on)
                ))
                self.assertIs(await self.failure(job), causes[-1])
                self.assertEqual(len(causes), expected)

    async def test_self_cancellation_is_not_retried(self):
        """処理自身の CancelledError は通常のリトライ対象にしない。"""
        calls = 0

        async def work():
            nonlocal calls
            calls += 1
            raise asyncio.CancelledError

        job = await self.engine.submit_job(JobSpec(work, retry=RetryPolicy(3)))
        await self.engine.wait(job)
        with self.assertRaises(JobCancelledError):
            await self.engine.get_result(job)
        self.assertEqual(calls, 1)

    async def test_backoff_sequence_state_slots_and_cancellation(self):
        """指数待機と上限を守り、待機中は枠を解放してキャンセルできる。"""
        loop = asyncio.get_running_loop()
        real_call_later = loop.call_later
        timers = asyncio.Queue()
        calls = 0

        def controlled_timer(delay, callback, *args, **kwargs):
            # バックオフだけ進行を制御し、実時間への依存を避ける。
            if getattr(callback, "__name__", "") == "_enqueue":
                handle = real_call_later(3600, callback, *args, **kwargs)
                timers.put_nowait((delay, callback, args, handle))
                return handle
            return real_call_later(delay, callback, *args, **kwargs)

        async def work():
            nonlocal calls
            calls += 1
            raise ValueError("retry")

        with patch.object(loop, "call_later", side_effect=controlled_timer):
            job = await self.engine.submit_job(JobSpec(
                work, retry=RetryPolicy(5, initial_delay=2, multiplier=3, max_delay=10)
            ))
            for expected_delay in (2, 6, 10):
                delay, callback, args, handle = await timers.get()
                self.assertEqual(delay, expected_delay)
                self.assertEqual(await self.engine.get_status(job), JobStatus.PENDING)
                with self.assertRaises(JobNotFinishedError):
                    await self.engine.get_result(job)

                async def other():
                    return "free slot"

                other_id = await self.engine.submit(other)
                self.assertEqual(await self.result(other_id), "free slot")
                handle.cancel()
                callback(*args)
            _, _, _, handle = await timers.get()
            self.assertTrue(await self.engine.cancel(job))
            self.assertTrue(handle.cancelled())
            await self.engine.wait(job)
            with self.assertRaises(JobCancelledError):
                await self.engine.get_result(job)
        self.assertEqual(calls, 4)

    async def test_timeout_retries_each_attempt(self):
        """各試行のタイムアウトを再試行し、最後の原因を TimeoutError にする。"""
        calls = 0

        async def work():
            nonlocal calls
            calls += 1
            await asyncio.Event().wait()

        job = await self.engine.submit_job(JobSpec(
            work, timeout=0.01, retry=RetryPolicy(2, retry_on=(TimeoutError,))
        ))
        self.assertIsInstance(await self.failure(job), TimeoutError)
        self.assertEqual(calls, 3)

    async def test_timeout_filter(self):
        """TimeoutError が対象外なら即座に失敗する。"""
        calls = 0

        async def work():
            nonlocal calls
            calls += 1
            await asyncio.Event().wait()

        job = await self.engine.submit_job(JobSpec(
            work, timeout=0.01, retry=RetryPolicy(3, retry_on=(ValueError,))
        ))
        self.assertIsInstance(await self.failure(job), TimeoutError)
        self.assertEqual(calls, 1)

    async def test_interruption_race_cleanup_and_actual_outcome(self):
        """先着の中断だけを通知し、実際の終了まで状態と実行枠を保持する。"""
        for source in ("timeout", "cancel"):
            for outcome in ("cancelled", "success", "failure"):
                with self.subTest(source=source, outcome=outcome):
                    started = asyncio.Event()
                    cleaning = asyncio.Event()
                    release = asyncio.Event()
                    next_started = asyncio.Event()
                    cause = ValueError("cleanup")
                    notifications = 0

                    async def work():
                        nonlocal notifications
                        started.set()
                        try:
                            await asyncio.Event().wait()
                        except asyncio.CancelledError:
                            notifications += 1
                            cleaning.set()
                            await release.wait()
                            if outcome == "success":
                                return 42
                            if outcome == "failure":
                                raise cause
                            raise

                    async def next_work():
                        next_started.set()

                    job = await self.engine.submit_job(JobSpec(work, timeout=0.01))
                    await started.wait()
                    if source == "cancel":
                        self.assertTrue(await self.engine.cancel(job))
                    await cleaning.wait()
                    self.assertFalse(await self.engine.cancel(job))
                    next_id = await self.engine.submit(next_work)
                    # 明示的キャンセル後にも元のタイムアウト時刻を通過させる。
                    await asyncio.sleep(0.02)
                    self.assertEqual(await self.engine.get_status(job), JobStatus.RUNNING)
                    self.assertFalse(next_started.is_set())
                    with self.assertRaises(JobNotFinishedError):
                        await self.engine.get_result(job)
                    release.set()
                    await self.engine.wait(job)
                    if outcome == "success":
                        self.assertEqual(await self.engine.get_result(job), 42)
                    elif outcome == "failure":
                        self.assertIs(await self.failure(job), cause)
                    elif source == "timeout":
                        self.assertIsInstance(await self.failure(job), TimeoutError)
                    else:
                        with self.assertRaises(JobCancelledError):
                            await self.engine.get_result(job)
                    await self.engine.wait(next_id)
                    self.assertEqual(notifications, 1)

    async def test_waiting_for_slot_does_not_consume_timeout(self):
        """実行枠待ちに費やした時間を試行の制限時間に含めない。"""
        started = asyncio.Event()
        release = asyncio.Event()

        async def blocker():
            started.set()
            await release.wait()

        async def work():
            return "ok"

        first = await self.engine.submit(blocker)
        await started.wait()
        job = await self.engine.submit_job(JobSpec(work, timeout=0.01))
        await asyncio.sleep(0.02)
        self.assertEqual(await self.engine.get_status(job), JobStatus.PENDING)
        release.set()
        self.assertEqual(await self.result(job), "ok")
        await self.engine.wait(first)

    async def test_dependencies_retry_then_success_and_priority(self):
        """依存先の最終成功を待ち、実行可能な優先度順に自動進行する。"""
        started = asyncio.Event()
        release = asyncio.Event()
        order = []
        attempts = 0

        async def root():
            nonlocal attempts
            attempts += 1
            started.set()
            await release.wait()
            if attempts == 1:
                raise ValueError("retry")

        async def work(name):
            order.append(name)

        first = await self.engine.submit_job(JobSpec(root, retry=RetryPolicy(1)))
        await started.wait()
        low = await self.engine.submit_job(JobSpec(work, ("low",), priority=-1))
        high = await self.engine.submit_job(JobSpec(work, ("high",), priority=10))
        child = await self.engine.submit_job(JobSpec(
            work, ("child",), priority=100, depends_on=(first,)
        ))
        release.set()
        await asyncio.gather(*(self.engine.wait(key) for key in (first, low, high, child)))
        self.assertEqual(order, ["high", "child", "low"])
        self.assertEqual(attempts, 2)

    async def test_multiple_dependencies_and_fifo(self):
        """複数依存先の全成功を待ち、同順位の実行可能順を保つ。"""
        order = []

        async def work(name):
            order.append(name)

        a = await self.engine.submit(work, "a")
        b = await self.engine.submit(work, "b")
        c = await self.engine.submit_job(JobSpec(work, ("c",), depends_on=(a, b)))
        d = await self.engine.submit(work, "d")
        await asyncio.gather(*(self.engine.wait(key) for key in (a, b, c, d)))
        self.assertEqual(order, ["a", "b", "d", "c"])

    async def test_dependency_ready_before_completion_waiter_submits(self):
        """完了待機者が新規登録する前に、既存の依存待ちを実行可能にする。"""
        order = []

        async def root():
            pass

        async def work(name):
            order.append(name)

        first = await self.engine.submit(root)
        child = await self.engine.submit_job(JobSpec(work, ("child",), depends_on=(first,)))
        await self.engine.wait(first)
        later = await self.engine.submit(work, "later")
        await asyncio.gather(self.engine.wait(child), self.engine.wait(later))
        self.assertEqual(order, ["child", "later"])

    async def test_dependency_failure_long_chain(self):
        """深い依存連鎖でも失敗伝播を完了し、再帰上限に達しない。"""
        async def root():
            raise ValueError("root")

        async def forbidden():
            self.fail("Failed dependency body ran")

        first = await self.engine.submit(root)
        previous = first
        for _ in range(1500):
            parent = previous
            previous = await self.engine.submit_job(JobSpec(forbidden, depends_on=(parent,)))
        self.assertEqual((await self.failure(previous)).dependency_id, parent)

    async def test_empty_retry_filter_raises_value_error(self):
        """空の対象例外指定は型エラーと区別して値エラーにする。"""
        async def work():
            pass

        with self.assertRaises(ValueError):
            await self.engine.submit_job(JobSpec(work, retry=RetryPolicy(retry_on=())))

    async def test_dependency_failure_propagates_through_diamond(self):
        """失敗・キャンセルを直接依存先の原因として連鎖させ、本体を実行しない。"""
        for cancelled in (False, True):
            with self.subTest(cancelled=cancelled):
                release = asyncio.Event()
                calls = []

                async def root():
                    await release.wait()
                    raise ValueError("root failed")

                async def child():
                    calls.append("unexpected")

                a = await self.engine.submit(root)
                b = await self.engine.submit_job(JobSpec(child, depends_on=(a,)))
                c = await self.engine.submit_job(JobSpec(child, depends_on=(a, b)))
                d = await self.engine.submit_job(JobSpec(child, depends_on=(b, c)))
                if cancelled:
                    await self.engine.cancel(a)
                release.set()
                for key, dependencies in ((b, (a,)), (c, (a, b)), (d, (b, c))):
                    cause = await self.failure(key)
                    self.assertIsInstance(cause, JobDependencyError)
                    self.assertIn(cause.dependency_id, dependencies)
                self.assertEqual(calls, [])
                # 既に失敗している依存先を指定した登録も同じ契約。
                late = await self.engine.submit_job(JobSpec(child, depends_on=(d,)))
                self.assertEqual((await self.failure(late)).dependency_id, d)

    async def test_cancel_dependency_waiter(self):
        """依存待ちの直接キャンセルは先行ジョブに影響しない。"""
        release = asyncio.Event()

        async def root():
            await release.wait()
            return 1

        async def forbidden():
            self.fail("Cancelled dependency waiter ran")

        first = await self.engine.submit(root)
        child = await self.engine.submit_job(JobSpec(forbidden, depends_on=(first,)))
        self.assertTrue(await self.engine.cancel(child))
        with self.assertRaises(JobCancelledError):
            await self.result(child)
        release.set()
        self.assertEqual(await self.result(first), 1)

    async def test_registration_snapshots_kwargs(self):
        """呼び出し元による登録後の kwargs 変更を実行に反映しない。"""
        async def work(*, value):
            return value

        kwargs = {"value": 1}
        spec = JobSpec(work, kwargs=kwargs)
        job = await self.engine.submit_job(spec)
        kwargs["value"] = 99
        kwargs["unexpected"] = True
        self.assertEqual(await self.result(job), 1)

    async def test_registration_validation_is_atomic(self):
        """無効な設定と未知・重複依存先を拒否し、処理を起動しない。"""
        invoked = []

        async def work():
            invoked.append(True)

        base = JobSpec(work)
        invalid = [None, replace(base, func=None), replace(base, args=[]),
                   replace(base, kwargs={1: 2}), replace(base, retry=None),
                   replace(base, depends_on=["id"])]
        for value in (True, 1.5, "1", None):
            invalid.append(replace(base, priority=value))
        for value in (-1, True, 1.5, "1"):
            invalid.append(replace(base, retry=RetryPolicy(max_retries=value)))
        for name in ("initial_delay", "multiplier", "max_delay", "timeout"):
            for value in (-1, float("nan"), float("inf"), True, "1"):
                if name == "timeout":
                    invalid.append(replace(base, timeout=value))
                else:
                    invalid.append(replace(base, retry=replace(RetryPolicy(), **{name: value})))
        invalid.extend([
            replace(base, timeout=0),
            replace(base, retry=RetryPolicy(multiplier=0.5)),
            replace(base, retry=RetryPolicy(initial_delay=None)),
            replace(base, retry=RetryPolicy(multiplier=None)),
        ])
        for value in ((), [Exception], (BaseException,), (asyncio.CancelledError,), (1,)):
            invalid.append(replace(base, retry=RetryPolicy(retry_on=value)))
        for spec in invalid:
            with self.subTest(spec=spec):
                with self.assertRaises((TypeError, ValueError)):
                    await self.engine.submit_job(spec)
        with self.assertRaises(JobNotFoundError):
            await self.engine.submit_job(replace(base, depends_on=("missing",)))
        with self.assertRaises(ValueError):
            await self.engine.submit_job(replace(base, depends_on=("missing", "missing")))
        await asyncio.sleep(0)
        self.assertEqual(invoked, [])
        valid = await self.engine.submit(work)
        await self.engine.wait(valid)
        self.assertEqual(invoked, [True])


if __name__ == "__main__":
    unittest.main()
