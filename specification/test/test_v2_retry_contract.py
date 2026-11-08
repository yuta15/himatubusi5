"""v2の公開設定・登録・リトライ・指数バックオフの共通契約。"""

import asyncio
import inspect
from dataclasses import FrozenInstanceError, fields

import pytest

from cancel_support import assert_terminal
from support import bounded, checkpoint, return_value
from v2_support import (
    EARLY_TOLERANCE, LATE_TOLERANCE, RETRY_DELAY, assert_pending,
    elapsed, failure_cause, status_is,
)


def test_submit_job_success_public_types(api):
    """V01/V10: 公開型の既定値・frozen、kwargsの独立性、依存例外を確認する。"""
    retry = api.RetryPolicy()
    assert [f.name for f in fields(retry)] == [
        "max_retries", "initial_delay", "multiplier", "max_delay", "retry_on"
    ]
    assert (retry.max_retries, retry.initial_delay, retry.multiplier,
            retry.max_delay, retry.retry_on) == (0, 0.0, 2.0, None, (Exception,))
    first, second = api.JobSpec(return_value), api.JobSpec(return_value)
    assert [f.name for f in fields(first)] == [
        "func", "args", "kwargs", "retry", "timeout", "priority", "depends_on"
    ]
    assert (first.func, first.args, first.kwargs, first.retry, first.timeout,
            first.priority, first.depends_on) == (return_value, (), {}, retry, None, 0, ())
    for obj, name in ((retry, "max_retries"), (first, "priority")):
        with pytest.raises(FrozenInstanceError):
            setattr(obj, name, 1)
    first.kwargs["local"] = 1
    assert second.kwargs == {}
    error = api.JobDependencyError("parent")
    assert isinstance(error, api.JobError)
    assert error.dependency_id == "parent"
    assert str(error) == "Dependency job did not succeed: parent"


def test_submit_job_success_public_method(engine, api):
    """V01: submit_jobは仕様の引数を受け付ける非同期の公開APIである。"""
    assert inspect.iscoroutinefunction(api.JobEngine.submit_job)
    assert list(inspect.signature(api.JobEngine.submit_job).parameters) == ["self", "spec"]
    assert inspect.iscoroutinefunction(engine.submit_job)
    inspect.signature(engine.submit_job).bind(spec=api.JobSpec(return_value))


@pytest.mark.parametrize("kind", ["async-function", "async-callable", "coroutine-factory", "awaitable", "future"])
async def test_submit_job_success_callable_arguments(engine, api, v2_jobs, kind):
    """V02: 各種Awaitableを返すCallableへ引数を渡せ、一意IDと対応した結果を得る。"""
    async def body(*args, **kwargs):
        return args, kwargs

    class AsyncCallable:
        async def __call__(self, *args, **kwargs):
            return await body(*args, **kwargs)

    class AwaitableValue:
        def __init__(self, value):
            self.value = value

        def __await__(self):
            return return_value(self.value).__await__()

    def future(*args, **kwargs):
        result = asyncio.get_running_loop().create_future()
        result.set_result((args, kwargs))
        return result

    funcs = {
        "async-function": body, "async-callable": AsyncCallable(),
        "coroutine-factory": lambda *a, **kw: body(*a, **kw),
        "awaitable": lambda *a, **kw: AwaitableValue((a, kw)), "future": future,
    }
    ids = [await v2_jobs.submit_spec(engine, api.JobSpec(
        funcs[kind], args=(index,), kwargs={"named": index + 1}
    )) for index in range(2)]
    assert all(isinstance(job_id, str) for job_id in ids)
    assert len(set(ids)) == 2
    for index, job_id in enumerate(ids):
        await assert_terminal(engine, api, job_id, api.JobStatus.SUCCEEDED,
                              ((index,), {"named": index + 1}))


@pytest.mark.parametrize("waiting", ["body", "capacity", "dependency"])
async def test_submit_job_success_nonblocking_registration(engine_factory, api, v2_jobs, waiting):
    """V03: 本体・実行枠・依存先の終了を待たずに登録が復帰する。"""
    engine = engine_factory(max_concurrency=1)
    holder = v2_jobs.attempt(["parent"], blocked=(0,))
    parent = await v2_jobs.submit_spec(engine, api.JobSpec(holder))
    await bounded(holder.starts[0].wait())
    if waiting == "body":
        assert await bounded(engine.get_status(parent)) is api.JobStatus.RUNNING
        return
    child = v2_jobs.attempt(["child"])
    job_id = await v2_jobs.submit_spec(engine, api.JobSpec(
        child, depends_on=(parent,) if waiting == "dependency" else ()
    ))
    await assert_pending(engine, api, job_id)
    assert child.calls == 0
    holder.gates[0].set()
    await assert_terminal(engine, api, job_id, api.JobStatus.SUCCEEDED, "child")


@pytest.mark.parametrize("field,value,error_type", [
    pytest.param("func", None, TypeError, id="func-non-callable"),
    *[pytest.param("max_retries", v, TypeError, id=f"retries-type-{i}")
      for i, v in enumerate([True, False, 1.0, "1", None])],
    pytest.param("max_retries", -1, ValueError, id="retries-negative"),
    *[pytest.param(f, v, TypeError, id=f"{f}-type-{i}")
      for f in ("initial_delay", "multiplier", "max_delay", "timeout")
      for i, v in enumerate([True, False, "1", []])],
    *[pytest.param(f, v, ValueError, id=f"{f}-range-{i}")
      for f in ("initial_delay", "max_delay", "timeout")
      for i, v in enumerate([-1, float("nan"), float("inf"), -float("inf")])],
    *[pytest.param("multiplier", v, ValueError, id=f"multiplier-range-{i}")
      for i, v in enumerate([0, 0.5, float("nan"), float("inf"), -float("inf")])],
    pytest.param("timeout", 0, ValueError, id="timeout-zero"),
    pytest.param("initial_delay", None, TypeError, id="initial-delay-none"),
    pytest.param("multiplier", None, TypeError, id="multiplier-none"),
    pytest.param("retry_on", (), ValueError, id="retry-on-empty"),
    *[pytest.param("retry_on", v, TypeError, id=f"retry-on-type-{i}")
      for i, v in enumerate([
          [ValueError], (ValueError("instance"),), (object,), (BaseException,),
          (asyncio.CancelledError,), (ValueError, "bad"), None,
      ])],
    *[pytest.param("priority", v, TypeError, id=f"priority-type-{i}")
      for i, v in enumerate([True, False, 1.0, "1", None])],
])
async def test_submit_job_failure_invalid_settings_atomic(
    engine_factory, api, v2_jobs, field, value, error_type
):
    """V04〜V08: 型・範囲違反を登録時に区別して拒否し、本体を後から実行しない。"""
    engine = engine_factory(max_concurrency=1)
    holder = v2_jobs.attempt([None], blocked=(0,))
    holder_id = await v2_jobs.submit(engine, holder)
    await bounded(holder.starts[0].wait())
    rejected = v2_jobs.attempt(["rejected"])
    if field in {f.name for f in fields(api.RetryPolicy)}:
        spec = api.JobSpec(rejected, retry=api.RetryPolicy(**{field: value}))
    else:
        spec = api.JobSpec(**{"func": rejected, field: value})
    with pytest.raises(error_type):
        await v2_jobs.submit_spec(engine, spec)
    holder.gates[0].set()
    await bounded(engine.wait(holder_id))
    healthy = await v2_jobs.submit_spec(engine, api.JobSpec(return_value, args=("healthy",)))
    await assert_terminal(engine, api, healthy, api.JobStatus.SUCCEEDED, "healthy")
    await checkpoint()
    assert rejected.invocations == 0


@pytest.mark.parametrize("priority", [-100, 0, 100])
async def test_submit_job_success_settings_snapshot(engine_factory, api, v2_jobs, priority):
    """V07/V09: 有効な境界値を受理し、kwargsの変更を初回・再試行へ反映しない。"""
    engine = engine_factory(max_concurrency=1)
    holder = v2_jobs.attempt([None], blocked=(0,))
    holder_id = await v2_jobs.submit(engine, holder)
    await bounded(holder.starts[0].wait())
    seen = []
    nested = []

    async def body(*args, **kwargs):
        seen.append((args, kwargs.copy()))
        if len(seen) == 1:
            raise ValueError("retry")
        return "snapshot"

    spec = api.JobSpec(body, args=(nested,), kwargs={"key": "before"},
                       retry=api.RetryPolicy(max_retries=1, multiplier=1, max_delay=0),
                       priority=priority)
    job_id = await v2_jobs.submit_spec(engine, spec)
    spec.kwargs["key"] = "after"
    spec.kwargs["added"] = 1
    holder.gates[0].set()
    await bounded(engine.wait(holder_id))
    await assert_terminal(engine, api, job_id, api.JobStatus.SUCCEEDED, "snapshot")
    assert len(seen) == 2
    assert all(args == (nested,) and kw == {"key": "before"} for args, kw in seen)
    # argsは不変なtuple。nestedの同一性やディープコピーは要求しない。


@pytest.mark.parametrize("max_retries", [0, 1, 2])
async def test_submit_job_failure_retry_budget(engine, api, v2_jobs, max_retries):
    """R01/R04/R05: 初回を除く予算、最後の例外の同一性、Callable再呼出しを確認する。"""
    causes = [ValueError(f"attempt-{i}") for i in range(max_retries + 1)]
    job = v2_jobs.attempt(causes)
    job_id = await v2_jobs.submit_spec(engine, api.JobSpec(
        job, retry=api.RetryPolicy(max_retries=max_retries)
    ))
    assert await failure_cause(engine, api, job_id, ValueError) is causes[-1]
    assert job.calls == job.invocations == max_retries + 1
    assert job.peak == 1


@pytest.mark.parametrize("failures", [0, 1, 2])
async def test_submit_job_success_retry_stops_at_success(engine, api, v2_jobs, failures):
    """R02: 成功した試行のNoneを返し、残りの試行を実行しない。"""
    job = v2_jobs.attempt([ValueError("retry")] * failures + [None])
    job_id = await v2_jobs.submit_spec(engine, api.JobSpec(job, retry=api.RetryPolicy(max_retries=3)))
    await assert_terminal(engine, api, job_id, api.JobStatus.SUCCEEDED, None)
    await checkpoint()
    assert job.calls == failures + 1


@pytest.mark.parametrize("outcomes,expected_calls,success", [
    ([ValueError("match"), "ok"], 2, True),
    ([UnicodeError("subclass"), "ok"], 2, True),
    ([RuntimeError("other"), "ok"], 1, False),
    ([ValueError("match"), RuntimeError("other"), "ok"], 2, False),
])
async def test_submit_job_success_retry_exception_filter(
    engine, api, v2_jobs, outcomes, expected_calls, success
):
    """R03: 指定型・派生型を再試行し、途中でも対象外例外は直ちに失敗確定する。"""
    job = v2_jobs.attempt(outcomes)
    job_id = await v2_jobs.submit_spec(engine, api.JobSpec(
        job, retry=api.RetryPolicy(max_retries=3, retry_on=(ValueError, KeyError))
    ))
    if success:
        await assert_terminal(engine, api, job_id, api.JobStatus.SUCCEEDED, "ok")
    else:
        assert await failure_cause(engine, api, job_id, RuntimeError) is outcomes[expected_calls - 1]
    assert job.calls == expected_calls


async def test_submit_job_success_spontaneous_cancel_not_retried(engine, api, v2_jobs):
    """R06: 自発的CancelledErrorは通常のリトライ対象にしない。"""
    job = v2_jobs.attempt([asyncio.CancelledError(), "must-not-run"])
    job_id = await v2_jobs.submit_spec(engine, api.JobSpec(job, retry=api.RetryPolicy(max_retries=2)))
    await assert_terminal(engine, api, job_id, api.JobStatus.CANCELLED)
    assert job.calls == 1


@pytest.mark.parametrize("initial,multiplier,cap,delays", [
    (RETRY_DELAY, 3, None, [RETRY_DELAY, RETRY_DELAY * 3, RETRY_DELAY * 9]),
    (RETRY_DELAY, 3, RETRY_DELAY * 2, [RETRY_DELAY, RETRY_DELAY * 2, RETRY_DELAY * 2]),
    (RETRY_DELAY, 1, None, [RETRY_DELAY] * 3),
    (RETRY_DELAY * 2, 2, RETRY_DELAY, [RETRY_DELAY] * 3),
    (0, 2, None, [0] * 3),
    (RETRY_DELAY, 2, 0, [0] * 3),
    (RETRY_DELAY, 1e308, RETRY_DELAY, [RETRY_DELAY] * 3),
])
async def test_submit_job_success_backoff_formula(
    engine, api, v2_jobs, initial, multiplier, cap, delays
):
    """B01/B02: 指数・上限・ゼロ・定数待機と、上限ありの累乗オーバーフローを確認する。"""
    job = v2_jobs.attempt([ValueError("retry")] * 3 + ["ok"])
    job_id = await v2_jobs.submit_spec(engine, api.JobSpec(job, retry=api.RetryPolicy(
        max_retries=3, initial_delay=initial, multiplier=multiplier, max_delay=cap
    )))
    for i, delay in enumerate(delays):
        await bounded(job.starts[i + 1].wait())
        measured = job.started_at[i + 1] - job.finished_at[i]
        assert delay - EARLY_TOLERANCE <= measured <= delay + LATE_TOLERANCE
    await assert_terminal(engine, api, job_id, api.JobStatus.SUCCEEDED, "ok")
    assert job.calls == job.invocations == 4
    assert job.peak == 1


async def test_submit_job_success_backoff_lifecycle_capacity(engine_factory, api, v2_jobs):
    """B03/B04: バックオフ中の状態・全体待機、枠解放と新規登録不要の再開を確認する。"""
    engine = engine_factory(max_concurrency=1)
    job = v2_jobs.attempt([ValueError("retry"), "ok"], blocked=(0, 1))
    job_id = await v2_jobs.submit_spec(engine, api.JobSpec(job, retry=api.RetryPolicy(
        max_retries=1, initial_delay=RETRY_DELAY * 4
    )))
    await bounded(job.starts[0].wait())
    waiter = await v2_jobs.waiter(engine, job_id)
    job.gates[0].set()
    await status_is(engine, job_id, api.JobStatus.PENDING)
    await assert_pending(engine, api, job_id, (waiter,))
    other = await v2_jobs.submit_spec(engine, api.JobSpec(return_value, args=("other",)))
    await assert_terminal(engine, api, other, api.JobStatus.SUCCEEDED, "other")
    await bounded(job.starts[1].wait())
    assert not waiter.done()
    job.gates[1].set()
    assert await bounded(asyncio.shield(waiter)) is None
    await assert_terminal(engine, api, job_id, api.JobStatus.SUCCEEDED, "ok")
    assert v2_jobs.record.peak == 1


async def test_cancel_success_backoff_stops_future_attempts(engine, api, v2_jobs):
    """B05: バックオフ中の取消しを確定し、待機期限後も再試行しない。"""
    job = v2_jobs.attempt([ValueError("retry"), "must-not-run"], blocked=(0,))
    job_id = await v2_jobs.submit_spec(engine, api.JobSpec(job, retry=api.RetryPolicy(
        max_retries=1, initial_delay=RETRY_DELAY * 3
    )))
    await bounded(job.starts[0].wait())
    job.gates[0].set()
    await status_is(engine, job_id, api.JobStatus.PENDING)
    assert await bounded(engine.cancel(job_id)) is True
    assert await bounded(engine.cancel(job_id)) is False
    await assert_terminal(engine, api, job_id, api.JobStatus.CANCELLED)
    await elapsed(RETRY_DELAY * 4)
    assert job.invocations == 1
