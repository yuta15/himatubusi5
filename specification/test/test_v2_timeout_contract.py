"""v2の試行タイムアウトと、キャンセル・実行枠との連携。"""

import asyncio

import pytest

from cancel_support import assert_terminal, assert_unfinished
from support import bounded, checkpoint
from v2_support import JOB_TIMEOUT, assert_pending, elapsed, failure_cause, status_is


@pytest.mark.parametrize("timeout", [None, JOB_TIMEOUT * 4])
@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
async def test_submit_job_success_no_timeout_before_completion(engine, api, v2_jobs, timeout, outcome):
    """T01: 制限なし・期限前の成功と失敗を保持し、古いタイマーで結果を変えない。"""
    job = v2_jobs.run_job(outcome=outcome)
    job_id = await v2_jobs.submit_spec(engine, api.JobSpec(job, timeout=timeout))
    await bounded(job.started.wait())
    if timeout is None:
        await elapsed(JOB_TIMEOUT * 2)
        await assert_unfinished(engine, api, job_id)
    job.release.set()
    expected = getattr(api.JobStatus, outcome.upper())
    await assert_terminal(engine, api, job_id, expected, job.result, job.cause)
    if timeout is not None:
        await elapsed(timeout * 1.2)
        await assert_terminal(engine, api, job_id, expected, job.result, job.cause)
    assert job.cancel_notifications == 0


@pytest.mark.parametrize("retries,retry_on,expected_calls", [
    (0, (TimeoutError,), 1), (1, (TimeoutError,), 2),
    (2, (ValueError,), 1), (1, (Exception,), 2),
])
async def test_submit_job_failure_timeout_retry_filter(
    engine, api, v2_jobs, retries, retry_on, expected_calls
):
    """T02/T05: 期限超過をTimeoutErrorへ変換し、条件と残回数に従って再試行する。"""
    job = v2_jobs.run_job()
    job_id = await v2_jobs.submit_spec(engine, api.JobSpec(
        job, timeout=JOB_TIMEOUT,
        retry=api.RetryPolicy(max_retries=retries, retry_on=retry_on),
    ))
    await failure_cause(engine, api, job_id, TimeoutError)
    assert job.calls == expected_calls
    assert job.cancel_notifications == expected_calls
    assert v2_jobs.record.peak == 1


async def test_submit_job_success_timeout_cleanup_holds_capacity(engine_factory, api, v2_jobs):
    """T03: finally途中をEventで停止し、実終了までRUNNING・未確定結果・枠保持を確認する。"""
    engine = engine_factory(max_concurrency=1)
    job = v2_jobs.run_job(cleanup=True)
    job_id = await v2_jobs.submit_spec(engine, api.JobSpec(job, timeout=JOB_TIMEOUT))
    await bounded(job.cleanup_started.wait())
    waiter = await v2_jobs.waiter(engine, job_id)
    other = v2_jobs.attempt(["other"], blocked=(0,))
    other_id = await v2_jobs.submit_spec(engine, api.JobSpec(other))
    await assert_unfinished(engine, api, job_id, (waiter,))
    await assert_pending(engine, api, other_id)
    assert other.invocations == 0
    assert not job.cleanup_finished.is_set()
    assert v2_jobs.record.active == 1
    job.cleanup_release.set()
    await failure_cause(engine, api, job_id, TimeoutError)
    assert await bounded(asyncio.shield(waiter)) is None
    assert job.cleanup_finished.is_set()
    await bounded(other.starts[0].wait())
    assert v2_jobs.record.peak == 1
    other.gates[0].set()
    await assert_terminal(engine, api, other_id, api.JobStatus.SUCCEEDED, "other")


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
async def test_submit_job_success_timeout_suppression_outcome(engine, api, v2_jobs, outcome):
    """T04: タイムアウトの中断を捕捉した場合も実際の戻り値・元の例外を採用する。"""
    job = v2_jobs.run_job(cleanup=True, suppress=True, outcome=outcome)
    job_id = await v2_jobs.submit_spec(engine, api.JobSpec(
        job, timeout=JOB_TIMEOUT,
        retry=api.RetryPolicy(max_retries=1, retry_on=(TimeoutError,)),
    ))
    await bounded(job.cleanup_started.wait())
    await assert_unfinished(engine, api, job_id)
    job.cleanup_release.set()
    await assert_terminal(engine, api, job_id, getattr(api.JobStatus, outcome.upper()), job.result, job.cause)
    assert job.calls == 1


async def test_submit_job_success_timeout_replacement_exception_retried(engine, api, v2_jobs):
    """T04: タイムアウト中断後の別例外は、その例外のリトライ条件に従う。"""
    first = v2_jobs.run_job(cleanup=True, suppress=True, outcome="failed")
    second = v2_jobs.attempt(["recovered"])
    calls = 0

    async def body():
        nonlocal calls
        calls += 1
        return await (first() if calls == 1 else second())

    job_id = await v2_jobs.submit_spec(engine, api.JobSpec(
        body, timeout=JOB_TIMEOUT,
        retry=api.RetryPolicy(max_retries=1, retry_on=(ValueError,)),
    ))
    await bounded(first.cleanup_started.wait())
    first.cleanup_release.set()
    await assert_terminal(engine, api, job_id, api.JobStatus.SUCCEEDED, "recovered")
    assert calls == 2


async def test_submit_job_success_timeout_resets_per_attempt(engine, api, v2_jobs):
    """T06: 初回期限超過後も次の試行には新しい期限を設定する。"""
    job = v2_jobs.attempt(["unused", "second"], blocked=(0, 1))
    timeout = JOB_TIMEOUT * 4
    job_id = await v2_jobs.submit_spec(engine, api.JobSpec(
        job, timeout=timeout, retry=api.RetryPolicy(max_retries=1, retry_on=(TimeoutError,))
    ))
    await bounded(job.starts[1].wait())
    await elapsed(timeout / 2)
    await assert_unfinished(engine, api, job_id)
    job.gates[1].set()
    await assert_terminal(engine, api, job_id, api.JobStatus.SUCCEEDED, "second")
    assert job.calls == 2


@pytest.mark.parametrize("waiting", ["capacity", "dependency", "backoff"])
async def test_submit_job_success_timeout_excludes_waiting(engine_factory, api, v2_jobs, waiting):
    """T07: 実行枠・依存関係・バックオフ待ちを各試行のtimeoutに含めない。"""
    engine = engine_factory(max_concurrency=1 if waiting == "capacity" else 2)
    if waiting == "backoff":
        job = v2_jobs.attempt([ValueError("retry"), "ok"], blocked=(0,))
        job_id = await v2_jobs.submit_spec(engine, api.JobSpec(
            job, timeout=JOB_TIMEOUT, retry=api.RetryPolicy(
                max_retries=1, initial_delay=JOB_TIMEOUT * 4, retry_on=(ValueError,)
            )
        ))
        await bounded(job.starts[0].wait())
        job.gates[0].set()
        await status_is(engine, job_id, api.JobStatus.PENDING)
        await elapsed(JOB_TIMEOUT * 2)
        await assert_pending(engine, api, job_id)
    else:
        parent = v2_jobs.attempt([None], blocked=(0,))
        parent_id = await v2_jobs.submit_spec(engine, api.JobSpec(parent))
        await bounded(parent.starts[0].wait())
        job = v2_jobs.attempt(["ok"])
        job_id = await v2_jobs.submit_spec(engine, api.JobSpec(
            job, timeout=JOB_TIMEOUT,
            depends_on=(parent_id,) if waiting == "dependency" else (),
        ))
        await elapsed(JOB_TIMEOUT * 2)
        await assert_pending(engine, api, job_id)
        assert job.invocations == 0
        parent.gates[0].set()
    await assert_terminal(engine, api, job_id, api.JobStatus.SUCCEEDED, "ok")


async def test_submit_job_success_old_timeout_does_not_interrupt_retry(engine, api, v2_jobs):
    """T08: 初回の期限前失敗後に残ったタイマーが次の試行を中断しない。"""
    job = v2_jobs.attempt([ValueError("retry"), "ok"], blocked=(0, 1))
    timeout = JOB_TIMEOUT * 4
    job_id = await v2_jobs.submit_spec(engine, api.JobSpec(
        job, timeout=timeout, retry=api.RetryPolicy(max_retries=1, retry_on=(ValueError,))
    ))
    await bounded(job.starts[0].wait())
    await elapsed(timeout / 2)
    job.gates[0].set()
    await bounded(job.starts[1].wait())
    # 初回の期限を越え、次の試行の期限より前の区間を観測する。
    await elapsed(timeout * 0.65)
    await assert_unfinished(engine, api, job_id)
    job.gates[1].set()
    await assert_terminal(engine, api, job_id, api.JobStatus.SUCCEEDED, "ok")


@pytest.mark.parametrize("outcome", ["cancelled", "succeeded", "failed"])
async def test_cancel_success_explicit_request_never_retries(engine, api, v2_jobs, outcome):
    """X01: 明示キャンセル後は実際の終了を採用し、別例外でも再試行しない。"""
    job = v2_jobs.run_job(cleanup=True, suppress=outcome != "cancelled", outcome=outcome)
    job_id = await v2_jobs.submit_spec(engine, api.JobSpec(
        job, timeout=JOB_TIMEOUT * 4, retry=api.RetryPolicy(max_retries=2)
    ))
    await bounded(job.started.wait())
    assert await bounded(engine.cancel(job_id)) is True
    await bounded(job.cleanup_started.wait())
    # 後からtimeoutが発生する区間でも終了処理を再中断させない。
    await elapsed(JOB_TIMEOUT * 5)
    assert job.cancel_notifications == 1
    assert await bounded(engine.cancel(job_id)) is False
    job.cleanup_release.set()
    await assert_terminal(engine, api, job_id, getattr(api.JobStatus, outcome.upper()), job.result, job.cause)
    assert job.calls == 1


@pytest.mark.parametrize("retries", [0, 1])
async def test_cancel_success_timeout_request_wins(engine, api, v2_jobs, retries):
    """X01: タイムアウト通知後の明示cancelを拒否し、終了処理への再通知を防ぐ。"""
    job = v2_jobs.run_job(cleanup=True)
    job_id = await v2_jobs.submit_spec(engine, api.JobSpec(
        job, timeout=JOB_TIMEOUT,
        retry=api.RetryPolicy(max_retries=retries, retry_on=(TimeoutError,)),
    ))
    await bounded(job.cleanup_started.wait())
    for _ in range(3):
        assert await bounded(engine.cancel(job_id)) is False
    await checkpoint()
    assert job.cancel_notifications == 1
    job.cleanup_release.set()
    await failure_cause(engine, api, job_id, TimeoutError)
    assert job.calls == retries + 1


async def test_submit_job_success_suppressed_timeout_keeps_capacity(engine_factory, api, v2_jobs):
    """X09: 中断を抑制してexcept後も本体が継続する間、実行枠を保持する。"""
    engine = engine_factory(max_concurrency=1)
    job = v2_jobs.continuing_job()
    job_id = await v2_jobs.submit_spec(engine, api.JobSpec(job, timeout=JOB_TIMEOUT))
    await bounded(job.continued.wait())
    other = v2_jobs.attempt(["other"])
    other_id = await v2_jobs.submit_spec(engine, api.JobSpec(other))
    await elapsed(JOB_TIMEOUT * 2)
    await assert_unfinished(engine, api, job_id)
    await assert_pending(engine, api, other_id)
    assert other.invocations == 0
    assert v2_jobs.record.active == 1
    assert job.cancel_notifications == 1
    job.continue_release.set()
    await assert_terminal(engine, api, job_id, api.JobStatus.SUCCEEDED, job.result)
    await assert_terminal(engine, api, other_id, api.JobStatus.SUCCEEDED, "other")
    assert v2_jobs.record.peak == 1
