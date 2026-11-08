"""v2の独立した組み合わせシナリオと、最後の全機能統合シナリオ。"""

import asyncio

import pytest

from cancel_support import assert_terminal, assert_unfinished
from run_count_support import ExecutionRecord
from support import bounded, return_value
from v2_support import (
    JOB_TIMEOUT, RETRY_DELAY, assert_pending, dependency_failed,
    elapsed, failure_cause, status_is,
)


async def test_submit_job_success_timeout_retry_capacity(engine_factory, api, v2_jobs):
    """X03a: timeout試行の実終了で枠を解放し、バックオフ中に他ジョブが進む。"""
    engine = engine_factory(max_concurrency=1)
    job = v2_jobs.attempt(["timed-out", "recovered"], blocked=(0, 1))
    job_id = await v2_jobs.submit_spec(engine, api.JobSpec(
        job, timeout=JOB_TIMEOUT,
        retry=api.RetryPolicy(max_retries=1, initial_delay=RETRY_DELAY * 4, retry_on=(TimeoutError,)),
    ))
    await bounded(job.starts[0].wait())
    other = v2_jobs.attempt(["other"], blocked=(0,))
    other_id = await v2_jobs.submit_spec(engine, api.JobSpec(other))
    await bounded(other.starts[0].wait())
    await assert_pending(engine, api, job_id)
    # timeoutの計測対象でない枠待ち・バックオフ中に待機者も保持される。
    waiter = await v2_jobs.waiter(engine, job_id)
    await elapsed(RETRY_DELAY * 5)
    assert job.calls == 1
    assert not waiter.done()
    other.gates[0].set()
    await bounded(engine.wait(other_id))
    await bounded(job.starts[1].wait())
    job.gates[1].set()
    await assert_terminal(engine, api, job_id, api.JobStatus.SUCCEEDED, "recovered")
    assert v2_jobs.record.peak == job.peak == 1


async def test_cancel_success_dag_retry_propagation(engine_factory, api, v2_jobs):
    """X03c: 先行ジョブのバックオフ取消しをDAGへ伝播し、独立ジョブを妨げない。"""
    engine = engine_factory(max_concurrency=1)
    parent_job = v2_jobs.attempt([ValueError("retry"), "must-not-run"], blocked=(0,))
    parent = await v2_jobs.submit_spec(engine, api.JobSpec(parent_job, retry=api.RetryPolicy(
        max_retries=1, initial_delay=RETRY_DELAY * 4
    )))
    await bounded(parent_job.starts[0].wait())
    child_job = v2_jobs.attempt(["must-not-run"])
    child = await v2_jobs.submit_spec(engine, api.JobSpec(child_job, depends_on=(parent,)))
    parent_job.gates[0].set()
    await status_is(engine, parent, api.JobStatus.PENDING)
    await assert_pending(engine, api, child)
    assert await bounded(engine.cancel(parent)) is True
    await assert_terminal(engine, api, parent, api.JobStatus.CANCELLED)
    await dependency_failed(engine, api, child, {parent})
    healthy = await v2_jobs.submit_spec(engine, api.JobSpec(return_value, args=("healthy",)))
    await assert_terminal(engine, api, healthy, api.JobStatus.SUCCEEDED, "healthy")
    await elapsed(RETRY_DELAY * 5)
    assert parent_job.invocations == 1
    assert child_job.invocations == 0


@pytest.mark.parametrize("waiting", ["capacity", "dependency"])
@pytest.mark.parametrize("request_first", [False, True], ids=["release-task-first", "cancel-task-first"])
async def test_cancel_success_start_race(engine_factory, api, v2_jobs, waiting, request_first):
    """X02: 実行枠解放・依存成立と取消しの競合で、勝者を固定せず状態・本体記録を照合する。"""
    engine = engine_factory(max_concurrency=1 if waiting == "capacity" else 2)
    parent_job = v2_jobs.attempt([None], blocked=(0,))
    parent = await v2_jobs.submit_spec(engine, api.JobSpec(parent_job))
    await bounded(parent_job.starts[0].wait())
    job = v2_jobs.attempt(["result"])
    job_id = await v2_jobs.submit_spec(engine, api.JobSpec(
        job, depends_on=(parent,) if waiting == "dependency" else (),
    ))
    await assert_pending(engine, api, job_id)
    gate = asyncio.Event()

    async def request():
        await gate.wait()
        return await engine.cancel(job_id)

    async def release():
        await gate.wait()
        parent_job.gates[0].set()

    funcs = (request, release) if request_first else (release, request)
    tasks = [v2_jobs.task(func()) for func in funcs]
    gate.set()
    results = await bounded(asyncio.gather(*(asyncio.shield(task) for task in tasks)))
    accepted = results[0 if request_first else 1]
    assert type(accepted) is bool
    await bounded(engine.wait(job_id))
    status = await bounded(engine.get_status(job_id))
    assert status in (api.JobStatus.SUCCEEDED, api.JobStatus.CANCELLED)
    if job.invocations == 0:
        assert accepted is True
        assert status is api.JobStatus.CANCELLED
    if status is api.JobStatus.CANCELLED:
        assert accepted is True
    assert job.calls <= 1
    await assert_terminal(engine, api, job_id, status, "result")
    assert await bounded(engine.cancel(job_id)) is False


async def test_cancel_success_backoff_expiry_race(engine, api, v2_jobs):
    """X02: バックオフ期限付近の取消しと再試行の勝者を固定せず、結果の整合を確認する。"""
    job = v2_jobs.attempt([ValueError("retry"), "result"], blocked=(0,))
    job_id = await v2_jobs.submit_spec(engine, api.JobSpec(job, retry=api.RetryPolicy(
        max_retries=1, initial_delay=RETRY_DELAY * 3
    )))
    await bounded(job.starts[0].wait())
    job.gates[0].set()
    await status_is(engine, job_id, api.JobStatus.PENDING)
    gate = asyncio.Event()
    loop = asyncio.get_running_loop()
    # 本体の出口時刻を基準にテスト側の要求を予定する。内部タイマーは参照しない。
    timer = loop.call_at(job.finished_at[0] + RETRY_DELAY * 3, gate.set)
    try:
        await bounded(gate.wait())
        accepted = await bounded(engine.cancel(job_id))
    finally:
        timer.cancel()
    assert type(accepted) is bool
    await bounded(engine.wait(job_id))
    status = await bounded(engine.get_status(job_id))
    assert status in (api.JobStatus.SUCCEEDED, api.JobStatus.CANCELLED)
    assert 1 <= job.invocations <= 2
    assert job.peak == 1
    if job.invocations == 1 or status is api.JobStatus.CANCELLED:
        assert accepted is True
        assert status is api.JobStatus.CANCELLED
    await assert_terminal(engine, api, job_id, status, "result")
    assert await bounded(engine.cancel(job_id)) is False


@pytest.mark.parametrize("waiting", ["dependency", "backoff", "timeout-cleanup"])
async def test_wait_success_v2_waiter_isolation(engine, api, v2_jobs, waiting):
    """X04: v2の待機中に一人を取り消してもジョブと他の待機者は終端まで継続する。"""
    if waiting == "timeout-cleanup":
        job = v2_jobs.run_job(cleanup=True)
        job_id = await v2_jobs.submit_spec(engine, api.JobSpec(job, timeout=JOB_TIMEOUT))
        await bounded(job.cleanup_started.wait())
    else:
        job = v2_jobs.attempt(["ok"], blocked=(0,))
        if waiting == "dependency":
            parent_job = v2_jobs.attempt([None], blocked=(0,))
            parent = await v2_jobs.submit_spec(engine, api.JobSpec(parent_job))
            job_id = await v2_jobs.submit_spec(engine, api.JobSpec(job, depends_on=(parent,)))
        else:
            job = v2_jobs.attempt([ValueError("retry"), "ok"], blocked=(0, 1))
            job_id = await v2_jobs.submit_spec(engine, api.JobSpec(job, retry=api.RetryPolicy(
                max_retries=1, initial_delay=RETRY_DELAY * 4
            )))
            await bounded(job.starts[0].wait())
            job.gates[0].set()
            await status_is(engine, job_id, api.JobStatus.PENDING)
    cancelled = await v2_jobs.waiter(engine, job_id)
    survivors = [await v2_jobs.waiter(engine, job_id) for _ in range(2)]
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await bounded(asyncio.shield(cancelled))
    if waiting == "timeout-cleanup":
        await assert_unfinished(engine, api, job_id, survivors)
        assert job.cancel_notifications == 1
        job.cleanup_release.set()
        await failure_cause(engine, api, job_id, TimeoutError)
    else:
        await assert_pending(engine, api, job_id, survivors)
        if waiting == "dependency":
            parent_job.gates[0].set()
        for gate in job.gates:
            gate.set()
        await assert_terminal(engine, api, job_id, api.JobStatus.SUCCEEDED, "ok")
    assert await bounded(asyncio.gather(*(asyncio.shield(t) for t in survivors))) == [None, None]


@pytest.mark.parametrize("kind", ["retry-success", "retry-exhausted", "dependency", "timeout", "cancelled"])
async def test_get_result_success_v2_terminal_retained(engine, api, v2_jobs, kind):
    """X05: v2の各終端状態は別ジョブの実行後も変わらず、最終結果・原因を保持する。"""
    job = v2_jobs.attempt([ValueError("first"), "ok" if kind == "retry-success" else ValueError("last")])
    if kind == "dependency":
        parent = await v2_jobs.submit_spec(engine, api.JobSpec(v2_jobs.attempt([ValueError("parent")])))
        await bounded(engine.wait(parent))
        job_id = await v2_jobs.submit_spec(engine, api.JobSpec(job, depends_on=(parent,)))
    elif kind == "timeout":
        job = v2_jobs.attempt(["unused"], blocked=(0,))
        job_id = await v2_jobs.submit_spec(engine, api.JobSpec(job, timeout=JOB_TIMEOUT))
    elif kind == "cancelled":
        job = v2_jobs.attempt(["unused"], blocked=(0,))
        job_id = await v2_jobs.submit_spec(engine, api.JobSpec(job))
        await bounded(job.starts[0].wait())
        assert await bounded(engine.cancel(job_id)) is True
    else:
        job_id = await v2_jobs.submit_spec(engine, api.JobSpec(job, retry=api.RetryPolicy(max_retries=1)))
    await bounded(engine.wait(job_id))
    status = await bounded(engine.get_status(job_id))
    expected = (api.JobStatus.SUCCEEDED if kind == "retry-success" else
                api.JobStatus.CANCELLED if kind == "cancelled" else api.JobStatus.FAILED)
    assert status is expected
    cause = None
    if status is api.JobStatus.FAILED:
        expected_cause = {"retry-exhausted": ValueError, "dependency": api.JobDependencyError,
                          "timeout": TimeoutError}[kind]
        cause = await failure_cause(engine, api, job_id, expected_cause)
    calls = job.calls
    for _ in range(2):
        other = await v2_jobs.submit(engine, return_value, "other")
        await bounded(engine.wait(other))
        await assert_terminal(engine, api, job_id, status, "ok", cause)
        assert await bounded(engine.cancel(job_id)) is False
    assert job.calls == calls


async def test_submit_job_success_legacy_submit_defaults(engine_factory, api, v2_jobs):
    """X06: submitとsubmit_jobを混在でき、旧submitの優先度0・リトライなしを維持する。"""
    engine = engine_factory(max_concurrency=1)
    holder = v2_jobs.attempt([None], blocked=(0,))
    await v2_jobs.submit(engine, holder)
    await bounded(holder.starts[0].wait())
    old = v2_jobs.attempt([ValueError("legacy"), "must-not-retry"], blocked=(0,))
    old_id = await v2_jobs.submit(engine, old)
    high = v2_jobs.attempt(["high"], blocked=(0,))
    high_id = await v2_jobs.submit_spec(engine, api.JobSpec(high, priority=1))
    low = v2_jobs.attempt(["low"], blocked=(0,))
    low_id = await v2_jobs.submit_spec(engine, api.JobSpec(low, priority=-1))
    holder.gates[0].set()
    await bounded(high.starts[0].wait())
    assert old.invocations == low.invocations == 0
    high.gates[0].set()
    await bounded(old.starts[0].wait())
    assert low.invocations == 0
    old.gates[0].set()
    await failure_cause(engine, api, old_id, ValueError)
    await bounded(low.starts[0].wait())
    low.gates[0].set()
    await assert_terminal(engine, api, high_id, api.JobStatus.SUCCEEDED, "high")
    await assert_terminal(engine, api, low_id, api.JobStatus.SUCCEEDED, "low")
    assert old.invocations == 1


async def test_create_engine_success_v2_independent_instances(engine_factory, api, v2_jobs):
    """X06: timeout終了処理中のエンジンが、別エンジンの枠・実行・取消しに影響しない。"""
    first, second = engine_factory(max_concurrency=1), engine_factory(max_concurrency=1)
    job = v2_jobs.run_job(cleanup=True)
    first_id = await v2_jobs.submit_spec(first, api.JobSpec(job, timeout=JOB_TIMEOUT))
    await bounded(job.cleanup_started.wait())
    other = v2_jobs.run_job(record=ExecutionRecord())
    other_id = await v2_jobs.submit_spec(second, api.JobSpec(other, retry=api.RetryPolicy(max_retries=1)))
    await bounded(other.started.wait())
    for method in (second.get_status, second.get_result, second.wait, second.cancel):
        with pytest.raises(api.JobNotFoundError):
            await bounded(method(first_id))
    assert other.cancel_notifications == 0
    other.release.set()
    await assert_terminal(second, api, other_id, api.JobStatus.SUCCEEDED, other.result)
    await assert_unfinished(first, api, first_id)
    job.cleanup_release.set()
    await failure_cause(first, api, first_id, TimeoutError)


async def test_submit_job_success_full_feature_scenario(engine_factory, api, v2_jobs):
    """X03統合: timeout・retry・backoff・DAG・priority・cancelを同一エンジンで連携する。"""
    engine = engine_factory(max_concurrency=1)
    root_job = v2_jobs.attempt(["timed-out", "root"], blocked=(0, 1))
    root = await v2_jobs.submit_spec(engine, api.JobSpec(
        root_job, priority=5, timeout=JOB_TIMEOUT * 3,
        retry=api.RetryPolicy(max_retries=1, initial_delay=RETRY_DELAY * 4, retry_on=(TimeoutError,)),
    ))
    await bounded(root_job.starts[0].wait())
    child_job = v2_jobs.attempt(["child"], blocked=(0,))
    child = await v2_jobs.submit_spec(engine, api.JobSpec(child_job, priority=100, depends_on=(root,)))
    cancelled_job = v2_jobs.attempt(["must-not-run"])
    cancelled = await v2_jobs.submit_spec(engine, api.JobSpec(cancelled_job, depends_on=(root,)))
    assert await bounded(engine.cancel(cancelled)) is True
    await assert_terminal(engine, api, cancelled, api.JobStatus.CANCELLED)
    await status_is(engine, root, api.JobStatus.PENDING)
    low_job = v2_jobs.attempt(["low"], blocked=(0,))
    low = await v2_jobs.submit_spec(engine, api.JobSpec(low_job, priority=-10))
    await bounded(low_job.starts[0].wait())
    peer_job = v2_jobs.attempt(["peer"], blocked=(0,))
    peer = await v2_jobs.submit_spec(engine, api.JobSpec(peer_job))
    await elapsed(RETRY_DELAY * 5)
    await assert_pending(engine, api, child)
    assert child_job.invocations == 0
    low_job.gates[0].set()
    await bounded(engine.wait(low))
    await bounded(root_job.starts[1].wait())
    root_job.gates[1].set()
    await bounded(engine.wait(root))
    await bounded(child_job.starts[0].wait())
    assert peer_job.invocations == 0
    child_job.gates[0].set()
    await bounded(peer_job.starts[0].wait())
    peer_job.gates[0].set()
    await assert_terminal(engine, api, root, api.JobStatus.SUCCEEDED, "root")
    await assert_terminal(engine, api, child, api.JobStatus.SUCCEEDED, "child")
    await assert_terminal(engine, api, peer, api.JobStatus.SUCCEEDED, "peer")
    assert root_job.invocations == 2
    assert cancelled_job.invocations == 0
    assert v2_jobs.record.peak == 1
