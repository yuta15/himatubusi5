"""追加仕様v1.1、C01〜C17の共通契約テスト。C18は既存44ケース。"""

import asyncio
import importlib.util
import inspect
from pathlib import Path

import pytest

from cancel_support import CancellationScope, assert_terminal, assert_unfinished
from support import bounded, checkpoint, return_value


@pytest.fixture
async def cancel_jobs():
    scope = CancellationScope()
    try:
        yield scope
    finally:
        await scope.finish()


def test_cancel_success_public_contract(engine, api):
    """C01: 追加定義と既存メソッドの公開シグネチャを維持する。"""
    path = Path(__file__).resolve().parents[1] / "interface.py"
    spec = importlib.util.spec_from_file_location("reference_cancel_interface", path)
    reference = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(reference)

    assert api.JobStatus.CANCELLED.value == "CANCELLED"
    assert issubclass(api.JobCancelledError, api.JobError)
    for name in ("submit", "get_status", "wait", "get_result", "cancel"):
        declared = getattr(api.JobEngine, name)
        expected = getattr(reference.JobEngine, name)
        assert inspect.iscoroutinefunction(declared)
        expected_signature = inspect.signature(expected)
        if expected_signature.return_annotation is reference.JobStatus:
            expected_signature = expected_signature.replace(return_annotation=api.JobStatus)
        assert inspect.signature(declared) == expected_signature
        method = getattr(engine, name)
        assert inspect.iscoroutinefunction(method)
        # 実装の型注釈の有無は制限せず、公開引数を受け付けることを確認する。
        signature = inspect.signature(method)
        if name == "submit":
            signature.bind(return_value, 1, value=2)
        else:
            signature.bind(job_id="contract-probe")


async def test_cancel_success_pending_without_body(engine, api, cancel_jobs):
    """C02: 開始前キャンセルを検証し、観測できない場合は明示的にskipする。"""
    job = cancel_jobs.cancellation_job()
    # wait_for/create_taskを挟まず、全体のpytest-timeoutでハングを検出する。
    job_id = await cancel_jobs.submit_direct(engine, job)
    status = await engine.get_status(job_id)
    assert isinstance(status, api.JobStatus)
    assert status in (api.JobStatus.PENDING, api.JobStatus.RUNNING)
    if status is not api.JobStatus.PENDING or job.started.is_set():
        pytest.skip("PENDINGを観測できず、開始前キャンセルの検証は未成立")
    accepted = await engine.cancel(job_id)
    assert accepted is True
    if job.started.is_set():
        pytest.skip("要求受付までに本体が開始し、開始前キャンセルの検証は未成立")
    assert await bounded(engine.wait(job_id)) is None
    assert not job.started.is_set()
    await assert_terminal(engine, api, job_id, api.JobStatus.CANCELLED)


async def test_cancel_success_running_lifecycle(engine, api, cancel_jobs):
    """C03: 受付・終了処理中・完了後の状態と結果、待機を区別する。"""
    job = cancel_jobs.cancellation_job()
    job_id = await cancel_jobs.submit(engine, job)
    await bounded(job.started.wait())
    before = await cancel_jobs.waiter(engine, job_id)
    assert not before.done()
    assert await bounded(engine.cancel(job_id)) is True
    await bounded(job.cancel_received.wait())
    assert not job.cleanup_finished.is_set()
    after = await cancel_jobs.waiter(engine, job_id)
    await assert_unfinished(engine, api, job_id, (before, after))
    job.cleanup_release.set()
    assert await bounded(asyncio.gather(
        asyncio.shield(before), asyncio.shield(after)
    )) == [None, None]
    assert job.cleanup_finished.is_set()
    assert not job.normal_path_finished
    await assert_terminal(engine, api, job_id, api.JobStatus.CANCELLED)


async def test_cancel_success_duplicate_does_not_interrupt_cleanup(engine, api, cancel_jobs):
    """C04: 重複要求はFalseとなり、終了処理を再度中断しない。"""
    job = cancel_jobs.cancellation_job()
    job_id = await cancel_jobs.submit(engine, job)
    await bounded(job.started.wait())
    assert await bounded(engine.cancel(job_id)) is True
    await bounded(job.cancel_received.wait())
    for _ in range(3):
        assert await bounded(engine.cancel(job_id)) is False
        await checkpoint()
        assert job.cancel_notifications == 1
    assert not job.cleanup_finished.is_set()
    job.cleanup_release.set()
    await assert_terminal(engine, api, job_id, api.JobStatus.CANCELLED)
    assert job.cleanup_finished.is_set()
    assert job.cancel_notifications == 1


async def test_cancel_success_concurrent_requests(engine, api, cancel_jobs):
    """C05: 同時要求は1件だけ受け付け、キャンセル通知も1回に留める。"""
    job = cancel_jobs.cancellation_job()
    job_id = await cancel_jobs.submit(engine, job)
    await bounded(job.started.wait())
    gate = asyncio.Event()

    async def request():
        await gate.wait()
        return await engine.cancel(job_id)

    requests = [cancel_jobs.task(request()) for _ in range(5)]
    gate.set()
    results = await bounded(asyncio.gather(*(asyncio.shield(t) for t in requests)))
    assert all(type(value) is bool for value in results)
    assert results.count(True) == 1
    await bounded(job.cancel_received.wait())
    await checkpoint()
    assert job.cancel_notifications == 1
    job.cleanup_release.set()
    await assert_terminal(engine, api, job_id, api.JobStatus.CANCELLED)


@pytest.mark.parametrize("outcome", ["succeeded", "failed", "cancelled"])
async def test_cancel_success_terminal_request_rejected(engine, api, cancel_jobs, outcome):
    """C06: 終端ジョブへの要求を拒否し、状態と結果を維持する。"""
    job = cancel_jobs.cancellation_job(outcome, spontaneous=True)
    job.release.set()
    job.cleanup_release.set()
    job_id = await cancel_jobs.submit(engine, job)
    await bounded(engine.wait(job_id))
    expected = getattr(api.JobStatus, outcome.upper())
    await assert_terminal(engine, api, job_id, expected, job.result, job.cause)
    assert await bounded(engine.cancel(job_id)) is False
    await assert_terminal(engine, api, job_id, expected, job.result, job.cause)


async def test_cancel_failure_unknown_id_preserves_engine(engine, api, cancel_jobs):
    """C07: 未登録IDを拒否しても既存・後続ジョブは正常に実行できる。"""
    job = cancel_jobs.controlled_job(result="existing")
    job_id = await cancel_jobs.submit(engine, job)
    await bounded(job.started.wait())
    with pytest.raises(api.JobNotFoundError):
        await bounded(engine.cancel("never-submitted"))
    await assert_unfinished(engine, api, job_id)
    job.release.set()
    await assert_terminal(engine, api, job_id, api.JobStatus.SUCCEEDED, "existing")
    later_id = await cancel_jobs.submit(engine, return_value, "later")
    await bounded(engine.wait(later_id))
    await assert_terminal(engine, api, later_id, api.JobStatus.SUCCEEDED, "later")


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
async def test_cancel_success_suppressed_cancellation(engine, api, cancel_jobs, outcome):
    """C08: キャンセルを抑制したジョブは実際の成功・失敗を優先する。"""
    job = cancel_jobs.cancellation_job(outcome)
    job_id = await cancel_jobs.submit(engine, job)
    await bounded(job.started.wait())
    assert await bounded(engine.cancel(job_id)) is True
    await bounded(job.cancel_received.wait())
    waiter = await cancel_jobs.waiter(engine, job_id)
    await assert_unfinished(engine, api, job_id, (waiter,))
    job.cleanup_release.set()
    assert await bounded(asyncio.shield(waiter)) is None
    await assert_terminal(
        engine, api, job_id, getattr(api.JobStatus, outcome.upper()), job.result, job.cause
    )


@pytest.mark.parametrize("outcome", ["cancelled", "succeeded", "failed"])
async def test_cancel_success_spontaneous_cancelled_error(engine, api, cancel_jobs, outcome):
    """C09: 要求なしのCancelledErrorも、捕捉後を含む実際の終了結果で分類する。"""
    job = cancel_jobs.cancellation_job(outcome, spontaneous=True)
    job.release.set()
    job.cleanup_release.set()
    job_id = await cancel_jobs.submit(engine, job)
    await bounded(engine.wait(job_id))
    await assert_terminal(
        engine, api, job_id, getattr(api.JobStatus, outcome.upper()), job.result, job.cause
    )


@pytest.mark.parametrize("fails", [False, True], ids=["success", "failure"])
@pytest.mark.parametrize("order", ["cancel-first", "finish-first", "concurrent"])
async def test_cancel_success_completion_race(engine, api, cancel_jobs, fails, order):
    """C10: 競合の勝者を固定せず、終端状態と結果の整合性・不変性を確認する。"""
    cause = ValueError("racing failure")
    job = cancel_jobs.controlled_job(result="racing result", error=cause if fails else None)
    job_id = await cancel_jobs.submit(engine, job)
    await bounded(job.started.wait())

    if order == "cancel-first":
        accepted = await bounded(engine.cancel(job_id))
        assert accepted is True
        job.release.set()
    elif order == "finish-first":
        job.release.set()
        await bounded(engine.wait(job_id))
        accepted = await bounded(engine.cancel(job_id))
        assert accepted is False
    else:
        gate = asyncio.Event()

        async def request():
            await gate.wait()
            return await engine.cancel(job_id)

        async def finish():
            await gate.wait()
            job.release.set()

        requester = cancel_jobs.task(request())
        releaser = cancel_jobs.task(finish())
        gate.set()
        accepted, _ = await bounded(asyncio.gather(
            asyncio.shield(requester), asyncio.shield(releaser)
        ))
    assert type(accepted) is bool
    await bounded(engine.wait(job_id))
    terminal = await bounded(engine.get_status(job_id))
    natural = api.JobStatus.FAILED if fails else api.JobStatus.SUCCEEDED
    assert terminal in (natural, api.JobStatus.CANCELLED)
    if terminal is api.JobStatus.CANCELLED:
        assert accepted is True
    for _ in range(3):
        await assert_terminal(engine, api, job_id, terminal, job.result, cause)
        assert await bounded(engine.cancel(job_id)) is False
        await checkpoint()
    await assert_terminal(engine, api, job_id, terminal, job.result, cause)


async def test_cancel_success_other_jobs_progress(engine, api, cancel_jobs):
    """C11: 終了処理が停止中でも他の実行中ジョブと後続ジョブが完了できる。"""
    cancelled = cancel_jobs.cancellation_job()
    healthy = cancel_jobs.controlled_job(result="healthy")
    cancelled_id = await cancel_jobs.submit(engine, cancelled)
    healthy_id = await cancel_jobs.submit(engine, healthy)
    await bounded(asyncio.gather(cancelled.started.wait(), healthy.started.wait()))
    healthy_waiter = await cancel_jobs.waiter(engine, healthy_id)
    assert await bounded(engine.cancel(cancelled_id)) is True
    await bounded(cancelled.cancel_received.wait())
    await assert_unfinished(engine, api, healthy_id, (healthy_waiter,))
    healthy.release.set()
    assert await bounded(asyncio.shield(healthy_waiter)) is None
    await assert_terminal(engine, api, healthy_id, api.JobStatus.SUCCEEDED, "healthy")
    later_id = await cancel_jobs.submit(engine, return_value, "later")
    await bounded(engine.wait(later_id))
    await assert_terminal(engine, api, later_id, api.JobStatus.SUCCEEDED, "later")
    await assert_unfinished(engine, api, cancelled_id)
    cancelled.cleanup_release.set()
    await bounded(engine.wait(cancelled_id))
    await assert_terminal(engine, api, cancelled_id, api.JobStatus.CANCELLED)


async def test_cancel_success_multiple_jobs_independent(engine, api, cancel_jobs):
    """C12: 複数ジョブへの要求と完了通知はジョブごとに独立する。"""
    first, second = cancel_jobs.cancellation_job(), cancel_jobs.cancellation_job()
    first_id = await cancel_jobs.submit(engine, first)
    second_id = await cancel_jobs.submit(engine, second)
    await bounded(asyncio.gather(first.started.wait(), second.started.wait()))
    first_waiter = await cancel_jobs.waiter(engine, first_id)
    second_waiter = await cancel_jobs.waiter(engine, second_id)
    assert await bounded(engine.cancel(first_id)) is True
    assert await bounded(engine.cancel(second_id)) is True
    await bounded(asyncio.gather(first.cancel_received.wait(), second.cancel_received.wait()))
    await assert_unfinished(engine, api, first_id, (first_waiter,))
    await assert_unfinished(engine, api, second_id, (second_waiter,))
    first.cleanup_release.set()
    assert await bounded(asyncio.shield(first_waiter)) is None
    await assert_terminal(engine, api, first_id, api.JobStatus.CANCELLED)
    await assert_unfinished(engine, api, second_id, (second_waiter,))
    second.cleanup_release.set()
    assert await bounded(asyncio.shield(second_waiter)) is None
    await assert_terminal(engine, api, second_id, api.JobStatus.CANCELLED)


async def test_wait_success_cancelled_multiple_waiters(engine, api, cancel_jobs):
    """C13: キャンセル前後の全待機者は終端状態への遷移後に正常復帰する。"""
    job = cancel_jobs.cancellation_job()
    job_id = await cancel_jobs.submit(engine, job)
    await bounded(job.started.wait())
    waiters = [await cancel_jobs.waiter(engine, job_id) for _ in range(3)]
    assert await bounded(engine.cancel(job_id)) is True
    await bounded(job.cancel_received.wait())
    waiters.extend([await cancel_jobs.waiter(engine, job_id) for _ in range(3)])
    await assert_unfinished(engine, api, job_id, waiters)
    job.cleanup_release.set()
    assert await bounded(asyncio.gather(
        *(asyncio.shield(waiter) for waiter in waiters)
    )) == [None] * len(waiters)
    await assert_terminal(engine, api, job_id, api.JobStatus.CANCELLED)


@pytest.mark.parametrize("during_cleanup", [False, True], ids=["running", "cleanup"])
async def test_wait_success_cancelled_waiter_isolation(engine, api, cancel_jobs, during_cleanup):
    """C14: 待機Taskの取消しはジョブと他の待機者を取り消さない。"""
    job = cancel_jobs.cancellation_job()
    job_id = await cancel_jobs.submit(engine, job)
    await bounded(job.started.wait())
    if during_cleanup:
        assert await bounded(engine.cancel(job_id)) is True
        await bounded(job.cancel_received.wait())
    cancelled_waiter = await cancel_jobs.waiter(engine, job_id)
    survivor = await cancel_jobs.waiter(engine, job_id)
    cancelled_waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await bounded(asyncio.shield(cancelled_waiter))
    await assert_unfinished(engine, api, job_id, (survivor,))
    assert job.cancel_notifications == (1 if during_cleanup else 0)
    if during_cleanup:
        job.cleanup_release.set()
        expected = api.JobStatus.CANCELLED
    else:
        job.release.set()
        expected = api.JobStatus.SUCCEEDED
    assert await bounded(asyncio.shield(survivor)) is None
    await assert_terminal(engine, api, job_id, expected, job.result)
    assert job.cancel_notifications == (1 if during_cleanup else 0)


@pytest.mark.parametrize("spontaneous", [False, True], ids=["requested", "spontaneous"])
async def test_cancel_success_terminal_data_retained(engine, api, cancel_jobs, spontaneous):
    """C15: キャンセル完了後も状態・結果取得・待機・再要求の契約を保持する。"""
    job = cancel_jobs.cancellation_job(spontaneous=spontaneous)
    job.cleanup_release.set()
    job_id = await cancel_jobs.submit(engine, job)
    await bounded(job.started.wait())
    if spontaneous:
        job.release.set()
    else:
        assert await bounded(engine.cancel(job_id)) is True
    await bounded(engine.wait(job_id))
    for _ in range(3):
        await assert_terminal(engine, api, job_id, api.JobStatus.CANCELLED)
        assert await bounded(engine.cancel(job_id)) is False
        other_id = await cancel_jobs.submit(engine, return_value, "other")
        await bounded(engine.wait(other_id))
        await assert_terminal(engine, api, other_id, api.JobStatus.SUCCEEDED, "other")
    await assert_terminal(engine, api, job_id, api.JobStatus.CANCELLED)


async def test_cancel_success_independent_engines(engine_factory, api, cancel_jobs):
    """C16: 他エンジンのIDへのキャンセル要求は登録元に影響しない。"""
    first, second = engine_factory(), engine_factory()
    job = cancel_jobs.controlled_job(result="first")
    job_id = await cancel_jobs.submit(first, job)
    await bounded(job.started.wait())
    with pytest.raises(api.JobNotFoundError):
        await bounded(second.cancel(job_id))
    await assert_unfinished(first, api, job_id)
    job.release.set()
    await bounded(first.wait(job_id))
    await assert_terminal(first, api, job_id, api.JobStatus.SUCCEEDED, "first")


async def test_cancel_success_other_async_work_progresses(engine, api, cancel_jobs):
    """C17: キャンセル受付と完了待機中も別Taskが通知・終了処理解放できる。"""
    job = cancel_jobs.cancellation_job()
    job_id = await cancel_jobs.submit(engine, job)
    await bounded(job.started.wait())
    requesting = asyncio.Event()
    progressed = asyncio.Event()
    waiter = await cancel_jobs.waiter(engine, job_id)

    async def request():
        requesting.set()
        return await engine.cancel(job_id)

    async def release_from_other_task():
        await requesting.wait()
        await job.cancel_received.wait()
        await checkpoint()
        progressed.set()
        job.cleanup_release.set()

    requester = cancel_jobs.task(request())
    releaser = cancel_jobs.task(release_from_other_task())
    results = await bounded(asyncio.gather(
        asyncio.shield(requester), asyncio.shield(releaser), asyncio.shield(waiter)
    ))
    assert results == [True, None, None]
    assert progressed.is_set()
    await assert_terminal(engine, api, job_id, api.JobStatus.CANCELLED)
