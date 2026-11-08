import asyncio

import pytest

from interface import JobCancelledError, JobExecutionError, JobNotFinishedError, JobStatus
from main import create_engine


pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("limit", [0, -1, -10, True, False, 1.0, "2", [], object()])
async def test_create_engine_failure_invalid_concurrency(limit):
    """正の整数とNone以外の上限を生成時に拒否する。"""
    with pytest.raises((ValueError, TypeError)):
        create_engine(limit)


@pytest.mark.parametrize("limit", [1, 2, 4, None])
async def test_submit_success_concurrency_limit_and_automatic_progress(limit):
    """上限まで並行実行し、残りをPENDINGで保持して全ジョブを一度ずつ実行する。"""
    engine = create_engine(limit)
    count = 12
    capacity = limit if limit is not None else count
    filled = asyncio.Event()
    release = asyncio.Event()
    active = 0
    peak = 0
    calls = []

    async def work(index):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        calls.append(index)
        if active == capacity:
            filled.set()
        try:
            await release.wait()
            await asyncio.sleep(0)
            return index
        finally:
            active -= 1

    ids = [await engine.submit(work, index) for index in range(count)]
    try:
        await asyncio.wait_for(filled.wait(), timeout=1)
        statuses = [await engine.get_status(job_id) for job_id in ids]
        assert statuses.count(JobStatus.RUNNING) == capacity
        assert statuses.count(JobStatus.PENDING) == count - capacity
    finally:
        release.set()
        await asyncio.wait_for(
            asyncio.gather(*(engine.wait(job_id) for job_id in ids)), timeout=1
        )

    assert peak == capacity
    assert sorted(calls) == list(range(count))
    assert [await engine.get_result(job_id) for job_id in ids] == list(range(count))


async def test_wait_success_pending_queries_and_waiter_isolation():
    """実行待ちの状態照会・結果取得が即座に返り、完了待機が実行を妨げない。"""
    engine = create_engine(1)
    started = asyncio.Event()
    release = asyncio.Event()

    async def first():
        started.set()
        await release.wait()

    async def second():
        return "finished"

    first_id = await engine.submit(first)
    await asyncio.wait_for(started.wait(), timeout=1)
    # submit自体が枠を待機しないことも確認する。
    second_id = await asyncio.wait_for(engine.submit(second), timeout=1)
    waiter = asyncio.create_task(engine.wait(second_id))
    cancelled_waiter = asyncio.create_task(engine.wait(second_id))
    try:
        await asyncio.sleep(0)
        assert await engine.get_status(second_id) is JobStatus.PENDING
        with pytest.raises(JobNotFinishedError):
            await engine.get_result(second_id)
        assert not waiter.done()
        cancelled_waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled_waiter
        assert not waiter.done()
    finally:
        release.set()
        await asyncio.wait_for(asyncio.gather(engine.wait(first_id), waiter), timeout=1)
    assert await engine.get_result(second_id) == "finished"


async def test_cancel_success_pending_does_not_invoke_body_or_block_others():
    """枠待機中のキャンセルは本体を呼ばず、後続ジョブの実行枠も失わない。"""
    engine = create_engine(1)
    started = asyncio.Event()
    release = asyncio.Event()
    invoked = False

    async def first():
        started.set()
        await release.wait()

    async def cancelled():
        nonlocal invoked
        invoked = True

    async def last():
        return "last"

    first_id = await engine.submit(first)
    await asyncio.wait_for(started.wait(), timeout=1)
    cancelled_id = await engine.submit(cancelled)
    last_id = await engine.submit(last)
    await asyncio.sleep(0)
    try:
        assert await engine.get_status(cancelled_id) is JobStatus.PENDING
        assert await engine.cancel(cancelled_id) is True
        assert await engine.cancel(cancelled_id) is False
        await asyncio.wait_for(engine.wait(cancelled_id), timeout=1)
        with pytest.raises(JobCancelledError):
            await engine.get_result(cancelled_id)
        assert await engine.get_status(first_id) is JobStatus.RUNNING
        assert await engine.get_status(last_id) is JobStatus.PENDING
    finally:
        release.set()
        await asyncio.wait_for(
            asyncio.gather(*(engine.wait(i) for i in (first_id, cancelled_id, last_id))),
            timeout=1,
        )
    assert not invoked
    assert await engine.get_status(cancelled_id) is JobStatus.CANCELLED
    assert await engine.get_result(last_id) == "last"


async def test_cancel_success_pending_after_slot_becomes_available():
    """空き枠の通知直後に待機ジョブをキャンセルしても枠を失わない。"""
    engine = create_engine(1)
    started = asyncio.Event()
    release = asyncio.Event()
    invoked = False

    async def first():
        started.set()
        await release.wait()

    async def cancelled():
        nonlocal invoked
        invoked = True

    async def last():
        return "last"

    first_id = await engine.submit(first)
    await asyncio.wait_for(started.wait(), timeout=1)
    cancelled_id = await engine.submit(cancelled)
    last_id = await engine.submit(last)
    await asyncio.sleep(0)
    release.set()
    # 完了通知でこのTaskが先に再開し、枠待機Taskの再開前にキャンセルする。
    await engine.wait(first_id)
    assert await engine.get_status(cancelled_id) is JobStatus.PENDING
    assert await engine.cancel(cancelled_id) is True
    await asyncio.wait_for(engine.wait(last_id), timeout=1)
    assert not invoked
    assert await engine.get_status(cancelled_id) is JobStatus.CANCELLED
    assert await engine.get_result(last_id) == "last"


@pytest.mark.parametrize("outcome", ["success", "failure", "cancelled"])
@pytest.mark.parametrize("request_cancel", [False, True])
async def test_cancel_success_slot_held_until_actual_completion(outcome, request_cancel):
    """終了処理やキャンセル抑制中は枠を保持し、各終端状態で次のジョブへ渡す。"""
    engine = create_engine(1)
    started = asyncio.Event()
    finishing = asyncio.Event()
    release = asyncio.Event()
    cause = ValueError("failed")

    async def first():
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            finishing.set()
            await release.wait()
            if outcome == "cancelled":
                raise
            if outcome == "failure":
                raise cause
            return "recovered"

    async def spontaneous():
        started.set()
        finishing.set()
        await release.wait()
        if outcome == "cancelled":
            raise asyncio.CancelledError()
        if outcome == "failure":
            raise cause
        return "recovered"

    async def second():
        return "next"

    first_id = await engine.submit(first if request_cancel else spontaneous)
    await asyncio.wait_for(started.wait(), timeout=1)
    second_id = await engine.submit(second)
    try:
        if request_cancel:
            assert await engine.cancel(first_id) is True
            assert await engine.get_status(first_id) is JobStatus.RUNNING
        await asyncio.wait_for(finishing.wait(), timeout=1)
        await asyncio.sleep(0)
        assert await engine.get_status(first_id) is JobStatus.RUNNING
        assert await engine.get_status(second_id) is JobStatus.PENDING
        if request_cancel:
            assert await engine.cancel(first_id) is False
    finally:
        release.set()
        await asyncio.wait_for(
            asyncio.gather(engine.wait(first_id), engine.wait(second_id)), timeout=1
        )

    if outcome == "success":
        assert await engine.get_result(first_id) == "recovered"
    elif outcome == "failure":
        with pytest.raises(JobExecutionError) as error:
            await engine.get_result(first_id)
        assert error.value.cause is cause
    else:
        with pytest.raises(JobCancelledError):
            await engine.get_result(first_id)
    assert await engine.get_result(second_id) == "next"


async def test_create_engine_success_independent_execution_slots():
    """一方のエンジンが上限に達しても、他方は独立して実行できる。"""
    first_engine = create_engine(1)
    second_engine = create_engine(1)
    started = [asyncio.Event(), asyncio.Event()]
    release = asyncio.Event()

    async def work(index):
        started[index].set()
        await release.wait()

    first_id = await first_engine.submit(work, 0)
    await asyncio.wait_for(started[0].wait(), timeout=1)
    second_id = await second_engine.submit(work, 1)
    try:
        await asyncio.wait_for(started[1].wait(), timeout=1)
        assert await first_engine.get_status(first_id) is JobStatus.RUNNING
        assert await second_engine.get_status(second_id) is JobStatus.RUNNING
    finally:
        release.set()
        await asyncio.wait_for(
            asyncio.gather(first_engine.wait(first_id), second_engine.wait(second_id)),
            timeout=1,
        )
