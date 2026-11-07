import asyncio

import pytest

from interface import (
    JobCancelledError,
    JobExecutionError,
    JobNotFinishedError,
    JobNotFoundError,
    JobStatus,
)
from main import create_engine


pytestmark = pytest.mark.asyncio


@pytest.fixture
def engine():
    return create_engine()


async def test_submit_success_arguments_and_repeated_registration(engine):
    """同じ処理に位置・キーワード引数を渡せて、IDと結果が独立する。"""
    async def add(value, *, increment):
        return value + increment

    first = await engine.submit(add, 2, increment=3)
    second = await engine.submit(add, 7, increment=4)
    await asyncio.gather(engine.wait(first), engine.wait(second))

    assert first != second
    assert await engine.get_result(first) == 5
    assert await engine.get_result(second) == 11


async def test_get_status_success_state_transitions_and_unfinished_result(engine):
    """登録は完了を待たず、実行前・中の結果取得は例外となる。"""
    started = asyncio.Event()
    release = asyncio.Event()

    async def work():
        started.set()
        await release.wait()
        return None

    job_id = await engine.submit(work)
    assert await engine.get_status(job_id) is JobStatus.PENDING
    with pytest.raises(JobNotFinishedError):
        await engine.get_result(job_id)

    await asyncio.wait_for(started.wait(), timeout=1)
    assert await engine.get_status(job_id) is JobStatus.RUNNING
    with pytest.raises(JobNotFinishedError):
        await engine.get_result(job_id)

    release.set()
    await asyncio.wait_for(engine.wait(job_id), timeout=1)
    assert await engine.get_status(job_id) is JobStatus.SUCCEEDED
    assert await engine.get_result(job_id) is None


async def test_submit_success_concurrent_execution(engine):
    """複数ジョブが互いの終了を待たずに進行する。"""
    first_started = asyncio.Event()
    second_started = asyncio.Event()

    async def first():
        first_started.set()
        await second_started.wait()

    async def second():
        second_started.set()
        await first_started.wait()

    first_id = await engine.submit(first)
    second_id = await engine.submit(second)
    await asyncio.wait_for(
        asyncio.gather(engine.wait(first_id), engine.wait(second_id)), timeout=1
    )
    assert await engine.get_status(first_id) is JobStatus.SUCCEEDED
    assert await engine.get_status(second_id) is JobStatus.SUCCEEDED


@pytest.mark.parametrize("cause", [ValueError("failed"), RuntimeError("failed")])
async def test_get_result_failure_preserves_original_exception(engine, cause):
    """失敗時もwaitは正常復帰し、元の例外オブジェクトを確認できる。"""
    async def fail():
        raise cause

    job_id = await engine.submit(fail)
    assert await asyncio.wait_for(engine.wait(job_id), timeout=1) is None
    assert await engine.get_status(job_id) is JobStatus.FAILED
    for _ in range(2):
        with pytest.raises(JobExecutionError) as error:
            await engine.get_result(job_id)
        assert error.value.cause is cause
    assert await engine.get_status(job_id) is JobStatus.FAILED


async def test_submit_success_failure_does_not_interrupt_other_jobs(engine):
    """あるジョブの失敗後も、実行中の別ジョブは正常に完了する。"""
    started = asyncio.Event()
    release = asyncio.Event()

    async def work():
        started.set()
        await release.wait()
        return "finished"

    async def fail():
        await started.wait()
        raise RuntimeError("failed")

    good_id = await engine.submit(work)
    bad_id = await engine.submit(fail)
    await asyncio.wait_for(engine.wait(bad_id), timeout=1)
    assert await engine.get_status(good_id) is JobStatus.RUNNING
    release.set()
    await engine.wait(good_id)
    assert await engine.get_result(good_id) == "finished"


@pytest.mark.parametrize("method", ["get_status", "get_result", "wait", "cancel"])
async def test_lookup_failure_unknown_job(engine, method):
    """全照会APIで存在しないジョブIDを区別できる。"""
    with pytest.raises(JobNotFoundError):
        await getattr(engine, method)("missing")


@pytest.mark.parametrize("func", [None, 42, "work"])
async def test_submit_failure_non_callable_is_not_registered(engine, func):
    """Callableでない入力は登録前にTypeErrorとなる。"""
    with pytest.raises(TypeError):
        await engine.submit(func)
    assert not engine._jobs


@pytest.mark.parametrize("failure", ["invocation", "not_awaitable"])
async def test_submit_failure_callable_errors_are_job_failures(engine, failure):
    """呼出し時の例外や非Awaitableの戻り値もジョブの失敗として記録する。"""
    cause = ValueError("invocation failed")

    def work():
        if failure == "invocation":
            raise cause
        return 42

    job_id = await engine.submit(work)
    await engine.wait(job_id)
    with pytest.raises(JobExecutionError) as error:
        await engine.get_result(job_id)
    if failure == "invocation":
        assert error.value.cause is cause
    else:
        assert isinstance(error.value.cause, TypeError)


async def test_submit_success_callable_returning_awaitable(engine):
    """async関数以外のCallableが返したAwaitableも実行できる。"""
    class Work:
        def __call__(self, value):
            future = asyncio.get_running_loop().create_future()
            future.set_result(value)
            return future

    job_id = await engine.submit(Work(), "result")
    await engine.wait(job_id)
    assert await engine.get_result(job_id) == "result"


async def test_wait_success_multiple_waiters_and_completed_job(engine):
    """複数の待機者が正常復帰し、完了後の再待機も速やかに終わる。"""
    async def work():
        await asyncio.sleep(0)
        return 7

    job_id = await engine.submit(work)
    results = await asyncio.wait_for(
        asyncio.gather(engine.wait(job_id), engine.wait(job_id)), timeout=1
    )
    assert results == [None, None]
    assert await asyncio.wait_for(engine.wait(job_id), timeout=1) is None
    assert await engine.get_result(job_id) == 7
    assert await engine.get_status(job_id) is JobStatus.SUCCEEDED


async def test_wait_success_cancelled_waiter_does_not_cancel_job(engine):
    """待機者のキャンセルはジョブや別の待機者へ伝播しない。"""
    release = asyncio.Event()

    async def work():
        await release.wait()
        return 1

    job_id = await engine.submit(work)
    waiter = asyncio.create_task(engine.wait(job_id))
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert await engine.get_status(job_id) is JobStatus.RUNNING
    release.set()
    await asyncio.wait_for(engine.wait(job_id), timeout=1)
    assert await engine.get_result(job_id) == 1


async def test_create_engine_success_independent_instances(engine):
    """生成するエンジンごとにジョブの登録情報が独立する。"""
    other = create_engine()

    async def work():
        return 1

    job_id = await engine.submit(work)
    with pytest.raises(JobNotFoundError):
        await other.get_status(job_id)
    await engine.wait(job_id)


async def test_cancel_success_pending_body_is_not_invoked(engine):
    """実行前のキャンセルはCallable自体を呼び出さず、完了を通知する。"""
    invoked = False

    def work():
        nonlocal invoked
        invoked = True
        return asyncio.sleep(0)

    job_id = await engine.submit(work)
    assert await engine.get_status(job_id) is JobStatus.PENDING
    assert await engine.cancel(job_id) is True
    assert await engine.cancel(job_id) is False
    assert await asyncio.wait_for(engine.wait(job_id), timeout=1) is None
    await asyncio.sleep(0)
    assert not invoked
    assert await engine.get_status(job_id) is JobStatus.CANCELLED
    with pytest.raises(JobCancelledError):
        await engine.get_result(job_id)


async def test_cancel_success_running_waits_for_cleanup_and_notifies_once(engine):
    """受付は終了を待たず、重複要求は非同期の終了処理を再中断しない。"""
    started = asyncio.Event()
    cleaning = asyncio.Event()
    release = asyncio.Event()

    async def work():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release.wait()

    job_id = await engine.submit(work)
    await asyncio.wait_for(started.wait(), timeout=1)
    # 受付から照会までイベントループを譲らず、受付が終了を待たないことを確認する。
    assert await engine.cancel(job_id) is True
    assert await engine.get_status(job_id) is JobStatus.RUNNING
    with pytest.raises(JobNotFinishedError):
        await engine.get_result(job_id)
    await asyncio.wait_for(cleaning.wait(), timeout=1)
    waiter = asyncio.create_task(engine.wait(job_id))
    await asyncio.sleep(0)
    assert not waiter.done()
    assert await engine.cancel(job_id) is False
    # 通知が重複すればcleanupが中断され、ジョブが終了してしまう。
    await asyncio.sleep(0)
    assert await engine.get_status(job_id) is JobStatus.RUNNING
    release.set()
    assert await asyncio.wait_for(waiter, timeout=1) is None
    for _ in range(2):
        assert await engine.get_status(job_id) is JobStatus.CANCELLED
        assert await engine.cancel(job_id) is False
        assert await asyncio.wait_for(engine.wait(job_id), timeout=1) is None
        with pytest.raises(JobCancelledError):
            await engine.get_result(job_id)


@pytest.mark.parametrize("outcome", ["success", "failure"])
async def test_cancel_success_suppressed_cancellation_preserves_outcome(engine, outcome):
    """CancelledErrorを捕捉した処理は、実際の戻り値または元の例外で確定する。"""
    started = asyncio.Event()
    cause = ValueError("cleanup failed")

    async def work():
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if outcome == "failure":
                raise cause
            return "recovered"

    job_id = await engine.submit(work)
    await asyncio.wait_for(started.wait(), timeout=1)
    assert await engine.cancel(job_id) is True
    await asyncio.wait_for(engine.wait(job_id), timeout=1)
    if outcome == "success":
        assert await engine.get_status(job_id) is JobStatus.SUCCEEDED
        assert await engine.get_result(job_id) == "recovered"
    else:
        assert await engine.get_status(job_id) is JobStatus.FAILED
        with pytest.raises(JobExecutionError) as error:
            await engine.get_result(job_id)
        assert error.value.cause is cause
    assert await engine.cancel(job_id) is False


@pytest.mark.parametrize("outcome", ["cancelled", "success", "failure"])
async def test_get_result_success_spontaneous_cancellation_outcome(engine, outcome):
    """要求なしのCancelledErrorも、処理から最終的に伝播した結果で分類する。"""
    cause = RuntimeError("failed after cancellation")

    async def work():
        try:
            raise asyncio.CancelledError()
        except asyncio.CancelledError:
            if outcome == "cancelled":
                raise
            if outcome == "failure":
                raise cause
            return 3

    job_id = await engine.submit(work)
    assert await asyncio.wait_for(engine.wait(job_id), timeout=1) is None
    if outcome == "cancelled":
        assert await engine.get_status(job_id) is JobStatus.CANCELLED
        with pytest.raises(JobCancelledError):
            await engine.get_result(job_id)
    elif outcome == "failure":
        assert await engine.get_status(job_id) is JobStatus.FAILED
        with pytest.raises(JobExecutionError) as error:
            await engine.get_result(job_id)
        assert error.value.cause is cause
    else:
        assert await engine.get_status(job_id) is JobStatus.SUCCEEDED
        assert await engine.get_result(job_id) == 3
    assert await engine.cancel(job_id) is False


async def test_wait_success_cancelled_waiter_isolated_during_job_cancellation(engine):
    """キャンセル中の待機者を中断しても、別の待機者と終了処理は継続する。"""
    started = asyncio.Event()
    release = asyncio.Event()

    async def work():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            await release.wait()

    job_id = await engine.submit(work)
    await asyncio.wait_for(started.wait(), timeout=1)
    waiters = [asyncio.create_task(engine.wait(job_id)) for _ in range(3)]
    await asyncio.sleep(0)
    assert await engine.cancel(job_id) is True
    waiters[0].cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiters[0]
    assert not any(waiter.done() for waiter in waiters[1:])
    assert await engine.get_status(job_id) is JobStatus.RUNNING
    release.set()
    assert await asyncio.wait_for(asyncio.gather(*waiters[1:]), timeout=1) == [None, None]
    assert await engine.get_status(job_id) is JobStatus.CANCELLED


async def test_cancel_success_concurrent_requests_and_independent_jobs(engine):
    """同一ジョブの受付は一度だけで、他ジョブの実行・キャンセルと独立する。"""
    started = [asyncio.Event() for _ in range(3)]
    release = asyncio.Event()

    async def work(index):
        started[index].set()
        await release.wait()
        return index

    ids = [await engine.submit(work, index) for index in range(3)]
    await asyncio.wait_for(asyncio.gather(*(event.wait() for event in started)), timeout=1)
    accepted = await asyncio.gather(*(engine.cancel(ids[0]) for _ in range(5)))
    assert accepted.count(True) == 1
    assert accepted.count(False) == 4
    assert await engine.get_status(ids[1]) is JobStatus.RUNNING
    assert await engine.cancel(ids[1]) is True
    await asyncio.wait_for(asyncio.gather(*(engine.wait(job_id) for job_id in ids[:2])), timeout=1)
    assert await engine.get_status(ids[2]) is JobStatus.RUNNING
    release.set()
    await asyncio.wait_for(engine.wait(ids[2]), timeout=1)
    for job_id in ids[:2]:
        assert await engine.get_status(job_id) is JobStatus.CANCELLED
    assert await engine.get_result(ids[2]) == 2


@pytest.mark.parametrize("outcome", ["success", "failure"])
@pytest.mark.parametrize("cancel_first", [True, False])
async def test_cancel_success_race_preserves_actual_terminal_state(engine, outcome, cancel_first):
    """処理の再開と要求の順序が競合しても、確定した終端状態は変わらない。"""
    started = asyncio.Event()
    release = asyncio.Event()
    cause = ValueError("finished with failure")

    async def work():
        started.set()
        await release.wait()
        if outcome == "failure":
            raise cause
        return "finished"

    job_id = await engine.submit(work)
    await asyncio.wait_for(started.wait(), timeout=1)
    # Taskを実行可能キューへ入れる順序で競合の両順序を再現する。
    if cancel_first:
        cancel_task = asyncio.create_task(engine.cancel(job_id))
        release.set()
    else:
        release.set()
        cancel_task = asyncio.create_task(engine.cancel(job_id))
    assert await cancel_task is cancel_first
    await asyncio.wait_for(engine.wait(job_id), timeout=1)
    expected = JobStatus.CANCELLED if cancel_first else (
        JobStatus.SUCCEEDED if outcome == "success" else JobStatus.FAILED
    )
    assert await engine.get_status(job_id) is expected
    assert await engine.cancel(job_id) is False
    assert await engine.get_status(job_id) is expected
    if expected is JobStatus.SUCCEEDED:
        assert await engine.get_result(job_id) == "finished"
    elif expected is JobStatus.FAILED:
        with pytest.raises(JobExecutionError) as error:
            await engine.get_result(job_id)
        assert error.value.cause is cause
    else:
        with pytest.raises(JobCancelledError):
            await engine.get_result(job_id)
