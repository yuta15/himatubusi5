import asyncio

import pytest

from interface import (
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


@pytest.mark.parametrize("cause", [ValueError("failed"), asyncio.CancelledError()])
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


@pytest.mark.parametrize("method", ["get_status", "get_result", "wait"])
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
