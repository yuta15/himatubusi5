"""仕様書v1.0の公開APIだけを使う共通契約テスト。"""

import asyncio

import pytest

from support import bounded, checkpoint, return_value


@pytest.mark.parametrize(
    "args, kwargs",
    [((), {}), ((1, "two"), {}), ((), {"value": 3}), ((1,), {"value": 3})],
    ids=["no-args", "positional", "keyword", "mixed"],
)
async def test_submit_success_arguments(engine, jobs, args, kwargs):
    """登録時の位置引数とキーワード引数がジョブに渡る。"""
    async def echo(*received_args, **received_kwargs):
        return received_args, received_kwargs

    job_id = await jobs.submit(engine, echo, *args, **kwargs)
    await bounded(engine.wait(job_id))
    assert await bounded(engine.get_result(job_id)) == (args, kwargs)


@pytest.mark.parametrize(
    "kind", ["async-function", "async-callable", "coroutine-factory", "awaitable", "future"]
)
async def test_submit_success_awaitable_callable(engine, jobs, kind):
    """Awaitableを返すCallableを形態によらず実行できる。"""
    class AsyncCallable:
        async def __call__(self, value):
            return value

    class ValueAwaitable:
        def __init__(self, value):
            self.value = value

        def __await__(self):
            return return_value(self.value).__await__()

    def future_factory(value):
        future = asyncio.get_running_loop().create_future()
        future.set_result(value)
        return future

    functions = {
        "async-function": return_value,
        "async-callable": AsyncCallable(),
        "coroutine-factory": lambda value: return_value(value),
        "awaitable": ValueAwaitable,
        "future": future_factory,
    }
    job_id = await jobs.submit(engine, functions[kind], 42)
    await bounded(engine.wait(job_id))
    assert await bounded(engine.get_result(job_id)) == 42


async def test_submit_success_unique_ids(engine, jobs):
    """同じ処理の複数登録に一意な文字列IDが割り当てられる。"""
    ids = [await jobs.submit(engine, return_value, value) for value in range(12)]
    assert all(isinstance(job_id, str) for job_id in ids)
    assert len(set(ids)) == len(ids)
    for value, job_id in enumerate(ids):
        await bounded(engine.wait(job_id))
        assert await bounded(engine.get_result(job_id)) == value


async def test_submit_success_returns_before_completion(engine, jobs, api):
    """ジョブを完了させなくても登録が終了する。"""
    job = jobs.controlled_job()
    job_id = await jobs.submit(engine, job)
    assert not job.release.is_set()
    status = await bounded(engine.get_status(job_id))
    assert isinstance(status, api.JobStatus)
    assert status in (api.JobStatus.PENDING, api.JobStatus.RUNNING)


@pytest.mark.parametrize("invalid", [None, 123, "job", []], ids=["none", "int", "str", "list"])
async def test_submit_failure_non_callable(engine, jobs, invalid):
    """非Callableの登録をTypeErrorで拒否し、その後も利用できる。"""
    with pytest.raises(TypeError):
        await jobs.submit(engine, invalid)
    job_id = await jobs.submit(engine, return_value, "healthy")
    await bounded(engine.wait(job_id))
    assert await bounded(engine.get_result(job_id)) == "healthy"


@pytest.mark.parametrize("fails", [False, True], ids=["succeeded", "failed"])
async def test_get_status_success_observable_transitions(engine, jobs, api, fails):
    """観測できる状態が実行待ち・実行中から正しい終端状態へ進む。"""
    job = jobs.controlled_job(error=ValueError("job failed") if fails else None)
    job_id = await jobs.submit(engine, job)
    initial = await bounded(engine.get_status(job_id))
    assert isinstance(initial, api.JobStatus)
    assert initial in (api.JobStatus.PENDING, api.JobStatus.RUNNING)
    await bounded(job.started.wait())
    for _ in range(2):
        assert await bounded(engine.get_status(job_id)) is api.JobStatus.RUNNING
    job.release.set()
    await bounded(engine.wait(job_id))
    expected = api.JobStatus.FAILED if fails else api.JobStatus.SUCCEEDED
    assert await bounded(engine.get_status(job_id)) is expected


@pytest.mark.parametrize("value", [42, False, "", [], {}, None], ids=["int", "false", "empty-str", "list", "dict", "none"])
async def test_get_result_success_return_values(engine, jobs, value):
    """正常終了の戻り値をNoneや偽値も含めて取得できる。"""
    job_id = await jobs.submit(engine, return_value, value)
    await bounded(engine.wait(job_id))
    result = await bounded(engine.get_result(job_id))
    assert type(result) is type(value)
    assert result == value


async def test_get_result_failure_unfinished_then_none(engine, jobs, api):
    """未完了時は待機せず例外となり、完了後のNoneと区別できる。"""
    job = jobs.controlled_job(result=None)
    job_id = await jobs.submit(engine, job)
    # 実装がPENDINGを公開する場合も、この時点の結果取得で検証する。
    with pytest.raises(api.JobNotFinishedError):
        await bounded(engine.get_result(job_id))
    await bounded(job.started.wait())
    with pytest.raises(api.JobNotFinishedError):
        await bounded(engine.get_result(job_id))
    job.release.set()
    await bounded(engine.wait(job_id))
    assert await bounded(engine.get_result(job_id)) is None


@pytest.mark.parametrize("error_type", [ValueError, RuntimeError])
async def test_get_result_failure_original_exception(engine, jobs, api, error_type):
    """失敗結果にジョブが送出した元の例外オブジェクトを保持する。"""
    cause = error_type("original failure")
    job = jobs.controlled_job(error=cause)
    job_id = await jobs.submit(engine, job)
    job.release.set()
    await bounded(engine.wait(job_id))
    with pytest.raises(api.JobExecutionError) as caught:
        await bounded(engine.get_result(job_id))
    assert caught.value.cause is cause


@pytest.mark.parametrize("fails", [False, True], ids=["succeeded", "failed"])
async def test_get_result_success_terminal_data_retained(engine, jobs, api, fails):
    """繰り返しの照会や他ジョブの実行後も終端状態と結果が保持される。"""
    cause = ValueError("retained failure")
    job = jobs.controlled_job(result={"value": 7}, error=cause if fails else None)
    job_id = await jobs.submit(engine, job)
    job.release.set()
    await bounded(engine.wait(job_id))
    expected = api.JobStatus.FAILED if fails else api.JobStatus.SUCCEEDED
    for _ in range(3):
        assert await bounded(engine.get_status(job_id)) is expected
        assert await bounded(engine.wait(job_id)) is None
        if fails:
            with pytest.raises(api.JobExecutionError) as caught:
                await bounded(engine.get_result(job_id))
            assert caught.value.cause is cause
        else:
            assert await bounded(engine.get_result(job_id)) == {"value": 7}
        other_id = await jobs.submit(engine, return_value, "other")
        await bounded(engine.wait(other_id))
    assert await bounded(engine.get_status(job_id)) is expected


@pytest.mark.parametrize("operation", ["get_status", "get_result", "wait"])
async def test_job_api_failure_unknown_id(engine, api, operation):
    """未登録のIDを指定した公開APIはJobNotFoundErrorを送出する。"""
    with pytest.raises(api.JobNotFoundError):
        await bounded(getattr(engine, operation)("never-submitted"))


@pytest.mark.parametrize("fails", [False, True], ids=["succeeded", "failed"])
async def test_wait_success_until_terminal(engine, jobs, api, fails):
    """waitはジョブ終了まで待ち、成功・失敗のどちらでもNoneを返す。"""
    job = jobs.controlled_job(result="not the wait result", error=ValueError("failure") if fails else None)
    job_id = await jobs.submit(engine, job)
    await bounded(job.started.wait())
    entered = asyncio.Event()

    async def wait_for_job():
        entered.set()
        return await engine.wait(job_id)

    waiter = jobs.task(wait_for_job())
    await bounded(entered.wait())
    await checkpoint()
    assert not waiter.done()
    job.release.set()
    assert await bounded(asyncio.shield(waiter)) is None
    expected = api.JobStatus.FAILED if fails else api.JobStatus.SUCCEEDED
    assert await bounded(engine.get_status(job_id)) is expected


@pytest.mark.parametrize("fails", [False, True], ids=["succeeded", "failed"])
async def test_wait_success_already_completed(engine, jobs, fails):
    """完了済みジョブの待機は繰り返しても速やかにNoneを返す。"""
    job = jobs.controlled_job(error=ValueError("failure") if fails else None)
    job_id = await jobs.submit(engine, job)
    job.release.set()
    await bounded(engine.wait(job_id))
    for _ in range(2):
        assert await bounded(engine.wait(job_id)) is None


async def test_wait_success_other_async_work_progresses(engine, jobs):
    """完了待機中も別の非同期処理が進み、ジョブを解放できる。"""
    job = jobs.controlled_job()
    job_id = await jobs.submit(engine, job)
    await bounded(job.started.wait())
    entered = asyncio.Event()
    progressed = asyncio.Event()

    async def wait_for_job():
        entered.set()
        return await engine.wait(job_id)

    async def release_from_other_task():
        await entered.wait()
        await checkpoint()
        progressed.set()
        job.release.set()

    waiter = jobs.task(wait_for_job())
    releaser = jobs.task(release_from_other_task())
    await bounded(asyncio.gather(asyncio.shield(waiter), asyncio.shield(releaser)))
    assert progressed.is_set()
    assert waiter.result() is None


async def test_submit_success_jobs_run_concurrently(engine, jobs):
    """待機しているジョブがあっても他のジョブが実行開始できる。"""
    controlled = [jobs.controlled_job(result=value) for value in range(3)]
    ids = [await jobs.submit(engine, job) for job in controlled]
    await bounded(asyncio.gather(*(job.started.wait() for job in controlled)))
    assert all(not job.release.is_set() for job in controlled)
    for job in controlled:
        job.release.set()
    for value, job_id in enumerate(ids):
        await bounded(engine.wait(job_id))
        assert await bounded(engine.get_result(job_id)) == value


async def test_get_status_success_jobs_are_independent(engine, jobs, api):
    """一方が完了しても他方の実行中状態と未確定結果が維持される。"""
    running = jobs.controlled_job(result="running")
    completed = jobs.controlled_job(result="completed")
    running_id = await jobs.submit(engine, running)
    completed_id = await jobs.submit(engine, completed)
    await bounded(asyncio.gather(running.started.wait(), completed.started.wait()))
    completed.release.set()
    await bounded(engine.wait(completed_id))
    assert await bounded(engine.get_status(completed_id)) is api.JobStatus.SUCCEEDED
    assert await bounded(engine.get_result(completed_id)) == "completed"
    assert await bounded(engine.get_status(running_id)) is api.JobStatus.RUNNING
    with pytest.raises(api.JobNotFinishedError):
        await bounded(engine.get_result(running_id))


async def test_submit_success_failure_isolation(engine, jobs, api):
    """ジョブの失敗が実行中の別ジョブや後続の登録に影響しない。"""
    healthy = jobs.controlled_job(result="healthy")
    healthy_id = await jobs.submit(engine, healthy)
    await bounded(healthy.started.wait())
    cause = ValueError("isolated failure")
    broken = jobs.controlled_job(error=cause)
    broken_id = await jobs.submit(engine, broken)
    broken.release.set()
    await bounded(engine.wait(broken_id))
    assert await bounded(engine.get_status(broken_id)) is api.JobStatus.FAILED
    assert await bounded(engine.get_status(healthy_id)) is api.JobStatus.RUNNING
    with pytest.raises(api.JobExecutionError) as caught:
        await bounded(engine.get_result(broken_id))
    assert caught.value.cause is cause
    healthy.release.set()
    await bounded(engine.wait(healthy_id))
    assert await bounded(engine.get_result(healthy_id)) == "healthy"
    later_id = await jobs.submit(engine, return_value, "later")
    await bounded(engine.wait(later_id))
    assert await bounded(engine.get_result(later_id)) == "later"


async def test_wait_success_only_selected_job(engine, jobs):
    """指定ジョブの待機は他ジョブの完了を待たず、別の待機を終了させない。"""
    first, second = jobs.controlled_job(), jobs.controlled_job()
    first_id = await jobs.submit(engine, first)
    second_id = await jobs.submit(engine, second)
    await bounded(asyncio.gather(first.started.wait(), second.started.wait()))
    first_waiter = jobs.task(engine.wait(first_id))
    second_waiter = jobs.task(engine.wait(second_id))
    await checkpoint()
    assert not first_waiter.done()
    assert not second_waiter.done()
    first.release.set()
    assert await bounded(asyncio.shield(first_waiter)) is None
    await checkpoint()
    assert not second_waiter.done()
    second.release.set()
    assert await bounded(asyncio.shield(second_waiter)) is None


async def test_wait_success_multiple_waiters(engine, jobs):
    """同じジョブの複数の待機者がすべて正常復帰できる。"""
    job = jobs.controlled_job()
    job_id = await jobs.submit(engine, job)
    await bounded(job.started.wait())
    waiters = [jobs.task(engine.wait(job_id)) for _ in range(3)]
    await checkpoint()
    assert all(not waiter.done() for waiter in waiters)
    job.release.set()
    assert await bounded(asyncio.gather(*(asyncio.shield(w) for w in waiters))) == [None] * 3


@pytest.mark.parametrize("operation", ["get_status", "get_result", "wait"])
async def test_create_engine_success_independent_instances(engine_factory, jobs, api, operation):
    """新しいエンジンは他のエンジンに登録したジョブを参照できない。"""
    first, second = engine_factory(), engine_factory()
    first_job = jobs.controlled_job(result="first")
    first_id = await jobs.submit(first, first_job)
    with pytest.raises(api.JobNotFoundError):
        await bounded(getattr(second, operation)(first_id))
    second_id = await jobs.submit(second, return_value, "second")
    await bounded(second.wait(second_id))
    assert await bounded(second.get_result(second_id)) == "second"
    first_job.release.set()
    await bounded(first.wait(first_id))
    assert await bounded(first.get_result(first_id)) == "first"
