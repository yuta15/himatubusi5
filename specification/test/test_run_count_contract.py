"""追加仕様v1.2、R01〜R18の共通契約テスト。既存73ケースも回帰実行する。"""

import asyncio
import inspect

import pytest

from cancel_support import assert_terminal, assert_unfinished
from run_count_support import ExecutionRecord, RunCountScope, assert_pending, next_started
from support import bounded, checkpoint


@pytest.fixture
async def run_jobs():
    scope = RunCountScope()
    try:
        yield scope
    finally:
        await scope.finish()


@pytest.mark.parametrize("limit", ["omitted", None, 1, 2, 3])
def test_create_engine_success_concurrency_contract(engine_factory, limit):
    """R01: 同期の生成APIが省略・None・正の上限を受け付ける。"""
    assert not inspect.iscoroutinefunction(engine_factory)
    parameters = list(inspect.signature(engine_factory).parameters.values())
    assert len(parameters) == 1
    assert parameters[0].name == "max_concurrency"
    assert parameters[0].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert parameters[0].default is None
    engine = engine_factory() if limit == "omitted" else engine_factory(max_concurrency=limit)
    for name in ("submit", "get_status", "get_result", "wait", "cancel"):
        assert inspect.iscoroutinefunction(getattr(engine, name))


@pytest.mark.parametrize(
    "invalid", [0, -1, -10, True, False, 1.0, 1.5, "2", "", [], {}, object()],
    ids=["zero", "negative-one", "negative-ten", "true", "false", "integral-float",
         "fraction", "numeric-string", "empty-string", "list", "dict", "object"],
)
def test_create_engine_failure_invalid_concurrency(engine_factory, invalid):
    """R02: boolを含む無効な上限を生成時にValueErrorまたはTypeErrorで拒否する。"""
    with pytest.raises((ValueError, TypeError)):
        engine_factory(max_concurrency=invalid)


@pytest.mark.parametrize("explicit_none", [False, True], ids=["omitted", "none"])
async def test_submit_success_unlimited_concurrency(engine_factory, api, run_jobs, explicit_none):
    """R03: 省略・Noneではどのジョブも解放前に全ジョブが開始できる。"""
    engine = engine_factory(max_concurrency=None) if explicit_none else engine_factory()
    jobs = [run_jobs.run_job() for _ in range(6)]
    ids = [await run_jobs.submit(engine, job) for job in jobs]
    await bounded(asyncio.gather(*(job.started.wait() for job in jobs)))
    assert run_jobs.record.active == len(jobs)
    for job in jobs:
        job.release.set()
    for job_id, job in zip(ids, jobs):
        await assert_terminal(engine, api, job_id, api.JobStatus.SUCCEEDED, job.result)


@pytest.mark.parametrize("limit", [1, 2, 3])
async def test_submit_success_full_capacity_registers_pending(engine_factory, api, run_jobs, limit):
    """R04: 満杯でも登録が復帰し、一意IDと本体未実行のPENDINGを維持する。"""
    engine = engine_factory(max_concurrency=limit)
    running = await run_jobs.fill(engine, limit)
    pending = []
    for _ in range(3):
        job = run_jobs.run_job()
        pending.append((await run_jobs.submit(engine, job), job))
    ids = [job_id for job_id, _ in running + pending]
    assert all(isinstance(job_id, str) for job_id in ids)
    assert len(set(ids)) == len(ids)
    for job_id, _ in running:
        assert await bounded(engine.get_status(job_id)) is api.JobStatus.RUNNING
    for job_id, job in pending:
        await assert_pending(engine, api, job_id, job)
    assert run_jobs.record.active == limit
    assert run_jobs.record.peak == limit


@pytest.mark.parametrize("limit", [1, 2, 3])
@pytest.mark.parametrize("concurrent", [False, True], ids=["sequential", "concurrent"])
async def test_submit_success_capacity_and_eventual_completion(
    engine_factory, api, run_jobs, limit, concurrent
):
    """R05: 開始した本体を順次Eventで解放し、上限以内・全件終端・各1回を確認する。"""
    engine = engine_factory(max_concurrency=limit)
    jobs = [run_jobs.run_job() for _ in range(limit * 3 + 2)]
    if concurrent:
        registrations = [run_jobs.task(run_jobs.submit(engine, job)) for job in jobs]
        ids = await bounded(asyncio.gather(*(asyncio.shield(t) for t in registrations)))
    else:
        ids = [await run_jobs.submit(engine, job) for job in jobs]
    assert len(set(ids)) == len(ids)
    by_job = dict(zip(jobs, ids))
    for _ in jobs:
        job = await next_started(run_jobs.record)
        assert job.calls == 1
        assert run_jobs.record.peak <= limit
        assert await bounded(engine.get_status(by_job[job])) is api.JobStatus.RUNNING
        job.release.set()
        await assert_terminal(engine, api, by_job[job], api.JobStatus.SUCCEEDED, job.result)
    assert all(job.calls == 1 for job in jobs)
    assert run_jobs.record.active == 0
    assert run_jobs.record.peak <= limit


@pytest.mark.parametrize("outcome", ["succeeded", "failed", "cancelled"])
async def test_wait_success_terminal_releases_capacity(engine_factory, api, run_jobs, outcome):
    """R06: 成功・失敗・自発的キャンセル後、追加登録なしで待機本体が開始する。"""
    engine = engine_factory(max_concurrency=1)
    first = run_jobs.run_job(outcome=outcome, spontaneous=outcome == "cancelled")
    first_id = await run_jobs.submit(engine, first)
    await bounded(first.started.wait())
    second = run_jobs.run_job()
    second_id = await run_jobs.submit(engine, second)
    await assert_pending(engine, api, second_id, second)
    first.release.set()
    await assert_terminal(
        engine, api, first_id, getattr(api.JobStatus, outcome.upper()), first.result, first.cause
    )
    await bounded(second.started.wait())
    assert await bounded(engine.get_status(second_id)) is api.JobStatus.RUNNING
    second.release.set()
    await assert_terminal(engine, api, second_id, api.JobStatus.SUCCEEDED, second.result)
    assert run_jobs.record.peak == 1


async def test_wait_success_pending_lifecycle(engine_factory, api, run_jobs):
    """R07: PENDINGの照会・未確定結果・待機が実行開始や実際の終了を妨げない。"""
    engine = engine_factory(max_concurrency=1)
    [(first_id, first)] = await run_jobs.fill(engine, 1)
    second = run_jobs.run_job()
    second_id = await run_jobs.submit(engine, second)
    waiter = await run_jobs.waiter(engine, second_id)
    await assert_pending(engine, api, second_id, second, (waiter,))
    first.release.set()
    await bounded(second.started.wait())
    await assert_unfinished(engine, api, second_id, (waiter,))
    second.release.set()
    assert await bounded(asyncio.shield(waiter)) is None
    await assert_terminal(engine, api, first_id, api.JobStatus.SUCCEEDED, first.result)
    await assert_terminal(engine, api, second_id, api.JobStatus.SUCCEEDED, second.result)


@pytest.mark.parametrize("duplicates", [False, True], ids=["initial", "duplicates"])
async def test_cancel_success_pending_capacity_isolation(engine_factory, api, run_jobs, duplicates):
    """R08/R09: 待機中の取消しは本体を実行せず、占有中の枠を余分に解放しない。"""
    engine = engine_factory(max_concurrency=1)
    [(first_id, first)] = await run_jobs.fill(engine, 1)
    cancelled, later = run_jobs.run_job(), run_jobs.run_job()
    cancelled_id = await run_jobs.submit(engine, cancelled)
    later_id = await run_jobs.submit(engine, later)
    await assert_pending(engine, api, cancelled_id, cancelled)
    await assert_pending(engine, api, later_id, later)
    waiter = await run_jobs.waiter(engine, cancelled_id)
    assert await bounded(engine.cancel(cancelled_id)) is True
    assert await bounded(asyncio.shield(waiter)) is None
    await assert_terminal(engine, api, cancelled_id, api.JobStatus.CANCELLED)
    if duplicates:
        for _ in range(3):
            assert await bounded(engine.cancel(cancelled_id)) is False
            await assert_pending(engine, api, later_id, later)
    await assert_unfinished(engine, api, first_id)
    await assert_pending(engine, api, later_id, later)
    first.release.set()
    await bounded(later.started.wait())
    later.release.set()
    await assert_terminal(engine, api, first_id, api.JobStatus.SUCCEEDED, first.result)
    await assert_terminal(engine, api, later_id, api.JobStatus.SUCCEEDED, later.result)
    await assert_terminal(engine, api, cancelled_id, api.JobStatus.CANCELLED)
    assert cancelled.calls == 0
    assert later.calls == 1
    assert run_jobs.record.peak == 1


@pytest.mark.parametrize("outcome", ["cancelled", "succeeded", "failed"])
async def test_cancel_success_cleanup_holds_capacity(engine_factory, api, run_jobs, outcome):
    """R10/R11: finally内の終了処理・キャンセル抑制中も実際の終了まで枠を保持する。"""
    engine = engine_factory(max_concurrency=1)
    first = run_jobs.run_job(outcome=outcome, cleanup=True, suppress=outcome != "cancelled")
    first_id = await run_jobs.submit(engine, first)
    await bounded(first.started.wait())
    later = run_jobs.run_job()
    later_id = await run_jobs.submit(engine, later)
    await assert_pending(engine, api, later_id, later)
    assert await bounded(engine.cancel(first_id)) is True
    await bounded(first.cleanup_started.wait())
    waiter = await run_jobs.waiter(engine, first_id)
    await assert_unfinished(engine, api, first_id, (waiter,))
    await assert_pending(engine, api, later_id, later)
    assert not first.cleanup_finished.is_set()
    assert run_jobs.record.active == 1
    first.cleanup_release.set()
    assert await bounded(asyncio.shield(waiter)) is None
    assert first.cleanup_finished.is_set()
    await assert_terminal(
        engine, api, first_id, getattr(api.JobStatus, outcome.upper()), first.result, first.cause
    )
    await bounded(later.started.wait())
    later.release.set()
    await assert_terminal(engine, api, later_id, api.JobStatus.SUCCEEDED, later.result)
    assert run_jobs.record.peak == 1


@pytest.mark.parametrize("concurrent", [False, True], ids=["sequential", "concurrent"])
async def test_cancel_success_duplicate_does_not_overrelease(engine_factory, api, run_jobs, concurrent):
    """R12: 終了処理到達後の重複要求は再通知も枠の重複解放も起こさない。"""
    engine = engine_factory(max_concurrency=1)
    [(first_id, first)] = await run_jobs.fill(engine, 1, cleanup=True)
    pending = []
    for _ in range(3):
        job = run_jobs.run_job()
        pending.append((await run_jobs.submit(engine, job), job))
    assert await bounded(engine.cancel(first_id)) is True
    await bounded(first.cleanup_started.wait())
    if concurrent:
        gate = asyncio.Event()

        async def request():
            await gate.wait()
            return await engine.cancel(first_id)

        tasks = [run_jobs.task(request()) for _ in range(4)]
        gate.set()
        results = await bounded(asyncio.gather(*(asyncio.shield(t) for t in tasks)))
    else:
        results = [await bounded(engine.cancel(first_id)) for _ in range(4)]
    assert all(value is False for value in results)
    await checkpoint()
    assert first.cancel_notifications == 1
    assert not first.cleanup_finished.is_set()
    await assert_unfinished(engine, api, first_id)
    for job_id, job in pending:
        await assert_pending(engine, api, job_id, job)
    first.cleanup_release.set()
    await assert_terminal(engine, api, first_id, api.JobStatus.CANCELLED)
    for _, job in pending:
        job.release.set()
    for job_id, job in pending:
        await assert_terminal(engine, api, job_id, api.JobStatus.SUCCEEDED, job.result)
        assert job.calls == 1
    assert first.cancel_notifications == 1
    assert first.cleanup_finished.is_set()
    assert run_jobs.record.peak == 1


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
@pytest.mark.parametrize("request_first", [False, True], ids=["release-task-first", "request-task-first"])
async def test_cancel_success_pending_start_race(engine_factory, api, run_jobs, outcome, request_first):
    """R13: 開始との競合は勝者を固定せず、受付・実行記録・終端結果の整合性を保つ。"""
    engine = engine_factory(max_concurrency=1)
    [(first_id, first)] = await run_jobs.fill(engine, 1)
    racing = run_jobs.run_job(outcome=outcome)
    later = run_jobs.run_job()
    racing_id = await run_jobs.submit(engine, racing)
    later_id = await run_jobs.submit(engine, later)
    await assert_pending(engine, api, racing_id, racing)
    await assert_pending(engine, api, later_id, later)
    # Cが先に選択されてもBを妨げず、Bが要求前に自然終了する場合も許容する。
    racing.release.set()
    later.release.set()
    gate = asyncio.Event()

    async def request():
        await gate.wait()
        return await engine.cancel(racing_id)

    async def release():
        await gate.wait()
        first.release.set()

    if request_first:
        requester = run_jobs.task(request())
        releaser = run_jobs.task(release())
    else:
        releaser = run_jobs.task(release())
        requester = run_jobs.task(request())
    gate.set()
    accepted, _ = await bounded(asyncio.gather(
        asyncio.shield(requester), asyncio.shield(releaser)
    ))
    assert type(accepted) is bool
    await bounded(engine.wait(racing_id))
    terminal = await bounded(engine.get_status(racing_id))
    natural = getattr(api.JobStatus, outcome.upper())
    assert racing.calls in (0, 1)
    if racing.calls == 0:
        assert accepted is True
        assert terminal is api.JobStatus.CANCELLED
    else:
        assert terminal in (natural, api.JobStatus.CANCELLED)
    if not accepted:
        assert terminal is natural
    if terminal is api.JobStatus.CANCELLED:
        assert accepted is True
    for _ in range(2):
        await assert_terminal(engine, api, racing_id, terminal, racing.result, racing.cause)
        assert await bounded(engine.cancel(racing_id)) is False
    await assert_terminal(engine, api, first_id, api.JobStatus.SUCCEEDED, first.result)
    await assert_terminal(engine, api, later_id, api.JobStatus.SUCCEEDED, later.result)
    assert later.calls == 1
    assert run_jobs.record.peak == 1


async def test_wait_success_simultaneous_capacity_release(engine_factory, api, run_jobs):
    """R14: 複数枠の同時解放でも上限を守り、待機ジョブを各1回自動実行する。"""
    engine = engine_factory(max_concurrency=2)
    gate = asyncio.Event()
    running = await run_jobs.fill(engine, 2, release=gate)
    pending = []
    for _ in range(5):
        job = run_jobs.run_job()
        pending.append((await run_jobs.submit(engine, job), job))
    for job_id, job in pending:
        await assert_pending(engine, api, job_id, job)
    gate.set()
    excluded = {job for _, job in running}
    # 空いた2枠が、どちらも新しい登録なしに利用できることを確認する。
    next_jobs = []
    for _ in range(2):
        job = await next_started(run_jobs.record, excluded)
        excluded.add(job)
        next_jobs.append(job)
    assert run_jobs.record.active == 2
    for job in next_jobs:
        job.release.set()
    for _ in range(len(pending) - len(next_jobs)):
        job = await next_started(run_jobs.record, excluded)
        excluded.add(job)
        job.release.set()
    for job_id, job in running + pending:
        await assert_terminal(engine, api, job_id, api.JobStatus.SUCCEEDED, job.result)
        assert job.calls == 1
    assert run_jobs.record.peak == 2


async def test_wait_success_pending_waiter_isolation(engine_factory, api, run_jobs):
    """R15: PENDINGへの待機Taskだけを取り消しても本体と他待機者は進行できる。"""
    engine = engine_factory(max_concurrency=1)
    [(first_id, first)] = await run_jobs.fill(engine, 1)
    later = run_jobs.run_job()
    later_id = await run_jobs.submit(engine, later)
    cancelled = await run_jobs.waiter(engine, later_id)
    survivor = await run_jobs.waiter(engine, later_id)
    await assert_pending(engine, api, later_id, later, (cancelled, survivor))
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await bounded(asyncio.shield(cancelled))
    await assert_pending(engine, api, later_id, later, (survivor,))
    first.release.set()
    await bounded(later.started.wait())
    await assert_unfinished(engine, api, later_id, (survivor,))
    later.release.set()
    assert await bounded(asyncio.shield(survivor)) is None
    await assert_terminal(engine, api, first_id, api.JobStatus.SUCCEEDED, first.result)
    await assert_terminal(engine, api, later_id, api.JobStatus.SUCCEEDED, later.result)
    assert later.cancel_notifications == 0


async def test_create_engine_success_independent_capacity(engine_factory, api, run_jobs):
    """R16: 別エンジンの上限・占有・解放・キャンセルが実行枠へ干渉しない。"""
    first, second = engine_factory(max_concurrency=1), engine_factory(max_concurrency=2)
    [(first_id, _)] = await run_jobs.fill(first, 1)
    first_later = run_jobs.run_job()
    first_later_id = await run_jobs.submit(first, first_later)
    second_record = ExecutionRecord()
    second_running = await run_jobs.fill(second, 2, record=second_record)
    second_later = run_jobs.run_job(record=second_record)
    second_later_id = await run_jobs.submit(second, second_later)
    await assert_pending(first, api, first_later_id, first_later)
    await assert_pending(second, api, second_later_id, second_later)
    assert await bounded(first.cancel(first_id)) is True
    await assert_terminal(first, api, first_id, api.JobStatus.CANCELLED)
    await bounded(first_later.started.wait())
    first_later.release.set()
    await assert_terminal(first, api, first_later_id, api.JobStatus.SUCCEEDED, first_later.result)
    for job_id, _ in second_running:
        await assert_unfinished(second, api, job_id)
    await assert_pending(second, api, second_later_id, second_later)
    second_running[0][1].release.set()
    await bounded(second_later.started.wait())
    for _, job in second_running:
        job.release.set()
    second_later.release.set()
    for job_id, job in second_running + [(second_later_id, second_later)]:
        await assert_terminal(second, api, job_id, api.JobStatus.SUCCEEDED, job.result)
    assert run_jobs.record.peak == 1
    assert second_record.peak == 2


async def test_wait_success_capacity_wait_allows_async_progress(engine_factory, api, run_jobs):
    """R17: 枠待ちと完了待機の間も別Taskが通知とジョブ解放を進められる。"""
    engine = engine_factory(max_concurrency=1)
    [(first_id, first)] = await run_jobs.fill(engine, 1)
    later = run_jobs.run_job()
    later_id = await run_jobs.submit(engine, later)
    waiter = await run_jobs.waiter(engine, later_id)
    await assert_pending(engine, api, later_id, later, (waiter,))
    progressed = asyncio.Event()

    async def release_from_other_task():
        progressed.set()
        first.release.set()
        await later.started.wait()
        later.release.set()

    releaser = run_jobs.task(release_from_other_task())
    assert await bounded(asyncio.gather(
        asyncio.shield(releaser), asyncio.shield(waiter)
    )) == [None, None]
    assert progressed.is_set()
    await assert_terminal(engine, api, first_id, api.JobStatus.SUCCEEDED, first.result)
    await assert_terminal(engine, api, later_id, api.JobStatus.SUCCEEDED, later.result)


@pytest.mark.parametrize("outcome", ["succeeded", "failed", "cancelled"])
async def test_get_result_success_limited_terminal_data_retained(engine_factory, api, run_jobs, outcome):
    """R18: 制限下の終端状態と結果は後続実行・反復操作でも保持され再実行されない。"""
    engine = engine_factory(max_concurrency=1)
    [(first_id, first)] = await run_jobs.fill(engine, 1)
    job = run_jobs.run_job(outcome=outcome)
    job_id = await run_jobs.submit(engine, job)
    await assert_pending(engine, api, job_id, job)
    first.release.set()
    await bounded(job.started.wait())
    if outcome == "cancelled":
        assert await bounded(engine.cancel(job_id)) is True
    else:
        job.release.set()
    expected = getattr(api.JobStatus, outcome.upper())
    await assert_terminal(engine, api, job_id, expected, job.result, job.cause)
    for _ in range(3):
        other = run_jobs.run_job()
        other.release.set()
        other_id = await run_jobs.submit(engine, other)
        await assert_terminal(engine, api, other_id, api.JobStatus.SUCCEEDED, other.result)
        await assert_terminal(engine, api, job_id, expected, job.result, job.cause)
        assert await bounded(engine.cancel(job_id)) is False
    await assert_terminal(engine, api, first_id, api.JobStatus.SUCCEEDED, first.result)
    assert job.calls == 1
    assert first.calls == 1
    assert run_jobs.record.peak == 1
