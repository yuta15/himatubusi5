"""v2のDAG・優先度と実行枠・リトライ・キャンセルの組み合わせ。"""

import asyncio

import pytest

from cancel_support import assert_terminal, assert_unfinished
from support import bounded, return_value
from v2_support import (
    RETRY_DELAY, assert_pending, dependency_failed, elapsed, status_is,
)


@pytest.mark.parametrize("count", [1, 2])
async def test_submit_job_success_all_dependencies_required(engine, api, v2_jobs, count):
    """D01/D09: 全依存成功まで本体を実行せず、指定引数で自動的に実行する。"""
    parents = [v2_jobs.attempt([f"parent-{i}"], blocked=(0,)) for i in range(count)]
    ids = [await v2_jobs.submit_spec(engine, api.JobSpec(job)) for job in parents]
    await bounded(asyncio.gather(*(job.starts[0].wait() for job in parents)))
    child = v2_jobs.attempt(["child"], blocked=(0,))
    received = []

    async def body(value, *, named):
        received.append((value, named))
        return await child()

    child_id = await v2_jobs.submit_spec(engine, api.JobSpec(
        body, args=("explicit",), kwargs={"named": 42}, depends_on=tuple(ids)
    ))
    waiter = await v2_jobs.waiter(engine, child_id)
    for index, (parent, parent_id) in enumerate(zip(parents, ids)):
        await assert_pending(engine, api, child_id, (waiter,))
        assert child.invocations == 0
        parent.gates[0].set()
        await bounded(engine.wait(parent_id))
        if index + 1 < count:
            assert child.invocations == 0
    await bounded(child.starts[0].wait())
    assert received == [("explicit", 42)]
    child.gates[0].set()
    assert await bounded(asyncio.shield(waiter)) is None
    await assert_terminal(engine, api, child_id, api.JobStatus.SUCCEEDED, "child")


@pytest.mark.parametrize("outcome", ["succeeded", "failed", "cancelled"])
async def test_submit_job_success_completed_dependency(engine, api, v2_jobs, outcome):
    """D02/D04: 登録前に依存先が完了していても、その最終結果を採用する。"""
    value = {"succeeded": "ok", "failed": ValueError("failed"),
             "cancelled": asyncio.CancelledError()}[outcome]
    parent = await v2_jobs.submit_spec(engine, api.JobSpec(v2_jobs.attempt([value])))
    await bounded(engine.wait(parent))
    child = v2_jobs.attempt(["child"])
    child_id = await v2_jobs.submit_spec(engine, api.JobSpec(child, depends_on=(parent,)))
    if outcome == "succeeded":
        await assert_terminal(engine, api, child_id, api.JobStatus.SUCCEEDED, "child")
    else:
        await dependency_failed(engine, api, child_id, {parent})
        assert child.invocations == 0


@pytest.mark.parametrize("invalid", ["unknown", "foreign", "duplicate"])
async def test_submit_job_failure_invalid_dependency_atomic(engine_factory, api, v2_jobs, invalid):
    """D03: 未登録・他エンジンのIDと重複依存を区別して拒否し、本体を残さない。"""
    engine = engine_factory()
    owner = engine_factory() if invalid == "foreign" else engine
    parent = await v2_jobs.submit_spec(owner, api.JobSpec(return_value))
    await bounded(owner.wait(parent))
    deps = {"unknown": ("unregistered-forward-reference",),
            "foreign": (parent,), "duplicate": (parent, parent)}[invalid]
    child = v2_jobs.attempt(["invalid"])
    expected = ValueError if invalid == "duplicate" else api.JobNotFoundError
    with pytest.raises(expected):
        await v2_jobs.submit_spec(engine, api.JobSpec(child, depends_on=deps))
    healthy = await v2_jobs.submit_spec(engine, api.JobSpec(return_value, args=("healthy",)))
    await assert_terminal(engine, api, healthy, api.JobStatus.SUCCEEDED, "healthy")
    assert child.invocations == 0


@pytest.mark.parametrize("outcome", [ValueError("failed"), asyncio.CancelledError()], ids=["failed", "cancelled"])
async def test_submit_job_failure_dependency_propagation(engine, api, v2_jobs, outcome):
    """D04/D06/D07/D09: 分岐・合流へ失敗を伝播し、原因は直接依存先とする。"""
    parent_job = v2_jobs.attempt([outcome], blocked=(0,))
    parent = await v2_jobs.submit_spec(engine, api.JobSpec(parent_job))
    children = [v2_jobs.attempt(["must-not-run"]) for _ in range(2)]
    child_ids = [await v2_jobs.submit_spec(engine, api.JobSpec(
        job, depends_on=(parent,), retry=api.RetryPolicy(max_retries=2)
    )) for job in children]
    leaf_job = v2_jobs.attempt(["must-not-run"])
    leaf = await v2_jobs.submit_spec(engine, api.JobSpec(leaf_job, depends_on=tuple(child_ids)))
    parent_job.gates[0].set()
    for child_id in child_ids:
        await dependency_failed(engine, api, child_id, {parent})
    await dependency_failed(engine, api, leaf, set(child_ids))
    assert all(job.invocations == 0 for job in children + [leaf_job])
    healthy = await v2_jobs.submit_spec(engine, api.JobSpec(return_value, args=("independent",)))
    await assert_terminal(engine, api, healthy, api.JobStatus.SUCCEEDED, "independent")


@pytest.mark.parametrize("during_retry", [False, True], ids=["before-retry", "during-retry-X07"])
@pytest.mark.parametrize("succeeds", [False, True], ids=["exhausted", "recovered"])
async def test_submit_job_success_dependency_waits_for_final_retry(
    engine, api, v2_jobs, during_retry, succeeds
):
    """D05/X07: 再試行中の先行ジョブへ登録した後続も、最終結果まで開始・失敗確定しない。"""
    parent_job = v2_jobs.attempt(
        [ValueError("first"), "ok" if succeeds else ValueError("last")], blocked=(0, 1)
    )
    parent = await v2_jobs.submit_spec(engine, api.JobSpec(parent_job, retry=api.RetryPolicy(
        max_retries=1, initial_delay=RETRY_DELAY * 4
    )))
    await bounded(parent_job.starts[0].wait())
    child_job = v2_jobs.attempt(["child"])
    child = None
    if not during_retry:
        child = await v2_jobs.submit_spec(engine, api.JobSpec(child_job, depends_on=(parent,)))
    parent_job.gates[0].set()
    await status_is(engine, parent, api.JobStatus.PENDING)
    if during_retry:
        child = await v2_jobs.submit_spec(engine, api.JobSpec(child_job, depends_on=(parent,)))
    await assert_pending(engine, api, child)
    assert child_job.invocations == 0
    await bounded(parent_job.starts[1].wait())
    await assert_pending(engine, api, child)
    assert child_job.invocations == 0
    parent_job.gates[1].set()
    if succeeds:
        await assert_terminal(engine, api, child, api.JobStatus.SUCCEEDED, "child")
    else:
        await dependency_failed(engine, api, child, {parent})
        assert child_job.invocations == 0


@pytest.mark.parametrize("cancel_child", [False, True])
async def test_cancel_success_dependency_wait_capacity(engine_factory, api, v2_jobs, cancel_child):
    """D08: 依存待ちは実行枠を消費せず、直接取消し後に先行が成功しても再実行しない。"""
    engine = engine_factory(max_concurrency=2)
    parent_job = v2_jobs.attempt(["parent"], blocked=(0,))
    parent = await v2_jobs.submit_spec(engine, api.JobSpec(parent_job))
    await bounded(parent_job.starts[0].wait())
    child_job = v2_jobs.attempt(["child"])
    child = await v2_jobs.submit_spec(engine, api.JobSpec(child_job, depends_on=(parent,)))
    await assert_pending(engine, api, child)
    if cancel_child:
        assert await bounded(engine.cancel(child)) is True
        assert await bounded(engine.cancel(child)) is False
        await assert_terminal(engine, api, child, api.JobStatus.CANCELLED)
    other = await v2_jobs.submit_spec(engine, api.JobSpec(return_value, args=("other",)))
    await assert_terminal(engine, api, other, api.JobStatus.SUCCEEDED, "other")
    parent_job.gates[0].set()
    await bounded(engine.wait(parent))
    if cancel_child:
        await assert_terminal(engine, api, child, api.JobStatus.CANCELLED)
        assert child_job.invocations == 0
    else:
        await assert_terminal(engine, api, child, api.JobStatus.SUCCEEDED, "child")


async def test_submit_job_success_priority_order(engine_factory, api, v2_jobs):
    """P01/P04: 実行中の低順位を中断せず、枠が空いたら実行可能な高順位から開始する。"""
    engine = engine_factory(max_concurrency=1)
    holder = v2_jobs.run_job()
    holder_id = await v2_jobs.submit_spec(engine, api.JobSpec(holder, priority=-100))
    await bounded(holder.started.wait())
    jobs = {p: v2_jobs.attempt([p], blocked=(0,)) for p in (-10, 0, 10)}
    ids = {p: await v2_jobs.submit_spec(engine, api.JobSpec(job, priority=p)) for p, job in jobs.items()}
    assert holder.cancel_notifications == 0
    await assert_unfinished(engine, api, holder_id)
    assert all(job.invocations == 0 for job in jobs.values())
    holder.release.set()
    for priority in (10, 0, -10):
        await bounded(jobs[priority].starts[0].wait())
        assert all(jobs[p].invocations == 0 for p in jobs if p < priority)
        jobs[priority].gates[0].set()
        await assert_terminal(engine, api, ids[priority], api.JobStatus.SUCCEEDED, priority)
    assert v2_jobs.record.peak == 1


async def test_submit_job_success_equal_priority_readiness_order(engine_factory, api, v2_jobs):
    """P02: 同順位は登録順でなく、別々に成立させた実行可能時刻の順に開始する。"""
    engine = engine_factory(max_concurrency=2)
    holder = v2_jobs.attempt([None], blocked=(0,))
    parent = v2_jobs.attempt([None], blocked=(0,))
    holder_id = await v2_jobs.submit_spec(engine, api.JobSpec(holder))
    parent_id = await v2_jobs.submit_spec(engine, api.JobSpec(parent))
    await bounded(asyncio.gather(holder.starts[0].wait(), parent.starts[0].wait()))
    older = v2_jobs.attempt(["older"], blocked=(0,))
    older_id = await v2_jobs.submit_spec(engine, api.JobSpec(older, depends_on=(parent_id,)))
    newer = v2_jobs.attempt(["newer"], blocked=(0,))
    newer_id = await v2_jobs.submit_spec(engine, api.JobSpec(newer))
    replacement = v2_jobs.attempt([None], blocked=(0,))
    await v2_jobs.submit_spec(engine, api.JobSpec(replacement, priority=100))
    # newerは実行可能、olderはまだ依存待ち。この後にparentを成功させる。
    await assert_pending(engine, api, newer_id)
    await assert_pending(engine, api, older_id)
    parent.gates[0].set()
    await bounded(engine.wait(parent_id))
    # 依存成立後の枠を高順位ジョブで保持し、2件を比較できる状態を作る。
    await bounded(replacement.starts[0].wait())
    holder.gates[0].set()
    await bounded(engine.wait(holder_id))
    await bounded(newer.starts[0].wait())
    assert older.invocations == 0
    newer.gates[0].set()
    await bounded(engine.wait(newer_id))
    await bounded(older.starts[0].wait())
    older.gates[0].set()
    await assert_terminal(engine, api, older_id, api.JobStatus.SUCCEEDED, "older")


async def test_submit_job_success_high_priority_dependency_wait_does_not_block(engine_factory, api, v2_jobs):
    """P03: 依存条件を満たさない高順位ジョブより、実行可能な低順位を実行する。"""
    engine = engine_factory(max_concurrency=2)
    parent_job = v2_jobs.attempt([None], blocked=(0,))
    parent = await v2_jobs.submit_spec(engine, api.JobSpec(parent_job))
    await bounded(parent_job.starts[0].wait())
    high_job = v2_jobs.attempt(["high"])
    high = await v2_jobs.submit_spec(engine, api.JobSpec(high_job, depends_on=(parent,), priority=100))
    low = await v2_jobs.submit_spec(engine, api.JobSpec(return_value, args=("low",), priority=-100))
    await assert_terminal(engine, api, low, api.JobStatus.SUCCEEDED, "low")
    await assert_pending(engine, api, high)
    assert high_job.invocations == 0
    parent_job.gates[0].set()
    await assert_terminal(engine, api, high, api.JobStatus.SUCCEEDED, "high")


@pytest.mark.parametrize("retry_priority", [0, 10])
async def test_submit_job_success_retry_priority_and_ready_order(engine_factory, api, v2_jobs, retry_priority):
    """P05/X08: バックオフ中は低順位が進み、再投入では元の優先度と新しい実行可能順に従う。"""
    engine = engine_factory(max_concurrency=1)
    retrying = v2_jobs.attempt([ValueError("retry"), "retry"], blocked=(0, 1))
    retry_id = await v2_jobs.submit_spec(engine, api.JobSpec(
        retrying, priority=retry_priority,
        retry=api.RetryPolicy(max_retries=1, initial_delay=RETRY_DELAY * 3),
    ))
    await bounded(retrying.starts[0].wait())
    retrying.gates[0].set()
    await status_is(engine, retry_id, api.JobStatus.PENDING)
    low = v2_jobs.attempt(["low"], blocked=(0,))
    low_id = await v2_jobs.submit_spec(engine, api.JobSpec(low, priority=-100))
    await bounded(low.starts[0].wait())
    peer = v2_jobs.attempt(["peer"], blocked=(0,))
    peer_id = await v2_jobs.submit_spec(engine, api.JobSpec(peer, priority=0))
    # peerを先に実行可能にしてから、バックオフ期限を越えるまで枠を保持する。
    await elapsed(RETRY_DELAY * 4)
    assert retrying.calls == 1
    low.gates[0].set()
    await bounded(engine.wait(low_id))
    first, second = ((retrying, peer) if retry_priority > 0 else (peer, retrying))
    first_index = 1 if first is retrying else 0
    second_index = 1 if second is retrying else 0
    await bounded(first.starts[first_index].wait())
    assert not second.starts[second_index].is_set()
    first.gates[first_index].set()
    await bounded(second.starts[second_index].wait())
    second.gates[second_index].set()
    await assert_terminal(engine, api, retry_id, api.JobStatus.SUCCEEDED, "retry")
    await assert_terminal(engine, api, peer_id, api.JobStatus.SUCCEEDED, "peer")
    assert v2_jobs.record.peak == 1
