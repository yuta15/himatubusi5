import asyncio
from dataclasses import replace

import pytest

from interface import (
    JobCancelledError,
    JobDependencyError,
    JobExecutionError,
    JobNotFinishedError,
    JobNotFoundError,
    JobSpec,
    JobStatus,
    RetryPolicy,
)
from main import create_engine


pytestmark = pytest.mark.asyncio


@pytest.fixture
def engine():
    return create_engine(1)


async def work():
    return "done"


async def wait_all(engine, *ids):
    await asyncio.wait_for(asyncio.gather(*(engine.wait(i) for i in ids)), timeout=2)


@pytest.mark.parametrize("field,value", [
    ("max_retries", -1), ("max_retries", True), ("max_retries", 1.5),
    ("initial_delay", -1), ("initial_delay", float("nan")),
    ("initial_delay", float("inf")), ("initial_delay", "1"),
    ("multiplier", 0.9), ("multiplier", float("nan")),
    ("multiplier", float("inf")), ("multiplier", None),
    ("max_delay", -1), ("max_delay", float("nan")),
    ("max_delay", float("inf")), ("max_delay", "1"),
    ("retry_on", ()), ("retry_on", [ValueError]),
    ("retry_on", (BaseException,)), ("retry_on", (asyncio.CancelledError,)),
    ("retry_on", (ValueError(),)), ("retry_on", (ValueError, 1)),
])
async def test_submit_job_failure_invalid_retry_is_atomic(engine, field, value):
    policy = replace(RetryPolicy(), **{field: value})
    with pytest.raises((ValueError, TypeError)):
        await engine.submit_job(JobSpec(work, retry=policy))
    assert not engine._jobs
    good = await engine.submit(work)
    await wait_all(engine, good)
    assert await engine.get_result(good) == "done"


@pytest.mark.parametrize("field,value", [
    ("func", None), ("args", []), ("kwargs", []), ("kwargs", {1: "value"}),
    ("retry", None), ("timeout", 0), ("timeout", -1),
    ("timeout", float("nan")), ("timeout", float("inf")), ("timeout", "1"),
    ("priority", True), ("priority", False), ("priority", 1.5),
    ("priority", "1"), ("depends_on", []), ("depends_on", (1,)),
])
async def test_submit_job_failure_invalid_spec_is_atomic(engine, field, value):
    with pytest.raises((ValueError, TypeError)):
        await engine.submit_job(replace(JobSpec(work), **{field: value}))
    assert not engine._jobs


async def test_submit_job_failure_dependencies_are_validated_before_registration(engine):
    other = create_engine()
    foreign = await other.submit(work)
    first = await engine.submit(work)
    for dependency in ("missing", foreign):
        with pytest.raises(JobNotFoundError):
            await engine.submit_job(JobSpec(work, depends_on=(dependency,)))
    with pytest.raises(ValueError):
        await engine.submit_job(JobSpec(work, depends_on=(first, first)))
    assert len(engine._jobs) == 1
    await asyncio.gather(wait_all(engine, first), wait_all(other, foreign))


async def test_submit_job_success_snapshots_keyword_settings_for_every_attempt(engine):
    values = []
    kwargs = {"value": "registered"}

    async def record(*, value):
        values.append(value)
        if len(values) == 1:
            kwargs["value"] = "changed during retry"
            raise ValueError()
        return value

    spec = JobSpec(record, kwargs=kwargs, retry=RetryPolicy(max_retries=1))
    job_id = await engine.submit_job(spec)
    kwargs["value"] = "changed before start"
    await wait_all(engine, job_id)
    assert values == ["registered", "registered"]
    assert await engine.get_result(job_id) == "registered"


@pytest.mark.parametrize("max_retries", [0, 1, 2])
@pytest.mark.parametrize("retry_on", [(Exception,), (ValueError,), (RuntimeError,)])
async def test_retry_failure_exhaustion_and_exception_filter(engine, max_retries, retry_on):
    causes = []

    async def fail():
        cause = ValueError(str(len(causes)))
        causes.append(cause)
        raise cause

    job_id = await engine.submit_job(JobSpec(
        fail, retry=RetryPolicy(max_retries=max_retries, retry_on=retry_on)
    ))
    await wait_all(engine, job_id)
    assert len(causes) == (1 if retry_on == (RuntimeError,) else max_retries + 1)
    with pytest.raises(JobExecutionError) as error:
        await engine.get_result(job_id)
    assert error.value.cause is causes[-1]


async def test_retry_success_subclass_and_no_extra_attempt(engine):
    calls = 0

    class TransientError(ValueError):
        pass

    async def flaky():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise TransientError()
        return calls

    job_id = await engine.submit_job(JobSpec(
        flaky, retry=RetryPolicy(max_retries=5, retry_on=(ValueError,))
    ))
    await wait_all(engine, job_id)
    assert calls == 3
    assert await engine.get_result(job_id) == 3


async def test_retry_failure_spontaneous_cancelled_error_is_not_retried(engine):
    calls = 0

    async def cancelled():
        nonlocal calls
        calls += 1
        raise asyncio.CancelledError()

    job_id = await engine.submit_job(JobSpec(cancelled, retry=RetryPolicy(max_retries=3)))
    await wait_all(engine, job_id)
    assert calls == 1
    with pytest.raises(JobCancelledError):
        await engine.get_result(job_id)


@pytest.mark.parametrize("initial,multiplier,cap,expected", [
    (0.015, 2, None, [0.015, 0.03, 0.06]),
    (0.015, 2, 0.025, [0.015, 0.025, 0.025]),
    (0.02, 3, 0.005, [0.005, 0.005, 0.005]),
    (0, 2, None, [0, 0, 0]),
])
async def test_backoff_success_exponential_delays(engine, initial, multiplier, cap, expected):
    times = []
    loop = asyncio.get_running_loop()

    async def flaky():
        times.append(loop.time())
        if len(times) <= 3:
            raise ValueError()
        return "recovered"

    job_id = await engine.submit_job(JobSpec(flaky, retry=RetryPolicy(
        max_retries=3, initial_delay=initial, multiplier=multiplier, max_delay=cap
    )))
    await wait_all(engine, job_id)
    assert len(times) == 4
    for start, end, delay in zip(times, times[1:], expected):
        assert end - start >= delay * 0.95
    assert await engine.get_result(job_id) == "recovered"


async def test_backoff_success_pending_releases_slot_and_can_be_cancelled(engine):
    calls = 0
    attempted = asyncio.Event()

    async def fail():
        nonlocal calls
        calls += 1
        attempted.set()
        raise ValueError()

    job_id = await engine.submit_job(JobSpec(
        fail, retry=RetryPolicy(max_retries=2, initial_delay=60)
    ))
    await asyncio.wait_for(attempted.wait(), timeout=1)
    assert await engine.get_status(job_id) is JobStatus.PENDING
    with pytest.raises(JobNotFinishedError):
        await engine.get_result(job_id)
    waiter = asyncio.create_task(engine.wait(job_id))
    other = await engine.submit(work)
    await wait_all(engine, other)
    assert not waiter.done()
    assert await engine.cancel(job_id) is True
    assert await engine.cancel(job_id) is False
    await asyncio.wait_for(waiter, timeout=1)
    assert calls == 1
    with pytest.raises(JobCancelledError):
        await engine.get_result(job_id)


@pytest.mark.parametrize("retry_timeouts", [False, True])
async def test_timeout_failure_each_attempt_can_retry(engine, retry_timeouts):
    calls = 0

    async def blocked():
        nonlocal calls
        calls += 1
        await asyncio.Event().wait()

    job_id = await engine.submit_job(JobSpec(blocked, timeout=0.01, retry=RetryPolicy(
        max_retries=2, retry_on=(TimeoutError,) if retry_timeouts else (ValueError,)
    )))
    await wait_all(engine, job_id)
    assert calls == (3 if retry_timeouts else 1)
    with pytest.raises(JobExecutionError) as error:
        await engine.get_result(job_id)
    assert isinstance(error.value.cause, TimeoutError)


@pytest.mark.parametrize("first_request", ["timeout", "cancel"])
@pytest.mark.parametrize("outcome", ["cancelled", "success", "failure"])
async def test_timeout_cancel_race_holds_slot_and_does_not_interrupt_cleanup_twice(
    engine, first_request, outcome
):
    started = asyncio.Event()
    cleaning = asyncio.Event()
    release = asyncio.Event()
    timer_elapsed = asyncio.Event()
    cause = ValueError("cleanup failed")
    calls = 0

    async def blocked():
        nonlocal calls
        calls += 1
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cleaning.set()
            await release.wait()
            if outcome == "success":
                return "recovered"
            if outcome == "failure":
                raise cause
            raise

    job_id = await engine.submit_job(JobSpec(blocked, timeout=0.02))
    await asyncio.wait_for(started.wait(), timeout=1)
    other = await engine.submit(work)
    try:
        if first_request == "cancel":
            assert await engine.cancel(job_id) is True
        await asyncio.wait_for(cleaning.wait(), timeout=1)
        # 明示キャンセルが先の場合も、元のタイムアウト期限を通過させる。
        asyncio.get_running_loop().call_later(0.03, timer_elapsed.set)
        await asyncio.wait_for(timer_elapsed.wait(), timeout=1)
        assert await engine.cancel(job_id) is False
        assert await engine.get_status(job_id) is JobStatus.RUNNING
        assert await engine.get_status(other) is JobStatus.PENDING
        with pytest.raises(JobNotFinishedError):
            await engine.get_result(job_id)
    finally:
        release.set()
        await wait_all(engine, job_id, other)
    assert calls == 1
    if outcome == "success":
        assert await engine.get_result(job_id) == "recovered"
    elif outcome == "failure" or first_request == "timeout":
        with pytest.raises(JobExecutionError) as error:
            await engine.get_result(job_id)
        if outcome == "failure":
            assert error.value.cause is cause
        else:
            assert isinstance(error.value.cause, TimeoutError)
    else:
        with pytest.raises(JobCancelledError):
            await engine.get_result(job_id)
    assert await engine.get_result(other) == "done"


async def test_timeout_success_waiting_time_is_not_part_of_timeout(engine):
    started = asyncio.Event()
    release = asyncio.Event()
    elapsed = asyncio.Event()
    calls = 0

    async def first():
        started.set()
        await release.wait()

    async def flaky():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ValueError()
        return "done"

    first_id = await engine.submit(first)
    await asyncio.wait_for(started.wait(), timeout=1)
    queued = await engine.submit_job(JobSpec(work, timeout=0.005))
    dependent = await engine.submit_job(JobSpec(
        flaky, depends_on=(first_id,), timeout=0.005,
        retry=RetryPolicy(max_retries=1, initial_delay=0.03)
    ))
    asyncio.get_running_loop().call_later(0.03, elapsed.set)
    await asyncio.wait_for(elapsed.wait(), timeout=1)
    assert await engine.get_status(queued) is JobStatus.PENDING
    assert await engine.get_status(dependent) is JobStatus.PENDING
    release.set()
    await wait_all(engine, first_id, queued, dependent)
    assert await engine.get_result(queued) == "done"
    assert await engine.get_result(dependent) == "done"


async def test_dependencies_success_multiple_parents_and_diamond_do_not_occupy_slots(engine):
    calls = []

    async def record(name):
        calls.append(name)
        return name

    root = await engine.submit_job(JobSpec(record, args=("root",)))
    left = await engine.submit_job(JobSpec(record, args=("left",), depends_on=(root,)))
    right = await engine.submit_job(JobSpec(record, args=("right",), depends_on=(root,)))
    leaf = await engine.submit_job(JobSpec(
        record, args=("leaf",), depends_on=(left, right), priority=100
    ))
    await wait_all(engine, root, left, right, leaf)
    assert calls[0] == "root"
    assert set(calls[1:3]) == {"left", "right"}
    assert calls[-1] == "leaf"
    # 完了済みの依存先も利用できる。
    late = await engine.submit_job(JobSpec(work, depends_on=(root, leaf)))
    await wait_all(engine, late)
    assert await engine.get_result(late) == "done"


@pytest.mark.parametrize("parent_outcome", ["failed", "cancelled"])
async def test_dependencies_failure_propagates_direct_cause_and_isolates_other_jobs(
    engine, parent_outcome
):
    started = asyncio.Event()
    release = asyncio.Event()
    calls = []

    async def parent():
        started.set()
        await release.wait()
        raise ValueError()

    async def forbidden():
        calls.append("called")

    root = await engine.submit(parent)
    child = await engine.submit_job(JobSpec(forbidden, depends_on=(root,)))
    leaf = await engine.submit_job(JobSpec(forbidden, depends_on=(child,)))
    independent = await engine.submit(work)
    await asyncio.wait_for(started.wait(), timeout=1)
    if parent_outcome == "cancelled":
        await engine.cancel(root)
    release.set()
    await wait_all(engine, root, child, leaf, independent)
    late = await engine.submit_job(JobSpec(forbidden, depends_on=(root,)))
    await wait_all(engine, late)
    assert not calls
    for job_id, direct_parent in [(child, root), (leaf, child), (late, root)]:
        with pytest.raises(JobExecutionError) as error:
            await engine.get_result(job_id)
        assert isinstance(error.value.cause, JobDependencyError)
        assert error.value.cause.dependency_id == direct_parent
    assert await engine.get_result(independent) == "done"


async def test_dependencies_success_waits_for_final_retry_result(engine):
    calls = 0

    async def flaky():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ValueError()

    parent = await engine.submit_job(JobSpec(
        flaky, retry=RetryPolicy(max_retries=1, initial_delay=0.01)
    ))
    child = await engine.submit_job(JobSpec(work, depends_on=(parent,)))
    await wait_all(engine, parent, child)
    assert calls == 2
    assert await engine.get_result(child) == "done"


async def test_dependencies_success_cancel_waiting_child_leaves_parent_running(engine):
    started = asyncio.Event()
    release = asyncio.Event()

    async def parent():
        started.set()
        await release.wait()
        return "parent"

    root = await engine.submit(parent)
    child = await engine.submit_job(JobSpec(work, depends_on=(root,)))
    leaf = await engine.submit_job(JobSpec(work, depends_on=(child,)))
    await asyncio.wait_for(started.wait(), timeout=1)
    assert await engine.cancel(child) is True
    await wait_all(engine, child, leaf)
    assert await engine.get_status(child) is JobStatus.CANCELLED
    assert await engine.get_status(leaf) is JobStatus.FAILED
    assert await engine.get_status(root) is JobStatus.RUNNING
    release.set()
    await wait_all(engine, root)
    assert await engine.get_result(root) == "parent"


async def test_dependencies_failure_long_chain_does_not_recurse(engine):
    async def fail():
        raise ValueError()

    root = await engine.submit(fail)
    previous = root
    for _ in range(1200):
        previous = await engine.submit_job(JobSpec(work, depends_on=(previous,)))
    await wait_all(engine, previous)
    assert await engine.get_status(previous) is JobStatus.FAILED


async def test_priority_success_highest_first_then_readiness_order(engine):
    started = asyncio.Event()
    release = asyncio.Event()
    calls = []

    async def blocker():
        started.set()
        await release.wait()

    async def record(name):
        calls.append(name)

    first = await engine.submit(blocker)
    await asyncio.wait_for(started.wait(), timeout=1)
    ids = [await engine.submit_job(JobSpec(record, args=(name,), priority=priority))
           for name, priority in [("low", -1), ("same1", 2), ("high", 10), ("same2", 2)]]
    assert await engine.get_status(first) is JobStatus.RUNNING
    release.set()
    await wait_all(engine, first, *ids)
    assert calls == ["high", "same1", "same2", "low"]


async def test_priority_success_equal_priority_follows_readiness_instead_of_registration(engine):
    calls = []

    async def record(name):
        calls.append(name)

    root = await engine.submit_job(JobSpec(record, args=("root",), priority=10))
    dependent = await engine.submit_job(JobSpec(record, args=("dependent",), depends_on=(root,)))
    ready = await engine.submit_job(JobSpec(record, args=("ready",)))
    await wait_all(engine, root, dependent, ready)
    assert calls == ["root", "ready", "dependent"]


async def test_priority_success_retry_reenters_queue_with_original_priority(engine):
    started = asyncio.Event()
    release = asyncio.Event()
    calls = []
    attempts = 0

    async def flaky():
        nonlocal attempts
        attempts += 1
        calls.append("retry")
        if attempts == 1:
            raise ValueError()

    async def blocker():
        started.set()
        await release.wait()

    async def low():
        calls.append("low")

    retried = await engine.submit_job(JobSpec(
        flaky, priority=10, retry=RetryPolicy(max_retries=1, initial_delay=0.01)
    ))
    blocked = await engine.submit(blocker)
    await asyncio.wait_for(started.wait(), timeout=1)
    last = await engine.submit_job(JobSpec(low, priority=-1))
    elapsed = asyncio.Event()
    asyncio.get_running_loop().call_later(0.03, elapsed.set)
    await asyncio.wait_for(elapsed.wait(), timeout=1)
    release.set()
    await wait_all(engine, retried, blocked, last)
    assert calls == ["retry", "retry", "low"]


@pytest.mark.parametrize("outcome", ["success", "failure"])
async def test_cancel_success_suppressed_request_never_starts_another_attempt(engine, outcome):
    started = asyncio.Event()
    calls = 0
    cause = ValueError("cancelled attempt failed")

    async def blocked():
        nonlocal calls
        calls += 1
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if outcome == "success":
                return "recovered"
            raise cause

    job_id = await engine.submit_job(JobSpec(
        blocked, retry=RetryPolicy(max_retries=3), timeout=1
    ))
    await asyncio.wait_for(started.wait(), timeout=1)
    assert await engine.cancel(job_id) is True
    await wait_all(engine, job_id)
    assert calls == 1
    if outcome == "success":
        assert await engine.get_result(job_id) == "recovered"
    else:
        with pytest.raises(JobExecutionError) as error:
            await engine.get_result(job_id)
        assert error.value.cause is cause


async def test_timeout_success_suppressed_timeout_retries_actual_exception(engine):
    calls = 0
    cause = ValueError("timed out attempt recovered with an error")

    async def blocked():
        nonlocal calls
        calls += 1
        if calls == 2:
            return "second attempt"
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise cause

    job_id = await engine.submit_job(JobSpec(
        blocked, timeout=0.01,
        retry=RetryPolicy(max_retries=1, retry_on=(ValueError,)),
    ))
    await wait_all(engine, job_id)
    assert calls == 2
    assert await engine.get_result(job_id) == "second attempt"


@pytest.mark.parametrize("limit", [2, 4, None])
async def test_submit_job_success_mixed_features_preserve_concurrency_and_attempt_isolation(limit):
    engine = create_engine(limit)
    capacity = limit if limit is not None else 8
    filled = asyncio.Event()
    release = asyncio.Event()
    active = set()
    peak = 0
    attempts = [0] * 8

    async def mixed(index):
        nonlocal peak
        assert index not in active
        active.add(index)
        peak = max(peak, len(active))
        if len(active) == capacity:
            filled.set()
        attempts[index] += 1
        try:
            await release.wait()
            await asyncio.sleep(0)
            if attempts[index] == 1:
                raise ValueError()
            return index
        finally:
            active.remove(index)

    ids = [await engine.submit_job(JobSpec(
        mixed, args=(index,), priority=index % 3,
        timeout=1, retry=RetryPolicy(max_retries=1, initial_delay=0.001),
    )) for index in range(8)]
    dependent = await engine.submit_job(JobSpec(work, depends_on=tuple(ids), priority=100))
    try:
        await asyncio.wait_for(filled.wait(), timeout=1)
        assert await engine.get_status(dependent) is JobStatus.PENDING
    finally:
        release.set()
        await wait_all(engine, *ids, dependent)
    assert peak == capacity
    assert not active
    assert attempts == [2] * 8
    assert [await engine.get_result(i) for i in ids] == list(range(8))
    assert await engine.get_result(dependent) == "done"


async def test_create_engine_success_v2_state_timers_and_queues_are_independent(engine):
    other = create_engine(1)
    attempted = asyncio.Event()

    async def fail():
        attempted.set()
        raise ValueError()

    delayed = await engine.submit_job(JobSpec(
        fail, retry=RetryPolicy(max_retries=1, initial_delay=60)
    ))
    await asyncio.wait_for(attempted.wait(), timeout=1)
    unrelated = await other.submit_job(JobSpec(work, priority=-100, timeout=0.01))
    await wait_all(other, unrelated)
    assert await other.get_result(unrelated) == "done"
    assert await engine.get_status(delayed) is JobStatus.PENDING
    with pytest.raises(JobNotFoundError):
        await other.cancel(delayed)
    await engine.cancel(delayed)
    await wait_all(engine, delayed)
