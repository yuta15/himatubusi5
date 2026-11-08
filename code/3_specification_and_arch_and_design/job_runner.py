"""Jobが保持する実行内容を非同期に実行する。"""

import asyncio

from interface import JobStatus
from job import Job


class JobRunner:
    def __init__(self, execution_slots: asyncio.Semaphore | None = None) -> None:
        self._execution_slots = execution_slots
        self._task: asyncio.Task[None] | None = None
        self._cancel_requested = False

    def schedule(self, job: Job) -> None:
        # 完了するまでTaskへの強参照を保持する。
        self._task = asyncio.create_task(self._run(job))

    def cancel(self, job: Job) -> bool:
        """要求を一度だけ通知する。実行中の終了結果は_runで確定する。"""
        if self._cancel_requested or job.status not in (JobStatus.PENDING, JobStatus.RUNNING):
            return False
        assert self._task is not None
        self._cancel_requested = True
        if job.status is JobStatus.PENDING:
            # 開始前にTaskをキャンセルすると_run自体が実行されない。
            # 本体が未実行と分かるこの時点で、状態と完了通知を確定する。
            job.cancelled()
        self._task.cancel()
        return True

    async def _run(self, job: Job) -> None:
        if self._execution_slots is None:
            await self._execute(job)
        else:
            # 枠の待機中はPENDINGのままにし、終了処理も含めて枠を保持する。
            async with self._execution_slots:
                await self._execute(job)

    async def _execute(self, job: Job) -> None:
        if not job.start():
            return
        try:
            result = await job.settings.func(*job.settings.args, **job.settings.kwargs)
        except asyncio.CancelledError:
            job.cancelled()
        except BaseException as cause:
            job.fail(cause)
        else:
            job.succeed(result)
