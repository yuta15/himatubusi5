"""Jobが保持する実行内容を非同期に実行する。"""

import asyncio

from job import Job


class JobRunner:
    def __init__(self) -> None:
        self._task: asyncio.Task[None] | None = None

    def schedule(self, job: Job) -> None:
        # 完了するまでTaskへの強参照を保持する。
        self._task = asyncio.create_task(self._run(job))

    async def _run(self, job: Job) -> None:
        if not job.start():
            return
        try:
            result = await job.settings.func(*job.settings.args, **job.settings.kwargs)
        except BaseException as cause:
            # CancelledErrorを含め、処理由来の例外はジョブの失敗として保持する。
            job.fail(cause)
        else:
            job.succeed(result)
