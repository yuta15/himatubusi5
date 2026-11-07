"""ジョブを独立した asyncio タスクとして実行する。"""

import asyncio
from typing import Awaitable, Callable

from job import Job


class JobRunner:
    def __init__(self) -> None:
        # 実行中タスクがガベージコレクションされないよう強参照を保持する。
        self._tasks: set[asyncio.Task[None]] = set()

    def schedule(
        self,
        job: Job,
        func: Callable[..., Awaitable[object]],
        /,
        *args: object,
        **kwargs: object,
    ) -> None:
        task = asyncio.create_task(self._run(job, func, args, kwargs))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _run(
        self,
        job: Job,
        func: Callable[..., Awaitable[object]],
        args: tuple[object, ...],
        kwargs: dict[str, object],
    ) -> None:
        job.start()
        try:
            result = await func(*args, **kwargs)
        except BaseException as cause:
            # CancelledError 等も当該ジョブの失敗として保持し、待機者へ通知する。
            job.fail(cause)
        else:
            job.succeed(result)
