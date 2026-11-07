"""ジョブを独立した asyncio タスクとして実行する。"""

import asyncio
from typing import Awaitable, Callable

from job import Job


class JobRunner:
    def __init__(self) -> None:
        # 実行中タスクがガベージコレクションされないよう強参照を保持する。
        self._tasks: dict[Job, asyncio.Task[None]] = {}

    def schedule(
        self,
        job: Job,
        func: Callable[..., Awaitable[object]],
        /,
        *args: object,
        **kwargs: object,
    ) -> None:
        task = asyncio.create_task(self._run(job, func, args, kwargs))
        self._tasks[job] = task
        task.add_done_callback(lambda completed: self._on_done(job, completed))

    def cancel(self, job: Job) -> bool:
        # 受付判定と通知の間で yield せず、終了処理中の重複通知を防ぐ。
        if not job.request_cancel():
            return False
        self._tasks[job].cancel()
        return True

    def _on_done(self, job: Job, task: asyncio.Task[None]) -> None:
        # 初回実行前に Task.cancel() された場合は _run 自体が実行されない。
        if task.cancelled():
            job.finish_cancelled()
        del self._tasks[job]

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
        except asyncio.CancelledError:
            job.finish_cancelled()
        except BaseException as cause:
            job.fail(cause)
        else:
            job.succeed(result)
