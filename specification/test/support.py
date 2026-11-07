"""テスト所有のジョブ・待機タスクを管理する共通ヘルパー。"""

import asyncio


OPERATION_TIMEOUT = 2.0


async def bounded(awaitable):
    return await asyncio.wait_for(awaitable, timeout=OPERATION_TIMEOUT)


async def checkpoint():
    """時間の経過ではなく、イベントループの次の実行機会を待つ。"""
    event = asyncio.Event()
    asyncio.get_running_loop().call_soon(event.set)
    await bounded(event.wait())


class ControlledJob:
    def __init__(self, result=None, error=None):
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.result = result
        self.error = error

    async def __call__(self):
        self.started.set()
        await self.release.wait()
        if self.error is not None:
            raise self.error
        return self.result


class JobScope:
    def __init__(self):
        self.controlled = []
        self.submitted = []
        self.tasks = []

    def controlled_job(self, result=None, error=None):
        job = ControlledJob(result, error)
        self.controlled.append(job)
        return job

    async def submit(self, engine, func, *args, **kwargs):
        # タイムアウトする不正実装でも、テストが作った登録タスクを回収できる。
        async def register():
            job_id = await engine.submit(func, *args, **kwargs)
            self.submitted.append((engine, job_id))
            return job_id

        task = self.task(register())
        return await bounded(asyncio.shield(task))

    def task(self, awaitable):
        task = asyncio.create_task(awaitable)
        self.tasks.append(task)
        return task

    async def finish(self):
        for job in self.controlled:
            job.release.set()

        # 登録中のタスクを含め、テスト自身が開始したタスクだけを回収する。
        if self.tasks:
            _, pending = await asyncio.wait(self.tasks, timeout=OPERATION_TIMEOUT)
            for task in pending:
                task.cancel()
            await bounded(asyncio.gather(*self.tasks, return_exceptions=True))

        async def finish_job(engine, job_id):
            await bounded(engine.wait(job_id))

        results = await asyncio.gather(
            *(finish_job(engine, job_id) for engine, job_id in self.submitted),
            return_exceptions=True,
        )
        errors = [result for result in results if isinstance(result, BaseException)]
        if errors:
            raise BaseExceptionGroup("登録したジョブの後処理に失敗しました", errors)


async def return_value(value=None):
    return value
