from enum import Enum
from typing import Awaitable, Callable, Protocol


class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class JobError(Exception):
    """ジョブ操作に関する例外。"""


class JobNotFoundError(JobError):
    """指定されたジョブIDが存在しない。"""


class JobNotFinishedError(JobError):
    """ジョブがまだ終了していない。"""


class JobExecutionError(JobError):
    """ジョブの実行が失敗した。"""

    def __init__(self, cause: BaseException) -> None:
        self.cause = cause
        super().__init__(str(cause))


class JobEngine(Protocol):
    async def submit(
        self,
        func: Callable[..., Awaitable[object]],
        *args: object,
        **kwargs: object,
    ) -> str:
        """ジョブを登録し、実行をスケジュールする。"""
        ...

    async def get_status(self, job_id: str) -> JobStatus:
        """ジョブの現在の状態を取得する。"""
        ...

    async def wait(self, job_id: str) -> None:
        """ジョブが終端状態になるまで待機する。"""
        ...

    async def get_result(self, job_id: str) -> object:
        """正常終了したジョブの戻り値を取得する。"""
        ...