from dataclasses import dataclass, field
from enum import Enum
from typing import Awaitable, Callable, Protocol


class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


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


class JobCancelledError(JobError):
    """ジョブがキャンセルされた。"""


class JobDependencyError(JobError):
    """依存ジョブが正常終了しなかった。"""

    def __init__(self, dependency_id: str) -> None:
        self.dependency_id = dependency_id
        super().__init__(f"Dependency job did not succeed: {dependency_id}")


@dataclass(frozen=True)
class RetryPolicy:
    max_retries: int = 0
    initial_delay: float = 0.0
    multiplier: float = 2.0
    max_delay: float | None = None
    retry_on: tuple[type[Exception], ...] = (Exception,)


@dataclass(frozen=True)
class JobSpec:
    func: Callable[..., Awaitable[object]]
    args: tuple[object, ...] = ()
    kwargs: dict[str, object] = field(default_factory=dict)
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    timeout: float | None = None
    priority: int = 0
    depends_on: tuple[str, ...] = ()


class JobEngine(Protocol):
    async def submit(
        self,
        func: Callable[..., Awaitable[object]],
        *args: object,
        **kwargs: object,
    ) -> str:
        """ジョブを登録し、実行をスケジュールする。"""
        ...

    async def submit_job(self, spec: JobSpec) -> str:
        """指定された設定でジョブを登録し、ジョブIDを返す。"""
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

    async def cancel(self, job_id: str) -> bool:
        """
        ジョブのキャンセルを要求する。

        Returns:
            True: キャンセル要求を新たに受け付けた。
            False: 既に要求済み、実行中の試行へタイムアウト通知済み、
                   またはジョブが終端状態にある。

        Raises:
            JobNotFoundError: 指定されたジョブIDが存在しない。
        """
        ...
