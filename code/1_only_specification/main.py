from interface import JobEngine
from engine import AsyncJobEngine


def create_engine(max_concurrency: int | None = None) -> JobEngine:
    """
    新しいジョブ実行エンジンを生成する。
    この関数の内部実装は自由に変更してよい。
    ただし、関数名・引数・戻り値の契約は変更しないこと。

    呼び出しごとに独立したエンジンを生成すること。
    """
    return AsyncJobEngine()
