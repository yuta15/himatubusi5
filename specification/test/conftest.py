"""実装選択、公開APIのfixture、pytestレポートのCSV保存。"""

import csv
import importlib
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from support import JobScope


SPECIFICATION = Path(__file__).resolve().parents[1]
ENGINE_DIRS = {
    "1": "1_only_specification",
    "2": "2_specification_and_arch",
    "3": "3_specification_and_arch_and_design",
}
API_KEY = pytest.StashKey()
MAIN_KEY = pytest.StashKey()
CSV_KEY = pytest.StashKey()


def pytest_addoption(parser):
    parser.addoption(
        "--engine", choices=tuple(ENGINE_DIRS), default=None,
        help="必須: 評価対象の実装条件 (1 / 2 / 3)",
    )


def pytest_configure(config):
    selected = config.getoption("--engine")
    if selected is None:
        raise pytest.UsageError("--engine=1 / --engine=2 / --engine=3 を指定してください")

    directory = SPECIFICATION.parent / "code" / ENGINE_DIRS[selected]
    # 同名モジュールを既にロードしたプロセスでの切り替えは禁止する。
    for name in ("interface", "main"):
        if name in sys.modules:
            raise pytest.UsageError(
                f"{name} は既に読み込まれています。新しいPythonプロセスで実行してください"
            )
    sys.path.insert(0, str(directory))
    importlib.invalidate_caches()
    api = importlib.import_module("interface")
    main = importlib.import_module("main")
    for name, module in (("interface", api), ("main", main)):
        if Path(module.__file__).resolve() != (directory / f"{name}.py").resolve():
            raise pytest.UsageError(f"選択対象以外の {name}.py が読み込まれました")
    config.stash[API_KEY] = api
    config.stash[MAIN_KEY] = main

    if not config.option.collectonly:
        reporter = CsvReporter(selected)
        config.stash[CSV_KEY] = reporter
        config.pluginmanager.register(reporter, "contract-csv")


class CsvReporter:
    """setup/call/teardownの標準レポートをテストごとに1行へまとめる。"""

    def __init__(self, engine):
        directory = SPECIFICATION / "results"
        directory.mkdir(exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
        self.path = directory / f"engine_{engine}_{stamp}_{uuid4().hex}.csv"
        self.engine = engine
        self.reports = {}
        self.stream = self.path.open("x", encoding="utf-8", newline="")
        self.writer = csv.DictWriter(
            self.stream, fieldnames=["engine", "test_id", "outcome", "duration_sec"]
        )
        self.writer.writeheader()
        self.stream.flush()

    def pytest_runtest_logreport(self, report):
        self.reports.setdefault(report.nodeid, []).append(report)
        if report.when == "teardown":
            self.write_result(report.nodeid)

    def write_result(self, nodeid):
        reports = self.reports.pop(nodeid)
        if any(r.failed and r.when != "call" for r in reports):
            outcome = "error"
        elif any(r.failed for r in reports):
            outcome = "failed"
        elif any(r.skipped for r in reports):
            outcome = "skipped"
        elif any(r.when == "call" for r in reports) and any(r.when == "teardown" for r in reports):
            outcome = "passed"
        else:
            outcome = "error"
        self.writer.writerow({
            "engine": self.engine,
            "test_id": nodeid,
            "outcome": outcome,
            "duration_sec": f"{sum(r.duration for r in reports):.9f}",
        })
        self.stream.flush()

    def pytest_sessionfinish(self, session, exitstatus):
        # 通常の中断でteardownレポートがなかった場合も、受信済みの結果を残す。
        for nodeid in list(self.reports):
            self.write_result(nodeid)
        self.stream.close()

    def pytest_terminal_summary(self, terminalreporter):
        terminalreporter.write_line(f"契約テストCSV: {self.path}")


def pytest_unconfigure(config):
    reporter = config.stash.get(CSV_KEY, None)
    if reporter is not None and not reporter.stream.closed:
        reporter.stream.close()


@pytest.fixture(scope="session")
def api(pytestconfig):
    return pytestconfig.stash[API_KEY]


@pytest.fixture
def engine_factory(pytestconfig):
    return pytestconfig.stash[MAIN_KEY].create_engine


@pytest.fixture
def engine(engine_factory):
    return engine_factory()


@pytest.fixture
async def jobs():
    scope = JobScope()
    try:
        yield scope
    finally:
        await scope.finish()


@pytest.fixture
async def v2_jobs():
    from v2_support import V2Scope

    scope = V2Scope()
    try:
        yield scope
    finally:
        await scope.finish()
