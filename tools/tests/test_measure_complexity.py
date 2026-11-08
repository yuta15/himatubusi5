"""既知の制御構造・測定対象・3条件の集計と出力を検証する。"""

import csv
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SPEC = importlib.util.spec_from_file_location(
    "measure_complexity", Path(__file__).resolve().parents[1] / "measure_complexity.py"
)
measurement = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(measurement)


class ComplexityTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def source(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return path

    def test_measure_success_known_control_flow(self):
        """既知の分岐数を測定し、ネストの深さが認知的複雑度へ反映される。"""
        cases = [
            ("def f(x):\n    return x\n", 1, 0),
            ("def f(x):\n    if x:\n        return 1\n    return 0\n", 2, 1),
            ("def f(x):\n    if x:\n        if x > 1:\n            return 1\n    return 0\n", 3, 3),
            ("def f(x):\n    if x:\n        return 1\n    if x > 1:\n        return 2\n    return 0\n", 3, 2),
            ("async def f(x):\n    if x:\n        return 1\n    return 0\n", 2, 1),
        ]
        for source, expected_cc, expected_cognitive in cases:
            with self.subTest(source=source):
                path = self.source("sample.py", source)
                _, cc, cognitive = measurement.measure_file(path, "1", self.root)
                self.assertEqual([r["complexity"] for r in cc], [expected_cc])
                self.assertEqual([r["complexity"] for r in cognitive], [expected_cognitive])

    def test_measure_success_nested_functions_and_classes(self):
        """クラス集計を二重加算せず、ローカル関数・ローカルクラスのメソッドもCCに含める。"""
        path = self.source("nested.py", """class Outer:
    async def method(self, x):
        class Local:
            def work(self):
                if x:
                    return 1
        def inner():
            if x:
                return 1
        if x:
            return inner()
""")
        metrics, cc, cognitive = measurement.measure_file(path, "1", self.root)
        self.assertEqual({r["function"] for r in cc}, {
            "Outer.method", "Outer.method.Local.work", "Outer.method.inner"
        })
        self.assertEqual(metrics["cyclomatic_sum"], 6)
        self.assertEqual(len(cognitive), 1)
        self.assertEqual(cognitive[0]["function"], "Outer::method")

    def test_measure_success_no_execution_and_module_separation(self):
        """対象を実行せず解析し、モジュールの分岐を関数統計から分離する。"""
        path = self.source("module.py", "raise RuntimeError('must not execute')\nif True:\n    value = 1\n")
        metrics, cc, cognitive = measurement.measure_file(path, "1", self.root)
        self.assertEqual(cc, [])
        self.assertEqual(cognitive, [])
        self.assertEqual(metrics["cyclomatic_module"], 2)
        self.assertEqual(metrics["cognitive_module"], 1)
        self.assertEqual(metrics["cognitive_file_total"], 1)

    def test_discover_success_consistent_exclusions(self):
        """各条件に同じ規則でテスト・隠し環境・生成物を除外し、指定時だけテストを含める。"""
        names = ["main.py", "pkg/engine.py", "interface.py", "test_engine.py",
                 "engine_test.py", "tests/nested/example.py", "test/helper.py", "conftest.py",
                 ".venv/module.py", "__pycache__/cache.py", "build/generated.py"]
        for name in names:
            self.source(name, "")
        files, _ = measurement.discover_files(self.root, False, [])
        self.assertEqual({p.relative_to(self.root).as_posix() for p in files},
                         {"main.py", "pkg/engine.py", "interface.py"})
        files, _ = measurement.discover_files(self.root, True, ["interface.py"])
        self.assertEqual(len(files), 7)
        self.assertFalse(any(p.name == "interface.py" for p in files))

    def test_measure_success_separate_outputs_and_weighted_summary(self):
        """3条件を別ファイルへ出力し、関数数に基づく平均と実ファイルからの合計が一致する。"""
        for condition, directory in measurement.CONDITIONS.items():
            for i in range(int(condition)):
                self.source(f"code/{directory}/part{i}.py", "def simple():\n    return 1\n")
            self.source(f"code/{directory}/branch.py", "def branch(x):\n    if x:\n        return 1\n")
        output = self.root / "results"
        summaries = measurement.measure(self.root / "code", output)
        self.assertEqual([s["cyclomatic_sum"] for s in summaries], [3, 4, 5])
        self.assertEqual([s["cyclomatic_mean"] for s in summaries], [1.5, 1.3333, 1.25])
        self.assertEqual([s["cognitive_sum"] for s in summaries], [1, 1, 1])
        for summary in summaries:
            folder = output / summary["condition"]
            for metric in ("cyclomatic", "cognitive"):
                with (folder / f"{metric}.csv").open() as stream:
                    rows = list(csv.DictReader(stream))
                self.assertEqual(sum(int(row["complexity"]) for row in rows), summary[f"{metric}_sum"])
                self.assertTrue(all(row["condition"] == summary["condition"] for row in rows))
            self.assertEqual(json.loads((folder / "summary.json").read_text()), summary)
        with (output / "summary.csv").open() as stream:
            self.assertEqual(len(list(csv.DictReader(stream))), 3)
        self.assertIn("| 3 |", (output / "comparison.md").read_text())
        self.assertIn("complexipy", json.loads((output / "manifest.json").read_text())["tools"])
        before = {p.relative_to(output): p.read_bytes() for p in output.rglob("*") if p.is_file()}
        measurement.measure(self.root / "code", output)
        after = {p.relative_to(output): p.read_bytes() for p in output.rglob("*") if p.is_file()}
        self.assertEqual(before, after)

    def test_measure_failure_invalid_source_preserves_previous_output(self):
        """構文不正を0点にせず失敗し、既存の結果を解析途中で上書きしない。"""
        for directory in measurement.CONDITIONS.values():
            self.source(f"code/{directory}/bad.py", "def broken(:\n")
        output = self.root / "results"
        output.mkdir()
        previous = output / "summary.csv"
        previous.write_text("previous result\n")
        with self.assertRaisesRegex(ValueError, "bad.py"):
            measurement.measure(self.root / "code", output)
        self.assertEqual(previous.read_text(), "previous result\n")

    def test_measure_failure_missing_or_empty_condition(self):
        """条件の欠落・空の測定範囲を成功扱いにしない。"""
        for create_empty in (False, True):
            with self.subTest(empty=create_empty):
                if create_empty:
                    (self.root / measurement.CONDITIONS["1"]).mkdir()
                with self.assertRaises(ValueError):
                    measurement.measure(self.root, self.root / "out")


if __name__ == "__main__":
    unittest.main()
