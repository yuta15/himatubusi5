# /// script
# requires-python = ">=3.14"
# dependencies = ["radon==6.0.1", "complexipy==8.3.0"]
# ///
"""3条件のPythonコードを同じ設定で静的解析し、CSV/JSON/Markdownへ保存する。"""

from __future__ import annotations

import argparse
import ast
import csv
import fnmatch
import hashlib
import json
import math
import platform
import sys
import tokenize
from importlib.metadata import version
from pathlib import Path
from statistics import mean

from complexipy import code_complexity
from radon.complexity import cc_visit_ast
from radon.visitors import ComplexityVisitor


REPOSITORY = Path(__file__).resolve().parents[1]
CONDITIONS = {
    "1": "1_only_specification",
    "2": "2_specification_and_arch",
    "3": "3_specification_and_arch_and_design",
}
IGNORED_DIRECTORIES = {
    "__pycache__", "node_modules", "build", "dist", "venv", "env", "site-packages",
}
SUMMARY_FIELDS = [
    "condition", "directory", "file_count", "source_lines",
    "cyclomatic_function_count", "cyclomatic_sum", "cyclomatic_mean",
    "cyclomatic_max", "cyclomatic_p90", "cognitive_function_count",
    "cognitive_sum", "cognitive_mean", "cognitive_max", "cognitive_p90",
    "cognitive_module_sum", "cognitive_file_total",
]


def is_test_file(path: Path) -> bool:
    return (any(part in {"test", "tests"} for part in path.parts[:-1])
            or path.name == "conftest.py" or path.name.startswith("test_")
            or path.name.endswith("_test.py"))


def discover_files(directory: Path, include_tests: bool, excludes: list[str]):
    included, excluded = [], []
    for path in sorted(directory.rglob("*.py")):
        relative = path.relative_to(directory)
        # 同じソースの別名計上と対象外への追跡を防ぐためsymlinkは辿らない。
        parents = [path, *path.parents[:len(relative.parts) - 1]]
        if (any(p.is_symlink() for p in parents)
                or any(part.startswith(".") or part in IGNORED_DIRECTORIES
                       for part in relative.parts[:-1])):
            excluded.append(relative.as_posix())
        elif ((not include_tests and is_test_file(relative))
              or any(fnmatch.fnmatchcase(relative.as_posix(), pattern) for pattern in excludes)):
            excluded.append(relative.as_posix())
        else:
            included.append(path)
    return included, excluded


def function_nodes(tree: ast.AST, prefix: str = ""):
    """名前付き関数を各1回列挙する。クラスの集計値は加算しない。"""
    for node in ast.iter_child_nodes(tree):
        scope = prefix
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            scope = f"{prefix}.{node.name}" if prefix else node.name
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield scope, node
        yield from function_nodes(node, scope)


def measure_file(path: Path, condition: str, directory: Path):
    with tokenize.open(path) as stream:
        source = stream.read()
    tree = ast.parse(source, filename=str(path))
    relative = path.relative_to(directory).as_posix()
    cyclomatic = []
    for name, node in function_nodes(tree):
        # 単位ごとにRadonへ渡す。ローカルクラスのメソッドも取りこぼさない。
        blocks = cc_visit_ast(node)
        block = next(b for b in blocks if b.lineno == node.lineno and b.name == node.name)
        cyclomatic.append({
            "condition": condition, "file": relative, "function": name,
            "line_start": node.lineno, "line_end": node.end_lineno,
            "complexity": block.complexity,
        })
    result = code_complexity(source, check_script=True, no_ignore=True, with_plans=False)
    cognitive, module = [], 0
    for function in result.functions:
        if function.name == "<module>":
            module += function.complexity
        else:
            cognitive.append({
                "condition": condition, "file": relative, "function": function.name,
                "line_start": function.line_start, "line_end": function.line_end,
                "complexity": function.complexity,
            })
    metrics = {
        "condition": condition, "file": relative,
        "source_lines": len(source.splitlines()),
        "cyclomatic_function_count": len(cyclomatic),
        "cyclomatic_sum": sum(row["complexity"] for row in cyclomatic),
        "cyclomatic_max": max((row["complexity"] for row in cyclomatic), default=0),
        "cyclomatic_module": ComplexityVisitor.from_ast(tree).complexity,
        "cognitive_function_count": len(cognitive),
        "cognitive_sum": sum(row["complexity"] for row in cognitive),
        "cognitive_max": max((row["complexity"] for row in cognitive), default=0),
        "cognitive_module": module, "cognitive_file_total": result.complexity,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
    return metrics, cyclomatic, cognitive


def distribution(rows):
    values = sorted(row["complexity"] for row in rows)
    return {
        "sum": sum(values), "mean": round(mean(values), 4) if values else 0.0,
        "max": max(values, default=0),
        "p90": values[math.ceil(len(values) * 0.9) - 1] if values else 0,
    }


def summarize(condition, directory, files, cyclomatic, cognitive):
    summary = {
        "condition": condition, "directory": directory,
        "file_count": len(files), "source_lines": sum(row["source_lines"] for row in files),
        "cyclomatic_function_count": len(cyclomatic),
        "cognitive_function_count": len(cognitive),
        "cognitive_module_sum": sum(row["cognitive_module"] for row in files),
        "cognitive_file_total": sum(row["cognitive_file_total"] for row in files),
    }
    for metric, rows in (("cyclomatic", cyclomatic), ("cognitive", cognitive)):
        summary.update({f"{metric}_{key}": value for key, value in distribution(rows).items()})
    return summary


def write_csv(path: Path, rows, fieldnames):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def comparison_markdown(summaries):
    lines = [
        "# 複雑度の比較", "",
        "各指標の合計・平均・最大・P90は関数／メソッド単位。クラス集計の二重加算はしない。",
        "CCはRadon、認知はcomplexipyの値であり、両指標の数値を互いに比較するものではない。", "",
        "| 条件 | ファイル数 | 行数 | CC単位数 | CC合計 | CC平均 | CC最大 | CC P90 | 認知単位数 | 認知合計 | 認知平均 | 認知最大 | 認知P90 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summaries:
        values = [row[key] for key in SUMMARY_FIELDS[:14] if key != "directory"]
        lines.append("| " + " | ".join(map(str, values)) + " |")
    lines += ["", "- 測定対象・除外ファイル・ツールバージョンは`manifest.json`を参照。",
              "- 条件別の`cyclomatic.csv`・`cognitive.csv`で関数名と位置を確認できる。",
              "- CCはローカル関数を別単位として測定。complexipyはネストした関数を外側に含めるため、単位数が異なることがある。",
              "- モジュール本体は関数統計に含めず、`files.csv`に別記。認知のファイル全体値は`cognitive_file_total`。",
              "- 合計は単位スコアの総和であり、プログラム全体の制御フローグラフの複雑度ではない。",
              "- 分割で単位数・平均が変わるため、合計・最大・分布と測定範囲も併せて比較する。", ""]
    return "\n".join(lines)


def measure(code_root: Path, output_dir: Path, include_tests=False, excludes=()):
    reports, summaries, inventory = {}, [], {}
    # 全ファイルの解析が成功してから出力する。解析エラーを0として集計しない。
    for condition, directory in CONDITIONS.items():
        root = code_root / directory
        if not root.is_dir():
            raise ValueError(f"条件{condition}のディレクトリがありません: {root}")
        paths, excluded = discover_files(root, include_tests, list(excludes))
        if not paths:
            raise ValueError(f"条件{condition}の測定対象が空です: {root}")
        files, cyclomatic, cognitive = [], [], []
        for path in paths:
            try:
                file_row, cc_rows, cognitive_rows = measure_file(path, condition, root)
            except (ValueError, SyntaxError) as error:
                raise ValueError(f"解析できません: {path}: {error}") from error
            files.append(file_row)
            cyclomatic.extend(cc_rows)
            cognitive.extend(cognitive_rows)
        summary = summarize(condition, directory, files, cyclomatic, cognitive)
        reports[condition] = (files, cyclomatic, cognitive, summary)
        summaries.append(summary)
        inventory[condition] = {"directory": directory, "excluded_files": excluded}
    output_dir.mkdir(parents=True, exist_ok=True)
    function_fields = ["condition", "file", "function", "line_start", "line_end", "complexity"]
    for condition, (files, cyclomatic, cognitive, summary) in reports.items():
        target = output_dir / condition
        target.mkdir(exist_ok=True)
        write_csv(target / "files.csv", files, list(files[0]))
        write_csv(target / "cyclomatic.csv", cyclomatic, function_fields)
        write_csv(target / "cognitive.csv", cognitive, function_fields)
        write_json(target / "summary.json", summary)
    write_csv(output_dir / "summary.csv", summaries, SUMMARY_FIELDS)
    (output_dir / "comparison.md").write_text(comparison_markdown(summaries), encoding="utf-8")
    write_json(output_dir / "manifest.json", {
        "schema_version": 1, "python": platform.python_version(),
        "tools": {name: version(name) for name in ("radon", "complexipy")},
        "code_root": str(code_root), "include_tests": include_tests,
        "exclude_patterns": list(excludes), "ignored_directories": sorted(IGNORED_DIRECTORIES),
        "ignore_hidden_directories": True, "follow_symlinks": False,
        "complexipy": {"check_script": True, "no_ignore": True, "with_plans": False},
        "percentile": "nearest-rank", "inventory": inventory,
    })
    return summaries


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--code-root", type=Path, default=REPOSITORY / "code")
    parser.add_argument("--output-dir", type=Path, default=REPOSITORY / "complexity_results")
    parser.add_argument("--include-tests", action="store_true", help="テストファイルも含める")
    parser.add_argument("--exclude", action="append", default=[], help="条件ディレクトリからの相対glob。複数指定可")
    args = parser.parse_args(argv)
    try:
        summaries = measure(args.code_root.resolve(), args.output_dir.resolve(),
                            args.include_tests, args.exclude)
    except (OSError, ValueError) as error:
        print(f"計測失敗: {error}", file=sys.stderr)
        return 1
    for row in summaries:
        print(f"条件{row['condition']}: CC合計={row['cyclomatic_sum']}, 認知合計={row['cognitive_sum']}")
    print(f"出力先: {args.output_dir.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
