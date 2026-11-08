# 複雑度の計測

リポジトリルートから実行する。

```bash
uv run tools/measure_complexity.py
```

Python 3.14以上。スクリプト内のPEP 723メタデータでRadon 6.0.1・complexipy 8.3.0を固定し、
uvが分離環境へ依存関係を用意する。各エンジンの依存関係・コード・仕様書は変更しない。
解析対象のPythonコードはimport・実行せず、ソースを静的解析する。

## 出力

```text
complexity_results/
  comparison.md       # 条件1・2・3の比較表
  summary.csv         # 条件ごとの合計・平均・最大・P90・単位数・ファイル数・行数
  manifest.json       # ツール/Pythonのバージョン、測定設定、除外ファイル
  1/                  # 1_only_specification
    summary.json
    files.csv         # ファイルごとの集計・SHA-256
    cyclomatic.csv    # 関数・メソッドごとの循環的複雑度、名前、行番号
    cognitive.csv     # 関数・メソッドごとの認知的複雑度、名前、行番号
  2/                  # 2_specification_and_arch（同じ4ファイル）
  3/                  # 3_specification_and_arch_and_design（同じ4ファイル）
```

再実行時は同じ名前の結果ファイルを更新する。異なるスナップショットを保存したい場合は、
`--output-dir`で出力先を分ける。構文不正や条件ディレクトリの欠落・測定対象が空の場合は
終了コード1とし、解析エラーを0点として集計しない。全条件の解析が成功してから出力する。

## 測定対象

3条件に同じルールを適用し、各ディレクトリ内の`.py`を再帰的に検索する。
標準では`interface.py`・`main.py`も含め、以下を除外する。

- `test/`・`tests/`配下、`test_*.py`・`*_test.py`・`conftest.py`
- 隠しディレクトリ（`.venv`等）、`__pycache__`、`venv`、`env`、`node_modules`、`build`、`dist`、`site-packages`
- シンボリックリンク

```bash
# テストも含む別の集計
uv run tools/measure_complexity.py --include-tests --output-dir complexity_results_with_tests

# 共通の公開インターフェースを除いて比較
uv run tools/measure_complexity.py --exclude interface.py --output-dir complexity_results_implementation

# 出力先・入力先を指定（各条件のディレクトリ名は同じ）
uv run tools/measure_complexity.py --code-root /path/to/code --output-dir /path/to/results
```

`--exclude`は各条件のディレクトリからの相対パスに対するglob。複数回指定できる。
比較では、同じ測定設定・ツールバージョンの結果を使う。

## 指標と集計方法

循環的複雑度は[RadonのAPI](https://radon.readthedocs.io/en/stable/api.html)を使用する。
名前付きの通常関数・非同期関数・メソッドを各1回測定し、クラス自体の集計スコアは加算しない。
ネストした関数・ローカルクラスのメソッドもそれぞれ独立した測定単位にする。
各単位の基礎点が1のため、関数を分割すると合計にも影響する。

認知的複雑度は[complexipyのAPI](https://complexipy.com/api-reference/)の値を使用する。
complexipyはSonarSourceの研究に着想を得た独立実装であり、Sonar製品と同一の測定結果を保証するものではない。
ネストした関数の寄与を外側の関数に含めるため、Radonとは単位数・行範囲が異なる場合がある。
両指標を行番号で無理に1対1結合せず、別CSVへそのまま出力する。
`no_ignore=True`でソース内のcomplexipy抑制コメントに依存しない測定を行う。

- 合計・平均・最大・P90は、各ツールの関数／メソッドのスコアから計算する。
- 平均は全関数を母数とした値で、ファイルごとの平均の単純平均ではない。小数第4位まで表示する。
- P90は昇順の`ceil(単位数 × 0.9)`番目（nearest-rank）。関数がない場合の集計は0。
- モジュール本体は関数統計から分離し、`files.csv`の`cyclomatic_module`・`cognitive_module`へ出力する。
- `cognitive_file_total`はcomplexipyが返すファイル全体の値。比較表の認知合計は関数部分だけの総和。
- ラムダは名前付き関数として独立行を作らず、各ツールの計算規則に従う。
- 行数は空行・コメントも含む物理行数。生成されるdataclassのメソッド等はソースに存在しないため対象外。
- 合計は単位スコアの総和であり、プロジェクト全体の制御フローグラフを作った値ではない。
- 分割方法で平均や単位数が変わるので、合計・最大・P90・ソース量も併せて比較する。

解析器の対応構文や算定規則に依存するため、バージョンを固定して評価する。
この値だけで設計の優劣・不具合の数を判定しない。

## スクリプトの検証

依存関係をインストールしたPythonで、標準ライブラリのunittestを実行できる。
uvを使う場合は次のとおり。

```bash
uv run --python 3.14 --with radon==6.0.1 --with complexipy==8.3.0 python -m unittest discover -s tools/tests -v
```

既知の分岐とネスト、ローカルクラス、モジュール分岐、除外規則、3条件の分離、集計整合性、
再実行の再現性、構文不正時の既存結果保持を確認する。
