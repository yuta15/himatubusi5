# 共通契約テスト

`specification/README.md` v1.0を、選択した実装の公開APIから検証する。
`main.create_engine()` と、その実装の `interface.py` だけを利用する。
内部のクラス、保存領域、タスク、スケジューリング方式は検査しない。

## 実行方法

Python 3.14以上を使用する。リポジトリルートから次のように実行する。

```bash
cd specification
uv run --group test python -m pytest test --engine=1
uv run --group test python -m pytest test --engine=2
uv run --group test python -m pytest test --engine=3
```

各コマンドは独立したpytest実行であり、指定した1つの実装だけを読み込む。

| オプション | 対象ディレクトリ |
| --- | --- |
| `--engine=1` | `code/1_only_specification` |
| `--engine=2` | `code/2_specification_and_arch` |
| `--engine=3` | `code/3_specification_and_arch_and_design` |

`--engine` の省略・未定義の値は使用方法エラーとなる。
同じPythonプロセスで `pytest.main()` を繰り返して対象を切り替える使い方はしない。
`main` / `interface` が既に読み込まれている場合はエラーにする。
実装ごとの追加依存がある場合は、テスト環境にその依存もインストールする。

uvを使わない場合は、通常の仮想環境でも実行できる。

```bash
python3.14 -m venv .venv
.venv/bin/python -m pip install 'pytest>=8.3,<10' 'pytest-asyncio>=1.0,<2' 'pytest-timeout>=2.3,<3'
.venv/bin/python -m pytest test --engine=1
```

一部のケースだけを実行する場合も同じオプションを指定する。

```bash
uv run --group test python -m pytest test --engine=1 -k failure_isolation
uv run --group test python -m pytest test --engine=1 --collect-only -q
```

## テストケース一覧

パラメーター化後は計44ケース。各テスト関数の説明はdocstringにも記載している。

| 関数名の `test_` 以降 | ケース数 | 検証内容 |
| --- | ---: | --- |
| `submit_success_arguments` | 4 | 引数なし・位置引数・キーワード引数・混在 |
| `submit_success_awaitable_callable` | 5 | 非同期関数、非同期Callable、通常関数が返すコルーチン、独自Awaitable、Future |
| `submit_success_unique_ids` | 1 | 同じ処理の複数登録、文字列IDの一意性、IDと結果の対応 |
| `submit_success_returns_before_completion` | 1 | ジョブを解放する前に登録が終了する |
| `submit_failure_non_callable` | 4 | 非CallableをTypeErrorで拒否し、その後も実行できる |
| `get_status_success_observable_transitions` | 2 | 観測できる初期状態、RUNNING、成功・失敗の終端状態 |
| `get_result_success_return_values` | 6 | 数値、False、空文字列、空リスト、空辞書、None |
| `get_result_failure_unfinished_then_none` | 1 | 未完了時の非待機エラーと正常終了のNoneの区別 |
| `get_result_failure_original_exception` | 2 | 元の例外オブジェクトをcauseに保持 |
| `get_result_success_terminal_data_retained` | 2 | 成功・失敗の状態と結果が反復操作や別ジョブの後も保持される |
| `job_api_failure_unknown_id` | 3 | 状態照会・結果取得・待機で未登録IDを拒否 |
| `wait_success_until_terminal` | 2 | 終了前の待機継続、成功・失敗後の正常復帰、戻り値None |
| `wait_success_already_completed` | 2 | 完了済みジョブの反復待機 |
| `wait_success_other_async_work_progresses` | 1 | 完了待機中の別の非同期処理の進行 |
| `submit_success_jobs_run_concurrently` | 1 | どのジョブも解放する前に複数ジョブが開始する |
| `get_status_success_jobs_are_independent` | 1 | 他ジョブが完了しても実行中状態・未確定結果が維持される |
| `submit_success_failure_isolation` | 1 | 失敗が実行中ジョブと後続ジョブに影響しない |
| `wait_success_only_selected_job` | 1 | 他ジョブを待たず、他ジョブの待機を終了させない |
| `wait_success_multiple_waiters` | 1 | 同じジョブの複数待機者がすべて復帰する |
| `create_engine_success_independent_instances` | 3 | 新規エンジンが他エンジンのジョブを3つのAPIで参照できない |

## 同期と後処理

- テストごとに新しいエンジンを生成する。独立性のテストでは2つ生成する。
- `asyncio.Event` でジョブの開始通知・終了許可を制御する。
- 待機タスクの実行機会はイベントループのコールバックとEventで確保する。
- `asyncio.sleep()` の経過時間で合否を判定しない。
- API操作・イベント待機の上限は `support.py` の `OPERATION_TIMEOUT`（2秒）。
- イベントループ自体をブロックする実装に備え、`pytest-timeout` でテスト全体にも15秒の上限を設ける。
- 時間の上限はハング検出用であり、性能要件ではない。遅い環境では両方を調整する。
- 後処理では制御用イベントを解放し、テスト所有のタスクを回収し、登録済みIDを公開の `wait()` で待つ。
- エンジン内部のタスクを列挙・キャンセルしない。後処理に失敗した場合はpytestのteardownエラーとして残す。

## CSV出力

実行ごとに `specification/results/` へ自動保存し、pytestの終了表示にファイルパスを出力する。

```text
engine_1_<UTC日時>_<UUID>.csv
```

日時・UUIDに加え、排他的な新規作成モードを使い、既存ファイルを上書きしない。
パラメーター化されたケースも、pytestのnode IDごとに1行で記録する。

| 列 | 内容 |
| --- | --- |
| `engine` | 選択した条件 `1` / `2` / `3` |
| `test_id` | pytestが発行したnode ID（パラメーター識別子を含む） |
| `outcome` | `passed` / `failed` / `skipped` / `error` |
| `duration_sec` | pytest標準レポートのsetup・call・teardownの所要時間の合計（秒） |

テスト本体の失敗は `failed`、準備・後処理の失敗は `error`。
両方ある場合は `error` を優先する。skipは `skipped` とする。
未実装の `create_engine()` がfixtureで例外を送出した場合は `error`、テスト本体で送出した場合は `failed` になり、pytestも非ゼロで終了する。
未実装をskipやxfailにはしない。

各ケースのteardownレポート受信時に行を書き込み・flushする。
`--collect-only` ではCSVを作らない。引数エラー・収集エラーはテストケースの実行結果ではないため、CSV行の対象外。
プロセスの強制終了や、環境によってプロセス終了方式となるタイムアウトでは、実行中ケースの記録・後処理は保証できない。既に書き込んだ行は残る。
独自ランナー、集計、スコアリング、グラフ生成は行わない。

## 仕様上の観測限界

- 登録が返る前に実行が始まってもよいため、PENDINGの観測を必須にしない。未完了結果取得時にPENDINGであれば、その状態の契約も検証される。
- 初期PENDINGを経由した事実や、APIの照会間に生じた短時間の状態遷移は外部から完全には検証できない。
- 一覧・件数APIがないため、非Callable拒否時の「登録件数が増えない」は直接検証できない。
- CallableだがAwaitableを返さない入力のエラー契約は未定義のため、期待結果を追加しない。
- キャンセル、リトライ、永続化、CPU並列高速化は対象外。
- `interface.py` のファイル不変性は動作テストの対象外。契約ファイルを変更しないことは別途差分で確認する。
- 3実装が未完成の間は、実エンジンでの成功・失敗・並行性の適合確認は完了しない。
