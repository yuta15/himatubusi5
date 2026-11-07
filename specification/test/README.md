# 共通契約テスト

`specification/README.md` v1.0と `specification/cancel.md` v1.1を、選択した実装の公開APIから検証する。
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

## v1.0のテストケース一覧（C18）

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

## v1.1のキャンセルテストケース一覧

`test_cancel_contract.py` にC01〜C17を追加する。パラメーター化後は29ケース、
既存44ケースと合わせて計73ケース。C18は `test_contract.py` の既存44ケースを変更せず実行する。
3条件とも同じテスト・補助クラス・期待結果を使用する。

| 計画 | 関数名の `test_` 以降 | ケース数 | 検証内容 |
| --- | --- | ---: | --- |
| C01 | `cancel_success_public_contract` | 1 | 追加Enum値・例外継承・非同期API、基準interfaceとのシグネチャ整合 |
| C02 | `cancel_success_pending_without_body` | 1 | PENDINGの観測と開始前キャンセル、本体未実行。観測できない場合は理由付きskip |
| C03 | `cancel_success_running_lifecycle` | 1 | 受付の非待機復帰、終了処理中の状態照会・結果取得・待機、キャンセル完了後のAPI |
| C04 | `cancel_success_duplicate_does_not_interrupt_cleanup` | 1 | 重複要求のFalse、終了処理への再通知防止と完遂 |
| C05 | `cancel_success_concurrent_requests` | 1 | 同時要求のTrueは1件だけ、通知も1回 |
| C06 | `cancel_success_terminal_request_rejected` | 3 | 成功・失敗・キャンセルの終端状態への要求拒否と結果保持 |
| C07 | `cancel_failure_unknown_id_preserves_engine` | 1 | 未登録IDの拒否と既存・後続ジョブの健全性 |
| C08 | `cancel_success_suppressed_cancellation` | 2 | 要求によるキャンセルを抑制した場合の成功・失敗、元の例外オブジェクトの保持 |
| C09 | `cancel_success_spontaneous_cancelled_error` | 3 | 要求なしのCancelledError、捕捉後の成功・失敗 |
| C10 | `cancel_success_completion_race` | 6 | 正常終了・異常終了と要求の競合、終端状態と結果の整合・不変性 |
| C11 | `cancel_success_other_jobs_progress` | 1 | 終了処理中も既存・後続ジョブが完了できる |
| C12 | `cancel_success_multiple_jobs_independent` | 1 | 複数キャンセルと終了処理・待機の独立性 |
| C13 | `wait_success_cancelled_multiple_waiters` | 1 | 要求前後に登録した全待機者の正常復帰 |
| C14 | `wait_success_cancelled_waiter_isolation` | 2 | 通常実行中・終了処理中の待機者取消しがジョブと他待機者に影響しない |
| C15 | `cancel_success_terminal_data_retained` | 2 | 要求あり・なしのキャンセル終端状態と各APIの反復操作、別ジョブ実行後の一貫性 |
| C16 | `cancel_success_independent_engines` | 1 | 他エンジンのIDへの要求が登録元に影響しない |
| C17 | `cancel_success_other_async_work_progresses` | 1 | 受付・完了待機と並行して別Taskが通知・終了処理解放できる |

追加ケースのみを実行する場合も、既存と同じ実装選択オプションを指定する。

```bash
uv run --group test python -m pytest test/test_cancel_contract.py --engine=1
uv run --group test python -m pytest test/test_cancel_contract.py --engine=2
uv run --group test python -m pytest test/test_cancel_contract.py --engine=3
```

キャンセル機能や追加定義が未実装の場合は失敗として記録する。
C02の観測不能によるskipとは区別する。

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

キャンセルテストは `cancel_support.py` の `CancellationJob` と `CancellationScope` を使う。
ジョブ本体の開始・終了許可に加えて、キャンセル受信・終了処理解放・終了処理完了をEventで区別する。
終了処理中の再通知は捕捉して回数を記録し、テストで不合格にする。
再通知を受けた不正実装でも、ゲート解放後にジョブを回収できるようにしている。
`cancel_jobs` fixtureの `finally` で終了処理ゲートを開き、既存の後処理で本体のゲートも開く。
失敗・skip時もテスト所有Taskと登録済みジョブを回収する。

C02だけは別Task生成によって開始前キャンセルの機会を失わないよう、
`submit()`・`get_status()`・`cancel()` を同じTaskから直接呼ぶ。
これらの直接呼び出しのハング検出にはテスト全体の15秒上限を使用する。
PENDINGを観測できなかった場合、または要求受付までに本体が始まった場合は理由付きskipとする。
受付後も本体が未開始だった場合は、完了を待ち、本体が最後まで未実行であることを検証する。

C10はキャンセル要求先行・完了確認後の要求・同じEventからの競合を、成功・失敗で検証する。
競合の勝者は固定せず、実際に確定した終端状態と結果の整合性、後続要求に対する不変性を確認する。
C14ではテスト所有の待機Taskだけを取り消す。この操作による `asyncio.CancelledError` は期待結果とし、
ジョブのキャンセル結果として `get_result()` が返すべき `JobCancelledError` と区別する。

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
- リトライ、永続化、CPU並列高速化は対象外。キャンセルはv1.1の追加ケースで検証する。
- `interface.py` のファイル不変性は動作テストの対象外。契約ファイルを変更しないことは別途差分で確認する。
- PENDINGの開始前キャンセルがskipとなった実装については、その受け入れ条件の検証は未完了。
  RUNNINGからのキャンセルはC03で独立して判定する。
- キャンセル機能が未実装の間は、実エンジンでのv1.1への適合確認は完了しない。
