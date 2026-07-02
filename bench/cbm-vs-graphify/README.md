# cbm vs graphify — 評価アーティファクト

`codebase-memory` (cbm) と `graphify` をこのリポジトリの C コアに対して比較した評価の
生データと再現スクリプト。結論レポートは [../REPORT_cbm_vs_graphify.md](../REPORT_cbm_vs_graphify.md)。

## 評価設計（3層）
- **Layer 1 構造抽出精度** — LLM不使用。独立オラクル(ctags+#include走査)に対する
  関数定義 recall/precision・include エッジ・構造統計。
- **Layer 2 Q&A 有効性** — Sillito et al. の D1–D5 次元、25問。3条件
  (cbm / graphify-AST / Explorer=grep) が同一質問に回答し、盲検2パス LLM 審査。
- **Layer 3 運用コスト** — cold build 中央値×3・クエリ遅延・増分更新・容量・トークン。

## ファイル
| パス | 内容 |
|---|---|
| `results/summary.json` | 全層の要約（判定に使う数値） |
| `results/layer1_results.json` | L1 構造精度の詳細 |
| `results/layer2_scores.json` | L2 次元別・条件別スコア |
| `results/layer2_full.json` | L2 全回答＋審査根拠 |
| `results/layer3_results.json` | L3 運用コスト |
| `results/oracle.json` | 独立オラクル（定義・include） |
| `results/qbank.jsonl` | 質問バンク（グラウンドトゥルース付き） |
| `results/corpus_sha.txt` / `filelist.txt` | 対象 commit と 347 ファイル一覧 |
| `scripts/*.py`, `scripts/qa_workflow.js` | 再現スクリプト |

## 主要結論（要約）
- 比較した2ツールでは **cbm が全3層で graphify を上回る**（構造精度・Q&A・運用速度）。
- graphify の **LLM 意味抽出は純コードでは構造に寄与しない**（コミュニティ命名のみ、16K行で11.8分）。
- グラフの優位は **D5 意味検索に集中**。中規模・純コードの D1–D4 では素の grep も強力。

## 注記（環境変更）
graphify の ollama バックエンドを試すため、graphify の uv tool 環境に `openai` extra を追加した
(`uv tool install "graphifyy[openai]==0.8.43" --force`)。バージョン(0.8.43)は固定で破壊的変更なし。
