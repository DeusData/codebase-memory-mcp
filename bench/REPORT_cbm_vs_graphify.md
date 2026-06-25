# codebase-memory (cbm) vs graphify — 性能比較評価レポート

> 対象リポジトリ: `codebase-memory-mcp-pro` 自身（C コア）／ commit `3ff76d2`
> 実施日: 2026-06-25 ／ 環境: macOS (Darwin 25.5.0), Apple Silicon, 128GB RAM
> 生データ・再現スクリプト: [bench/cbm-vs-graphify/](cbm-vs-graphify/)

---

## 0. エグゼクティブサマリー（結論先出し）

**比較した2ツールの中では、コードベース解析という用途において cbm が全3層（構造抽出精度・Q&A有効性・運用コスト）で graphify を上回った。** ただし「素の grep/Read で探索する有能なエージェント（Explorer）」を基準線に置くと、本コーパス（中規模・純Cの187K行）では **cbm と Explorer がほぼ互角**で、グラフの優位は**意味/横断検索(D5)**に集中した。

| 層 | 指標 | cbm | graphify-AST | Explorer(grep) | 勝者 |
|---|---|---|---|---|---|
| **L1 構造精度** | ノード種別 / dup | 11種 / 0 | 1種(フラット) | — | **cbm** |
| | 関数定義 recall | 0.997 | 0.999 | — | 互角 |
| | include 依存エッジ | **326** | 68 | — | **cbm (4.8倍)** |
| **L2 Q&A** | 総合スコア(0–4) | **3.48** | 2.60 | 3.48 (調整後3.95) | cbm ≈ Explorer ≫ graphify |
| | 意味/横断 D5 | **3.8** | 3.4 | 2.2 | **cbm**（グラフ勢が grep に勝つ唯一の次元） |
| **L3 運用** | cold build (中央値) | **2.44s** | 4.21s | 0（不要） | **cbm (1.7倍速)** |
| | クエリ遅延 (中央値) | **40ms** | 126ms | — | **cbm (3.2倍速)** |
| | インデックス時 API コスト | 0 | 0 | 0 | 互角 |

**一言で:** *コード特化・ローカル・構造クエリの cbm が、汎用グラフ化ツールの graphify をコード用途で明確に上回る。graphify の真価は純コードでは発揮されない（後述）。グラフを入れるべき主因は「意味検索」と「大規模コードでの速度」。*

---

## 1. なぜこの設計か（評価の正確性・公平性）

2ツールは**設計思想が根本的に異なる**ため、単純な「回答の良さ」比較ではツール性能と LLM 性能が混ざり不正確になる。これを避けるため**3層に分けて測定**した。

- **cbm** = コード特化 MCP サーバー。AST + 9言語ハイブリッド LSP。インデックスは**完全ローカル（LLM不使用、vendored nomic 埋め込み）**。openCypher で問い合わせ。
- **graphify** = 汎用知識グラフ化ツール（コード/文書/論文/画像/動画）。AST 抽出（無料・決定論的）＋ **LLM 意味エッジ生成**。`query/path/explain` で問い合わせ。

### 公平性の原則（Threats to Validity 対応）
1. **同一コーパス**: C コア347ファイル/187K行を `eval-corpus/` に固定し両ツールに与えた。vendored（SQLite/mimalloc 等）と tree-sitter 文法は「製品のロジックでないパーサ群」のため除外。
2. **cold ビルド**: キャッシュを毎回削除。時間は3回の中央値。
3. **L1 は LLM 不使用**: `ctags`＋純Python の独立オラクルで機械検証 → 最も反論しにくい客観バックボーン。
4. **graphify を2モードで測定**: AST-only（cbm と対等な無料条件）と フル意味抽出（LLM、後述）を分離。
5. **盲検 LLM 審査**: 回答を匿名化・スロットをローテーション。**審査2パスは完全一致**（不一致ゼロ）で高信頼。
6. **質問はオラクル種で生成**: 対象シンボル/ファイルは ctags/git から機械抽出（どちらのツール出力からも作らない＝home-field 排除）。Sillito et al. の開発者質問分類 D1–D5 に紐付け。
7. **基準線 Explorer(grep/Read)** を併走させ「グラフは素の grep に勝つのか」を可視化。

---

## 2. Layer 1 — 構造抽出精度（決定論的・LLM不使用）

独立オラクル: `ctags -x`（定義集合, pattern 分類）＋ 純Python の `#include` 走査。

| 指標 | cbm | graphify-AST | 解釈 |
|---|---:|---:|---|
| ノード | 4,595 | 4,077 | 同等規模 |
| エッジ | 16,924 | 14,516 | 同等規模 |
| **ノード種別** | **11**（Function/Macro/File/Module/Field/Variable/Class/Folder/Route…） | **1**（全"code"フラット） | cbm が型付き・リッチ |
| エッジ種別 | USAGE/DEFINES/CALLS/WRITES/IMPORTS/**SIMILAR_TO**/**SEMANTICALLY_RELATED**/… | calls/references/contains/imports | cbm に意味エッジ（ローカル埋め込み由来） |
| dup_nodes | **0** | — | cbm の既知バグ(M2)は解消済 |
| **関数定義 recall**（gold=ctags, N=2281） | **0.997** | **0.999** | **互角・両者ほぼ完璧** |
| 関数定義 precision | 0.74 | 0.74 | 両者同値。※低いのは**ctagsの取りこぼし**（`c_lsp_init`等 static 関数を ctags が見逃し）で、両ツールはこれらを正しく抽出。tool 誤りではない |
| **include 依存エッジ**（gold=対コーパス内 #include, N=692） | **326** (recall 0.256) | **68** (recall 0.039) | **cbm が4.8倍**。graphify の68本は大半が自己参照ノイズ（`ac.c→ac.c`）で、**C の #include 依存を実質モデル化していない** |

**呼び出しグラフ parity**（caller 数, grep はノイズ上限）: cbm は LSP 型解決で**保守的かつ安定**（arena 系で 20–31）。graphify は**ばらつき大**（同じ arena 系で 0–7、一方 `cbm_node_text` は217と過大）。

→ **L1 の結論: 関数発見は互角。だが型付きスキーマ・依存(#include)・呼び出しグラフの安定性で cbm が明確に優位。**

スポット検証: オラクル3件を手作業確認（例 `ac_build_trie` は実際に `ac.c:72` の static 関数）。cbm のトークンコスト=0（`token_vectors` はローカル nomic 埋め込み）、graphify-AST は "no LLM needed" をログで確認。

---

## 3. Layer 2 — Q&A 有効性（D1–D5・盲検2パス審査, 0–4）

各次元5問、計25問。3条件（cbm / graphify-AST / Explorer）が同一質問に回答 → オラクル正解に照らし盲検採点。

| 次元 | 内容 | cbm | graphify | Explorer | 備考 |
|---|---|---:|---:|---:|---|
| D1 | 定義列挙（ファイル内の関数一覧） | **4.0** | 3.2 | 3.2 | graphify は D1.2 で「ac.c はグラフに存在しない」と誤答(0点)←実際は存在。query の不安定さ |
| D2 | 呼び出し元（callers） | 3.1 | 2.8 | **4.0** | graphify の callgraph は**断片化・局所的**（クロスファイル呼び出しを欠落）。cbm は LSP で保守的・一部過少 |
| D3 | 定義位置＋署名取得 | **4.0** | 2.1 | **4.0** | **graphify は位置は当てるが verbatim 署名を出せない**（構造グラフのみ・ソース本文を保持しない）。cbm は `get_code_snippet` で実ソース |
| D4 | アーキテクチャ/構造 | 2.5 | 1.5 | **4.0** | graphify は D4.4/D4.5 で退化出力("test", 0点)。**ディレクトリ/層の意味を持たない**。cbm も hub特定(D4.2=1)・MCP層説明(D4.5=0)で弱点 |
| D5 | 意味/横断（概念で探す） | **3.8** | 3.4 | 2.2 | **グラフ勢が grep に勝つ唯一の次元**。意味エッジが効く |
| **総合** | | **3.48** | **2.60** | **3.48** | cbm ≈ Explorer ≫ graphify |

**重要な注記（公平性）:**
- Explorer は3問（D1.5/D5.4/D5.5）で**ハーネスのスキーマ失敗**（ツール能力でなく出力形式の retry 上限超過）により0点。これを除外した **Explorer の真の総合は 3.95**（D1–D4 は満点近く、弱点は D5 のみ 3.67）。
- 審査2パスの不一致は**ゼロ**（採点の再現性・信頼性が高い）。
- ツール呼び出し回数（労力）: cbm 4.0 / graphify 3.7 / Explorer 3.6（平均）。大差なし。

### 質的知見（graphify の具体的失敗）
- **ソース本文を返せない**（D3）: graphify は「グラフから再構成した」曖昧な説明を返すのみ。実署名 `static int ac_build_trie(CBMAutomaton *ac, …)` を出せない。cbm は実ソースを返す。
- **呼び出しグラフが局所的**（D2.1）: `cbm_arena_alloc` を「2つの別関数、呼び出し元は同一ファイル内のみ」と誤認。実際は400+のクロスファイル呼び出し。
- **アーキ/ディレクトリ意味の欠如**（D4.4/D4.5）: 「LSP リゾルバを含むディレクトリ」「MCP 層の構成」に query が答えられず退化。
- **query インターフェースの不安定さ**（D1.2）: グラフに存在するファイルを「存在しない」と誤答。BFS 近傍探索は**精密な絞り込みに不向き**（cbm の Cypher は `MATCH (f:File)-[:DEFINES]->(fn) ...` で正確）。

### 重要なメタ知見
本コーパス（187K行・純C）では、**有能なエージェント＋素の grep/Read が cbm と互角で graphify を上回った**。グラフの優位は **D5（意味/概念検索）に集中**。グラフ導入の主因は「(a) 意味検索、(b) grep が遅く高コストになる大規模コード、(c) 大規模での精密な構造クエリ」であり、中規模では grep が依然強力。

---

## 4. Layer 3 — 運用コスト（決定論的）

| 指標 | cbm | graphify-AST | graphify-FULL | 勝者 |
|---|---:|---:|---:|---|
| cold build 時間（中央値3回） | **2.44s** | 4.21s | — | **cbm (1.7倍速)** |
| クエリ遅延（中央値） | **40ms** | 126ms | — | **cbm (3.2倍速)** |
| 増分更新（1ファイル変更） | 1.47s(再index) | 3.24s(`update`) | — | **cbm** |
| ディスク容量 | .db **53MB**（埋め込み込） | graphify-out 33.6MB（graph.json 6.1MB + html 5MB + cache） | — | 用途次第（cbm は意味検索用埋め込みで増） |
| インデックス時 API トークン | **0**（ローカル nomic） | **0**（決定論的） | — | 互角 |

### graphify-FULL（LLM 意味抽出）の実体 — 重要知見
`graphify extract --backend ollama --mode deep` を純コードに実行すると、**LLM 意味抽出はスキップされ AST のみが出力された**（636ノード、graphify-AST と同一）。検証の結果:
- **graphify の LLM はコード–コード間エッジを増やさない**。コードの意味/INFERRED エッジは AST ヒューリスティック由来。LLM が効くのは①**コミュニティ命名**（GRAPH_REPORT の god node 名）と②**文書/論文/画像など横断ドメイン**（純Cには無し）。
- コミュニティ命名を有効化（`graphify label --backend ollama`, ローカル gemma4:26b）した結果、**サブセット16K行・27コミュニティの命名に 11.8分**。生成物は "Cypher Query Parsing" "LSH Indexing" "AST Profiling" 等の有用なラベルだが、**グラフ構造は AST と不変**で、検索精度(L1/L2)には寄与しない。

→ **L3 の結論: 構築・クエリ・増分すべて cbm が高速。graphify-FULL は純コードでは高コスト（11.8分/16K行）に見合う構造的便益が無い。**

---

## 5. バランス総合判定 と 用途別の勝者

### 総合（精度・速度・コスト・網羅性を加重）
比較対象の2ツールに限れば、**cbm が全層で graphify を上回る**（構造精度・Q&A・運用すべて）。graphify が cbm に肉薄するのは D5 意味検索(3.4 vs 3.8)と、query が成功した時の D1 のみ。

### 用途別の推奨
| 重視点 | 推奨 | 理由 |
|---|---|---|
| **正確さ・依存解析・構造クエリ** | **cbm** | 型付きスキーマ・#include 4.8倍・安定した callgraph・実ソース取得・Cypher の精密性 |
| **速度/常用の軽さ** | **cbm** | build 1.7倍速・クエリ 3.2倍速・増分も速い（graphify 比） |
| **意味/概念で探す（D5）** | **cbm**(3.8) ≈ graphify(3.4) | 両者とも grep(2.2) を上回る。グラフ導入の最大の動機 |
| **コード+文書+図表+論文の混在コーパス / 可視化 / コミュニティ命名 / Neo4j・Obsidian 出力** | **graphify** | 汎用グラフ化が本領。純コード Q&A では発揮されない強み |
| **ゼロセットアップで素早く1問** | **grep/エージェント** | index 不要で D1–D4 はほぼ満点。中規模なら十分 |
| **超大規模コード（数百万行〜）** | **cbm**（推測） | grep が遅く高コストになる領域。L1/L3 の速度・精密性が効く（本評価は中規模のため未実測） |

---

## 6. 妥当性への脅威（Threats to Validity）

- **カテゴリ差**: cbm はコード特化、graphify は汎用。本評価のスコープは**「この C リポジトリのコードベース Q&A」に限定**。graphify が想定する混在コーパス・可視化用途は測っていない（純コードでは不利）。
- **オラクル限界**: BSD ctags は universal-ctags より粒度が粗く（macro 検出57件など過少）、関数 precision の絶対値は過小評価。**relative な比較と recall(0.997+)** を主指標とした。include の resolve は basename 一致ベース。
- **回答生成は全条件 Claude エージェント**: 審査の自己選好は**両条件に対称**で勝敗には影響しない。審査は両ツールのビルド backend(gemma)と独立な Claude で実施。`gemma4:26b` による非Claude スポット審査は将来拡張余地。
- **Explorer のハーネス失敗3件**: スキーマ retry 上限超過（ツール能力でない）。raw 3.48 と調整後 3.95 を併記。
- **規模**: 187K 行は中規模。grep が強く出たのは規模要因が大きく、大規模では結論が cbm 寄りに振れる可能性。
- **graphify-FULL**: ローカル LLM コストのため**サブセット(16K行)で実測**。フルコーパスでの意味抽出は未実施（純コードでは構造便益が無いと判明したため）。

---

## 7. 再現方法

すべて [bench/cbm-vs-graphify/](cbm-vs-graphify/) に格納。
```
results/   summary.json, layer1_results.json, layer2_scores.json, layer2_full.json,
           layer3_results.json, oracle.json, qbank.jsonl, corpus_sha.txt, filelist.txt
scripts/   build_oracle.py    # 独立オラクル生成 (ctags+rg)
           compare_layer1.py  # L1 構造精度 (cbm/graphify vs oracle)
           generate_qbank.py  # D1-D5 質問バンク (オラクル種)
           qa_workflow.js     # L2 盲検 Q&A Workflow (3条件×25問×2審査)
           measure_layer3.py  # L3 運用コスト (cold build/latency/incremental)
           analyze_layer2.py  # L2 集計・診断
```
手順: ① C コアを `eval-corpus/` にコピー（vendored/grammars 除外）② `codebase-memory-mcp cli index_repository` と `graphify update` でビルド ③ `build_oracle.py`→`compare_layer1.py`（L1）④ `generate_qbank.py`→ `qa_workflow.js`（L2）⑤ `measure_layer3.py`（L3）。
ツール: cbm `dev`（`/Users/yf.jin/.local/bin/codebase-memory-mcp`）, graphify `0.8.43`。審査=Claude 盲検2パス。
