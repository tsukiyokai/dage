# dage

*從前，編排 AI agent 意味著寫膠水腳本、逐步看護、祈禱第三步不要在你睡著前崩潰。*

dage 是解藥：把工作流寫成一份 YAML DAG，按下 run，然後走開。節點按拓撲序執行，gate 失敗即短路，能並行的全部並行。你醒來時，只剩一份乾淨的日誌告訴你什麼成了、什麼沒成。

```
你                        dage                         AI agents
 │                          │                              │
 │  dage run workflow.yaml  │                              │
 │ ─────────────────────> │                              │
 │                          │  topo sort ─> layer 0        │
 │                          │  ┌─ scan ──────────────────> │ 讀代碼、寫筆記
 │                          │  └─ read_docs ─────────────> │ 讀文檔、寫摘要
 │                          │  gate_test (cargo test)       │
 │                          │  ┌─ impl_ir ───────────────> │ 寫 IR 類型
 │                          │  └─ impl_topo ─────────────> │ 寫拓撲模組
 │                          │  gate ─> auto-commit+push    │
 │                          │  ...                         │
 │  <── 完整報告 ──────────  │                              │
```

## 核心概念

```
workflow.yaml
│
├─ nodes:
│   ├─ scan        (claude/context)   ── 讀代碼庫結構
│   ├─ implement   (claude/produce)   ── 寫代碼
│   ├─ gate_test   (shell/gate)       ── cargo test
│   └─ report      (claude/meta)      ── 撰寫報告
│
├─ deps: [scan] ──────────────────────── 資料依賴，構成 DAG
├─ adaptive: true ────────────────────── 執行中可觸發 replan
└─ skills: [vibe-opt] ───────────────── 注入領域知識
```

兩種節點類型：`claude`（啟動 AI agent 經由 ccx）和 `shell`（跑命令）。同一層的節點自動並行，`gate` 失敗則跳過所有下游 —— 測試不過就不寫報告。

## 快速開始

```bash
# 從自然語言生成工作流
dage plan "分析代碼庫並重構認證模組"

# 執行
dage run workflow.yaml

# 中途失敗？從某個節點恢復
dage run workflow.yaml --from report
```

## 工作流 YAML

```yaml
description: "實現 Plan Compiler"

defaults:
  skills: [vibe-opt]          # 所有 claude 節點注入此 skill

auto_commit:
  push: true                  # gate 通過即 commit + push

nodes:
  scan:
    role: context
    max_runs: 1               # 讀文檔，單輪足夠
    prompt: |
      精讀設計文檔，提煉架構和關鍵類型。

  implement:
    deps: [scan]
    prompt: |                 # max_runs 預設 0（無限，completion signal 終止）
      實現功能。上游摘要：${nodes.scan.output}

  gate_test:
    role: gate
    deps: [implement]
    type: shell
    cmd: "cargo test"

  report:
    role: meta
    max_runs: 1
    deps: [gate_test]
    prompt: |
      撰寫總結。測試結果：${nodes.gate_test.status}
```

## 執行引擎

```
while True:
    layer = next_runnable(nodes, results, blocked)
    if not layer: break
    ┌──────────────────────────────────────────────┐
    │ Phase 1   condition filter                   │
    │ Phase 2   parallel execution (ThreadPool)    │
    │ Phase 2.5 worktree merge (git merge)         │
    │ Phase 3   gate propagation + autofix + commit │
    │ Phase 4   adaptive replan                    │
    └──────────────────────────────────────────────┘
    hot-reload: YAML 變更自動生效
```

不是靜態的 `for layer in layers` —— 是動態的 `while + next_runnable()`。replan 新增的節點在下一輪自動被拾起。

## 特性一覽

| 特性 | 說明 |
|------|------|
| 動態排程 | `next_runnable()` 每輪重算可執行節點 |
| 並行 worktree | 同層 claude 節點自動分配 git worktree，git merge 回主庫 |
| Gate 短路 | gate 失敗 → 所有下游標記 skipped |
| Gate Autofix | gate 失敗時自動啟動 claude 診斷修復，修好後重試 |
| Auto-commit | gate 通過即 `git add -A && commit`，可選 push |
| Adaptive Replan | `adaptive: true` 節點輸出 `[REPLAN: reason]` 觸發 AI 重規劃 |
| Replan 治理 | `mode: auto/confirm/log` 控制自治邊界，強制 justification |
| Skill 注入 | `skills: [name]` 經由 `--append-system-prompt` 注入到 `-p` 模式 |
| YAML 熱載入 | 運行中修改 YAML，下一輪自動生效 |
| TUI 面板 | rich 全螢幕：DAG 狀態面板 + 彩色日誌流 |
| `--from` 恢復 | 從指定節點恢復，跳過已完成節點 |
| 變數插值 | `${nodes.NAME.output}` / `${vars.X}` / `${run.summary}` |

## Adaptive Replan

```
step1 (adaptive: true)
  輸出: "分析完成 [REPLAN: 需要額外驗證步驟]"
     │
     ▼
  引擎偵測 [REPLAN: ...] ─> 呼叫 AI replanner
     │
     ▼
  replanner 返回:
    justification: "插入驗證節點確保信號檢測正確"
    remove: [step2]
    add:
      validate: {type: shell, deps: [step1], cmd: "make check"}
      step2:    {type: shell, deps: [validate], cmd: "make final"}
     │
     ▼
  mode: auto    → 直接套用
  mode: confirm → 暫停等人批准
  mode: log     → 只記錄不動作
```

## TUI

```
  scaffold │ 💾 Cargo.toml
  scaffold │ 💻 cargo build
  scaffold │ 💬 骨架搭建完成
    impl_ir │ 📖 plan.rs
    impl_ir │ 💬 PlanHeader needs repr(C)...
  impl_topo │ 💾 topo.rs
╭─────────────────── Planck v0.1 Phase A ───────────────────╮
│  L0  ✓ read_design 54s   ✓ read_plan 43s   ✓ scan 0s     │
│  L1  ✓ scaffold 3:12                                     │
│  L2  ✓ gate_build 2s                                     │
│  L3  ◐ impl_ir 5:12   ◐ impl_topo 3:08                   │
│  L4  ○ gate_ir_topo                                      │
│  L5  ○ impl_cost                                         │
│      ⋮  (8 more)                                         │
│                                                          │
│  ◐ 2 running   ✓ 5 success   ○ 12 pending               │
╰──────────────────────── 5/19 ── 10:14 ───────────────────╯
```

日誌在上滾動，面板固定在底部。節點名彩色編碼，右對齊 15 字元。全螢幕模式，0.5 秒刷新。

## 檔案管理

```
.dage/runs/{run_id}/
  original-nodes.json    # 運行起始節點快照
  results.json           # 最終結果
  replan-1.json          # replan 事件 {seq, added, removed, justification}
  replan-1-raw.yaml      # replanner 原始輸出
  {node}/ccx.log         # 每個節點的 ccx 日誌
  {node}.notes.md        # 節點產出（下游經由 ${nodes.NAME.output} 引用）
```

## 環境需求

Python 3.9+、PyYAML、[rich](https://github.com/Textualize/rich)、[ccx](https://github.com/tsukiyokai/dotfiles/blob/main/bin/ccx)。一台機器，一份 YAML，一條命令。

## Changelog

- 0.1 — DAG engine, shell/claude executors, gate, interpolation, `--from` resume
- 0.2 — intra-layer parallel execution (ThreadPoolExecutor)
- 0.3 — `dage plan`: natural language → workflow YAML
- 0.4 — adaptive replan + 兩層治理 (mode + justification)
- 0.5 — skill 注入 (`--append-system-prompt`)、auto-commit、worktree merge、TUI、hot-reload、autofix
