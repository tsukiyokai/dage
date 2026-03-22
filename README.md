# dage

> 编排 AI agent 曾经意味着写胶水脚本、逐步看护、祈祷第三步不要在你睡着前崩溃。
>
> dage 是解药。

把工作流写成一份 YAML DAG，按下 run，走开。节点按拓扑序执行，gate 失败即短路，能并行的全部并行。你回来时只剩一份干净的日志，告诉你什么成了、什么没成。

```
  你                       dage                         AI agents
   │                         │                              │
   │  dage run workflow.yaml │                              │
   │ ───────────────────────>│                              │
   │                         │  topo sort -> layer 0        │
   │                         │  ├─ scan ───────────────────>│  读代码、写笔记
   │                         │  └─ read_docs ─────────────>│  读文档、写摘要
   │                         │  gate_test (cargo test)       │
   │                         │  ├─ impl_ir ───────────────>│  写 IR 类型
   │                         │  └─ impl_topo ─────────────>│  写拓扑模块
   │                         │  gate -> auto-commit + push  │
   │                         │  ...                         │
   │  <── 完整报告 ────────── │                              │
```

---

## 快速开始

```bash
dage plan "分析代码库并重构认证模块"           # 从自然语言生成工作流
dage run workflow.yaml                        # 执行
dage run workflow.yaml --from report          # 从某个节点恢复
```

## 工作原理

两种节点: `claude`(经由 [ccx] 启动 AI agent)和 `shell`(跑命令)。
同层节点自动并行，`gate` 失败则跳过所有下游。

```yaml
description: "实现 Plan Compiler"

defaults:
  skills: [vibe-opt]                  # 所有 claude 节点注入此 skill

auto_commit:
  push: true                          # gate 通过即 commit + push

nodes:
  scan:
    role: context
    max_runs: 1                        # 有界任务: 读文档，单轮
    prompt: |
      精读设计文档，提炼架构和关键类型。

  implement:
    deps: [scan]                       # max_runs 默认 0 (无限，completion signal 终止)
    prompt: |
      实现功能。上游摘要: ${nodes.scan.output}

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
      撰写总结。测试结果: ${nodes.gate_test.status}
```

## 执行引擎

```
while true:
    layer = next_runnable(nodes, results, blocked)
    if empty: break
    ┌─────────────────────────────────────────────────┐
    │  Phase 1    条件过滤                             │
    │  Phase 2    并行执行 (ThreadPool)                │
    │  Phase 2.5  worktree 合并 (git merge)            │
    │  Phase 3    gate 传播 + autofix + commit         │
    │  Phase 4    自适应 replan                        │
    └─────────────────────────────────────────────────┘
    热加载: YAML 变更下一轮自动生效
```

动态 `while + next_runnable()`，不是静态 `for layer in layers`。replan 新增的节点在下一轮自动被拾起。

## 特性

| 特性 | 说明 |
|------|------|
| 动态调度 | `next_runnable()` 每轮重算可执行节点 |
| 内联gate处理 | `wait(FIRST_COMPLETED)` 逐个处理，gate失败即时阻断下游 |
| 并发控制 | `max_concurrent: N` 限制同时运行的节点数 |
| 并行 worktree | 同层 claude 节点自动分配 git worktree，git merge 回主库 |
| worktree 复用 | 稳定命名 `dage-{node}`，跨 run 复用，免去重建开销 |
| 合并冲突处理 | worktree merge 冲突时 abort 并保留 worktree 供手动 resolve |
| gate 短路 | gate 失败 → 所有下游标记 skipped |
| gate autofix | 失败时自动启动 claude 诊断修复，修好后重试 |
| auto-commit | gate 通过即 `git add -A && commit`(排除 .dage/)，可选 push |
| 自适应 replan | `adaptive: true` 节点输出 `[REPLAN: reason]` 触发 AI 重规划 |
| replan 治理 | `mode: auto/confirm/log` 控制自治边界，强制 justification |
| skill 注入 | `skills: [name]` 经由 `--append-system-prompt` 注入到 `-p` 模式 |
| YAML 热加载 | 运行中修改 YAML，下一轮自动生效 |
| TUI 面板 | rich 全屏: DAG 状态面板 + 彩色日志流，自动滚动 |
| `--from` 恢复 | 从指定节点恢复，跳过已完成节点(含 output 还原) |
| 变量插值 | `${nodes.NAME.output}` / `${vars.X}` / `${run.summary}` |
| output 截断 | `max_output: N` 防止上游巨量 output 膨胀下游 prompt |
| 优雅关闭 | SIGTERM/Ctrl+C terminate 所有子进程，保存进度 |

## 自适应 Replan

```
step1 (adaptive: true)
  输出: "分析完成 [REPLAN: 需要额外验证步骤]"
      │
      ▼
  引擎检测 [REPLAN: ...] ──> 调用 AI replanner
      │
      ▼
  replanner 返回:
    justification: "插入验证节点确保信号检测正确"
    remove: [step2]
    add:
      validate: { type: shell, deps: [step1], cmd: "make check" }
      step2:    { type: shell, deps: [validate], cmd: "make final" }
      │
      ▼
  mode: auto    ── 直接套用
  mode: confirm ── 暂停等人批准
  mode: log     ── 只记录不动作
```

## TUI

```
       scaffold │ Cargo.toml created
       scaffold │ cargo build: 0 warnings
        impl_ir │ reading plan.rs...
        impl_ir │ PlanHeader repr(C) + serialize
      impl_topo │ writing topo.rs
╭──────────────────── Planck v0.1 Phase A ────────────────────╮
│  L0  ✓ read_design 54s  read_plan 43s  scan 0s              │
│  L1  ✓ scaffold 3:12                   ◐ impl_ir 5:12       │
│  L2  ✓ gate_build 2s                     Plan IR types...   │
│  L3  ◐ impl_ir 5:12  ◐ impl_topo 3:08                       │
│  L4  ○ gate_ir_topo                    ◐ impl_topo 3:08     │
│      ⋮  (9 more)                         hccs_8card()...    │
│                                                              │
│  ◐ 2 running   ✓ 5 success   ○ 12 pending                   │
╰─────────────────────────── 5/19 ── 10:14 ───────────────────╯
```

日志在上滚动，面板固定在底部。节点名彩色编码，右对齐。全屏模式，0.5 秒刷新。面板随进度自动滚动，已完成层折叠为一行。

## 文件布局

```
.dage/
  runs/{run_id}/
    original-nodes.json       运行起始节点快照
    results.json              最终结果
    replan-{n}.json           replan 事件 {seq, added, removed, justification}
    replan-{n}-raw.yaml       replanner 原始输出
    {node}/ccx.log            每个节点的 ccx 日志
    {node}.notes.md           节点产出 (下游经由 ${nodes.NAME.output} 引用)
  worktrees/
    dage-{node}/              稳定 worktree，跨 run 复用
  latest                      最近 run_id
```

## 路线图

```
v0.1  静态 DAG 执行                           done
v0.2  层内并行                                 done
v0.3  AI 生成工作流 (dage plan)                done
v0.4  自适应 replan + 两层治理                  done
v0.5  skill / commit / worktree / TUI / autofix    done
v0.5.1  引擎健壮性: 内联gate / 中断恢复 / 冲突处理  done
v0.6  目标驱动循环                                next
v0.7  多仓库编排                                  future
```

## TODO

| 优先 | 项目 | 说明 |
|------|------|------|
| ~~P0~~ | ~~worktree 合并冲突~~ | ~~done: 冲突时 abort + 保留 worktree~~ |
| ~~P0~~ | ~~中断恢复健壮性~~ | ~~done: SIGTERM handler + output 持久化~~ |
| P1 | 目标驱动循环 | `dage goal "描述" --verify "cmd"` 外层循环 |
| P1 | replan 范围约束 | 限制 replanner 只能加特定类型节点 |
| P1 | 成本追踪 | NodeResult.cost 字段已就位，待 ccx 暴露 usage 数据 |
| P2 | 多仓库编排 | 跨 repo 的 DAG |
| P2 | Web UI | 浏览器实时查看 |
| P2 | 通知 | Slack / 邮件通知 gate 失败或 workflow 完成 |
| P3 | DAG 可视化导出 | Mermaid / Graphviz |
| P3 | 历史分析 | 跨 run 的性能趋势 |

## 环境需求

Python 3.9+, PyYAML, [rich](可选 TUI), [ccx]。一台机器，一份 YAML，一条命令。

[ccx]: https://github.com/tsukiyokai/dotfiles/blob/main/bin/ccx
[rich]: https://github.com/Textualize/rich

## Changelog

- 0.1 — DAG 引擎, shell/claude 执行器, gate 短路, 变量插值, `--from` 恢复
- 0.2 — 层内并行执行 (ThreadPoolExecutor)
- 0.3 — `dage plan`: 自然语言生成工作流 (两阶段 brainstorm)
- 0.4 — 自适应 replan + 两层治理 (审批模式 + justification)
- 0.5 — skill 注入, auto-commit, worktree 合并/复用, gate autofix, TUI, 热加载
- 0.5.1 — 内联gate处理, 中断恢复(SIGTERM+output持久化), worktree冲突处理, max_concurrent, max_output截断, git add排除.dage
