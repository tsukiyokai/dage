# dage.py Modularization Design

## Purpose

将1898行的单体 `dage.py` 拆分为职责清晰的模块包，使每个文件可独立阅读、测试和修改。
拆分后保持完全相同的功能和CLI行为，不引入新特性。

## Approach Selection

### A. Package with focused modules (recommended)

12个文件（含`__init__`/`__main__`），平均~175行/模块。
沿代码中已有的逻辑边界切割，依赖单向流动。

Trade-off: 文件数量中等，但每个模块都能在一屏内读完，职责无歧义。

### B. Fewer, larger modules (6 files)

将 executor+engine+git+replan 合并为一个~800行的 `core.py`。
Trade-off: 文件更少，但 core.py 会重新变成"什么都往里塞"的杂物间。

### C. Feature-oriented (6 files)

按功能域分（dag、runners、ai、ui），每个模块混合数据和逻辑。
Trade-off: 直觉上好找，但模块内聚性差，修改时容易牵连无关代码。

选择A。原因：dage.py内部已经有清晰的section分隔（数据结构 / YAML加载 / 调度 / 执行 / git / replan / 引擎 / TUI / prompt / plan / CLI），沿这些边界切割是阻力最小的路径，每个模块的职责用一句话就能说清。

## Architecture

### Directory Layout

```
dage/                      # Python package
├── __init__.py            #  ~20 lines   version + public exports
├── __main__.py            #  ~5  lines   python -m dage entry
├── models.py              #  ~90 lines   enums, dataclasses, constants
├── workflow.py            # ~160 lines   YAML load/build/validate, interpolate, scheduling
├── executor.py            # ~290 lines   process mgmt, shell/claude runners, skills
├── git_ops.py             # ~165 lines   worktree merge/prune, auto-commit
├── replan.py              # ~330 lines   adaptive replanning
├── engine.py              # ~280 lines   DAG execution loop, gate/resume/persistence
├── tui.py                 # ~175 lines   rich TUI display
├── prompts.py             # ~250 lines   all prompt template strings
├── planner.py             # ~250 lines   AI four-phase plan generation
└── cli.py                 # ~140 lines   argparse commands, main()

(root)
├── dage.py                # ~5  lines    backward-compat shim (see below)
├── examples/
├── tests/
└── pyproject.toml         # optional, for pip install entry point
```

### Backward Compatibility

Python不允许同目录下同时存在 `dage.py` 和 `dage/` 作为可导入模块（文件优先于目录）。
解决方案：保留根目录的 `dage.py` 作为纯shim，内容仅为：

```python
#!/usr/bin/env python3
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from dage.cli import main
main()
```

关键点：shim在 `sys.path` 插入自身目录后，`import dage` 会解析到 `dage/` 包而非shim自身（因为此时Python已经在执行shim，不会重新导入它）。实际上更安全的做法是：将shim命名为不同的文件（如保留原名但内容为纯转发），或确认Python的导入行为。

更简洁的替代方案：直接删除根目录 `dage.py`，在 `pyproject.toml` 中声明entry point：

```toml
[project.scripts]
dage = "dage.cli:main"
```

用户通过 `pip install -e .` 安装后即可直接运行 `dage` 命令。同时 `python -m dage` 通过 `__main__.py` 也始终可用。

设计决策：采用删除shim + pyproject.toml entry point方案。这是Python打包的标准做法，比维护一个容易出问题的shim更可靠。

## Components

### 1. models.py — Data Foundation

职责：定义所有共享数据类型，无业务逻辑。

内容：
- `Role` enum (CONTEXT, PRODUCE, GATE, EVALUATE, GC, META)
- `NodeType` enum (CLAUDE, SHELL)
- `Status` enum (PENDING, RUNNING, SUCCESS, FAILED, SKIPPED)
- `Node` dataclass (name, type, role, deps, prompt, cmd, condition, max_runs, worktree, timeout, retry, adaptive, skills)
- `NodeResult` dataclass (status, output, duration, retries, cost)
- Constants: `_ROLE_MAX_RUNS`, `_ANSI_COLORS`, `_SKILL_SEARCH_PATHS`

依赖：无（纯标准库）。

### 2. workflow.py — DAG Definition Layer

职责：YAML加载、节点构建、DAG验证、变量插值、拓扑排序和调度。

内容：
- `load_workflow(path)` — YAML文件加载
- `_build_one_node(name, spec, defaults)` — 单节点构建 + max_runs auto-cap
- `build_nodes(workflow)` — 批量构建
- `validate_workflow(nodes)` — 依赖检查 + 环路检测
- `_resolve_path(path, context)` — 点号路径解析 `${nodes.X.output}`
- `interpolate(template, context)` — 模板渲染
- `topo_layers(nodes)` — 分层拓扑排序
- `next_runnable(nodes, results, blocked)` — 动态可执行节点计算
- `find_blocked(nodes, results, gate_name)` — gate失败后下游传播
- `_extract_yaml(text)` — 从AI输出中提取YAML（被planner和replan共用）

模块级可变状态：`_max_output`（workflow级output截断上限）。
通过 `set_max_output(n)` 函数设置，避免直接暴露全局变量。

依赖：models

### 3. executor.py — Process Execution

职责：管理子进程生命周期，执行shell和claude节点。

内容：
- `_active_procs` / `_active_procs_lock` — 进程追踪（模块级状态）
- `kill_active_procs()` — SIGTERM处理器调用
- `_run_streamed(name, cmd, shell, cwd, timeout)` — 核心流式执行器
- `_load_skills(skill_names)` — 技能文件加载
- `run_shell(node, run_dir, cwd)` — shell节点执行
- `run_claude(node, run_dir, cwd, ...)` — ccx执行（构建命令、读取notes）
- `execute_node(node, run_dir, cwd, context, ...)` — 带重试的执行包装
- `call_claude(prompt, timeout)` — 单次claude CLI调用（plan/replan/autofix共用）
- `_parse_duration(s)` — 时间字符串解析

依赖：models, workflow (interpolate), prompts (_META_STYLE)

### 4. git_ops.py — Git Operations

职责：worktree生命周期管理和自动提交。

内容：
- `_merge_single_worktree(node_name, worktree_path, main_dir)` — 单个worktree合并
- `merge_worktrees(nodes, results, run_dir, main_dir)` — 批量合并调度
- `prune_worktrees(base_dir)` — 清理已合并的worktree
- `auto_commit(gate_name, upstream_names, main_dir, push)` — gate通过后自动commit+push
- `setup_worktree(node_name, base_dir)` — 创建worktree目录和分支

依赖：models, executor (_run_streamed，用于git命令)

### 5. replan.py — Adaptive Replanning

职责：运行时DAG动态修改。

内容：
- `detect_replan(nodes, results)` — 扫描adaptive节点output中的[REPLAN:]信号
- `call_replanner(nodes, results, trigger, reason, config)` — 调用claude生成replan YAML
- `apply_replan(nodes, results, replan_result, defaults)` — 应用replan（添加/删除节点 + 验证）
- `handle_replan(nodes, results, trigger, reason, config, run_dir)` — 完整replan流程（检测→生成→确认→应用）
- `_confirm_replan(replan_result)` — 交互式用户确认

依赖：models, workflow (validate, build, _extract_yaml), executor (call_claude), prompts (_REPLAN_PROMPT)

### 6. engine.py — DAG Execution Loop

职责：编排整个DAG的执行，协调所有子系统。

这是最核心的模块，将各组件粘合在一起。

内容：
- `run_dag(nodes, workflow_config, run_dir, ...)` — 主循环（ThreadPoolExecutor + wait(FIRST_COMPLETED)）
- `_handle_gate_fail(gate_name, nodes, results, blocked, ...)` — gate失败处理（autofix + 下游阻塞）
- `_autofix_gate(gate_name, gate_cmd, error_output, ...)` — 调用claude修复gate
- `_annotate_design_docs(gate_name, doc_path, ...)` — gate成功后注释设计文档
- `_load_previous_run(run_dir)` — 从上次运行恢复状态（--from）
- `save_results(results, run_dir)` — 持久化results.json
- `print_summary(nodes, results, duration)` — 执行摘要输出
- `_should_skip(node, context)` — condition表达式求值

依赖：models, workflow, executor, git_ops, replan, tui, prompts

### 7. tui.py — Terminal UI

职责：基于rich的实时DAG状态面板。

内容：
- `_HAS_RICH` — rich库可用性检测
- `DageDisplay` class — Live面板管理
  - `update_node(name, status, detail)` — 更新节点状态
  - `log(message)` — 追加日志行
  - `refresh()` — 重绘面板
  - `_build_dag_panel()` — DAG状态面板构建
  - `_build_log_panel()` — 日志面板构建
- `get_display(enabled) -> DageDisplay | None` — 工厂函数
- `log_line(name, line, color)` — 节点输出着色日志（fallback到print）

依赖：models (Status enum)。rich为可选依赖。

### 8. prompts.py — Prompt Templates

职责：存放所有AI prompt模板字符串。纯数据，无逻辑。

内容（全部为模块级字符串常量）：
- `META_STYLE` — 猫娘+雌小鬼风格注入
- `DAGE_KNOWLEDGE` — dage框架知识（注入给replanner/planner）
- `MATURE_PROMPT` — Phase 1: 原始想法成熟化
- `PLAN_DOC_PROMPT` — Phase 2: 设计拆分为工作流
- `BRAINSTORM_PROMPT` — Phase 3: 工作流映射DAG
- `PLAN_PROMPT` — Phase 4: 生成最终YAML
- `REPLAN_PROMPT` — 自适应重规划
- `AUTOFIX_PROMPT` — Gate失败自动修复
- `ANNOTATE_PROMPT` — 设计文档注释

命名变更：去掉前导下划线（不再是模块私有），如 `_META_STYLE` → `META_STYLE`。

依赖：无。

### 9. planner.py — AI Plan Generation

职责：四阶段AI工作流生成。

内容：
- `generate_plan(description, output_path, skills, run_after)` — 四阶段生成主流程
- `_run_phase(phase_num, prompt, timeout)` — 单阶段执行
- `_inject_skills(prompt, skill_names)` — 技能知识注入

依赖：models, workflow (_extract_yaml), executor (call_claude), prompts

### 10. cli.py — Command-Line Interface

职责：argparse定义、命令分发、main入口。

内容：
- `build_parser()` — argparse配置
- `cmd_run(args)` — `dage run` 命令
- `cmd_validate(args)` — `dage validate` 命令
- `cmd_status(args)` — `dage status` 命令
- `cmd_plan(args)` — `dage plan` 命令
- `main()` — 入口函数

依赖：workflow, engine, planner, tui

## Data Flow

```
                    YAML file
                       │
                  load_workflow()          ← workflow.py
                       │
                  build_nodes()            ← workflow.py
                       │
                validate_workflow()        ← workflow.py
                       │
                   run_dag()               ← engine.py
                       │
            ┌──────────┼──────────┐
            │          │          │
      next_runnable() ...    tui.refresh()
            │                     ← tui.py
      ┌─────┴─────┐
      │            │
  execute_node()  ...              ← executor.py
      │
  ┌───┴───┐
  │       │
shell   claude                     ← executor.py
  │       │
  └───┬───┘
      │
  gate pass? ──no──→ _handle_gate_fail()  ← engine.py
      │                    │
      │              find_blocked()        ← workflow.py
      │              _autofix_gate()       ← engine.py + executor.py
      │
  adaptive? ──yes─→ handle_replan()       ← replan.py
      │                    │
      │              call_replanner()      ← replan.py + executor.py
      │              apply_replan()        ← replan.py + workflow.py
      │
  worktree? ──yes─→ merge_worktrees()     ← git_ops.py
      │
  auto_commit? ───→ auto_commit()         ← git_ops.py
      │
  save_results()                           ← engine.py
  print_summary()                          ← engine.py
```

## Dependency Graph (modules)

```
prompts          models            (no deps)
   │               │
   │          workflow.py
   │          │    │    │
   │     executor  │    │
   │     │    │    │    │
   │  git_ops │    │    │
   │     │    │    │    │
   │     replan────┘    │
   │     │              │
   └──engine────────────┘
        │
       tui
        │
       cli
```

单向依赖，无环路。engine是汇聚点，将所有子系统粘合。

## Global Mutable State Handling

当前代码有以下全局可变状态，模块化时需要明确归属：

| State | Current | After | Rationale |
|-------|---------|-------|-----------|
| `_max_output` | dage.py全局 | workflow.py模块级 + setter函数 | 被interpolate使用，engine在run_dag开头设置 |
| `_active_procs` | dage.py全局 | executor.py模块级 | 仅被_run_streamed和signal handler使用 |
| `_display` | dage.py全局 | tui.py模块级 | 仅被TUI函数使用 |
| `_HAS_RICH` | dage.py全局 | tui.py模块级 | 仅被TUI使用 |

原则：全局可变状态留在使用它的模块内，通过函数接口访问，不跨模块直接读写。

## Error Handling

- 各模块用异常（ValueError, RuntimeError）报告错误
- `sys.exit()` 仅在 cli.py 中使用
- executor.py 中的进程错误封装为 NodeResult(status=FAILED)

## Testing

现有测试为YAML集成测试（test_parallel.yaml等），不受模块化影响。
模块化后，每个模块可独立编写单元测试（尤其是workflow.py的纯函数）。

建议后续添加：
- `tests/test_workflow.py` — interpolate、topo_layers、find_blocked的单元测试
- `tests/test_extract_yaml.py` — YAML提取的边界用例

## Migration Steps

1. 创建 `dage/` 目录和 `__init__.py`
2. 从 dage.py 提取 models.py（纯数据，无需改动）
3. 提取 prompts.py（纯字符串常量）
4. 提取 workflow.py（load/build/validate/interpolate/scheduling/extract_yaml）
5. 提取 tui.py（DageDisplay + 日志函数）
6. 提取 executor.py（进程管理 + 执行器）
7. 提取 git_ops.py（worktree + auto-commit）
8. 提取 replan.py（自适应重规划）
9. 提取 planner.py（AI plan生成）
10. 提取 engine.py（DAG主循环 + gate处理）
11. 提取 cli.py（argparse + main）
12. 创建 `__main__.py`
13. 删除原 `dage.py`（或替换为shim）
14. 添加 `pyproject.toml` 声明entry point
15. 运行所有现有YAML测试验证功能不变

顺序原则：先提取无依赖的底层模块，逐层向上，最后提取依赖最多的engine和cli。每提取一个模块后立即运行测试验证。
