# dage.py Modularization — 跨轮次共享笔记

## Stream 1: Foundation Layer (DONE)

5个文件已创建并验证: `__init__.py`, `models.py`, `prompts.py`, `workflow.py`, `tui.py`

关键决策:
1. `extract_yaml`移除`nodes` key硬校验 — caller自行做语义检查
2. `print_status(run_dir)` 参数从repo_dir改为run_dir
3. `log()` 无`_display`时fallback到stderr
4. Python 3中`dage/`包优先于同名`dage.py`文件加载，import测试可直接在repo根目录运行

## Stream 2: Execution Layer — 执行计划

### 目标

从`dage.py`提取4个模块，构建在Stream 1之上。

### 文件清单与依赖

```
dage/executor.py  ← models, workflow.interpolate, prompts.META_STYLE, tui.{log, log_line}
dage/git_ops.py   ← models, executor._run_streamed, tui.log
dage/replan.py    ← models, workflow.{validate_workflow, _build_one_node, extract_yaml},
                     executor.call_claude, prompts.{REPLAN_PROMPT, DAGE_KNOWLEDGE},
                     tui.log, models.save_json
dage/planner.py   ← models, workflow.extract_yaml,
                     executor.{call_claude, _load_skills},
                     prompts.{PLAN_PROMPT, BRAINSTORM_PROMPT, MATURE_PROMPT, PLAN_DOC_PROMPT},
                     tui.log
```

### 执行步骤

- [x] 1. `dage/executor.py` — 12个函数/变量提取
  - `_active_procs`/`_active_procs_lock` (module-private)
  - `kill_active_procs` (去前缀), `register_signal_handlers` (新wrapper), `_sigterm_handler`
  - `_run_streamed`, `_SKILL_SEARCH_PATHS`, `_load_skills`
  - `run_shell`, `run_claude`, `_parse_timeout`
  - `execute_node` (引用META_STYLE从prompts, interpolate从workflow)
  - `call_claude` (去前缀, 原`_call_claude` L1594)
- [x] 2. `dage/git_ops.py` — 4个函数提取
  - `_merge_single_worktree` (保持私有)
  - `merge_worktrees` (去前缀), `prune_worktrees` (去前缀), `auto_commit` (去前缀)
  - 所有git命令通过`executor._run_streamed`执行
- [x] 3. `dage/replan.py` — 5个函数提取
  - `detect_replan`, `call_replanner`, `apply_replan`
  - `_format_replan_proposal`, `_confirm_replan`
  - call_replanner不验证`nodes` key (replan YAML是justification/remove/add)
- [x] 4. `dage/planner.py` — 2个函数提取
  - `generate_plan`, `_generate_yaml`
  - _generate_yaml需新增`nodes` key检查(原来由extract_yaml做，现在推给caller)
- [x] 5. 验证: import测试

### 重命名映射 (dage.py → 模块)

| 原名 | 新名 | 目标模块 |
|------|------|----------|
| `_kill_active_procs` | `kill_active_procs` | executor |
| `_call_claude` | `call_claude` | executor |
| `_merge_worktrees` | `merge_worktrees` | git_ops |
| `_prune_worktrees` | `prune_worktrees` | git_ops |
| `_auto_commit` | `auto_commit` | git_ops |
| `_log(...)` | `log(...)` | 全部模块(from tui import) |
| `_log_line(...)` | `log_line(...)` | executor(from tui import) |
| `_save_json(...)` | `save_json(...)` | replan(from models import) |
| `_extract_yaml(...)` | `extract_yaml(...)` | replan,planner(from workflow import) |
| `_META_STYLE` | `META_STYLE` | executor(from prompts import) |

### 风险点

1. `call_claude`的CLI flags必须完整保留: `--permission-mode bypassPermissions`, `--add-dir ~/.claude/skills`, `--add-dir /`
2. `planner._generate_yaml`必须新增`nodes` key检查，否则无效YAML会静默通过
3. `_run_streamed`被git_ops跨模块导入(convention-private但设计允许)

### 验证结果

```
import测试: 全部Stream 2符号可导入，无circular import
签名验证: 13个公开函数参数名+默认值与源码一致
关键flag: call_claude保留--permission-mode/bypassPermissions/--add-dir全部flags
语义检查: planner._generate_yaml新增nodes key验证，replan不检查nodes key
```

### 已完成的工作

- Stream 2全部4个文件已创建并验证
- executor.py: 12个函数/变量, ~210行
- git_ops.py: 4个函数, ~105行
- replan.py: 5个函数, ~120行
- planner.py: 2个函数, ~55行

### 独立验证 (2026-03-23)

逐函数比对 dage.py 原始代码 vs 提取模块，结论: 4个模块代码正确。

- executor.py: 12个函数/常量完全匹配 L235-519 + L1594-1609
- git_ops.py: 4个函数完全匹配 L521-636
- replan.py: 5个函数完全匹配 L691-892
- planner.py: 2个函数匹配 L1715-1757，`_generate_yaml`新增本地`nodes` key校验(设计要求)

环境注意: 需用 conda Python 3.13 运行 (`/Users/shanshan/miniconda3/bin/python`)。
系统 Python 3.9.6 缺 PyYAML 且不支持 `str | None` 语法 (PEP 604)。

### 当前阶段

Stream 2 DONE。

---

## Stream 3: Orchestration + Integration — 执行计划

### 目标

提取 engine/cli，完成包组装，删除 dage.py，全量验证行为不变。

### 文件清单与依赖

```
dage/engine.py  ← models.{Node, NodeResult, NodeType, Role, Status, save_json, node_to_dict}
                   workflow.{load_workflow, _build_one_node, build_nodes, validate_workflow,
                            interpolate, set_max_output, topo_layers, next_runnable, find_blocked}
                   executor.{execute_node, run_claude, run_shell, kill_active_procs,
                            register_signal_handlers}
                   git_ops.{merge_worktrees, prune_worktrees, auto_commit}
                   replan.{call_replanner, apply_replan, _format_replan_proposal, _confirm_replan}
                   tui.{log, set_display, get_display, DageDisplay, _HAS_RICH, print_summary}
                   prompts.{AUTOFIX_PROMPT, ANNOTATE_PROMPT}

dage/cli.py     ← workflow.{load_workflow, build_nodes, validate_workflow}
                   engine.{run_dag, _find_latest_run}
                   planner.{generate_plan, _generate_yaml}
                   executor._load_skills
                   tui.{log, print_plan, print_status}
```

### 执行步骤

- [ ] 1. `dage/engine.py` — 14个函数提取
  - `build_context`, `_build_summary` (L175-184)
  - `should_skip` (L423-433)
  - `_annotate_design_docs` (L462-489, 用ANNOTATE_PROMPT from prompts)
  - `_autofix_gate` (L653-689, 用AUTOFIX_PROMPT from prompts)
  - `_hot_reload` (L895-936, 用load_workflow/_build_one_node/validate_workflow from workflow)
  - `_reload_config` (L940-951)
  - `_handle_gate_fail` (L953-974)
  - `_handle_replan` (L976-1020)
  - `run_dag` (L1022-1186, 主循环)
  - `_load_resume_state` (L1188-1221)
  - `save_state` (L1240-1242)
  - `save_latest_link` (L1244-1246)
  - `_find_latest_run` (L1248-1255)
- [ ] 2. `dage/cli.py` — argparse + 4子命令 + main()
  - `cmd_status`: 先调`_find_latest_run(repo_dir)`, 再传run_dir给`tui.print_status`
  - `cmd_plan`: `--from-design`路径用`planner._generate_yaml`, skills用`executor._load_skills`
  - sys.exit()仅在此模块
- [ ] 3. `dage/__main__.py` + `pyproject.toml`
- [ ] 4. 验证: import + CLI help + integration tests
- [ ] 5. 删除 dage.py, 最终验证

### 重命名映射 (engine.py内)

| dage.py原名          | engine.py新名                         |
|----------------------|---------------------------------------|
| `_log(...)`          | `log(...)` (from tui)                 |
| `_save_json(...)`    | `save_json(...)` (from models)        |
| `_node_to_dict(...)` | `node_to_dict(...)` (from models)     |
| `_max_output = n`    | `set_max_output(n)` (from workflow)   |
| `_display`读写       | `get_display()/set_display()` (tui)   |
| `_HAS_RICH`          | `_HAS_RICH` (from tui)               |
| `_kill_active_procs` | `kill_active_procs` (from executor)   |
| `_sigterm_handler`   | `register_signal_handlers()` (executor, returns prev) |
| `_merge_worktrees`   | `merge_worktrees` (from git_ops)      |
| `_prune_worktrees`   | `prune_worktrees` (from git_ops)      |
| `_auto_commit`       | `auto_commit` (from git_ops)          |
| `_AUTOFIX_PROMPT`    | `AUTOFIX_PROMPT` (from prompts)       |
| `_ANNOTATE_PROMPT`   | `ANNOTATE_PROMPT` (from prompts)      |

### 风险点

1. `run_dag()`中`_max_output`是global变量赋值(L1035-1036, L1082)，需改为`set_max_output()`调用
2. `run_dag()`中`_display`直接读写(L1064,L1067-1068,L1117-1118,L1169-1171)，需改为`get_display()/set_display()`
3. `_handle_replan()`中L1017直接读`_display`检查是否存在，需改为`get_display()`
4. `cmd_status`接口变化: 原dage.py的`print_status(repo_dir)`内部resolve，新版需cli.py先resolve再传run_dir
5. circular import: engine imports几乎所有模块，但不会被下层模块反向import(仅cli依赖engine)

### 验证结果

```
10个模块全部无circular import导入成功
CLI --help正常 (dage, dage run, dage validate, dage status, dage plan)
test_parallel.yaml: 4节点并行执行, 全部success
test_parallel_gate.yaml: gate失败正确阻断下游
examples/test-shell.yaml: 8节点, context传递/gate pass+fail/condition skip全部正确
dage status: 正确读取最新run并显示
dage.py已删除
```

### 已完成的工作

- Stream 3全部文件已创建并验证
- engine.py: 14个函数, ~310行
- cli.py: build_parser + 4个cmd_ + main(), ~175行
- __main__.py: 2行
- pyproject.toml: entry point dage = "dage.cli:main"
- dage.py已从仓库根目录删除

### 修复

- pyproject.toml `build-backend`: `setuptools.backends._legacy:_Backend` → `setuptools.build_meta`
  (原值是不存在的路径，导致`pip install -e .`失败)

### 最终验证 (2026-03-23)

```
10个模块无circular import导入成功          ✓
python -m dage --help                     ✓
test_parallel.yaml: 4节点并行             ✓
test_parallel_gate.yaml: gate阻断下游     ✓
examples/test-shell.yaml: 8节点全覆盖     ✓
dage status: 读取最新run                  ✓
dage.py已删除                             ✓
pip install -e . && dage --help           ✓
```

### 当前阶段

全部3个Stream + pip install验证 = DONE。项目目标完成。

---

## Bug Fix: Gate退出码 (2026-03-23)

### 问题

`cli.py`的`cmd_run`和`cmd_plan`中，`any(r.status == Status.FAILED ...)`不区分gate节点和普通节点。
gate失败是正常控制流(条件不满足 → 阻塞下游)，不应导致`sys.exit(1)`。

### 修复

`dage/cli.py`:
- line 16: 增加`Role` import
- line 79 (`cmd_run`): 排除`Role.GATE`节点的失败
- line 178 (`cmd_plan`): 同上

### 验证

三个测试文件全部通过: `test_parallel.yaml`, `test_parallel_gate.yaml`, `test-shell.yaml`

### 环境注意

交互式shell中 `python` 可能被alias到系统3.9，需用 `python3` 或在非交互式shell中运行。
CI/脚本环境中 PATH 优先，正确解析到 miniconda 3.13，无此问题。

### 最终CI验证 (2026-03-23)

在干净非交互式shell (`bash --noprofile --norc`) 中执行完整gate命令:
```
set -e
python -m dage run tests/test_parallel.yaml          ✓ exit 0
python -m dage run tests/test_parallel_gate.yaml      ✓ exit 0 (gate fail正确排除)
python -m dage run examples/test-shell.yaml           ✓ exit 0 (gate fail正确排除)
echo "integration tests OK"                           ✓
```
全部通过。FINAL EXIT: 0。
