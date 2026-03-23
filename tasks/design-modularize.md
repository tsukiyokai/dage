# dage.py Modularization Design (v2)

## Purpose

将1952行单体dage.py拆为`dage/`包(12个文件，平均~170行/模块)。沿代码中已有的section分隔切割，每个模块职责一句话说清。不引入新功能，CLI行为完全不变。

## Approach Selection

### A. Package with focused modules (recommended)

12个文件，依赖单向无环。沿代码的`# ====`分隔线切割。每个模块一屏读完。

Trade-off: 文件数量中等，但每个文件的边界不需要发明，代码中已经画好了。

### B. Fewer, larger modules (6 files)

将executor+engine+git_ops+replan合并为core.py(~800行)。文件更少，但core.py会重新变成杂物间，违背了拆分的初衷。

### C. Feature-oriented (6 files)

按功能域(dag/runners/ai/ui)分组。每个模块混合数据和逻辑，修改时容易牵连无关代码。

选择A。理由: 代码中已有15个`# ====`section，这些就是天然的模块边界。沿这些边界切割是阻力最小、正确性最高的路径。

## Architecture

### Directory Layout

```
dage/                         # Python package
  __init__.py       ~5  lines   version only, no bulk re-export
  __main__.py       ~5  lines   python -m dage
  models.py         ~90 lines   enums, dataclasses, data utils
  prompts.py        ~250 lines  all prompt template strings
  workflow.py       ~170 lines  YAML load/build/validate, interpolate, scheduling
  executor.py       ~200 lines  process mgmt, shell/claude runners
  git_ops.py        ~120 lines  worktree merge/prune, auto-commit
  replan.py         ~200 lines  adaptive replanning
  engine.py         ~300 lines  DAG execution loop + gate/resume/state
  tui.py            ~250 lines  rich TUI + log/log_line + print_*
  planner.py        ~150 lines  AI four-phase plan generation
  cli.py            ~160 lines  argparse, main()

(root)
  pyproject.toml               entry point: dage = "dage.cli:main"
  examples/
  tests/
  tasks/
```

### Backward Compatibility

Python不允许`dage.py`和`dage/`同级共存(file优先于package)。设计决策: 删除根目录`dage.py`，通过pyproject.toml声明entry point。用户通过`pip install -e .`安装后运行`dage`命令，或直接`python -m dage`。这是Python打包的标准做法。

### Dependency Graph

```
  models     prompts             (no deps, foundation)
     |          |
  workflow     |
     |    \    |
     tui    \  |                 (tui: owns log, depends on models + workflow)
     | \     \ |
  executor ---+                  (imports: models, workflow, prompts, tui.log)
     |
  git_ops                        (imports: models, executor, tui.log)
     |
  replan                         (imports: models, workflow, executor, prompts, tui.log)
     |
  planner                        (imports: models, workflow, executor, prompts, tui.log)
     |
  engine                         (imports: all above)
     |
  cli                            (imports: workflow, engine, planner, tui)
```

Single directional, acyclic. engine is the convergence point. tui is a widely-imported
leaf dependency (depends only on models + workflow).

## Module Specifications

### 1. models.py -- Data Foundation

Responsibility: all shared data types and data utility functions. Zero business logic.

Contents:
- `Role` enum (CONTEXT, PRODUCE, GATE, EVALUATE, GC, META)
- `NodeType` enum (CLAUDE, SHELL)
- `Status` enum (PENDING, RUNNING, SUCCESS, FAILED, SKIPPED)
- `Node` dataclass (name, type, role, deps, prompt, cmd, condition, max_runs, worktree, timeout, retry, adaptive, skills)
- `NodeResult` dataclass (status, output, duration, retries, cost) + `to_dict()` method
- `_ROLE_MAX_RUNS` dict -- Role semantic constant, used by workflow._build_one_node
- `node_to_dict(node)` -- Node serialization (current L1229-1238)
- `save_json(path, data)` -- JSON write utility (current L1225-1227), shared by engine and replan

NOT in models.py:
- `_ANSI_COLORS` -> tui.py (pure display constant)
- `_SKILL_SEARCH_PATHS` -> executor.py (only used by _load_skills)

Dependencies: none (pure stdlib).

### 2. prompts.py -- Prompt Templates

Responsibility: all AI prompt template strings. Pure data, no logic.

Contents (all module-level constants, strip leading underscore):
- `META_STYLE` -- cat-girl + brat style (current L491)
- `DAGE_KNOWLEDGE` -- dage framework knowledge (current L706-742)
- `ANNOTATE_PROMPT` -- design doc annotation (current L437)
- `AUTOFIX_PROMPT` -- gate failure fix (current L639)
- `REPLAN_PROMPT` -- adaptive replan (current L744), template references `{dage_knowledge}`
- `PLAN_PROMPT` -- Phase 4 YAML generation (current L1492), concatenates DAGE_KNOWLEDGE
- `BRAINSTORM_PROMPT` -- Phase 3 DAG mapping (current L1550), concatenates DAGE_KNOWLEDGE
- `MATURE_PROMPT` -- Phase 1 design maturation (current L1611)
- `PLAN_DOC_PROMPT` -- Phase 2 workflow decomposition (current L1662)

`{{`/`}}` escaping in `_DAGE_KNOWLEDGE` is handled at format() call sites.

Dependencies: none.

### 3. workflow.py -- DAG Definition Layer

Responsibility: YAML loading, node building, DAG validation, variable interpolation, topo sort, scheduling.

Contents:
- `load_workflow(path)` -- YAML file loading (L86)
- `_build_one_node(name, spec, defaults)` -- single node build + max_runs auto-cap (L96)
- `build_nodes(wf)` -- batch build (L120)
- `validate_workflow(nodes)` -- dependency check + cycle detection (L125)
- `_resolve_path(ctx, path)` -- dot-path resolution (L148)
- `interpolate(template, ctx)` -- `${...}` template rendering (L169)
- `topo_layers(nodes)` -- layered topo sort (L188)
- `next_runnable(nodes, results, blocked)` -- dynamic scheduling (L203)
- `find_blocked(nodes, failed_gate)` -- gate failure downstream propagation (L220)
- `extract_yaml(text)` -- extract YAML text from AI output (current `_extract_yaml` L1759)
  - CRITICAL FIX: remove `nodes` key hard-validation. Only do text extraction + YAML parse
    validity check. Semantic key validation is the caller's responsibility.
    Reason: replan YAML schema is `{justification, remove, add}`, no `nodes` key.
    Current code (commit 9f6903e) has `nodes` validation that would break replan.

Module-level state: `_max_output: int = 0` (workflow-level output truncation cap)
- Access via `set_max_output(n)` / `get_max_output()` functions
- Never expose variable directly to other modules

Dependencies: models

### 4. tui.py -- Terminal UI + Logging

Responsibility: rich-based TUI panel + global log interface + output formatting functions.

This is the biggest design change from v1: `_log` and `_log_line` belong to tui.py.

Rationale: `_log` (L1436) and `_log_line` (L256) both directly read/write `_display`.
`_log` is the most widely used function in dage (97 occurrences, across executor/git_ops/
replan/planner/engine). Putting `_log` in tui.py is the ONLY approach that requires no
callback injection or dependency inversion, because tui.py depends only on models+workflow,
so any module can safely `from dage.tui import log` without creating circular deps.

Contents:
- `_HAS_RICH` -- rich availability detection
- `_ANSI_COLORS` / `_ANSI_RESET` -- terminal color constants (current L253-254)
- `_display: DageDisplay | None` -- module-level TUI object (current L1434)
- `log(msg)` -- global log entry (current `_log` L1436, public naming)
- `log_line(name, line)` -- node output colored log (current `_log_line` L256)
- `DageDisplay` class -- Live panel (L1285-1432)
- `_LiveProxy` class -- Rich render proxy (L1280)
- `_STATUS_ICON` dict -- status icon mapping (L1272)
- `set_display(d)` -- called by engine to set `_display`
- `get_display()` -- called by engine to read `_display`
- `print_summary(results)` -- execution summary (L1442)
- `print_plan(nodes)` -- plan display (L1458)
- `print_status(run_dir)` -- run status (L1471)
  - Interface change: param is `run_dir` (resolved path), not `repo_dir`.
    Latest run resolution moves to cli.py/engine.py.

Dependencies: models, workflow (for `topo_layers` used in `DageDisplay._render()` L1326
and `print_plan` L1459)

### 5. executor.py -- Process Execution

Responsibility: manage subprocess lifecycle, execute shell and claude nodes.

Contents:
- `_active_procs` / `_active_procs_lock` -- process tracking (module-level private)
- `_SKILL_SEARCH_PATHS` -- skill search paths (current L312)
- `kill_active_procs()` -- terminate all processes (L240)
- `register_signal_handlers()` -- explicitly register SIGTERM handler, NOT at import time
  - `_sigterm_handler` calls `kill_active_procs()` then raises KeyboardInterrupt
- `_run_streamed(name, cmd, ...)` -- streaming subprocess execution (L269)
  - Calls `tui.log_line` for output logging
- `_load_skills(names)` -- skill file loading (L317)
  - Calls `tui.log` for logging
- `run_shell(node, cmd, cwd)` -- shell node execution (L338)
- `run_claude(node, prompt, run_dir, run_id, repo_dir, worktree)` -- ccx node execution (L358)
- `execute_node(node, ctx, run_dir, run_id, repo_dir, dry_run, worktree)` -- retry wrapper (L494)
  - Injects META_STYLE for claude nodes (from prompts)
- `call_claude(prompt, timeout, system)` -- lightweight claude CLI query (current `_call_claude` L1594)
  - Uses `claude` CLI (not ccx), for planner and replan
- `_parse_timeout(timeout)` -- time string parsing (L410)

Dependencies: models, workflow(interpolate), prompts(META_STYLE), tui(log, log_line)

### 6. git_ops.py -- Git Operations

Responsibility: worktree lifecycle and auto-commit.

Contents:
- `_merge_single_worktree(node_name, wt_name, repo_dir)` -- single worktree merge (L523)
- `merge_worktrees(auto_wt, repo_dir, run_id)` -- batch merge (L561)
- `prune_worktrees(repo_dir)` -- clean up merged worktrees (L566)
- `auto_commit(gate_name, nodes, repo_dir, push)` -- auto commit after gate pass (L605)

All git commands run via `executor._run_streamed`, logging via `tui.log`.

Dependencies: models(Node), executor(_run_streamed), tui(log)

### 7. replan.py -- Adaptive Replanning

Responsibility: runtime DAG dynamic modification.

Contents:
- `detect_replan(nodes, results, layer)` -- scan [REPLAN:] signals (L693)
- `call_replanner(wf, nodes, results, trigger, reason, seq, run_dir)` -- call claude for replan (L785)
  - Uses `executor.call_claude` and `workflow.extract_yaml`
  - Note: does NOT validate `nodes` key on extract_yaml result (replan YAML has no such key)
- `apply_replan(nodes, results, blocked, replan_result, defaults, run_dir, seq)` -- apply replan (L824)
- `_format_replan_proposal(replan_result)` -- format proposal (L864)
- `_confirm_replan()` -- interactive confirmation (L881)

Dependencies: models, workflow(validate_workflow, _build_one_node, extract_yaml),
executor(call_claude), prompts(REPLAN_PROMPT, DAGE_KNOWLEDGE), tui(log), models(save_json)

### 8. engine.py -- DAG Execution Loop

Responsibility: orchestrate DAG execution, coordinate all subsystems. The core module.

Contents:
- `build_context(wf, results, run_id)` -- build execution context (L175)
- `_build_summary(results)` -- result summary (L182)
- `should_skip(node, ctx)` -- condition expression evaluation (L423)
- `_reload_config(wf)` -- extract mutable config (L940)
- `_handle_gate_fail(name, nodes, results, blocked, ...)` -- gate failure handling (L953)
- `_autofix_gate(gate, gate_result, nodes, ctx, ...)` -- call claude to fix gate (L653)
- `_annotate_design_docs(wf, nodes, results, gate_name, ...)` -- annotate design docs after gate pass (L462)
- `_handle_replan(name, nodes, results, blocked, ...)` -- single node replan check (L976)
- `_hot_reload(yaml_path, nodes, results, blocked, wf)` -- YAML hot reload (L895)
- `run_dag(wf, nodes, repo_dir, dry_run, from_node)` -- main loop (L1022)
  - ThreadPoolExecutor + wait(FIRST_COMPLETED)
  - At start: set `_max_output` (via workflow.set_max_output), create DageDisplay (via tui)
  - Register/restore signal handler
- `_load_resume_state(nodes, from_node, repo_dir)` -- resume from last run (L1188)
- `save_state(run_dir, results)` -- persist results.json (L1240)
- `save_latest_link(repo_dir, run_id)` -- write latest file (L1244)
- `_find_latest_run(repo_dir)` -- find most recent run dir (L1248)

Dependencies: models, workflow, executor, git_ops, replan, tui, prompts

### 9. planner.py -- AI Plan Generation

Responsibility: four-phase AI workflow generation.

Contents:
- `generate_plan(description, skills)` -- four-phase main flow (L1731)
  - Phase 1: mature (MATURE_PROMPT)
  - Phase 2: decompose (PLAN_DOC_PROMPT)
  - Phase 3: map to DAG (BRAINSTORM_PROMPT)
  - Phase 4: generate YAML (_generate_yaml)
- `_generate_yaml(design, description, skill_ctx)` -- YAML generation (L1715)
  - Calls `workflow.extract_yaml` then additionally validates `nodes` key (planner-specific semantic)

Dependencies: models, workflow(extract_yaml), executor(call_claude, _load_skills),
prompts(*), tui(log)

### 10. cli.py -- Command-Line Interface

Responsibility: argparse definition, command dispatch, program entry. `sys.exit()` only here.

Contents:
- `build_parser()` -- argparse configuration
- `cmd_run(args)` -- `dage run`
- `cmd_validate(args)` -- `dage validate`
- `cmd_status(args)` -- `dage status`
  - Resolves latest run dir, passes to `tui.print_status(run_dir)`
- `cmd_plan(args)` -- `dage plan`
- `main()` -- entry function
  - Calls `executor.register_signal_handlers()` (not at import time)

Dependencies: workflow, engine, planner, tui

### 11. `__init__.py`

```python
"""dage -- DAG-based Agent Workflow Orchestrator."""
__version__ = "0.1.0"
```

Version only, no bulk re-export. Users import via explicit paths:
`from dage.workflow import load_workflow`

### 12. `__main__.py`

```python
from dage.cli import main
main()
```

## Cross-Cutting Concerns

### `_log` Resolution (core design decision)

`_log` is used 97 times across the entire codebase. Three placement options:

Option 1 -- tui.py owns `log` (CHOSEN):
- `_log`'s entire logic: "if display exists, call display.log; else print to stderr"
- It only reads `_display` (tui state), needs nothing from other modules
- All modules: `from dage.tui import log`
- tui depends only on models+workflow; any upper module importing tui creates no cycle

Option 2 -- callback injection:
- engine injects `_log` callback into executor/git_ops/replan at init
- Requires changing all function signatures or using module-level setters
- Over-engineered, complexity not justified

Option 3 -- separate logging.py:
- An extra 5-line file for one function
- Semantically `_log` IS a display concern; extracting it is purely to avoid "tui dependency"
- But tui only depends on models+workflow, being depended upon is harmless

Conclusion: `log` and `log_line` live in tui.py. Drop leading underscore (no longer module-private).

### `_extract_yaml` Flexibility

Current implementation (commit 9f6903e) hard-validates `nodes` key after YAML extraction.
This prevents replan from using this function (replan YAML top-level keys are
`justification/remove/add`).

Fix:
- `extract_yaml(text) -> str`: extract YAML text, verify it's valid YAML dict, no key validation
- Callers do semantic validation:
  - `_generate_yaml` (planner): check result dict contains `nodes`
  - `call_replanner` (replan): check result is dict (no specific key)

### Global Mutable State

| State              | Owner module | Access pattern                            |
|--------------------|-------------|-------------------------------------------|
| `_max_output`      | workflow.py | `set_max_output(n)` / `get_max_output()`  |
| `_active_procs` + lock | executor.py | module-private, via `kill_active_procs()` |
| `_display`         | tui.py      | `set_display(d)` / `get_display()` by engine |
| `_HAS_RICH`        | tui.py      | module-private                            |

Principle: state stays in the module that uses it, cross-module access via function interface.

### Signal Handler

Current `signal.signal(SIGTERM, _sigterm_handler)` is registered inside `run_dag()` (L1070),
not at import time. Modularized version preserves this:
- `executor.py` provides `register_signal_handlers()` function
- `engine.run_dag()` calls it at start, restores at end (current L1070/L1168 pattern)
- No side effects at module import time

## Data Flow

```
                   YAML file
                      |
                 load_workflow()          workflow.py
                      |
                 build_nodes()            workflow.py
                      |
               validate_workflow()        workflow.py
                      |
                  run_dag()               engine.py
                      |
           +----------+----------+
           |          |          |
     next_runnable() ...    tui.refresh()
           |                     tui.py
     +-----+-----+
     |            |
 execute_node()  ...              executor.py
     |
 +---+---+
 |       |
shell   ccx                      executor.py
 |       |
 +---+---+
     |
 gate pass?  --no--> _handle_gate_fail()    engine.py
     |                    |
     |              find_blocked()          workflow.py
     |              _autofix_gate()         engine.py + executor.py
     |
 adaptive?  --yes-> _handle_replan()        engine.py
     |                    |
     |              call_replanner()        replan.py + executor.py
     |              apply_replan()          replan.py + workflow.py
     |
 worktree?  --yes-> merge_worktrees()      git_ops.py
     |
 auto_commit? ----> auto_commit()          git_ops.py
     |
 save_state()                              engine.py
 print_summary()                           tui.py
```

## Error Handling

- Modules raise standard exceptions (ValueError, RuntimeError) for errors
- Process execution errors wrapped as `NodeResult(status=FAILED)`
- `sys.exit()` only in cli.py
- `_extract_yaml`'s ValueError caught by callers (planner/replan) for graceful degradation

## Testing

Existing YAML integration tests (tests/ directory) are unaffected by modularization.

Each module can be unit-tested independently after modularization. Priority:
1. `workflow.py` -- interpolate, topo_layers, find_blocked, extract_yaml (pure functions, easiest to test)
2. `models.py` -- node_to_dict, NodeResult.to_dict

Verification commands:
```bash
# package importable
python -c "from dage.cli import main; print('ok')"

# CLI help works
python -m dage --help

# all existing integration tests pass
python -m dage run tests/test_parallel.yaml
python -m dage run tests/test_parallel_gate.yaml
python -m dage run examples/test-shell.yaml

# entry point works
pip install -e . && dage --help
```

## Migration Steps

Order principle: extract dependency-free bottom modules first, work upward layer by layer.
Verify imports after each step.

1. `mkdir dage/` + create `__init__.py`
2. Extract models.py (pure data, zero changes)
3. Extract prompts.py (pure string constants, strip leading underscores)
4. Extract workflow.py (load/build/validate/interpolate/scheduling + extract_yaml fix)
5. Extract tui.py (DageDisplay + log + log_line + print_* + ANSI constants)
6. Extract executor.py (process mgmt + runners + call_claude + register_signal_handlers)
7. Extract git_ops.py (worktree + auto-commit)
8. Extract replan.py (adaptive replanning)
9. Extract planner.py (AI plan generation)
10. Extract engine.py (DAG main loop + gate handling + state persistence)
11. Extract cli.py (argparse + main)
12. Create `__main__.py`
13. Create pyproject.toml
14. Delete root dage.py
15. Run full verification

Parallelizable steps: 2+3 (both zero-dep), 7+8+9 (all depend on executor but not each other).

## Changes from v1

1. `_log` ownership: from "TBD/callback" to explicit tui.py. tui depends only on
   models+workflow, so any module importing it creates no cycle. This is the only
   approach requiring no function signature changes.

2. `_extract_yaml`: remove `nodes` key hard-validation, fixing latent bug where replan
   would fail (replan YAML format is `{justification, remove, add}`, no `nodes` key).
   Semantic validation pushed to callers.

3. Constant placement follows consumers: `_ANSI_COLORS` -> tui.py, `_SKILL_SEARCH_PATHS`
   -> executor.py. Only `_ROLE_MAX_RUNS` stays in models.py (describes Role semantic constraints).

4. tui.py dependency correction: tui imports workflow.topo_layers (used in DageDisplay._render
   and print_plan). v1 missed this dependency.

5. Backward compat: shim approach dropped. Delete dage.py, use pyproject.toml entry point.
