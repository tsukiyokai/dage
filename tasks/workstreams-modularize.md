# Work Streams: dage.py Modularization

Design source: `tasks/design-modularize.md`

## Overview

将1898行单体 `dage.py` 拆为 `dage/` 包（12个文件）。
3个work stream: 2个并行提取 + 1个集成收尾。

```
  Stream 1 (foundation)  ──┐
                            ├──→  Stream 3 (integration)
  Stream 2 (execution)   ──┘
```

Stream 1 和 2 完全并行——各自创建不同的 .py 文件，无文件级冲突。
Stream 3 在 1+2 合并后运行，完成最终组装和验证。

---

## Stream 1: Foundation Layer Extraction

Goal: 创建 `dage/` 包骨架，提取零依赖和仅依赖 models 的4个模块。

Dependencies: none (parallel with Stream 2)

### Scope

从 `dage.py` 提取以下模块，建立正确的跨模块 import：

| Target file        | Source sections in dage.py         | Key contents                                                  |
|--------------------|------------------------------------|---------------------------------------------------------------|
| `dage/__init__.py` | (new)                              | `__version__`, minimal public exports                         |
| `dage/models.py`   | Enums + Data Structures (L28-83)   | Role, NodeType, Status, Node, NodeResult, constants           |
| `dage/prompts.py`  | Prompt Templates (L434-520)        | All prompt template strings, rename `_X` to `X` (now public)  |
| `dage/workflow.py`  | YAML Loading thru Gate Propagation + _extract_yaml (L84-239, L1733-1757) | load/build/validate, interpolate, topo_layers, next_runnable, find_blocked, _extract_yaml, _max_output state |
| `dage/tui.py`      | TUI Display (L1250-1479)           | DageDisplay, _LiveProxy, log_line, print_summary, print_plan, print_status, _display state |

### Context

- `dage.py` is the sole source of truth; read it to extract exact code
- models.py and prompts.py have zero import dependencies (pure data)
- workflow.py imports only from models
- tui.py imports only from models; `rich` is an optional dependency (guard with try/except)
- Global mutable state `_max_output` goes to workflow.py with a `set_max_output()` setter
- Global mutable state `_display` and `_HAS_RICH` go to tui.py
- `_extract_yaml()` lives in workflow.py (shared by planner and replan via import)
- Keep `_ROLE_MAX_RUNS`, `_ANSI_COLORS`, `_SKILL_SEARCH_PATHS` constants in models.py

### Verification

```bash
python -c "
from dage.models import Node, Role, Status, NodeType, NodeResult
from dage.prompts import META_STYLE, REPLAN_PROMPT, PLAN_PROMPT
from dage.workflow import load_workflow, interpolate, topo_layers, next_runnable, find_blocked
from dage.tui import DageDisplay
print('foundation OK')
"
```

### Constraints

- Do NOT modify or delete the original `dage.py` (Stream 3 handles cutover)
- Do NOT create engine.py, cli.py, executor.py, git_ops.py, replan.py, planner.py (Stream 2/3 scope)
- Preserve all existing function signatures exactly
- Module-level `_max_output` in workflow.py must be accessible via `set_max_output(n)` / `get_max_output()` functions, not direct variable access from other modules
- `__init__.py` should be minimal (version + selective re-exports), not import everything

---

## Stream 2: Execution Layer Extraction

Goal: 提取执行、git操作、重规划、AI计划生成这4个中间层模块。

Dependencies: none (parallel with Stream 1)

### Scope

从 `dage.py` 提取以下模块：

| Target file         | Source sections in dage.py            | Key contents                                                        |
|---------------------|---------------------------------------|---------------------------------------------------------------------|
| `dage/executor.py`  | Executors (L238-419) + _parse_timeout | _active_procs, kill_active_procs, _run_streamed, run_shell, run_claude, execute_node, call_claude, _load_skills |
| `dage/git_ops.py`   | Worktree Merge + Gate Auto-commit (L521-643) | _merge_single_worktree, merge_worktrees, prune_worktrees, auto_commit, setup_worktree |
| `dage/replan.py`    | Adaptive Replanning (L683-884)        | detect_replan, call_replanner, apply_replan, handle_replan, _confirm_replan |
| `dage/planner.py`   | Plan Generation (L1480-1757, excluding _extract_yaml) | generate_plan, _run_phase, _inject_skills, _call_claude |

### Context

- These modules import from foundation layer (models, workflow, prompts) — write the imports as if those modules exist, even though they may not in your worktree
- executor.py owns `_active_procs` / `_active_procs_lock` global mutable state
- executor.py imports: models, workflow.interpolate, prompts.META_STYLE
- git_ops.py imports: models, executor._run_streamed (for running git commands)
- replan.py imports: models, workflow (validate, build, _extract_yaml), executor.call_claude, prompts.REPLAN_PROMPT
- planner.py imports: models, workflow._extract_yaml, executor.call_claude, prompts (multiple templates)
- `_call_claude()` in plan generation section (L1579) is the same concept as `call_claude` but may have different signature — unify into executor.call_claude or keep planner-specific version if signature differs significantly
- `_parse_timeout()` (L410) belongs in executor.py

### Verification

```bash
python -c "
import ast
for m in ['executor', 'git_ops', 'replan', 'planner']:
    ast.parse(open(f'dage/{m}.py').read())
    print(f'{m}.py syntax OK')
"
```

(Full import verification requires Stream 1's modules; deferred to Stream 3)

### Constraints

- Do NOT modify or delete the original `dage.py`
- Do NOT create models.py, prompts.py, workflow.py, tui.py (Stream 1 scope)
- Do NOT create engine.py, cli.py, __init__.py, __main__.py (Stream 3 scope)
- Preserve all existing function signatures exactly
- `_active_procs` and `_active_procs_lock` must stay module-private in executor.py, exposed only via `kill_active_procs()`
- Signal handler registration (`signal.signal(SIGTERM, ...)`) should NOT happen at import time — provide a `register_signal_handlers()` function that cli.py or engine.py calls explicitly

---

## Stream 3: Orchestration & Integration

Goal: 提取最终的 engine/cli 模块，完成包组装，删除旧文件，全量验证功能不变。

Dependencies: Stream 1 + Stream 2 (needs all 8 foundation+execution modules)

### Scope

1. 从 `dage.py` 提取剩余模块：

| Target file        | Source sections in dage.py               | Key contents                                                    |
|--------------------|------------------------------------------|-----------------------------------------------------------------|
| `dage/engine.py`   | DAG Engine + DAG Runner + State Persistence + Hot Reload (L420-433, L885-1248) | run_dag, _handle_gate_fail, _autofix_gate, _annotate_design_docs, save_state, _load_resume_state, print_summary, should_skip, _hot_reload |
| `dage/cli.py`      | CLI (L1758-1898)                         | build_parser, cmd_run, cmd_validate, cmd_status, cmd_plan, main |

2. Package wiring:
   - Finalize `dage/__init__.py` (ensure correct public API exports)
   - Create `dage/__main__.py` (`from dage.cli import main; main()`)
   - Create `pyproject.toml` with `[project.scripts] dage = "dage.cli:main"`
   - Delete original `dage.py` from repository root

3. Fix any cross-module import issues discovered during integration testing

### Context

- engine.py is the convergence point — it imports from ALL other modules (models, workflow, executor, git_ops, replan, tui, prompts)
- cli.py imports from workflow, engine, planner, tui
- `build_context()` and `_build_summary()` (Execution Context section, L173-185) go to engine.py (used only by run_dag)
- `_reload_config()` (L932) goes to engine.py
- `sys.exit()` calls must only appear in cli.py
- Signal handler registration happens in cli.py's `main()` function
- Original `dage.py` must be deleted (not shimmed) per design decision
- Existing YAML tests in `tests/` and `examples/` must produce identical behavior

### Verification

```bash
# Package importable
python -c "from dage.cli import main; print('import OK')"

# CLI help works
python -m dage --help

# All existing YAML integration tests pass
python -m dage run tests/test_parallel.yaml
python -m dage run tests/test_parallel_gate.yaml
python -m dage run examples/test-shell.yaml

# Entry point works after pip install
pip install -e . && dage --help
```

### Constraints

- Every test that passed with the old `dage.py` must still pass
- No new features, no behavior changes — pure structural refactor
- `sys.exit()` only in cli.py
- Circular imports are a hard failure — dependency must flow strictly: models → workflow → executor → git_ops/replan → engine → cli

---

## Risk Areas

1. Circular imports: engine.py depends on nearly everything. If any lower module accidentally imports from engine (e.g., for a utility function that should have been placed lower), Python will raise ImportError at startup. Watch for functions that are called from multiple layers.

2. `_extract_yaml` placement: This function is called by both planner.py and replan.py but lives in workflow.py. Both must import from workflow, not copy the function. If either stream places it elsewhere, integration will have duplicated logic.

3. Global state migration: Three pieces of mutable state (`_max_output`, `_active_procs`, `_display`) must each land in exactly one module. If an agent misplaces state or creates duplicate state, runtime behavior will silently diverge (e.g., `kill_active_procs()` killing a different list than `_run_streamed()` populates).

4. `_call_claude` vs `call_claude`: The plan generation section (L1579) has its own `_call_claude()` that may differ from the general `call_claude` pattern. Need to check if they can unify or must stay separate. Wrong unification could break plan generation's timeout/system-prompt handling.

5. Signal handler timing: Currently `signal.signal(SIGTERM, _sigterm_handler)` runs at module import. After modularization, this must be explicitly called (not at import time) to avoid side effects during testing or library use.

6. `_log_line` and display coupling: `_log_line()` (L256) references the global `_display` object. After split, this function is in executor.py but `_display` is in tui.py. Need a clean bridge (callback or import) without creating executor→tui dependency that violates the dependency graph.

7. Python import resolution: A directory named `dage/` coexisting with the original `dage.py` in the same parent directory is ambiguous to Python's import system. During Stream 1+2 (when dage.py still exists), imports like `from dage.models import ...` might resolve to the file instead of the package. Agents should test with explicit `PYTHONPATH` or by running from a different directory.
