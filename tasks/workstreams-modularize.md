# Work Streams: dage.py Modularization (v2)

Design source: `tasks/design-modularize.md` (v2)

## Overview

将1952行单体 `dage.py` 拆为 `dage/` 包(12个文件)。
3个 work stream，严格串行。

```
  Stream 1 (foundation)  ──→  Stream 2 (execution)  ──→  Stream 3 (integration)
```

v1曾设计Stream 1/2并行，但v2将`_log`归属tui.py后，所有执行层模块
(`executor`, `git_ops`, `replan`, `planner`)都需要`from dage.tui import log`，
消除了并行可能性。串行3步是对依赖链的诚实反映。

---

## Stream 1: Foundation Layer

Goal: 创建 `dage/` 包骨架，提取所有零依赖/底层模块，建立日志基础设施。

Dependencies: none

### Scope

从 `dage.py` 提取5个模块(含package scaffold):

| Target file        | Source sections in dage.py                          | Lines | Key contents                                        |
|--------------------|-----------------------------------------------------|-------|-----------------------------------------------------|
| `dage/__init__.py` | (new)                                               | ~5    | `__version__` only, no bulk re-export               |
| `dage/models.py`   | Enums + Data Structures (L28-83)                    | ~90   | Role, NodeType, Status, Node, NodeResult            |
| `dage/prompts.py`  | Prompt Templates (L435-520, L1492-1670)             | ~250  | all prompt strings, `_X` renamed to `X`             |
| `dage/workflow.py`  | YAML Loading thru Gate Propagation + extract_yaml (L84-239, L1759-1792) | ~170 | load/build/validate, interpolate, topo/scheduling, extract_yaml |
| `dage/tui.py`      | TUI Display + Output (L253-267, L1259-1488)         | ~250  | DageDisplay, log, log_line, print_*, ANSI constants |

### Context

Design v2 relative to v1 has 4 critical changes affecting this stream:

1. `_log` (L1436, 97 occurrences) and `_log_line` (L256) belong to tui.py.
   Rationale: both read/write `_display` (tui state). tui depends only on
   models+workflow, so any module can safely `from dage.tui import log`.
   Public name: `log` (not `_log`). Same for `log_line`.

2. `_ANSI_COLORS` (L253) and `_ANSI_RESET` belong to tui.py (display constants),
   NOT models.py. Only `_ROLE_MAX_RUNS` stays in models.py (Role semantic constraint).

3. `_SKILL_SEARCH_PATHS` (L312) goes to executor.py (Stream 2), NOT models.py.

4. `_extract_yaml` (L1759-1792): remove `nodes` key hard-validation added in
   commit 9f6903e. Only do text extraction + YAML parse validity check.
   Semantic key validation (e.g. `nodes` key) is the caller's responsibility.
   Reason: replan YAML schema is `{justification, remove, add}` with no `nodes` key.

Dependency truth:
- models.py: no deps
- prompts.py: no deps
- workflow.py: imports models only
- tui.py: imports models AND workflow (for `topo_layers` used in `DageDisplay._render()` L1326 and `print_plan` L1459)

Global mutable state in this stream:
- `_max_output` (L146) → workflow.py, accessed via `set_max_output(n)` / `get_max_output()`
- `_display` (L1434) → tui.py, accessed via `set_display(d)` / `get_display()`
- `_HAS_RICH` → tui.py, module-private

`print_status(run_dir)`: parameter is the resolved run directory path (not repo_dir).
Latest-run resolution logic moves to cli.py (Stream 3).

### Verification

```bash
python -c "
from dage.models import Node, Role, Status, NodeType, NodeResult
from dage.prompts import META_STYLE, REPLAN_PROMPT, PLAN_PROMPT, DAGE_KNOWLEDGE
from dage.workflow import load_workflow, build_nodes, validate_workflow
from dage.workflow import interpolate, topo_layers, next_runnable, find_blocked
from dage.workflow import extract_yaml, set_max_output
from dage.tui import log, log_line, DageDisplay, print_summary, print_plan
print('Stream 1 OK')
"
```

### Constraints

- Do NOT modify or delete the original `dage.py` (Stream 3 handles cutover)
- Do NOT create executor.py, git_ops.py, replan.py, planner.py (Stream 2 scope)
- Do NOT create engine.py, cli.py, __main__.py, pyproject.toml (Stream 3 scope)
- Preserve all existing function signatures exactly (only rename leading underscore on public exports)
- `save_json` (L1225-1227) and `node_to_dict` (L1229-1238): move to models.py (data utilities)
- `tui.log` must work WITHOUT `_display` set (fallback to stderr print) -- test this

---

## Stream 2: Execution Layer

Goal: 提取进程执行、git操作、自适应重规划、AI计划生成4个模块。

Dependencies: Stream 1 (needs models, prompts, workflow, tui modules)

### Scope

从 `dage.py` 提取4个模块:

| Target file         | Source sections in dage.py                   | Lines | Key contents                                                    |
|---------------------|----------------------------------------------|-------|-----------------------------------------------------------------|
| `dage/executor.py`  | Executors (L235-419) + _call_claude (L1594)  | ~200  | process mgmt, shell/claude runners, call_claude, signal handler |
| `dage/git_ops.py`   | Worktree Merge + Auto-commit (L521-636)      | ~120  | worktree merge/prune, auto_commit                               |
| `dage/replan.py`    | Adaptive Replanning (L691-892)               | ~200  | detect/call/apply replan, confirm                               |
| `dage/planner.py`   | Plan Generation (L1490-1757, excl extract_yaml) | ~150 | four-phase AI plan generation                                   |

### Context

Import dependencies for each module:

```
executor.py  ← models, workflow.interpolate, prompts.META_STYLE, tui.log, tui.log_line
git_ops.py   ← models, executor._run_streamed, tui.log
replan.py    ← models, workflow.{validate_workflow, _build_one_node, extract_yaml},
               executor.call_claude, prompts.{REPLAN_PROMPT, DAGE_KNOWLEDGE},
               tui.log, models.save_json
planner.py   ← models, workflow.extract_yaml, executor.{call_claude, _load_skills},
               prompts.{PLAN_PROMPT, BRAINSTORM_PROMPT, MATURE_PROMPT, PLAN_DOC_PROMPT},
               tui.log
```

Key decisions:

1. `_call_claude` (L1594-1609) unifies into `executor.call_claude()`.
   It wraps `_run_streamed("_plan", ...)` with `claude` CLI (not ccx).
   Used by: replan.call_replanner, planner._generate_yaml (5 call sites total).
   Signature: `call_claude(prompt, timeout=1800, system="") -> str`

2. `register_signal_handlers()`: executor.py provides this function.
   Contains `_sigterm_handler` (L249-251) which calls `kill_active_procs()`.
   Engine (Stream 3) calls it in `run_dag()`, NOT at import time.

3. `_SKILL_SEARCH_PATHS` (L312-314): lives in executor.py (only used by `_load_skills`).

4. `_parse_timeout` (L410-419): lives in executor.py.

5. `_extract_yaml` call-site differences:
   - `planner._generate_yaml`: after `extract_yaml(text)`, additionally checks result dict has `nodes` key
   - `replan.call_replanner`: after `extract_yaml(text)`, expects dict (no specific key requirement)
   Both import from `workflow.extract_yaml` which does NOT check `nodes` key.

### Verification

```bash
python -c "
from dage.executor import execute_node, run_shell, run_claude, call_claude
from dage.executor import kill_active_procs, register_signal_handlers
from dage.git_ops import merge_worktrees, prune_worktrees, auto_commit
from dage.replan import detect_replan, call_replanner, apply_replan
from dage.planner import generate_plan
print('Stream 2 OK')
"
```

### Constraints

- Do NOT modify or delete the original `dage.py`
- Do NOT modify Stream 1 modules (models, prompts, workflow, tui) unless fixing an import bug
- Do NOT create engine.py, cli.py, __main__.py, pyproject.toml (Stream 3 scope)
- Preserve all existing function signatures exactly
- `_active_procs` and `_active_procs_lock` are module-private in executor.py
- No side effects at import time (no signal registration, no process creation)

---

## Stream 3: Orchestration + Integration

Goal: 提取 engine/cli，完成包组装，删除原 `dage.py`，全量验证行为不变。

Dependencies: Stream 1 + Stream 2 (needs all 9 modules)

### Scope

1. Extract remaining modules:

| Target file        | Source sections in dage.py                              | Lines | Key contents                                           |
|--------------------|---------------------------------------------------------|-------|--------------------------------------------------------|
| `dage/engine.py`   | Execution Context + DAG Engine/Runner + State + Hot Reload (L173-185, L421-433, L637-692, L893-1248) | ~300 | run_dag, gate handling, autofix, state, hot reload |
| `dage/cli.py`      | CLI (L1792-1952)                                        | ~160  | argparse, cmd_run/validate/status/plan, main()         |

2. Package finalization:
   - Create `dage/__main__.py` (`from dage.cli import main; main()`)
   - Create `pyproject.toml` with entry point `dage = "dage.cli:main"`
   - Verify and finalize `dage/__init__.py`
   - Delete `dage.py` from repository root

### Context

engine.py is the convergence point with maximum imports:

```
engine.py ← models, workflow.{topo_layers, next_runnable, find_blocked, interpolate,
             set_max_output, build_nodes, validate_workflow, load_workflow},
             executor.{execute_node, kill_active_procs, register_signal_handlers},
             git_ops.{merge_worktrees, prune_worktrees, auto_commit},
             replan.{detect_replan, call_replanner, apply_replan},
             tui.{log, set_display, get_display, DageDisplay, print_summary},
             prompts.{AUTOFIX_PROMPT, ANNOTATE_PROMPT}
```

```
cli.py ← workflow.{load_workflow, build_nodes, validate_workflow},
          engine.{run_dag}, planner.{generate_plan},
          tui.{print_plan, print_status},
          executor.{register_signal_handlers}
```

Key content placement:
- `build_context()` (L175) and `_build_summary()` (L182) → engine.py
- `should_skip()` (L423) → engine.py
- `_reload_config()` (L940) → engine.py
- `_handle_gate_fail()` (L953) → engine.py
- `_autofix_gate()` (L653) → engine.py (uses executor.call_claude + prompts.AUTOFIX_PROMPT)
- `_annotate_design_docs()` (L462) → engine.py
- `_handle_replan()` (L976) → engine.py (calls replan module functions)
- `_hot_reload()` (L895) → engine.py
- `run_dag()` (L1022) → engine.py (main loop, ThreadPoolExecutor)
- `_load_resume_state()` (L1188) → engine.py
- `save_state()` (L1240) → engine.py (uses models.save_json)
- `save_latest_link()` (L1244) → engine.py
- `_find_latest_run()` (L1248) → engine.py

Signal handler flow:
- `engine.run_dag()` calls `executor.register_signal_handlers()` at start
- Restores previous handler in finally block (current L1070/L1168 pattern)

`sys.exit()` only in cli.py.

`cmd_status` in cli.py: resolves latest run dir (calls `engine._find_latest_run()`),
then passes resolved path to `tui.print_status(run_dir)`.

### Verification

```bash
# 1. Package importable
python -c "from dage.cli import main; print('import OK')"

# 2. CLI help works
python -m dage --help

# 3. Integration tests pass
python -m dage run tests/test_parallel.yaml
python -m dage run tests/test_parallel_gate.yaml
python -m dage run examples/test-shell.yaml

# 4. Entry point works
pip install -e . && dage --help

# 5. Original dage.py is gone
test ! -f dage.py && echo 'dage.py removed OK'

# 6. No circular imports (import each module individually)
python -c "
import importlib
for m in ['models','prompts','workflow','tui','executor','git_ops','replan','planner','engine','cli']:
    importlib.import_module(f'dage.{m}')
    print(f'dage.{m} OK')
"
```

### Constraints

- Every test that passed with the old `dage.py` must still pass
- No new features, no behavior changes -- pure structural refactor
- `sys.exit()` only in cli.py
- Circular imports are a hard failure
- Dependency flow: models/prompts → workflow → tui → executor → git_ops/replan/planner → engine → cli
- Do NOT leave a `dage.py` shim -- delete it entirely per design decision

---

## Risk Areas

1. Circular imports: engine.py depends on nearly everything. If any lower module
   accidentally imports from engine (e.g., for a utility function that should be
   placed lower), Python raises ImportError at startup. Watch for `_find_latest_run`
   and `save_state` which are utility-shaped but belong in engine (only called there).

2. `_log` migration (97 call sites): Every occurrence of `_log(` must become
   `log(` with `from dage.tui import log` at the module top. Miss one and you
   get NameError at runtime. The agent should grep for `_log(` after extraction
   to confirm zero remaining bare references.

3. `_extract_yaml` semantic validation: The fix removes `nodes` key check from
   the shared function. If the planner agent forgets to add its own `nodes` key
   check after calling `extract_yaml`, malformed YAML will pass silently.
   Conversely, if the fix is not applied, replan will break on valid replan YAML.

4. `_display` lifecycle: `_display` is set by engine.run_dag() via tui.set_display(),
   read by tui.log()/log_line(). If engine forgets to call set_display, all log
   output falls through to stderr (functional but loses TUI). If engine forgets
   to call set_display(None) on exit, subsequent non-TUI code may write to a
   stopped display.

5. `topo_layers` in tui.py: tui imports workflow.topo_layers for DageDisplay._render().
   This creates tui → workflow dependency. workflow → models, tui → workflow → models:
   still no cycle. But if someone later adds workflow → tui import, it cycles.

6. Python file/package conflict: During Stream 1/2 (while `dage.py` still exists),
   `from dage.models import ...` may resolve to the file, not the package.
   Agents should test with `python -c` from the repo root. Stream 3 resolves this
   by deleting `dage.py`.

7. `call_claude` unification: L1594 (`_call_claude`) uses `_run_streamed("_plan", ...)`
   with specific flags (`--permission-mode bypassPermissions`, `--add-dir`).
   If the agent creating executor.py misses any flag, all AI calls (replan + plan)
   silently degrade.
