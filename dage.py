#!/usr/bin/env python3
"""dage — DAG-based Agent Workflow Orchestrator.

Orchestrates multi-step AI agent workflows defined as YAML DAGs.
Each node is either a `claude` node (runs ccx subprocess) or a `shell` node.
Gate nodes enforce mechanical constraints: gate failure skips all downstream.
"""

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from dataclasses import dataclass, field
from enum import Enum
from graphlib import TopologicalSorter, CycleError
from pathlib import Path
from typing import Any

import yaml

# ==== Enums

class Role(Enum):
    CONTEXT  = "context"
    PRODUCE  = "produce"
    GATE     = "gate"
    EVALUATE = "evaluate"
    GC       = "gc"
    META     = "meta"

class NodeType(Enum):
    CLAUDE = "claude"
    SHELL  = "shell"

class Status(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED  = "failed"
    SKIPPED = "skipped"

# ==== Data Structures

@dataclass
class Node:
    name:      str
    type:      NodeType
    role:      Role
    deps:      list[str]       = field(default_factory=list)
    prompt:    str             = ""
    cmd:       str             = ""
    condition: str             = ""
    max_runs:  int             = 0
    worktree:  str             = ""
    timeout:   str             = ""
    retry:     int             = 0
    adaptive:  bool            = False
    skills:    list[str]       = field(default_factory=list)

@dataclass
class NodeResult:
    status:   Status  = Status.PENDING
    output:   str     = ""
    duration: float   = 0.0
    retries:  int     = 0
    cost:     float   = 0.0

    def to_dict(self) -> dict:
        d = {"status": self.status.value, "output_len": len(self.output),
             "duration": round(self.duration, 1), "retries": self.retries}
        if self.output:
            d["output"] = self.output
        if self.cost > 0:
            d["cost"] = round(self.cost, 4)
        return d

# ==== YAML Loading

def load_workflow(path: str) -> dict:
    with open(path) as f:
        wf = yaml.safe_load(f)
    if not isinstance(wf, dict) or "nodes" not in wf:
        raise ValueError("invalid workflow: 'nodes' key required")
    return wf

# bounded roles: auto-cap max_runs to prevent goal drift
_ROLE_MAX_RUNS = {Role.CONTEXT: 1, Role.META: 1}

def _build_one_node(name: str, spec: dict, defaults: dict) -> Node:
    if not isinstance(spec, dict):
        raise ValueError(f"node '{name}': spec must be a mapping")
    role     = Role(spec.get("role", "produce"))
    max_runs = spec.get("max_runs", defaults.get("max_runs", 0))
    # bounded roles auto-cap if not explicitly set
    if max_runs == 0 and role in _ROLE_MAX_RUNS:
        max_runs = _ROLE_MAX_RUNS[role]
    return Node(
        name      = name,
        type      = NodeType(spec.get("type", defaults.get("type", "claude"))),
        role      = role,
        deps      = spec.get("deps", []),
        prompt    = spec.get("prompt", ""),
        cmd       = spec.get("cmd", ""),
        condition = spec.get("condition", ""),
        max_runs  = max_runs,
        worktree  = spec.get("worktree", ""),
        timeout   = spec.get("timeout", defaults.get("timeout", "")),
        retry     = spec.get("retry", 0),
        adaptive  = spec.get("adaptive", False),
        skills    = spec.get("skills", defaults.get("skills", [])),
    )

def build_nodes(wf: dict) -> dict[str, Node]:
    defaults = wf.get("defaults", {})
    return {name: _build_one_node(name, spec, defaults)
            for name, spec in wf["nodes"].items()}

def validate_workflow(nodes: dict[str, Node]) -> list[str]:
    """Returns list of errors (empty = valid)."""
    errors = []
    for name, node in nodes.items():
        for dep in node.deps:
            if dep not in nodes:
                errors.append(f"node '{name}': unknown dep '{dep}'")
        if node.type == NodeType.CLAUDE and not node.prompt:
            errors.append(f"node '{name}': claude node requires 'prompt'")
        if node.type == NodeType.SHELL and not node.cmd:
            errors.append(f"node '{name}': shell node requires 'cmd'")
    graph = {name: set(node.deps) for name, node in nodes.items()}
    try:
        ts = TopologicalSorter(graph)
        ts.prepare()
    except CycleError as e:
        errors.append(f"cycle detected: {e}")
    return errors

# ==== Variable Interpolation

_max_output: int = 0  # workflow-level cap for ${nodes.X.output}, 0 = unlimited

def _resolve_path(ctx: dict, path: str) -> str:
    """Resolve dotted path like 'nodes.harvest.output' against context dict."""
    cur: Any = ctx
    for part in path.split("."):
        if isinstance(cur, dict):
            if part not in cur:
                return f"<unresolved:{path}>"
            cur = cur[part]
        elif isinstance(cur, NodeResult):
            if   part == "output":
                text = cur.output
                if _max_output and len(text) > _max_output:
                    text = text[:_max_output] + \
                        f"\n[truncated: {len(cur.output)} chars total]"
                cur = text
            elif part == "status": cur = cur.status.value
            else: return f"<unresolved:{path}>"
        else:
            return f"<unresolved:{path}>"
    return str(cur) if cur is not None else ""

def interpolate(template: str, ctx: dict) -> str:
    """Replace ${...} references with values from context."""
    return re.sub(r'\$\{([^}]+)\}', lambda m: _resolve_path(ctx, m.group(1)), template)

# ==== Execution Context

def build_context(wf: dict, results: dict[str, NodeResult], run_id: str) -> dict:
    return {
        "vars":  wf.get("vars", {}),
        "nodes": results,
        "run":   {"id": run_id, "summary": _build_summary(results)},
    }

def _build_summary(results: dict[str, NodeResult]) -> str:
    return "\n".join(f"  {n}: {r.status.value} ({r.duration:.0f}s)"
                     for n, r in results.items())

# ==== Topo Sort

def topo_layers(nodes: dict[str, Node]) -> list[list[str]]:
    """Return nodes grouped by topological layers (ready-at-same-time)."""
    graph = {name: set(node.deps) for name, node in nodes.items()}
    ts = TopologicalSorter(graph)
    ts.prepare()
    layers = []
    while ts.is_active():
        ready = sorted(ts.get_ready())
        layers.append(ready)
        for name in ready:
            ts.done(name)
    return layers

# ==== Dynamic Scheduling

def next_runnable(nodes: dict[str, Node], results: dict[str, NodeResult],
                  blocked: set[str]) -> list[str]:
    """Compute currently runnable nodes: deps done + self PENDING + not blocked."""
    runnable = []
    for name, node in nodes.items():
        if results[name].status != Status.PENDING:
            continue
        if name in blocked:
            continue
        if all(results[d].status in (Status.SUCCESS, Status.SKIPPED)
               for d in node.deps):
            runnable.append(name)
    runnable.sort()
    return runnable

# ==== Gate Propagation

def find_blocked(nodes: dict[str, Node], failed_gate: str) -> set[str]:
    """Find all nodes transitively downstream of a failed gate."""
    children: dict[str, list[str]] = {n: [] for n in nodes}
    for name, node in nodes.items():
        for dep in node.deps:
            children[dep].append(name)
    blocked: set[str] = set()
    queue = deque(children[failed_gate])
    while queue:
        n = queue.popleft()
        if n not in blocked:
            blocked.add(n)
            queue.extend(children[n])
    return blocked

# ==== Executors

_active_procs: list[subprocess.Popen] = []
_active_procs_lock = threading.Lock()

def _kill_active_procs():
    """Terminate all tracked child processes."""
    with _active_procs_lock:
        for proc in _active_procs:
            try:
                proc.terminate()
            except OSError:
                pass

def _sigterm_handler(signum, frame):
    _kill_active_procs()
    raise KeyboardInterrupt

_ANSI_COLORS = [36, 32, 33, 35, 34, 91, 96, 92, 93, 95]  # cyan,green,yellow,magenta,blue,...
_ANSI_RESET  = "\033[0m"

def _log_line(name: str, line: str):
    """Format node output: color-coded right-aligned name │ content."""
    if _display and not name.startswith("_"):
        stripped = re.sub(r'\[[\d;]*m', '', line).strip()
        if stripped:
            _display.node_last[name] = stripped
        _display.node_lines[name] = _display.node_lines.get(name, 0) + 1
    if name.startswith("_"):
        _log(f"\033[2m  {name:>18} │ {line}{_ANSI_RESET}")
    else:
        c = _ANSI_COLORS[hash(name) % len(_ANSI_COLORS)]
        _log(f"  \033[{c}m{name:>15}{_ANSI_RESET} │ {line}")

def _run_streamed(name: str, cmd, *, shell=False, cwd=None,
                  timeout=None) -> tuple[int, str, str]:
    """Run subprocess with [name]-prefixed live output. Returns (rc, stdout, stderr)."""
    env = os.environ.copy()
    env["CCX_MANAGED"] = "1"
    proc = subprocess.Popen(
        cmd, shell=shell, cwd=cwd, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    with _active_procs_lock:
        _active_procs.append(proc)
    stdout_buf: list[str] = []
    stderr_buf: list[str] = []

    def _drain(stream, buf):
        for line in stream:
            buf.append(line)
            _log_line(name, line.rstrip())

    t1 = threading.Thread(target=_drain, args=(proc.stdout, stdout_buf), daemon=True)
    t2 = threading.Thread(target=_drain, args=(proc.stderr, stderr_buf), daemon=True)
    t1.start()
    t2.start()

    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        t1.join(timeout=5)
        t2.join(timeout=5)
        raise
    finally:
        with _active_procs_lock:
            try:
                _active_procs.remove(proc)
            except ValueError:
                pass

    t1.join()
    t2.join()
    return proc.returncode, "".join(stdout_buf), "".join(stderr_buf)

_SKILL_SEARCH_PATHS = [
    os.path.expanduser("~/.claude/skills/{name}"),
    ".claude/skills/{name}",
]

def _load_skills(names: list[str]) -> str:
    """Load SKILL.md content for each named skill. Returns concatenated text."""
    parts = []
    for name in names:
        for pattern in _SKILL_SEARCH_PATHS:
            skill_dir = pattern.format(name=name)
            skill_dir = os.path.realpath(skill_dir)
            skill_file = os.path.join(skill_dir, "SKILL.md")
            if os.path.exists(skill_file):
                content = Path(skill_file).read_text().strip()
                # rewrite relative paths to absolute (e.g. references/foo.md -> /abs/path/references/foo.md)
                ref_dir = os.path.join(skill_dir, "references")
                if os.path.isdir(ref_dir):
                    content = content.replace("references/", ref_dir + "/")
                parts.append(f"# Skill: {name}\n\n{content}")
                _log(f"  skill loaded: {name} ({skill_file})")
                break
        else:
            _log(f"  skill not found: {name}")
    return "\n\n".join(parts)

def run_shell(node: Node, cmd: str, cwd: str | None = None) -> NodeResult:
    t0 = time.monotonic()
    try:
        rc, stdout, stderr = _run_streamed(
            node.name, cmd, shell=True, cwd=cwd,
            timeout=_parse_timeout(node.timeout),
        )
        elapsed = time.monotonic() - t0
        output = stdout.strip()
        if rc != 0 and stderr:
            output += f"\n[stderr] {stderr.strip()}"
        return NodeResult(
            status   = Status.SUCCESS if rc == 0 else Status.FAILED,
            output   = output,
            duration = elapsed,
        )
    except subprocess.TimeoutExpired:
        return NodeResult(status=Status.FAILED, output="[timeout]",
                          duration=time.monotonic() - t0)

def run_claude(node: Node, prompt: str, run_dir: str, run_id: str,
               repo_dir: str, worktree: str = "") -> NodeResult:
    node_dir = os.path.join(run_dir, node.name)
    os.makedirs(node_dir, exist_ok=True)

    notes_file = os.path.join(os.path.abspath(run_dir), f"{node.name}.notes.md")
    cmd = [
        "ccx",
        "-p",                  prompt,
        "-m",                  str(node.max_runs),
        "--completion-signal", "NODE_COMPLETE",
        "--notes-file",        notes_file,
        "--disable-commits",
        "--disable-branches",
    ]
    wt = worktree or node.worktree
    if wt:
        cmd += ["--worktree", wt, "--worktree-base-dir", ".dage/worktrees"]
    if node.timeout:
        cmd += ["--max-duration", node.timeout]
    if node.skills:
        skill_content = _load_skills(node.skills)
        if skill_content:
            cmd += ["--append-system-prompt", skill_content]

    t0 = time.monotonic()
    try:
        timeout_s = _parse_timeout(node.timeout)
        outer_timeout = timeout_s + 120 if timeout_s else None
        rc, stdout, stderr = _run_streamed(
            node.name, cmd, cwd=repo_dir, timeout=outer_timeout,
        )
        elapsed = time.monotonic() - t0

        notes_path = Path(notes_file)
        output = notes_path.read_text().strip() if notes_path.exists() else ""

        os.makedirs(node_dir, exist_ok=True)
        with open(os.path.join(node_dir, "ccx.log"), "w") as f:
            f.write(f"=== stdout ===\n{stdout}\n")
            f.write(f"=== stderr ===\n{stderr}\n")
            f.write(f"=== returncode: {rc} ===\n")

        return NodeResult(
            status   = Status.SUCCESS if rc == 0 else Status.FAILED,
            output   = output,
            duration = elapsed,
        )
    except subprocess.TimeoutExpired:
        return NodeResult(status=Status.FAILED, output="[timeout]",
                          duration=time.monotonic() - t0)

def _parse_timeout(timeout: str) -> float | None:
    if not timeout:
        return None
    total = 0.0
    for val, unit in re.findall(r'(\d+)([hms])', timeout.lower()):
        n = int(val)
        if   unit == 'h': total += n * 3600
        elif unit == 'm': total += n * 60
        elif unit == 's': total += n
    return total if total > 0 else None

# ==== DAG Engine

def should_skip(node: Node, ctx: dict) -> bool:
    if not node.condition:
        return False
    rendered = interpolate(node.condition, ctx)
    if "!=" in rendered:
        left, right = [s.strip() for s in rendered.split("!=", 1)]
        return left == right
    if "==" in rendered:
        left, right = [s.strip() for s in rendered.split("==", 1)]
        return left != right
    return not rendered.strip()

# ==== Prompt Templates (en + zh)

_ANNOTATE_PROMPT = """Review design docs against the actual implementation. Think deeply about whether
each difference is a real problem or an intentional design evolution.

Design docs: {design_docs}

What was just implemented and verified:
{impl_summary}

For each confirmed discrepancy:
1. Fix the design doc text to match reality (update numbers, constraints, descriptions)
2. Insert an HTML comment ABOVE the fix recording what changed and why:

<!-- dage-note: {date}
CHANGED: [original text] -> [new text]
REASON: [why the implementation diverged, what was discovered during implementation]
-->

Rules:
- Think before acting: is this a real error, or was it an intentional simplification in the doc?
- Fix real issues: wrong numbers, outdated assumptions, missing constraints, incorrect formulas
- Skip: style differences, wording preferences, level-of-detail choices
- Every fix MUST have a dage-note comment above it recording the change
- If no real discrepancies, do nothing
"""

def _annotate_design_docs(wf: dict, nodes: dict[str, Node],
                          results: dict[str, NodeResult],
                          gate_name: str, run_dir: str, run_id: str,
                          repo_dir: str):
    """After gate passes, review design docs and annotate discrepancies."""
    design_docs = wf.get("design_docs", [])
    if not design_docs:
        return

    gate = nodes[gate_name]
    upstreams = [d for d in gate.deps if d in nodes]
    impl_summary = "\n".join(
        f"- {d}: {nodes[d].prompt.strip().split(chr(10))[0][:80]}"
        for d in upstreams if nodes[d].prompt)

    prompt = _ANNOTATE_PROMPT.format(
        design_docs = ", ".join(design_docs),
        impl_summary = impl_summary or "(no details)",
        date = time.strftime("%Y-%m-%d"),
    )

    fix_node = Node(name=f"_annotate_{gate_name}", type=NodeType.CLAUDE,
                    role=Role.PRODUCE, prompt=prompt, max_runs=1,
                    skills=wf.get("defaults", {}).get("skills", []))
    _log(f"[_annotate_{gate_name}] reviewing design docs ...")
    result = run_claude(fix_node, prompt, run_dir, run_id, repo_dir)
    _log(f"[_annotate_{gate_name}] {'ok' if result.status == Status.SUCCESS else 'skip'}"
         f"  {result.duration:.1f}s")

_META_STYLE = """
写作风格: 猫娘+雌小鬼。连贯段落，不要标题或列表。
"雌小鬼"是二次元常见的一类角色，她们总是以高傲、嚣张、轻视、调笑的态度对待年长者，
而一旦被"教训"后总是很容易服软并露出该年龄应有的行为。
语气特征: 句尾偶尔带猫叫口癖但不过度、对上游设计挑刺吐槽、对自己成果嘴硬炫耀、
遇到困难傲娇不肯承认、偶尔用♡调皮。技术内容必须准确，语气不牺牲信息量。
"""

def execute_node(node: Node, ctx: dict, run_dir: str, run_id: str,
                 repo_dir: str, dry_run: bool = False,
                 worktree: str = "") -> NodeResult:
    if dry_run:
        return NodeResult(status=Status.SUCCESS, output="[dry-run]")

    last_result = NodeResult(status=Status.FAILED)
    max_attempts = 1 + node.retry

    for attempt in range(max_attempts):
        prompt_or_cmd = interpolate(node.prompt or node.cmd, ctx)
        if node.role == Role.META and node.type == NodeType.CLAUDE:
            prompt_or_cmd += _META_STYLE
        if node.type == NodeType.SHELL:
            result = run_shell(node, prompt_or_cmd, cwd=repo_dir)
        else:
            result = run_claude(node, prompt_or_cmd, run_dir, run_id,
                                repo_dir, worktree=worktree)
        last_result = result
        last_result.retries = attempt
        if result.status == Status.SUCCESS:
            return result
        if attempt < max_attempts - 1:
            _log(f"  retry {attempt + 1}/{node.retry} for '{node.name}'...")

    return last_result

# ==== Worktree Merge

def _merge_single_worktree(node_name: str, wt_name: str,
                           repo_dir: str) -> bool:
    """Merge one worktree branch back to main. Returns True on success."""
    wt_base = os.path.join(repo_dir, ".dage", "worktrees")
    wt_path = os.path.realpath(os.path.join(wt_base, wt_name))
    if not os.path.isdir(wt_path):
        return True
    try:
        # commit worktree changes on its branch
        _run_streamed(
            f"_commit_{node_name}",
            f'cd "{wt_path}" && git add -A && '
            f'git diff --cached --quiet || git commit -m "dage: {node_name}"',
            shell=True)
        # attempt merge
        rc, out, err = _run_streamed(
            f"_merge_{node_name}",
            f'cd "{repo_dir}" && git merge --no-edit "{wt_name}"',
            shell=True)
        if rc != 0:
            # conflict — abort merge, preserve worktree for manual resolution
            _run_streamed(f"_abort_{node_name}",
                         f'cd "{repo_dir}" && git merge --abort 2>/dev/null; true',
                         shell=True)
            _log(f"  CONFLICT merging {node_name} — resolve in: {wt_path}")
            return False
        _log(f"  merge: {node_name} -> main")
        # reset worktree to main HEAD for reuse next run
        _run_streamed(
            f"_reset_{node_name}",
            f'cd "{wt_path}" && git checkout -B "{wt_name}" HEAD 2>/dev/null; '
            f'git reset --hard main 2>/dev/null; true',
            shell=True)
        return True
    except Exception as e:
        _log(f"  merge failed ({node_name}): {e}")
        return False

def _merge_worktrees(auto_wt: dict[str, str], repo_dir: str, run_id: str):
    """Merge worktree branches back to main via git merge."""
    for node_name, wt_name in auto_wt.items():
        _merge_single_worktree(node_name, wt_name, repo_dir)

def _prune_worktrees(repo_dir: str):
    """Remove worktrees whose branches have been merged. Called at workflow end."""
    wt_base = os.path.join(repo_dir, ".dage", "worktrees")
    if not os.path.isdir(wt_base):
        return
    pruned = []
    for name in os.listdir(wt_base):
        wt_path = os.path.join(wt_base, name)
        if not os.path.isdir(wt_path):
            continue
        # check if branch has unmerged changes
        rc, _, _ = _run_streamed(
            f"_check_{name}",
            f'cd "{wt_path}" && git diff --quiet HEAD main 2>/dev/null',
            shell=True)
        if rc != 0:
            continue  # unmerged changes — keep
        # remove worktree + delete branch
        try:
            _run_streamed(f"_prune_{name}",
                         f'cd "{repo_dir}" && git worktree remove "{wt_path}" --force 2>/dev/null; '
                         f'git branch -D "{name}" 2>/dev/null; true',
                         shell=True)
            pruned.append(name)
        except Exception:
            pass
    if pruned:
        _log(f"  pruned worktrees: {pruned}")

# ==== Gate Auto-commit

def _auto_commit(gate_name: str, nodes: dict[str, Node],
                 repo_dir: str, push: bool = False):
    """Commit all changes after a gate passes. Optionally push."""
    # collect upstream produce node names for commit message
    gate = nodes[gate_name]
    upstreams = [d for d in gate.deps
                 if d in nodes and nodes[d].role != Role.GATE]

    msg = f"feat({gate_name}): {', '.join(upstreams)} verified"

    try:
        # check if there are changes to commit
        rc, out, _ = _run_streamed(
            f"_commit_{gate_name}",
            "git diff --quiet HEAD 2>/dev/null; echo $?",
            shell=True, cwd=repo_dir)
        has_changes = out.strip() != "0"
        if not has_changes:
            return

        _run_streamed(f"_commit_{gate_name}",
                      f'git add -A -- . ":!.dage" && git commit -m "{msg}"',
                      shell=True, cwd=repo_dir)
        _log(f"[commit] {msg}")

        if push:
            rc, _, _ = _run_streamed(f"_push_{gate_name}",
                                     "git push", shell=True, cwd=repo_dir)
            _log(f"[push] {'ok' if rc == 0 else 'FAIL (no remote?)'}")
    except Exception as e:
        _log(f"[commit] failed: {e}")

# ==== Gate Autofix

_AUTOFIX_PROMPT = """A build/test gate failed. Diagnose and fix the issue.

Gate command:
{cmd}

Error output:
{error_output}
{upstream_context}
Instructions:
1. Read the error carefully, identify root cause
2. Fix it (install tools, fix code, etc.)
3. Run the gate command yourself to verify
"""

def _autofix_gate(gate: Node, gate_result: NodeResult,
                  nodes: dict[str, Node], ctx: dict,
                  wf: dict, run_dir: str, run_id: str,
                  repo_dir: str) -> NodeResult | None:
    """Spawn a temporary claude node to diagnose & fix a failed gate, then retry."""
    upstream = "\n".join(
        f"Upstream '{d}' goal:\n{nodes[d].prompt[:500]}"
        for d in gate.deps if d in nodes and nodes[d].prompt
    )
    resolved_cmd = interpolate(gate.cmd, ctx)
    prompt = _AUTOFIX_PROMPT.format(
        cmd              = resolved_cmd,
        error_output     = gate_result.output[-3000:],
        upstream_context = f"\n{upstream}" if upstream else "",
    )

    fix_name = f"_autofix_{gate.name}"
    defaults = wf.get("defaults", {})
    fix_node = Node(
        name=fix_name, type=NodeType.CLAUDE, role=Role.PRODUCE,
        prompt=prompt, max_runs=defaults.get("max_runs", 0),
        timeout="10m", skills=defaults.get("skills", []),
    )

    _log(f"[{fix_name}] attempting auto-fix ...")
    fix_result = run_claude(fix_node, prompt, run_dir, run_id, repo_dir)
    _log(f"[{fix_name}] {'ok' if fix_result.status == Status.SUCCESS else 'FAIL'}"
         f"  {fix_result.duration:.1f}s")

    if fix_result.status != Status.SUCCESS:
        return None

    _log(f"[{gate.name}] retrying after autofix ...")
    retry = run_shell(gate, resolved_cmd, cwd=repo_dir)
    _log(f"[{gate.name}] retry {'ok' if retry.status == Status.SUCCESS else 'FAIL'}"
         f"  {retry.duration:.1f}s")
    return retry

# ==== Adaptive Replanning

def detect_replan(nodes: dict[str, Node], results: dict[str, NodeResult],
                  layer: list[str]) -> tuple[str, str] | None:
    """Scan just-executed layer for [REPLAN: reason] from adaptive nodes."""
    for name in layer:
        if not nodes[name].adaptive:
            continue
        if results[name].status != Status.SUCCESS:
            continue
        m = re.search(r'\[REPLAN:\s*(.+?)\]', results[name].output)
        if m:
            return name, m.group(1).strip()
    return None

_DAGE_KNOWLEDGE = """How dage works:
- Each `claude` node spawns a ccx session — an iterative Claude Code development loop.
  ccx runs Claude Code in multiple iterations (controlled by max_runs).
  Iteration 1: agent plans the task and creates a notes file.
  Iterations 2+: agent executes against the plan, reading previous notes as context.
  ccx automatically handles: notes file read/write, completion signal, iteration context.
  The final notes file content becomes ${{nodes.NAME.output}} for downstream nodes.
- Each `shell` node runs a command. Use for: git, test, build, lint, benchmarks.
- Nodes in the same layer (no mutual deps) run in parallel automatically.
- A `gate` node that fails skips ALL its downstream nodes (short-circuit).

ccx prompt writing guide:
- The prompt is your GOAL, not a script. ccx wraps it in workflow context automatically.
- Focus on: What to achieve + upstream context. Do NOT say "write to notes" (ccx does it).
- Inject upstream context via ${{nodes.NAME.output}} — the upstream node's notes file text.
- max_runs = ccx iterations (each is a full Claude Code session):
    0     unlimited: stopped by completion signal (default, recommended)
    1-3   cap for simple tasks if you want to limit cost
    5-10  cap for moderate tasks
    10+   cap for complex tasks (usually unnecessary with completion signal)
- For simple info gathering: use `type: shell` with a command instead of ccx.
- After implementation nodes, always add a shell gate node (cargo test, pytest, make).

Node schema:
  <name>:
    type: shell | claude
    role: produce|context|gate|evaluate|gc|meta
    deps: [a, b]
    cmd: "..." # required for shell
    prompt: | # required for claude
      Goal: ...
      Context from upstream: ${{nodes.upstream.output}}
      Specific tasks: 1. ... 2. ...
    retry: N
    timeout: "30m" # e.g. 1h, 5m, 30s
    max_runs: 0 # ccx iterations (0=unlimited, completion-signal-driven)
"""

_REPLAN_PROMPT = """You are a workflow replanner. A running DAG needs adjustment.

{dage_knowledge}

Original task: {task}

Completed nodes (cannot be changed):
{completed}

Trigger node '{trigger}' signals: {reason}
Trigger output (last 2000 chars):
{output}

Pending nodes (may be removed):
{pending}

Replan #{replan_seq} of max {max_replans}. Minimize changes.

Rules:
- ADD new nodes (may depend on completed or new nodes)
- REMOVE pending nodes that are no longer needed
- Cannot touch completed nodes. No cycles allowed.
- For claude nodes: prompt is the GOAL (ccx auto-handles notes and iteration context)
- For shell nodes: cmd must be a valid shell command
- You MUST provide a justification explaining how these changes serve the original task

Output ONLY valid YAML (no fences, no commentary):
  justification: "one sentence: how this replan serves the original task"
  remove: [name, ...]
  add:
    name:
      type: shell | claude
      role: produce | context | gate
      deps: [...]
      cmd: "..." # for shell
      prompt: | # for claude
        Goal: ...
        Context: ...
      max_runs: 0 # ccx iterations (0=unlimited, default)
"""

def call_replanner(wf: dict, nodes: dict[str, Node],
                   results: dict[str, NodeResult],
                   trigger: str, reason: str,
                   replan_seq: int, run_dir: str) -> dict | None:
    completed = {n for n, r in results.items() if r.status != Status.PENDING}
    pending   = {n for n in nodes if n not in completed}

    comp_summary = "\n".join(
        f"  {n}: {results[n].status.value} ({results[n].duration:.0f}s)"
        for n in sorted(completed))
    pend_summary = "\n".join(
        f"  {n}: deps={nodes[n].deps}" for n in sorted(pending))

    prompt = _REPLAN_PROMPT.format(
        dage_knowledge = _DAGE_KNOWLEDGE.replace("{{", "{").replace("}}", "}"),
        task        = wf.get("description", "(no description)"),
        completed   = comp_summary or "  (none)",
        trigger     = trigger,
        reason      = reason,
        output      = results[trigger].output[-2000:],
        pending     = pend_summary or "  (none)",
        replan_seq  = replan_seq,
        max_replans = wf.get("replan", {}).get("max_replans", 3),
    )

    try:
        raw = _call_claude(prompt, timeout=120)
        raw = _extract_yaml(raw)
        result = yaml.safe_load(raw)
        if not isinstance(result, dict):
            _log("[replan] invalid response (not a dict), skipping")
            return None
        with open(os.path.join(run_dir, f"replan-{replan_seq}-raw.yaml"), "w") as f:
            f.write(raw)
        return result
    except Exception as e:
        _log(f"[replan] replanner failed: {e}")
        return None

def apply_replan(nodes: dict[str, Node], results: dict[str, NodeResult],
                 blocked: set[str], replan_result: dict,
                 defaults: dict, run_dir: str, seq: int) -> dict:
    """Apply replan: remove pending nodes, add new ones. Rollback on validation error."""
    removed = []
    for name in replan_result.get("remove", []):
        if name in nodes and results[name].status == Status.PENDING:
            del nodes[name]
            del results[name]
            blocked.discard(name)
            for n in nodes.values():
                if name in n.deps:
                    n.deps.remove(name)
            removed.append(name)

    added = []
    for name, spec in replan_result.get("add", {}).items():
        if name not in nodes:
            try:
                nodes[name] = _build_one_node(name, spec, defaults)
                results[name] = NodeResult()
                added.append(name)
            except Exception as e:
                _log(f"[replan] failed to build node '{name}': {e}")

    errors = validate_workflow(nodes)
    if errors:
        for name in added:
            del nodes[name]
            del results[name]
        _log(f"[replan] rejected (validation errors): {errors}")
        if removed:
            _log(f"[replan] warning: {len(removed)} removed nodes lost in rollback")
        return {"seq": seq, "added": [], "removed": []}

    event = {"seq": seq, "added": added, "removed": removed,
             "justification": replan_result.get("justification", "")}
    _save_json(os.path.join(run_dir, f"replan-{seq}.json"), event)
    return event

def _format_replan_proposal(replan_result: dict) -> str:
    """Format a replan proposal for human review."""
    lines = []
    justification = replan_result.get("justification", "(none)")
    lines.append(f"  justification: {justification}")

    removed = replan_result.get("remove", [])
    if removed:
        lines.append(f"  remove: {removed}")

    added = replan_result.get("add", {})
    for name, spec in added.items():
        t    = spec.get("type", "claude")
        deps = spec.get("deps", [])
        lines.append(f"  add: {name} ({t}) deps={deps}")
    return "\n".join(lines)

def _confirm_replan() -> bool:
    """Ask user for interactive approval. Returns True if approved."""
    try:
        if not sys.stdin.isatty():
            _log("  [confirm] stdin not a tty, auto-approving")
            return True
        _log("  approve? [Y/n] ", )
        answer = input().strip().lower()
        return answer in ("", "y", "yes")
    except (EOFError, KeyboardInterrupt):
        return False

# ==== Hot Reload

def _hot_reload(yaml_path: str, nodes: dict[str, Node],
                results: dict[str, NodeResult], blocked: set[str],
                wf: dict) -> bool:
    """Reload YAML and apply changes to pending nodes. Returns True if changed."""
    try:
        new_wf = load_workflow(yaml_path)
        new_specs = new_wf.get("nodes", {})
        defaults  = new_wf.get("defaults", {})

        added, updated, removed = [], [], []

        for name, spec in new_specs.items():
            if name in nodes:
                if results[name].status != Status.PENDING:
                    continue
                nodes[name] = _build_one_node(name, spec, defaults)
                updated.append(name)
            else:
                nodes[name] = _build_one_node(name, spec, defaults)
                results[name] = NodeResult()
                added.append(name)

        for name in list(nodes.keys()):
            if name not in new_specs and results[name].status == Status.PENDING:
                del nodes[name]
                del results[name]
                blocked.discard(name)
                removed.append(name)

        errors = validate_workflow(nodes)
        if errors:
            _log(f"[hot-reload] rejected: {errors}")
            return False

        wf.update(new_wf)

        if added or updated or removed:
            _log(f"[hot-reload] +{len(added)} ~{len(updated)} -{len(removed)}")
        return bool(added or updated or removed)
    except Exception as e:
        _log(f"[hot-reload] failed: {e}")
        return False

# ==== DAG Runner

def _reload_config(wf: dict) -> dict:
    """Extract mutable config from workflow dict."""
    replan_cfg = wf.get("replan", {})
    commit_cfg = wf.get("auto_commit", {})
    return {
        "replan_mode":  replan_cfg.get("mode", "auto"),
        "max_replans":  replan_cfg.get("max_replans", 3),
        "max_nodes":    replan_cfg.get("max_nodes", 50),
        "do_commit":    bool(commit_cfg) if isinstance(commit_cfg, dict) else bool(commit_cfg),
        "do_push":      commit_cfg.get("push", False) if isinstance(commit_cfg, dict) else False,
        "autofix":      wf.get("autofix", True),
    }

def _handle_gate_fail(name: str, nodes: dict[str, Node], results: dict[str, NodeResult],
                      blocked: set[str], autofixed: set[str], cfg: dict,
                      ctx: dict, wf: dict, run_dir: str, run_id: str,
                      repo_dir: str):
    """Handle a failed gate: autofix attempt, then block downstream."""
    if cfg["autofix"] and name not in autofixed:
        autofixed.add(name)
        fix_result = _autofix_gate(
            nodes[name], results[name], nodes, ctx,
            wf, run_dir, run_id, repo_dir)
        if fix_result and fix_result.status == Status.SUCCESS:
            results[name] = fix_result
            _log(f"[{name}] gate passed after autofix")
            return
    downstream = find_blocked(nodes, name)
    blocked |= downstream
    _log(f"[{name}] gate failed -> blocking {sorted(downstream)}")
    for b in downstream:
        if results.get(b, NodeResult()).status == Status.PENDING:
            results[b] = NodeResult(status=Status.SKIPPED,
                                    output="blocked by failed gate")
            _log(f"[{b}] SKIPPED (gate)")

def _handle_replan(name: str, nodes: dict[str, Node], results: dict[str, NodeResult],
                   blocked: set[str], wf: dict, run_dir: str,
                   cfg: dict, replan_count: int) -> int:
    """Check and handle replan signal from a single node. Returns updated replan_count."""
    node = nodes[name]
    if not node.adaptive or results[name].status != Status.SUCCESS:
        return replan_count
    if replan_count >= cfg["max_replans"] or len(nodes) >= cfg["max_nodes"]:
        return replan_count
    m = re.search(r'\[REPLAN:\s*(.+?)\]', results[name].output)
    if not m:
        return replan_count

    reason = m.group(1).strip()
    seq = replan_count + 1
    _log(f"[replan {seq}/{cfg['max_replans']}] "
         f"triggered by '{name}': {reason}")

    if cfg["replan_mode"] == "log":
        _log(f"[replan] mode=log, signal recorded but not acted on")
        _save_json(os.path.join(run_dir, f"replan-{seq}-signal.json"),
                   {"seq": seq, "trigger": name, "reason": reason, "mode": "log"})
        return replan_count + 1

    replan_result = call_replanner(wf, nodes, results, name, reason, seq, run_dir)
    if not replan_result:
        return replan_count

    justification = replan_result.get("justification", "")
    if not justification:
        _log("[replan] rejected: no justification provided")
        return replan_count + 1

    _log(f"[replan] proposal:\n{_format_replan_proposal(replan_result)}")

    if cfg["replan_mode"] == "confirm" and not _confirm_replan():
        _log("[replan] rejected by user")
        return replan_count + 1

    event = apply_replan(nodes, results, blocked, replan_result,
                         wf.get("defaults", {}), run_dir, seq)
    if _display:
        _display.replan_count = replan_count + 1
    _log(f"[replan] +{len(event['added'])} -{len(event['removed'])} nodes")
    return replan_count + 1

def run_dag(wf: dict, nodes: dict[str, Node], repo_dir: str,
            dry_run: bool = False, from_node: str | None = None) -> dict[str, NodeResult]:
    """Execute the full DAG with dynamic scheduling and adaptive replanning."""
    run_id  = time.strftime("%Y%m%d-%H%M%S")
    run_dir = os.path.join(repo_dir, ".dage", "runs", run_id)
    os.makedirs(run_dir, exist_ok=True)

    results: dict[str, NodeResult] = {name: NodeResult() for name in nodes}
    blocked: set[str] = set()
    autofixed: set[str] = set()
    cfg = _reload_config(wf)

    # workflow-level output truncation
    global _max_output
    _max_output = wf.get("max_output", 0)

    # concurrency cap
    max_concurrent = wf.get("max_concurrent", 0) or None

    if from_node:
        results, blocked = _load_resume_state(nodes, from_node, repo_dir)
        resumed = [n for n, r in results.items() if r.status == Status.SUCCESS]
        if resumed:
            _log(f"resumed: {sorted(resumed)}")

    replan_count = 0

    _save_json(os.path.join(run_dir, "original-nodes.json"),
               {n: _node_to_dict(nodes[n]) for n in nodes})

    yaml_path = wf.get("_yaml_path")
    yaml_mtime = os.path.getmtime(yaml_path) if yaml_path else 0

    opts = []
    if max_concurrent: opts.append(f"workers={max_concurrent}")
    if _max_output:    opts.append(f"max_output={_max_output}")
    opts_str = f"  ({', '.join(opts)})" if opts else ""
    _log(f"run {run_id}  nodes={len(nodes)}{opts_str}")
    if dry_run:
        _log("[dry-run mode]")
    _log("")

    global _display
    start_time = time.monotonic()
    if _HAS_RICH and sys.stderr.isatty() and not dry_run:
        _display = DageDisplay(wf, nodes, results, start_time)
        _display.start()

    prev_sigterm = signal.signal(signal.SIGTERM, _sigterm_handler)

    try:
        with ThreadPoolExecutor(max_workers=max_concurrent) as pool:
            while True:
                # hot-reload: detect YAML changes between rounds
                if yaml_path:
                    new_mtime = os.path.getmtime(yaml_path)
                    if new_mtime != yaml_mtime:
                        yaml_mtime = new_mtime
                        if _hot_reload(yaml_path, nodes, results, blocked, wf):
                            cfg = _reload_config(wf)
                            _max_output = wf.get("max_output", 0)

                layer = next_runnable(nodes, results, blocked)
                if not layer:
                    break

                # phase 1: condition filter
                to_run = []
                ctx = build_context(wf, results, run_id)
                for name in layer:
                    node = nodes[name]
                    if should_skip(node, ctx):
                        results[name] = NodeResult(status=Status.SKIPPED,
                                                   output="condition not met")
                        _log(f"[{name}] SKIPPED (condition)")
                        continue
                    _log(f"[{name}] {node.role.value.upper()} ({node.type.value}) ...")
                    to_run.append(name)

                if not to_run:
                    continue

                # auto-worktree: only for parallel claude nodes that may write files
                claude_no_wt = [n for n in to_run
                                if nodes[n].type == NodeType.CLAUDE
                                and not nodes[n].worktree
                                and nodes[n].role != Role.CONTEXT]
                auto_wt = ({n: f"dage-{n}" for n in claude_no_wt}
                           if len(claude_no_wt) > 1 else {})
                if auto_wt:
                    _log(f"  auto-worktree: {sorted(auto_wt)}")

                # phase 2: parallel execution with inline gate handling
                for n in to_run:
                    results[n] = NodeResult(status=Status.RUNNING)
                    if _display:
                        _display.node_start[n] = time.monotonic()
                in_flight = {
                    pool.submit(execute_node, nodes[n], ctx, run_dir,
                                run_id, repo_dir, dry_run,
                                worktree=auto_wt.get(n, "")): n
                    for n in to_run
                }
                gates_passed = []

                while in_flight:
                    done, _ = wait(in_flight.keys(),
                                   return_when=FIRST_COMPLETED)
                    for fut in done:
                        name = in_flight.pop(fut)
                        results[name] = fut.result()
                        r = results[name]
                        icon = "ok" if r.status == Status.SUCCESS else "FAIL"
                        _log(f"[{name}] {icon}  {r.duration:.1f}s"
                             + (f"  retries={r.retries}" if r.retries else ""))

                        # inline gate failure: block downstream immediately
                        node = nodes[name]
                        if node.role == Role.GATE and r.status == Status.FAILED:
                            _handle_gate_fail(name, nodes, results, blocked,
                                              autofixed, cfg, ctx, wf,
                                              run_dir, run_id, repo_dir)
                        elif node.role == Role.GATE and r.status == Status.SUCCESS:
                            gates_passed.append(name)

                        # inline replan
                        replan_count = _handle_replan(
                            name, nodes, results, blocked,
                            wf, run_dir, cfg, replan_count)

                # phase 2.5: merge worktree changes back to main
                if auto_wt:
                    _merge_worktrees(auto_wt, repo_dir, run_id)

                # phase 3: gate success actions (after worktree merge for safe ordering)
                for name in gates_passed:
                    if cfg["do_commit"]:
                        _auto_commit(name, nodes, repo_dir, push=cfg["do_push"])
                    if wf.get("design_docs"):
                        _annotate_design_docs(wf, nodes, results, name,
                                              run_dir, run_id, repo_dir)

    except KeyboardInterrupt:
        _log("\n[interrupted] killing child processes...")
        _kill_active_procs()
    finally:
        signal.signal(signal.SIGTERM, prev_sigterm)
        if _display:
            _display.stop()
            _display = None

    save_state(run_dir, results)
    _prune_worktrees(repo_dir)
    _log("")
    print_summary(results)

    # print report from meta nodes
    for name, node in nodes.items():
        r = results.get(name)
        if r and r.status == Status.SUCCESS and node.role == Role.META and r.output:
            _log("")
            _log(r.output.strip())

    save_latest_link(repo_dir, run_id)
    return results

def _load_resume_state(nodes: dict[str, Node], from_node: str,
                       repo_dir: str) -> tuple[dict[str, NodeResult], set[str]]:
    results = {name: NodeResult() for name in nodes}
    blocked: set[str] = set()

    latest = _find_latest_run(repo_dir)
    if not latest:
        _log("warning: no prior run found, starting from scratch")
        return results, blocked

    state_file = os.path.join(latest, "results.json")
    if not os.path.exists(state_file):
        return results, blocked

    with open(state_file) as f:
        saved = json.load(f)

    layers = topo_layers(nodes)
    reached = False
    for layer in layers:
        for name in layer:
            if name == from_node:
                reached = True
                break
            if name in saved:
                s = saved[name]
                results[name] = NodeResult(
                    status   = Status(s["status"]),
                    output   = s.get("output", ""),
                    duration = s.get("duration", 0),
                )
        if reached:
            break
    return results, blocked

# ==== State Persistence

def _save_json(path: str, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def _node_to_dict(node: Node) -> dict:
    d = {"type": node.type.value, "role": node.role.value}
    if node.deps:      d["deps"]      = node.deps
    if node.prompt:    d["prompt"]    = node.prompt
    if node.cmd:       d["cmd"]       = node.cmd
    if node.condition: d["condition"] = node.condition
    if node.adaptive:  d["adaptive"]  = True
    if node.retry:     d["retry"]     = node.retry
    if node.timeout:   d["timeout"]   = node.timeout
    return d

def save_state(run_dir: str, results: dict[str, NodeResult]):
    _save_json(os.path.join(run_dir, "results.json"),
               {name: r.to_dict() for name, r in results.items()})

def save_latest_link(repo_dir: str, run_id: str):
    with open(os.path.join(repo_dir, ".dage", "latest"), "w") as f:
        f.write(run_id)

def _find_latest_run(repo_dir: str) -> str | None:
    latest_file = os.path.join(repo_dir, ".dage", "latest")
    if os.path.exists(latest_file):
        run_id = open(latest_file).read().strip()
        run_dir = os.path.join(repo_dir, ".dage", "runs", run_id)
        if os.path.isdir(run_dir):
            return run_dir
    return None

# ==== Output

# ==== TUI Display

try:
    from rich.console import Console as RichConsole
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table as RichTable
    from rich.text import Text
    _HAS_RICH = True
except ImportError:
    _HAS_RICH = False

_STATUS_ICON = {
    Status.SUCCESS: ("✓", "green"),
    Status.RUNNING: ("◐", "yellow"),
    Status.PENDING: ("○", "dim"),
    Status.FAILED:  ("✗", "red"),
    Status.SKIPPED: ("⊘", "dim"),
}

class _LiveProxy:
    """Proxy that calls fn() on each Rich render, enabling live data refresh."""
    def __init__(self, fn): self.fn = fn
    def __rich__(self):     return self.fn()

class DageDisplay:
    """Real-time DAG status panel + log tail, rendered as one Live block."""

    def __init__(self, wf, nodes, results, start_time):
        self.wf          = wf
        self.nodes        = nodes
        self.results      = results
        self.start_time   = start_time
        self.node_start:  dict[str, float] = {}
        self.node_last:   dict[str, str]   = {}
        self.node_lines:  dict[str, int]   = {}
        self.replan_count = 0
        self.log_buf: list[str] = []
        self.console      = RichConsole(stderr=True)
        self.live         = Live(_LiveProxy(self._render), console=self.console,
                                 refresh_per_second=2, screen=True)

    def start(self):
        self.live.start()

    def stop(self):
        self.live.stop()

    def log(self, msg: str):
        self.log_buf.append(msg)
        if len(self.log_buf) > 200:
            self.log_buf = self.log_buf[-200:]
        self.live.refresh()

    def _fmt_dur(self, s: float) -> str:
        if s < 60:  return f"{s:.0f}s"
        if s < 3600: return f"{int(s)//60}:{int(s)%60:02d}"
        return f"{int(s)//3600}h{int(s)%3600//60:02d}"

    def _render(self) -> Panel:
        elapsed = time.monotonic() - self.start_time
        total   = len(self.nodes)
        done    = sum(1 for r in self.results.values()
                      if r.status not in (Status.PENDING, Status.RUNNING))

        lines = []
        layers = topo_layers(self.nodes)
        max_show = 8

        # find first active layer (has RUNNING or PENDING nodes)
        first_active = 0
        for i, layer in enumerate(layers):
            if any(self.results.get(n, NodeResult()).status in (Status.RUNNING, Status.PENDING)
                   for n in layer):
                first_active = i
                break

        # scroll window: show 1 completed layer for context, then active+pending
        start = max(0, first_active - 1)
        if start > 0:
            done_nodes = sum(len(layers[j]) for j in range(start))
            lines.append(f"  [dim]✓ L0-L{start-1}  ({done_nodes} nodes done)[/]")

        shown = 0
        for i in range(start, len(layers)):
            if shown >= max_show:
                remaining = sum(len(layers[j]) for j in range(i, len(layers)))
                lines.append(f"  [dim]     ⋮  ({remaining} more)[/]")
                break
            layer = layers[i]
            parts = []
            for name in layer:
                r = self.results.get(name, NodeResult())
                icon, style = _STATUS_ICON.get(r.status, ("?", "dim"))
                if r.status == Status.RUNNING:
                    t0 = self.node_start.get(name, time.monotonic())
                    dur = self._fmt_dur(time.monotonic() - t0)
                    parts.append(f"[{style}]{icon} {name} {dur}[/]")
                elif r.status == Status.SUCCESS:
                    parts.append(f"[green]{icon} {name}[/] [dim]{self._fmt_dur(r.duration)}[/]")
                elif r.status == Status.FAILED:
                    parts.append(f"[{style}]{icon} {name}[/]")
                else:
                    parts.append(f"[{style}]{icon} {name}[/]")
                shown += 1
            lines.append(f"  [dim]L{i:<2}[/]  {'   '.join(parts)}")

        counts = {}
        for r in self.results.values():
            counts[r.status] = counts.get(r.status, 0) + 1
        status_parts = []
        for s in (Status.RUNNING, Status.SUCCESS, Status.FAILED, Status.SKIPPED, Status.PENDING):
            if counts.get(s, 0):
                icon, style = _STATUS_ICON[s]
                status_parts.append(f"[{style}]{icon} {counts[s]} {s.value}[/]")

        lines.append("")
        rp = f"  [dim]replans {self.replan_count}[/]" if self.replan_count else ""
        lines.append(f"  {'   '.join(status_parts)}{rp}")

        # right column: running node details
        right_lines = []
        for name in sorted(self.nodes):
            r = self.results.get(name, NodeResult())
            if r.status != Status.RUNNING:
                continue
            t0 = self.node_start.get(name, time.monotonic())
            dur = self._fmt_dur(time.monotonic() - t0)
            prompt = self.nodes[name].prompt.strip().split("\n")[0] if self.nodes[name].prompt else ""
            n_lines = self.node_lines.get(name, 0)
            last = self.node_last.get(name, "")

            right_lines.append(f"[yellow]◐ {name}[/] [dim]{dur}  {n_lines} lines[/]")
            if prompt:
                right_lines.append(f"  [dim]{prompt}[/]")
            if last:
                right_lines.append(f"  {last}")
            right_lines.append("")

        desc = self.wf.get("description", "dage")
        left_text  = Text.from_markup("\n".join(lines))

        if right_lines:
            right_text = Text.from_markup("\n".join(right_lines))
            table = RichTable(show_header=False, show_edge=False, box=None,
                              pad_edge=False, expand=True, padding=(0, 1))
            table.add_column(ratio=3, no_wrap=True, overflow="ellipsis")
            table.add_column(ratio=2, no_wrap=True, overflow="ellipsis")
            table.add_row(left_text, right_text)
            body = table
        else:
            body = left_text

        # panel height = max(left lines, right lines) + border
        content_h = max(len(lines), len(right_lines)) + 2
        panel = Panel(body,
                      title=f"[bold] {desc} [/]",
                      subtitle=f"[dim] {done}/{total} ── {self._fmt_dur(elapsed)} [/]",
                      border_style="blue", padding=(0, 1))

        try:
            term_h = os.get_terminal_size().lines
        except OSError:
            term_h = 40
        log_h = max(term_h - content_h, 3)
        log_text = Text.from_ansi("\n".join(self.log_buf[-log_h:]))

        layout = Layout()
        layout.split_column(
            Layout(log_text, name="log"),
            Layout(panel, name="status", size=content_h),
        )
        return layout

_display: DageDisplay | None = None

def _log(msg: str):
    if _display:
        _display.log(msg)
    else:
        print(msg, file=sys.stderr)

def print_summary(results: dict[str, NodeResult]):
    _log("=" * 60)
    _log(f"{'Node':<20} {'Status':<10} {'Time':>8}  {'Retries':>7}")
    _log("-" * 60)
    total_time = 0.0
    counts: dict[str, int] = {}
    for name, r in results.items():
        s = r.status.value
        counts[s] = counts.get(s, 0) + 1
        total_time += r.duration
        _log(f"{name:<20} {s:<10} {r.duration:>7.1f}s  {r.retries:>7}")
    _log("-" * 60)
    parts = [f"{v} {k}" for k, v in sorted(counts.items())]
    _log(f"total: {' / '.join(parts)}  time: {total_time:.0f}s")
    _log("=" * 60)

def print_plan(nodes: dict[str, Node]):
    layers = topo_layers(nodes)
    _log("Execution plan:")
    _log("")
    for i, layer in enumerate(layers):
        _log(f"  layer {i}:")
        for name in layer:
            node = nodes[name]
            deps  = f" <- [{', '.join(node.deps)}]" if node.deps else ""
            adapt = " [adaptive]" if node.adaptive else ""
            _log(f"    {name} ({node.type.value}/{node.role.value}){adapt}{deps}")
    _log("")

def print_status(repo_dir: str):
    run_dir = _find_latest_run(repo_dir)
    if not run_dir:
        _log("no runs found")
        return
    state_file = os.path.join(run_dir, "results.json")
    if not os.path.exists(state_file):
        _log("no results found")
        return
    with open(state_file) as f:
        data = json.load(f)
    _log(f"latest run: {os.path.basename(run_dir)}")
    _log("")
    _log(f"{'Node':<20} {'Status':<10} {'Time':>8}  {'Retries':>7}")
    _log("-" * 60)
    for name, r in data.items():
        _log(f"{name:<20} {r['status']:<10} {r['duration']:>7.1f}s  {r['retries']:>7}")
    _log("-" * 60)

# ==== Plan Generation

_PLAN_PROMPT = """You are a workflow planner for dage, a DAG-based workflow orchestrator.
Turn the task description into a valid dage YAML workflow.

""" + _DAGE_KNOWLEDGE.replace("{{", "{").replace("}}", "}") + """
Additional schema fields (plan-only):
  condition: "expr"    # skip if false
  adaptive: true       # enable replan signal detection (default: false)
  vars:
    key: value

Interpolation: ${vars.KEY}, ${nodes.NAME.output}, ${nodes.NAME.status}

Example — codebase analysis + implementation pipeline:
  nodes:
    scan:
      role: context
      prompt: |
        Scan the codebase structure, key modules, build system, and test coverage.
        Be thorough — read actual files, don't guess.
    read_docs:
      role: context
      prompt: |
        Read docs/design.md and docs/implementation-plan.md.
        Summarize architecture, key decisions, and implementation tasks.
    implement:
      deps: [scan, read_docs]
      prompt: |
        Implement the feature based on the plan.

        Codebase context: ${nodes.scan.output}
        Implementation plan: ${nodes.read_docs.output}

        Write tests first (TDD), then implement. Ensure all tests pass.
    test:
      role: gate
      deps: [implement]
      type: shell
      cmd: "make test"
    report:
      deps: [test]
      role: meta
      prompt: |
        Summarize: what was implemented, test=${nodes.test.status}.
        Include any issues and next steps.

Rules:
- deps only when B needs A's output or A must succeed first
- maximize parallelism: independent tasks have no deps between them
- gate after every implementation node (test/build/lint must pass before continuing)
- context nodes gather info, produce nodes create artifacts, gate nodes verify
- shell for deterministic commands (git/test/build), claude for reasoning/analysis/coding
- short descriptive snake_case node names

Output ONLY valid YAML. No fences, no commentary.

Task: """

_BRAINSTORM_PROMPT = """You are a workflow architect. An execution plan is provided below (already brainstormed
and structured). Design a DAG execution plan from it.
Think step by step, making all decisions autonomously.

1. DECOMPOSE: Break the execution plan into concrete subtasks suitable for DAG execution.
2. CLASSIFY each subtask:
   - claude (AI reasoning/analysis/coding) or shell (deterministic command)?
   - role: context (gather info), produce (create artifacts), gate (verify), meta (report)?
3. DEPENDENCIES: Which subtasks need outputs from others? Be precise — only add
   a dependency when subtask B actually reads subtask A's output.
4. PARALLELISM: Which subtasks are independent? Maximize concurrent execution.
5. GATES: After every implementation/coding subtask, add a shell verification
   step (test/build/lint) as a gate. Gate failure blocks all downstream work.
6. RESOURCE ESTIMATE: For each claude subtask, default is max_runs 0 (unlimited,
   completion-signal-driven). Only set max_runs or timeout to cap cost:
   - Light (reading/summarizing): max_runs 3 if capping
   - Medium (analysis/planning): max_runs 8 if capping
   - Heavy (implementation/coding): usually leave unlimited

Output a structured design document. Be specific about what each subtask does,
what it reads as input, and what it produces as output.

Task: """

def _call_claude(prompt: str, timeout: int = 120, system: str = "") -> str:
    cmd = ["claude", "-p", prompt, "--output-format", "text"]
    if system:
        cmd += ["--append-system-prompt", system]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        raise RuntimeError("'claude' CLI not found — install Claude Code first")
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"claude timed out ({timeout}s)")
    if proc.returncode != 0:
        raise RuntimeError(f"claude failed: {proc.stderr.strip()}")
    return proc.stdout.strip()

_MATURE_PROMPT = """You are a product design thinker. Turn a raw idea into a fully formed design spec.
Make ALL decisions autonomously — do not ask questions, do not wait for input.

Anti-pattern: "This is too simple to need a design." Every project gets a design. "Simple" projects are where unexamined assumptions cause the most wasted work.

Process (execute all steps in one pass):

1. EXPLORE CONTEXT: Mentally simulate checking the project state — what files, docs, existing patterns, and constraints likely exist? What's the current state of things?

2. SCOPE CHECK: Does this request describe multiple independent subsystems? If so, decompose into sub-projects first. Each sub-project gets its own design. Don't refine details of something that needs decomposition first.

3. UNDERSTAND PURPOSE: What is the user trying to achieve? What problem does this solve? What are the constraints and success criteria? Focus on purpose, not just mechanics.

4. EXPLORE APPROACHES: Propose 2-3 different approaches with trade-offs. Lead with your recommended option and explain why. Don't just list — reason about which is best and why.

5. PRESENT DESIGN: Cover these aspects, scaling each to its complexity
   (a few sentences if straightforward, up to 200-300 words if nuanced):
   - Architecture: overall structure and key components
   - Components: what each piece does and how they fit together
   - Data flow: how information moves through the system
   - Error handling: what can go wrong and how to handle it
   - Testing: how to verify correctness

6. DESIGN FOR ISOLATION AND CLARITY:
   - Break into smaller units with one clear purpose each
   - Each unit communicates through well-defined interfaces
   - Each unit can be understood and tested independently
   - For each unit: what does it do, how do you use it, what does it depend on?
   - Test: can someone understand what a unit does without reading its internals?
     Can you change the internals without breaking consumers? If not, boundaries need work.
   - Smaller, well-bounded units are easier to reason about — you think better about code
     you can hold in context at once, and edits are more reliable when files are focused.
     When a file grows large, that's often a signal it's doing too much.

7. EXISTING CODEBASE AWARENESS:
   - Follow existing patterns. Don't propose unrelated refactoring.
   - Where existing code has problems affecting the work (file too large, unclear boundaries, tangled responsibilities), include targeted improvements as part of the design — the way a good developer improves code they're working in.

8. APPLY YAGNI RUTHLESSLY: Remove every feature that isn't strictly necessary. Fewer features done well beats many features done poorly.

Before outputting, self-review your design against these criteria (fix issues inline, do not output the review separately):
- Completeness: no TODOs, placeholders, or "TBD" sections
- Consistency: no internal contradictions or conflicting requirements
- Clarity: no requirement ambiguous enough to cause building the wrong thing
- Scope: focused enough for a single implementation plan, not covering unrelated subsystems
- YAGNI: no unrequested features or over-engineering

Output a design document. Be specific and actionable, not vague. No code — just design.

Idea: """

_PLAN_DOC_PROMPT = """You are a technical planner. Turn a design spec into a structured implementation plan.
Make ALL decisions autonomously — do not ask questions.

Assume the engineer executing this plan has zero context for the codebase and questionable taste. Document everything they need: which files to touch, how to test, what docs to check. Give them the whole plan as bite-sized tasks.

Process:

1. SCOPE CHECK: If the design covers multiple independent subsystems that weren't decomposed, break into separate plans — one per subsystem. Each plan should produce working, testable software on its own.

2. FILE STRUCTURE: Before defining tasks, map out which files will be created or modified and what each one is responsible for. This is where decomposition decisions get locked in.
   - Design units with clear boundaries and well-defined interfaces
   - One clear responsibility per file. Prefer smaller, focused files over large ones
   - Files that change together should live together. Split by responsibility, not by technical layer
   - In existing codebases, follow established patterns. If a file has grown unwieldy, including a split in the plan is reasonable

3. TASK BREAKDOWN: Ordered list of concrete tasks. Bite-sized granularity —
   each step is one action (2-5 minutes, not an hour). For each task:
   - What to build (specific, not "add validation")
   - Which files to create/modify (exact paths)
   - How to verify it works (exact test commands with expected output)
   - What to commit and commit message
   - Include key interface signatures and critical logic — not "add validation" but
     the actual function signature and what it validates

   TDD cycle for each task (this IS the granularity):
     Step 1: Write the failing test
     Step 2: Run it to make sure it fails
     Step 3: Implement the minimal code to make the test pass
     Step 4: Run the tests and make sure they pass
     Step 5: Commit

4. DEPENDENCY ORDER: Which tasks must complete before others can start?
   Which can run in parallel? Be explicit.

5. RISK AREAS: Where things are most likely to go wrong. What to watch for.

Principles: DRY. YAGNI. TDD (write failing test first, then implement). Frequent commits.
Smaller, well-bounded units are easier to reason about — you think better about code
you can hold in context at once, and edits are more reliable when files are focused.

Before outputting, self-review your plan against these criteria (fix issues inline, do not output the review separately):
- Completeness: no TODOs, placeholders, or incomplete tasks
- Spec alignment: plan covers all design requirements, no major scope creep
- Task decomposition: tasks have clear boundaries, steps are actionable
- Buildability: could an engineer follow this plan without getting stuck?

Output a structured plan document with key interface signatures and logic for each task.

Design: """

def generate_plan(description: str) -> tuple[str, str]:
    """Four-phase plan generation: mature idea → execution plan → DAG design → YAML."""

    # phase 1: mature the raw idea into a well-scoped design
    _log("  phase 1/4: maturing idea...")
    mature = _call_claude(_MATURE_PROMPT + description, timeout=300)
    _log(f"  design: {len(mature)} chars")

    # phase 2: turn design into structured execution plan
    _log("  phase 2/4: writing execution plan...")
    plan = _call_claude(_PLAN_DOC_PROMPT + mature, timeout=300)
    _log(f"  plan: {len(plan)} chars")

    # phase 3: decompose plan into DAG subtasks with deps/gates
    _log("  phase 3/4: designing DAG...")
    design = _call_claude(
        _BRAINSTORM_PROMPT + f"\n\nExecution plan:\n{plan}",
        timeout=300)
    _log(f"  dag: {len(design)} chars")

    # phase 4: generate YAML
    _log("  phase 4/4: generating YAML...")
    gen_prompt = _PLAN_PROMPT + (
        f"\nDesign document:\n{design}\n\n"
        f"Original task: {description}"
    )
    raw = _call_claude(gen_prompt, timeout=300)
    return _extract_yaml(raw), design

def _extract_yaml(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    return text

# ==== CLI

def main():
    parser = argparse.ArgumentParser(
        prog="dage",
        description="DAG-based Agent Workflow Orchestrator",
    )
    sub = parser.add_subparsers(dest="command")

    # run
    p_run = sub.add_parser("run", help="execute a workflow")
    p_run.add_argument("workflow", help="path to workflow YAML")
    p_run.add_argument("--dry-run", action="store_true", help="show plan only")
    p_run.add_argument("--from", dest="from_node", help="resume from node")
    p_run.add_argument("--repo-dir", default=".", help="repo working directory")

    # validate
    p_val = sub.add_parser("validate", help="validate a workflow YAML")
    p_val.add_argument("workflow", help="path to workflow YAML")

    # status
    p_st = sub.add_parser("status", help="show latest run status")
    p_st.add_argument("--repo-dir", default=".", help="repo working directory")

    # plan
    p_plan = sub.add_parser("plan", help="AI-generate workflow from description")
    p_plan.add_argument("description", help="task description or path to idea file")
    p_plan.add_argument("-o", "--output", default="workflow.yaml",
                        help="output file (default: workflow.yaml)")

    args = parser.parse_args()

    if args.command == "run":
        wf    = load_workflow(args.workflow)
        wf["_yaml_path"] = os.path.abspath(args.workflow)
        nodes = build_nodes(wf)
        errors = validate_workflow(nodes)
        if errors:
            for e in errors:
                _log(f"error: {e}")
            sys.exit(1)

        repo_dir = os.path.abspath(
            wf.get("vars", {}).get("repo_dir", args.repo_dir)
        )

        if args.dry_run:
            print_plan(nodes)
            return

        results = run_dag(wf, nodes, repo_dir, from_node=args.from_node)
        if any(r.status == Status.PENDING for r in results.values()):
            sys.exit(130)  # interrupted
        if any(r.status == Status.FAILED for r in results.values()):
            sys.exit(1)

    elif args.command == "validate":
        wf    = load_workflow(args.workflow)
        nodes = build_nodes(wf)
        errors = validate_workflow(nodes)
        if errors:
            for e in errors:
                _log(f"error: {e}")
            sys.exit(1)
        _log(f"valid: {len(nodes)} nodes, {sum(len(n.deps) for n in nodes.values())} edges")
        print_plan(nodes)

    elif args.command == "status":
        print_status(os.path.abspath(args.repo_dir))

    elif args.command == "plan":
        desc = args.description
        if os.path.isfile(desc):
            desc = Path(desc).read_text().strip()
            _log(f"loaded idea from: {args.description}")
        _log("generating workflow...")
        try:
            raw, design = generate_plan(desc)
        except RuntimeError as e:
            _log(f"error: {e}")
            sys.exit(1)

        plan_dir = os.path.join(".dage", "plans")
        os.makedirs(plan_dir, exist_ok=True)
        design_file = os.path.join(plan_dir,
            f"{time.strftime('%Y%m%d-%H%M%S')}-design.md")
        with open(design_file, "w") as f:
            f.write(f"# Design: {desc[:80]}\n\n{design}\n")
        _log(f"  design: {design_file}")

        try:
            wf     = yaml.safe_load(raw)
            nodes  = build_nodes(wf)
            errors = validate_workflow(nodes)
            if errors:
                for e in errors:
                    _log(f"  warning: {e}")
            else:
                print_plan(nodes)
        except Exception as e:
            _log(f"warning: validation failed: {e}")

        with open(args.output, "w") as f:
            f.write(raw + "\n")
        _log(f"wrote {args.output}")

    else:
        parser.print_help()

if __name__ == "__main__":
    main()
