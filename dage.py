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
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
from graphlib import TopologicalSorter, CycleError
from pathlib import Path
from typing import Any

import yaml

# ==== Enums ================================================================

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

# ==== Data Structures ======================================================

@dataclass
class Node:
    name:      str
    type:      NodeType
    role:      Role
    deps:      list[str]       = field(default_factory=list)
    prompt:    str             = ""
    cmd:       str             = ""
    condition: str             = ""
    max_runs:  int             = 5
    worktree:  str             = ""
    timeout:   str             = ""
    retry:     int             = 0
    adaptive:  bool            = False

@dataclass
class NodeResult:
    status:   Status  = Status.PENDING
    output:   str     = ""
    duration: float   = 0.0
    retries:  int     = 0

    def to_dict(self) -> dict:
        return {"status": self.status.value, "output_len": len(self.output),
                "duration": round(self.duration, 1), "retries": self.retries}

# ==== YAML Loading =========================================================

def load_workflow(path: str) -> dict:
    """Load and return raw YAML workflow definition."""
    with open(path) as f:
        wf = yaml.safe_load(f)
    if not isinstance(wf, dict) or "nodes" not in wf:
        raise ValueError(f"invalid workflow: 'nodes' key required")
    return wf

def _build_one_node(name: str, spec: dict, defaults: dict) -> Node:
    """Build a single Node from spec dict, applying defaults."""
    if not isinstance(spec, dict):
        raise ValueError(f"node '{name}': spec must be a mapping")
    return Node(
        name     = name,
        type     = NodeType(spec.get("type", defaults.get("type", "claude"))),
        role     = Role(spec.get("role", "produce")),
        deps     = spec.get("deps", []),
        prompt   = spec.get("prompt", ""),
        cmd      = spec.get("cmd", ""),
        condition= spec.get("condition", ""),
        max_runs = spec.get("max_runs", defaults.get("max_runs", 5)),
        worktree = spec.get("worktree", ""),
        timeout  = spec.get("timeout", defaults.get("timeout", "")),
        retry    = spec.get("retry", 0),
        adaptive = spec.get("adaptive", False),
    )

def build_nodes(wf: dict) -> dict[str, Node]:
    """Build Node objects from workflow dict, applying defaults."""
    defaults = wf.get("defaults", {})
    nodes = {}
    for name, spec in wf["nodes"].items():
        nodes[name] = _build_one_node(name, spec, defaults)
    return nodes

def validate_workflow(nodes: dict[str, Node]) -> list[str]:
    """Validate DAG structure. Returns list of errors (empty = valid)."""
    errors = []
    # check dep references
    for name, node in nodes.items():
        for dep in node.deps:
            if dep not in nodes:
                errors.append(f"node '{name}': unknown dep '{dep}'")
        # type-specific checks
        if node.type == NodeType.CLAUDE and not node.prompt:
            errors.append(f"node '{name}': claude node requires 'prompt'")
        if node.type == NodeType.SHELL and not node.cmd:
            errors.append(f"node '{name}': shell node requires 'cmd'")
    # check for cycles
    graph = {name: set(node.deps) for name, node in nodes.items()}
    try:
        ts = TopologicalSorter(graph)
        ts.prepare()
    except CycleError as e:
        errors.append(f"cycle detected: {e}")
    return errors

# ==== Variable Interpolation ===============================================

def _resolve_path(ctx: dict, path: str) -> str:
    """Resolve dotted path like 'nodes.harvest.output' against context dict."""
    parts = path.split(".")
    cur: Any = ctx
    for part in parts:
        if isinstance(cur, dict):
            if part not in cur:
                return f"<unresolved:{path}>"
            cur = cur[part]
        elif isinstance(cur, NodeResult):
            if part == "output":
                cur = cur.output
            elif part == "status":
                cur = cur.status.value
            else:
                return f"<unresolved:{path}>"
        else:
            return f"<unresolved:{path}>"
    return str(cur) if cur is not None else ""

def interpolate(template: str, ctx: dict) -> str:
    """Replace ${...} references with values from context."""
    def replacer(m: re.Match) -> str:
        return _resolve_path(ctx, m.group(1))
    return re.sub(r'\$\{([^}]+)\}', replacer, template)

# ==== Execution Context ====================================================

def build_context(wf: dict, results: dict[str, NodeResult], run_id: str) -> dict:
    """Build interpolation context from workflow vars and current results."""
    ctx: dict[str, Any] = {}
    ctx["vars"]  = wf.get("vars", {})
    ctx["nodes"] = results
    ctx["run"]   = {"id": run_id, "summary": _build_summary(results)}
    return ctx

def _build_summary(results: dict[str, NodeResult]) -> str:
    lines = []
    for name, r in results.items():
        lines.append(f"  {name}: {r.status.value} ({r.duration:.0f}s)")
    return "\n".join(lines)

# ==== Topo Sort ============================================================

def topo_layers(nodes: dict[str, Node]) -> list[list[str]]:
    """Return nodes grouped by topological layers (ready-at-same-time)."""
    graph = {name: set(node.deps) for name, node in nodes.items()}
    ts = TopologicalSorter(graph)
    ts.prepare()
    layers = []
    while ts.is_active():
        ready = list(ts.get_ready())
        ready.sort()  # deterministic order within layer
        layers.append(ready)
        for name in ready:
            ts.done(name)
    return layers

# ==== Dynamic Scheduling ===================================================

def next_runnable(nodes: dict[str, Node], results: dict[str, NodeResult],
                  blocked: set[str]) -> list[str]:
    """Compute currently runnable nodes: deps all done + self PENDING + not blocked."""
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

# ==== Gate Propagation =====================================================

def find_blocked(nodes: dict[str, Node], failed_gate: str) -> set[str]:
    """Find all nodes transitively downstream of a failed gate."""
    # build adjacency: parent -> children
    children: dict[str, list[str]] = {n: [] for n in nodes}
    for name, node in nodes.items():
        for dep in node.deps:
            children[dep].append(name)
    # BFS from failed gate
    blocked: set[str] = set()
    queue = list(children[failed_gate])
    while queue:
        n = queue.pop(0)
        if n not in blocked:
            blocked.add(n)
            queue.extend(children[n])
    return blocked

# ==== Executors ============================================================

def _run_streamed(name: str, cmd, *, shell=False, cwd=None,
                  timeout=None) -> tuple[int, str, str]:
    """Run subprocess with [name]-prefixed live output on stderr.

    Returns (returncode, stdout_text, stderr_text).
    """
    proc = subprocess.Popen(
        cmd, shell=shell, cwd=cwd,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    stdout_buf: list[str] = []
    stderr_buf: list[str] = []

    def _drain(stream, buf):
        for line in stream:
            buf.append(line)
            _log(f"  {name} | {line.rstrip()}")

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

    t1.join()
    t2.join()
    return proc.returncode, "".join(stdout_buf), "".join(stderr_buf)


def run_shell(node: Node, cmd: str, cwd: str | None = None) -> NodeResult:
    """Execute a shell command node."""
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
    """Execute a claude node via ccx subprocess."""
    node_dir = os.path.join(run_dir, node.name)
    os.makedirs(node_dir, exist_ok=True)

    notes_file = f".dage/runs/{run_id}/{node.name}.notes.md"
    cmd = [
        "ccx",
        "-p", prompt,
        "-m", str(node.max_runs),
        "--completion-signal", "NODE_COMPLETE",
        "--notes-file", notes_file,
        "--disable-commits",
        "--disable-branches",
    ]
    wt = worktree or node.worktree
    if wt:
        cmd += ["--worktree", wt]
    if node.timeout:
        cmd += ["--max-duration", node.timeout]

    t0 = time.monotonic()
    try:
        timeout_s = _parse_timeout(node.timeout)
        outer_timeout = timeout_s + 120 if timeout_s else None
        rc, stdout, stderr = _run_streamed(
            node.name, cmd, cwd=repo_dir, timeout=outer_timeout,
        )
        elapsed = time.monotonic() - t0

        # read output from notes file
        notes_path = Path(repo_dir) / notes_file
        output = notes_path.read_text().strip() if notes_path.exists() else ""

        # save ccx log
        log_path = os.path.join(node_dir, "ccx.log")
        with open(log_path, "w") as f:
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
    """Parse timeout string like '30m', '1h', '1h30m' to seconds."""
    if not timeout:
        return None
    total = 0.0
    for val, unit in re.findall(r'(\d+)([hms])', timeout.lower()):
        n = int(val)
        if   unit == 'h': total += n * 3600
        elif unit == 'm': total += n * 60
        elif unit == 's': total += n
    return total if total > 0 else None

# ==== DAG Engine ===========================================================

def should_skip(node: Node, ctx: dict) -> bool:
    """Check if node's condition evaluates to false."""
    if not node.condition:
        return False
    rendered = interpolate(node.condition, ctx)
    # simple equality check: "X == Y" or "X != Y"
    if "!=" in rendered:
        left, right = [s.strip() for s in rendered.split("!=", 1)]
        return left == right
    if "==" in rendered:
        left, right = [s.strip() for s in rendered.split("==", 1)]
        return left != right
    # bare truthy: non-empty string = run
    return not rendered.strip()

def execute_node(node: Node, ctx: dict, run_dir: str, run_id: str,
                 repo_dir: str, dry_run: bool = False,
                 worktree: str = "") -> NodeResult:
    """Execute a single node with retry support."""
    if dry_run:
        return NodeResult(status=Status.SUCCESS, output="[dry-run]")

    last_result = NodeResult(status=Status.FAILED)
    max_attempts = 1 + node.retry

    for attempt in range(max_attempts):
        prompt_or_cmd = interpolate(node.prompt or node.cmd, ctx)

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

# ==== Adaptive Replanning ==================================================

def detect_replan(nodes: dict[str, Node], results: dict[str, NodeResult],
                  layer: list[str]) -> tuple[str, str] | None:
    """Scan just-executed layer for replan signals from adaptive nodes."""
    for name in layer:
        if not nodes[name].adaptive:
            continue
        if results[name].status != Status.SUCCESS:
            continue
        m = re.search(r'\[REPLAN:\s*(.+?)\]', results[name].output)
        if m:
            return name, m.group(1).strip()
    return None

_DAGE_KNOWLEDGE = """\
How dage works:
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
- Sizing (max_runs = ccx iterations, each is a full Claude Code session):
    1     one-shot: simple query, single-file edit
    3     small: targeted fix, read + edit a few files
    5     moderate: multi-file change with some analysis
    8-10  heavy: deep analysis, or implementation with tests
    10+   complex: large feature, needs planning + multi-step execution
- For simple info gathering: use `type: shell` with a command instead of ccx.
- After implementation nodes, always add a shell gate node (cargo test, pytest, make).

Node schema:
  <name>:
    type: shell | claude
    role: produce|context|gate|evaluate|gc|meta
    deps: [a, b]
    cmd: "..."                    # required for shell
    prompt: |                     # required for claude
      Goal: ...
      Context from upstream: ${{nodes.upstream.output}}
      Specific tasks: 1. ... 2. ...
    retry: N
    timeout: "30m"                # e.g. 1h, 5m, 30s
    max_runs: 5                   # ccx iterations (full Claude Code sessions)
"""

_REPLAN_PROMPT = """\
You are a workflow replanner. A running DAG needs adjustment.

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

Output ONLY valid YAML (no fences, no commentary):
  remove: [name, ...]
  add:
    name:
      type: shell | claude
      role: produce | context | gate
      deps: [...]
      cmd: "..."       # for shell
      prompt: |        # for claude
        Goal: ...
        Context: ...
      max_runs: 5      # ccx iterations (1=one-shot, 5=moderate, 10+=complex)
"""

def call_replanner(wf: dict, nodes: dict[str, Node],
                   results: dict[str, NodeResult],
                   trigger: str, reason: str,
                   replan_seq: int, run_dir: str) -> dict | None:
    """Call AI replanner and return parsed replan instructions."""
    completed = {n for n, r in results.items() if r.status != Status.PENDING}
    pending   = {n for n in nodes if n not in completed}

    comp_summary = "\n".join(
        f"  {n}: {results[n].status.value} ({results[n].duration:.0f}s)"
        for n in sorted(completed))
    pend_summary = "\n".join(
        f"  {n}: deps={nodes[n].deps}" for n in sorted(pending))

    trigger_output = results[trigger].output[-2000:]

    prompt = _REPLAN_PROMPT.format(
        dage_knowledge = _DAGE_KNOWLEDGE.replace("{{", "{").replace("}}", "}"),
        task        = wf.get("description", "(no description)"),
        completed   = comp_summary or "  (none)",
        trigger     = trigger,
        reason      = reason,
        output      = trigger_output,
        pending     = pend_summary or "  (none)",
        replan_seq  = replan_seq,
        max_replans = wf.get("replan", {}).get("max_replans", 3),
    )

    try:
        raw = _call_claude(prompt, timeout=120)
        raw = _extract_yaml(raw)
        result = yaml.safe_load(raw)
        if not isinstance(result, dict):
            _log(f"[replan] invalid response (not a dict), skipping")
            return None
        # save raw response for debugging
        with open(os.path.join(run_dir, f"replan-{replan_seq}-raw.yaml"), "w") as f:
            f.write(raw)
        return result
    except Exception as e:
        _log(f"[replan] replanner failed: {e}")
        return None

def apply_replan(nodes: dict[str, Node], results: dict[str, NodeResult],
                 blocked: set[str], replan_result: dict,
                 defaults: dict, run_dir: str, seq: int) -> dict:
    """Apply replan: remove pending nodes, add new ones. Validates and rolls back on error."""
    removed = []
    for name in replan_result.get("remove", []):
        if name in nodes and results[name].status == Status.PENDING:
            del nodes[name]
            del results[name]
            blocked.discard(name)
            # clean deps referencing removed nodes
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

    # validate resulting DAG
    errors = validate_workflow(nodes)
    if errors:
        # rollback: remove added, restore removed
        for name in added:
            del nodes[name]
            del results[name]
        # cannot fully restore removed nodes — log warning
        _log(f"[replan] rejected (validation errors): {errors}")
        if removed:
            _log(f"[replan] warning: {len(removed)} removed nodes lost in rollback")
        return {"seq": seq, "added": [], "removed": []}

    event = {"seq": seq, "added": added, "removed": removed}
    _save_json(os.path.join(run_dir, f"replan-{seq}.json"), event)
    return event

def run_dag(wf: dict, nodes: dict[str, Node], repo_dir: str,
            dry_run: bool = False, from_node: str | None = None) -> dict[str, NodeResult]:
    """Execute the full DAG with dynamic scheduling and adaptive replanning."""
    run_id  = time.strftime("%Y%m%d-%H%M%S")
    run_dir = os.path.join(repo_dir, ".dage", "runs", run_id)
    os.makedirs(run_dir, exist_ok=True)

    results: dict[str, NodeResult] = {name: NodeResult() for name in nodes}
    blocked: set[str] = set()

    # load prior results for --from resume
    if from_node:
        results, blocked = _load_resume_state(nodes, from_node, repo_dir)
        resumed = [n for n, r in results.items() if r.status == Status.SUCCESS]
        if resumed:
            _log(f"resumed: {sorted(resumed)}")

    # replan config
    replan_cfg   = wf.get("replan", {})
    max_replans  = replan_cfg.get("max_replans", 3)
    max_nodes    = replan_cfg.get("max_nodes", 50)
    replan_count = 0

    # snapshot original nodes for audit trail
    _save_json(os.path.join(run_dir, "original-nodes.json"),
               {n: _node_to_dict(nodes[n]) for n in nodes})

    _log(f"run {run_id}  nodes={len(nodes)}")
    if dry_run:
        _log("[dry-run mode]")
    _log("")

    try:
        with ThreadPoolExecutor() as pool:
            while True:
                layer = next_runnable(nodes, results, blocked)
                if not layer:
                    break

                # phase 1: filter condition (serial, pure logic)
                to_run = []
                ctx = build_context(wf, results, run_id)
                for name in layer:
                    node = nodes[name]

                    if should_skip(node, ctx):
                        results[name] = NodeResult(status=Status.SKIPPED,
                                                   output="condition not met")
                        _log(f"[{name}] SKIPPED (condition)")
                        continue

                    role_tag = node.role.value.upper()
                    _log(f"[{name}] {role_tag} ({node.type.value}) ...")
                    to_run.append(name)

                if not to_run:
                    continue  # all skipped by condition, loop picks up next wave

                # auto-worktree for parallel claude nodes
                claude_no_wt = [n for n in to_run
                                if nodes[n].type == NodeType.CLAUDE
                                and not nodes[n].worktree]
                auto_wt = ({n: f"dage-{run_id}-{n}" for n in claude_no_wt}
                           if len(claude_no_wt) > 1 else {})
                if auto_wt:
                    _log(f"  auto-worktree: {sorted(auto_wt)}")

                # phase 2: parallel execution
                futures = {
                    pool.submit(execute_node, nodes[n], ctx, run_dir,
                                run_id, repo_dir, dry_run,
                                worktree=auto_wt.get(n, "")): n
                    for n in to_run
                }
                for fut in as_completed(futures):
                    name = futures[fut]
                    results[name] = fut.result()
                    r = results[name]
                    status_icon = "ok" if r.status == Status.SUCCESS else "FAIL"
                    _log(f"[{name}] {status_icon}  {r.duration:.1f}s"
                         + (f"  retries={r.retries}" if r.retries else ""))

                # phase 3: gate propagation — mark blocked nodes SKIPPED immediately
                for name in to_run:
                    if nodes[name].role == Role.GATE and results[name].status == Status.FAILED:
                        downstream = find_blocked(nodes, name)
                        blocked |= downstream
                        _log(f"[{name}] gate failed -> blocking {sorted(downstream)}")
                        for b in downstream:
                            if results.get(b, NodeResult()).status == Status.PENDING:
                                results[b] = NodeResult(status=Status.SKIPPED,
                                                        output="blocked by failed gate")
                                _log(f"[{b}] SKIPPED (gate)")

                # phase 4: adaptive replan check
                if replan_count < max_replans and len(nodes) < max_nodes:
                    signal = detect_replan(nodes, results, to_run)
                    if signal:
                        trigger, reason = signal
                        _log(f"[replan {replan_count+1}/{max_replans}] "
                             f"triggered by '{trigger}': {reason}")
                        replan_result = call_replanner(
                            wf, nodes, results, trigger, reason,
                            replan_count + 1, run_dir)
                        if replan_result:
                            event = apply_replan(
                                nodes, results, blocked, replan_result,
                                wf.get("defaults", {}), run_dir, replan_count + 1)
                            replan_count += 1
                            _log(f"[replan] +{len(event['added'])} "
                                 f"-{len(event['removed'])} nodes")

    except KeyboardInterrupt:
        _log("\n[interrupted] saving progress...")

    # save state (both normal exit and interrupt)
    save_state(run_dir, results)
    _log("")
    print_summary(results)
    save_latest_link(repo_dir, run_id)

    return results

def _load_resume_state(nodes: dict[str, Node], from_node: str,
                       repo_dir: str) -> tuple[dict[str, NodeResult], set[str]]:
    """Load results from latest run for --from resume."""
    results = {name: NodeResult() for name in nodes}
    blocked: set[str] = set()

    latest = _find_latest_run(repo_dir)
    if not latest:
        _log(f"warning: no prior run found, starting from scratch")
        return results, blocked

    state_file = os.path.join(latest, "results.json")
    if not os.path.exists(state_file):
        return results, blocked

    with open(state_file) as f:
        saved = json.load(f)

    # mark all nodes before from_node as their saved status
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
                    output   = "",  # don't carry full output, re-read if needed
                    duration = s.get("duration", 0),
                )
        if reached:
            break

    return results, blocked

# ==== State Persistence ====================================================

def _save_json(path: str, data):
    """Write data as formatted JSON."""
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def _node_to_dict(node: Node) -> dict:
    """Serialize Node to plain dict for JSON snapshots."""
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
    """Save run results to JSON."""
    data = {name: r.to_dict() for name, r in results.items()}
    path = os.path.join(run_dir, "results.json")
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def save_latest_link(repo_dir: str, run_id: str):
    """Write latest run ID for quick lookup."""
    path = os.path.join(repo_dir, ".dage", "latest")
    with open(path, "w") as f:
        f.write(run_id)

def _find_latest_run(repo_dir: str) -> str | None:
    latest_file = os.path.join(repo_dir, ".dage", "latest")
    if os.path.exists(latest_file):
        run_id = open(latest_file).read().strip()
        run_dir = os.path.join(repo_dir, ".dage", "runs", run_id)
        if os.path.isdir(run_dir):
            return run_dir
    return None

# ==== Output ===============================================================

def _log(msg: str):
    print(msg, file=sys.stderr)

def print_summary(results: dict[str, NodeResult]):
    """Print execution summary table."""
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
    """Print dry-run execution plan."""
    layers = topo_layers(nodes)
    _log("Execution plan:")
    _log("")
    for i, layer in enumerate(layers):
        _log(f"  layer {i}:")
        for name in layer:
            node = nodes[name]
            deps = f" <- [{', '.join(node.deps)}]" if node.deps else ""
            adapt = " [adaptive]" if node.adaptive else ""
            _log(f"    {name} ({node.type.value}/{node.role.value}){adapt}{deps}")
    _log("")

def print_status(repo_dir: str):
    """Print status of the latest run."""
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
    run_id = os.path.basename(run_dir)
    _log(f"latest run: {run_id}")
    _log("")
    _log(f"{'Node':<20} {'Status':<10} {'Time':>8}  {'Retries':>7}")
    _log("-" * 60)
    for name, r in data.items():
        _log(f"{name:<20} {r['status']:<10} {r['duration']:>7.1f}s  {r['retries']:>7}")
    _log("-" * 60)

# ==== Plan Generation ======================================================

_PLAN_PROMPT = """\
You are a workflow planner for dage, a DAG-based workflow orchestrator.
Turn the task description into a valid dage YAML workflow.

""" + _DAGE_KNOWLEDGE.replace("{{", "{").replace("}}", "}") + """
Additional schema fields (plan-only):
  condition: "expr"             # skip if false
  adaptive: true                # enable replan signal detection (default: false)
  vars:
    key: value

Interpolation: ${vars.KEY}, ${nodes.NAME.output}, ${nodes.NAME.status}

Example — codebase analysis + implementation pipeline:
  nodes:
    scan:
      role: context
      max_runs: 8
      prompt: |
        Scan the codebase structure, key modules, build system, and test coverage.
        Be thorough — read actual files, don't guess.
    read_docs:
      role: context
      max_runs: 3
      prompt: |
        Read docs/design.md and docs/implementation-plan.md.
        Summarize architecture, key decisions, and implementation tasks.
    implement:
      deps: [scan, read_docs]
      max_runs: 10
      timeout: 1h
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
      max_runs: 1
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


_BRAINSTORM_PROMPT = """\
You are a workflow architect. Analyze the task and design a DAG execution plan.
Think step by step, making all decisions autonomously.

1. DECOMPOSE: Break the task into concrete subtasks.
2. CLASSIFY each subtask:
   - claude (AI reasoning/analysis/coding) or shell (deterministic command)?
   - role: context (gather info), produce (create artifacts), gate (verify), meta (report)?
3. DEPENDENCIES: Which subtasks need outputs from others? Be precise — only add
   a dependency when subtask B actually reads subtask A's output.
4. PARALLELISM: Which subtasks are independent? Maximize concurrent execution.
5. GATES: After every implementation/coding subtask, add a shell verification
   step (test/build/lint) as a gate. Gate failure blocks all downstream work.
6. RESOURCE ESTIMATE: For each claude subtask, estimate complexity:
   - Light (reading/summarizing): max_runs 5, timeout 30m
   - Medium (analysis/planning): max_runs 8, timeout 30m
   - Heavy (implementation/coding): max_runs 10+, timeout 45m-1h

Output a structured design document. Be specific about what each subtask does,
what it reads as input, and what it produces as output.

Task: """


def _call_claude(prompt: str, timeout: int = 120) -> str:
    """Call claude CLI with a prompt, return stdout text."""
    try:
        proc = subprocess.run(
            ["claude", "-p", prompt, "--output-format", "text"],
            capture_output=True, text=True, timeout=timeout,
        )
    except FileNotFoundError:
        raise RuntimeError("'claude' CLI not found — install Claude Code first")
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"claude timed out ({timeout}s)")
    if proc.returncode != 0:
        raise RuntimeError(f"claude failed: {proc.stderr.strip()}")
    return proc.stdout.strip()


def generate_plan(description: str) -> tuple[str, str]:
    """Two-phase plan generation: brainstorm design, then generate YAML.

    Returns (yaml_text, design_text).
    """
    # phase 1: brainstorm — explore task space, make design decisions
    _log("  phase 1: brainstorming...")
    design = _call_claude(_BRAINSTORM_PROMPT + description, timeout=120)
    _log(f"  design: {len(design)} chars")

    # phase 2: generate YAML from design + schema
    _log("  phase 2: generating YAML...")
    gen_prompt = _PLAN_PROMPT + (
        f"\nDesign document (from brainstorming phase):\n{design}\n\n"
        f"Original task: {description}"
    )
    raw = _call_claude(gen_prompt, timeout=120)
    return _extract_yaml(raw), design


def _extract_yaml(text: str) -> str:
    """Strip markdown fences if present."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    return text


# ==== CLI ==================================================================

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
    p_plan.add_argument("description", help="task description in natural language")
    p_plan.add_argument("-o", "--output", default="workflow.yaml",
                        help="output file (default: workflow.yaml)")

    args = parser.parse_args()

    if args.command == "run":
        wf    = load_workflow(args.workflow)
        nodes = build_nodes(wf)
        errors = validate_workflow(nodes)
        if errors:
            for e in errors:
                _log(f"error: {e}")
            sys.exit(1)

        # resolve repo_dir from vars or CLI
        repo_dir = os.path.abspath(
            wf.get("vars", {}).get("repo_dir", args.repo_dir)
        )

        if args.dry_run:
            print_plan(nodes)
            return

        results = run_dag(wf, nodes, repo_dir,
                          from_node=args.from_node)
        # exit 130 if interrupted (pending nodes remain)
        if any(r.status == Status.PENDING for r in results.values()):
            sys.exit(130)
        # exit 1 if any non-skipped node failed
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
        repo_dir = os.path.abspath(args.repo_dir)
        print_status(repo_dir)

    elif args.command == "plan":
        _log("generating workflow...")
        try:
            raw, design = generate_plan(args.description)
        except RuntimeError as e:
            _log(f"error: {e}")
            sys.exit(1)

        # save brainstorm design to .dage/plans/
        plan_dir = os.path.join(".dage", "plans")
        os.makedirs(plan_dir, exist_ok=True)
        design_file = os.path.join(plan_dir,
            f"{time.strftime('%Y%m%d-%H%M%S')}-design.md")
        with open(design_file, "w") as f:
            f.write(f"# Design: {args.description}\n\n{design}\n")
        _log(f"  design: {design_file}")

        # validate and preview
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
