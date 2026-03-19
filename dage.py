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

def build_nodes(wf: dict) -> dict[str, Node]:
    """Build Node objects from workflow dict, applying defaults."""
    defaults = wf.get("defaults", {})
    def_type     = defaults.get("type", "claude")
    def_max_runs = defaults.get("max_runs", 5)
    def_timeout  = defaults.get("timeout", "")

    nodes = {}
    for name, spec in wf["nodes"].items():
        if not isinstance(spec, dict):
            raise ValueError(f"node '{name}': spec must be a mapping")
        nodes[name] = Node(
            name     = name,
            type     = NodeType(spec.get("type", def_type)),
            role     = Role(spec.get("role", "produce")),
            deps     = spec.get("deps", []),
            prompt   = spec.get("prompt", ""),
            cmd      = spec.get("cmd", ""),
            condition= spec.get("condition", ""),
            max_runs = spec.get("max_runs", def_max_runs),
            worktree = spec.get("worktree", ""),
            timeout  = spec.get("timeout", def_timeout),
            retry    = spec.get("retry", 0),
        )
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

def run_shell(node: Node, cmd: str, cwd: str | None = None) -> NodeResult:
    """Execute a shell command node."""
    t0 = time.monotonic()
    try:
        proc = subprocess.run(
            cmd, shell=True, cwd=cwd,
            capture_output=True, text=True, timeout=_parse_timeout(node.timeout),
        )
        elapsed = time.monotonic() - t0
        output = proc.stdout.strip()
        if proc.returncode != 0 and proc.stderr:
            output += f"\n[stderr] {proc.stderr.strip()}"
        return NodeResult(
            status   = Status.SUCCESS if proc.returncode == 0 else Status.FAILED,
            output   = output,
            duration = elapsed,
        )
    except subprocess.TimeoutExpired:
        return NodeResult(status=Status.FAILED, output="[timeout]",
                          duration=time.monotonic() - t0)

def run_claude(node: Node, prompt: str, run_dir: str, run_id: str,
               repo_dir: str) -> NodeResult:
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
    if node.worktree:
        cmd += ["--worktree", node.worktree]
    if node.timeout:
        cmd += ["--max-duration", node.timeout]

    t0 = time.monotonic()
    try:
        timeout_s = _parse_timeout(node.timeout)
        # add buffer beyond ccx's own timeout so ccx can clean up
        outer_timeout = timeout_s + 120 if timeout_s else None
        proc = subprocess.run(
            cmd, cwd=repo_dir,
            capture_output=True, text=True, timeout=outer_timeout,
        )
        elapsed = time.monotonic() - t0

        # read output from notes file
        notes_path = Path(repo_dir) / notes_file
        output = notes_path.read_text().strip() if notes_path.exists() else ""

        # save ccx log
        log_path = os.path.join(node_dir, "ccx.log")
        with open(log_path, "w") as f:
            f.write(f"=== stdout ===\n{proc.stdout}\n")
            f.write(f"=== stderr ===\n{proc.stderr}\n")
            f.write(f"=== returncode: {proc.returncode} ===\n")

        return NodeResult(
            status   = Status.SUCCESS if proc.returncode == 0 else Status.FAILED,
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
                 repo_dir: str, dry_run: bool = False) -> NodeResult:
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
            result = run_claude(node, prompt_or_cmd, run_dir, run_id, repo_dir)

        last_result = result
        last_result.retries = attempt

        if result.status == Status.SUCCESS:
            return result

        if attempt < max_attempts - 1:
            _log(f"  retry {attempt + 1}/{node.retry} for '{node.name}'...")

    return last_result

def run_dag(wf: dict, nodes: dict[str, Node], repo_dir: str,
            dry_run: bool = False, from_node: str | None = None) -> dict[str, NodeResult]:
    """Execute the full DAG in topological order."""
    run_id  = time.strftime("%Y%m%d-%H%M%S")
    run_dir = os.path.join(repo_dir, ".dage", "runs", run_id)
    os.makedirs(run_dir, exist_ok=True)

    results: dict[str, NodeResult] = {name: NodeResult() for name in nodes}
    blocked: set[str] = set()

    # load prior results for --from resume
    if from_node:
        results, blocked = _load_resume_state(nodes, from_node, repo_dir)

    layers = topo_layers(nodes)

    _log(f"run {run_id}  nodes={len(nodes)}  layers={len(layers)}")
    if dry_run:
        _log("[dry-run mode]")
    _log("")

    with ThreadPoolExecutor() as pool:
        for layer_idx, layer in enumerate(layers):
            # phase 1: filter skip/blocked/condition (serial, pure logic)
            to_run = []
            ctx = build_context(wf, results, run_id)
            for name in layer:
                node = nodes[name]

                if from_node and results[name].status == Status.SUCCESS:
                    _log(f"[{name}] skip (resumed)")
                    continue

                if name in blocked:
                    results[name] = NodeResult(status=Status.SKIPPED,
                                               output="blocked by failed gate")
                    _log(f"[{name}] SKIPPED (gate)")
                    continue

                if should_skip(node, ctx):
                    results[name] = NodeResult(status=Status.SKIPPED,
                                               output="condition not met")
                    _log(f"[{name}] SKIPPED (condition)")
                    continue

                role_tag = node.role.value.upper()
                _log(f"[{name}] {role_tag} ({node.type.value}) ...")
                to_run.append(name)

            # phase 2: parallel execution
            futures = {
                pool.submit(execute_node, nodes[n], ctx, run_dir,
                            run_id, repo_dir, dry_run): n
                for n in to_run
            }
            for fut in as_completed(futures):
                name = futures[fut]
                results[name] = fut.result()
                r = results[name]
                status_icon = "ok" if r.status == Status.SUCCESS else "FAIL"
                _log(f"[{name}] {status_icon}  {r.duration:.1f}s"
                     + (f"  retries={r.retries}" if r.retries else ""))

            # phase 3: gate propagation after whole layer completes
            for name in to_run:
                if nodes[name].role == Role.GATE and results[name].status == Status.FAILED:
                    downstream = find_blocked(nodes, name)
                    blocked |= downstream
                    _log(f"[{name}] gate failed -> blocking {sorted(downstream)}")

    # save state
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
            _log(f"    {name} ({node.type.value}/{node.role.value}){deps}")
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

Schema:
  nodes:
    <name>:                         # snake_case
      type: shell | claude          # shell=command, claude=AI reasoning
      role: produce|context|gate|evaluate|gc  # gate failure blocks downstream
      deps: [a, b]                  # data/order dependencies
      cmd: "..."                    # shell nodes
      prompt: "..."                 # claude nodes, supports interpolation
      condition: "expr"             # skip if false
      retry: N                      # optional retry count
      timeout: "30m"                # e.g. 1h, 5m, 30s
  vars:
    key: value                      # global vars

Interpolation: ${vars.KEY}, ${nodes.NAME.output}, ${nodes.NAME.status}

Rules:
- deps only when B needs A's output or A must succeed first
- maximize parallelism: independent tasks have no deps between them
- gate role for checks that must pass (tests, lint, validation)
- shell for deterministic commands, claude for reasoning/analysis
- short descriptive snake_case node names

Output ONLY valid YAML. No fences, no commentary.

Task: """


def generate_plan(description: str) -> str:
    """Call claude CLI to generate a workflow YAML from description."""
    prompt = _PLAN_PROMPT + description
    try:
        proc = subprocess.run(
            ["claude", "-p", prompt, "--output-format", "text"],
            capture_output=True, text=True, timeout=120,
        )
    except FileNotFoundError:
        raise RuntimeError("'claude' CLI not found — install Claude Code first")
    except subprocess.TimeoutExpired:
        raise RuntimeError("claude timed out (120s)")

    if proc.returncode != 0:
        raise RuntimeError(f"claude failed: {proc.stderr.strip()}")
    return _extract_yaml(proc.stdout)


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
            raw = generate_plan(args.description)
        except RuntimeError as e:
            _log(f"error: {e}")
            sys.exit(1)

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
