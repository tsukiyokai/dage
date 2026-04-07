import re
from collections import deque
from graphlib import TopologicalSorter, CycleError
from typing import Any

import yaml

from dage.models import Role, NodeType, Status, Node, NodeResult, _ROLE_MAX_RUNS

# ==== Workflow Loading

def load_workflow(path: str) -> dict:
    with open(path) as f:
        wf = yaml.safe_load(f)
    if not isinstance(wf, dict) or "nodes" not in wf:
        raise ValueError("invalid workflow: 'nodes' key required")
    return wf

# ==== Node Building

def _build_one_node(name: str, spec: dict, defaults: dict) -> Node:
    if not isinstance(spec, dict):
        raise ValueError(f"node '{name}': spec must be a mapping")
    role     = Role(spec.get("role", "produce"))
    max_runs = spec.get("max_runs", defaults.get("max_runs", 0))
    if max_runs == 0 and role in _ROLE_MAX_RUNS:
        max_runs = _ROLE_MAX_RUNS[role]
    return Node(
        name      = name,
        type      = NodeType(spec.get("type", defaults.get("type", "claude"))),
        role      = role,
        deps      = spec.get("deps", []),
        soft_deps = spec.get("soft_deps", []),
        prompt    = spec.get("prompt", ""),
        cmd       = spec.get("cmd", ""),
        condition = spec.get("condition", ""),
        max_runs  = max_runs,
        worktree  = spec.get("worktree", ""),
        timeout   = spec.get("timeout", defaults.get("timeout", "")),
        retry     = spec.get("retry", 0),
        adaptive  = spec.get("adaptive", False),
        skills    = spec.get("skills", defaults.get("skills", [])),
        outputs   = spec.get("outputs", []),
    )

def build_nodes(wf: dict) -> dict[str, Node]:
    defaults = wf.get("defaults", {})
    return {name: _build_one_node(name, spec, defaults)
            for name, spec in wf["nodes"].items()}

# ==== Validation

def validate_workflow(nodes: dict[str, Node]) -> list[str]:
    """Returns list of errors (empty = valid)."""
    errors = []
    for name, node in nodes.items():
        for dep in node.deps:
            if dep not in nodes:
                errors.append(f"node '{name}': unknown dep '{dep}'")
        for dep in node.soft_deps:
            if dep not in nodes:
                errors.append(f"node '{name}': unknown soft_dep '{dep}'")
        if node.type == NodeType.CLAUDE and not node.prompt:
            errors.append(f"node '{name}': claude node requires 'prompt'")
        if node.type == NodeType.SHELL and not node.cmd:
            errors.append(f"node '{name}': shell node requires 'cmd'")
        if node.role == Role.PRODUCE and not node.outputs:
            from dage.tui import log
            log(f"  warn: produce node '{name}' has no outputs declared")
    graph = {name: set(node.deps) for name, node in nodes.items()}
    try:
        ts = TopologicalSorter(graph)
        ts.prepare()
    except CycleError as e:
        errors.append(f"cycle detected: {e}")
    return errors

# ==== Variable Interpolation

_max_output: int = 0

def set_max_output(n: int):
    global _max_output
    _max_output = n

def get_max_output() -> int:
    return _max_output

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
            elif part == "status":    cur = cur.status.value
            elif part == "changeset": cur = cur.changeset
            elif part == "artifacts":
                cur = "\n".join(a["path"] for a in cur.artifacts) if cur.artifacts else ""
            else: return f"<unresolved:{path}>"
        else:
            return f"<unresolved:{path}>"
    return str(cur) if cur is not None else ""

def interpolate(template: str, ctx: dict) -> str:
    """Replace ${...} references with values from context.

    Supports per-reference truncation: ${nodes.X.output:300} truncates to 300 chars.
    """
    def _replace(m: re.Match) -> str:
        expr = m.group(1)
        limit = 0
        if ":" in expr:
            expr, suffix = expr.rsplit(":", 1)
            if suffix.isdigit():
                limit = int(suffix)
        text = _resolve_path(ctx, expr)
        if limit and len(text) > limit:
            text = text[:limit] + f"\n[…{len(text)} chars total]"
        return text
    return re.sub(r'\$\{([^}]+)\}', _replace, template)

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

# ==== YAML Extraction

def extract_yaml(text: str) -> str:
    m = re.search(r'```(?:ya?ml)?\s*\n(.+?)```', text, re.DOTALL)
    if m:
        candidate = m.group(1).strip()
    else:
        lines = text.strip().split("\n")
        start = None
        for i, line in enumerate(lines):
            s = line.strip()
            if (s and ":" in s and not s.startswith("`")) or s.startswith("---"):
                start = i
                break
        if start is None:
            raise ValueError("no YAML structure found in AI output")
        end = len(lines)
        for i in range(start + 1, len(lines)):
            s = lines[i].strip()
            if s.startswith("`"):
                end = i
                break
        candidate = "\n".join(lines[start:end]).strip()
    try:
        parsed = yaml.safe_load(candidate)
    except yaml.YAMLError as e:
        raise ValueError(f"extracted text is not valid YAML: {e}") from e
    if not isinstance(parsed, dict):
        raise ValueError("extracted YAML is not a mapping")
    return candidate
