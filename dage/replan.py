import os
import re
import sys

import yaml

from dage.models import Node, NodeResult, Status, save_json
from dage.workflow import validate_workflow, _build_one_node, extract_yaml
from dage.executor import call_claude
from dage.prompts import REPLAN_PROMPT, DAGE_KNOWLEDGE
from dage.tui import log

# ==== Replan Detection

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

# ==== Replanner

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

    prompt = REPLAN_PROMPT.format(
        dage_knowledge = DAGE_KNOWLEDGE.replace("{{", "{").replace("}}", "}"),
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
        raw = call_claude(prompt, timeout=1800)
        raw = extract_yaml(raw)
        result = yaml.safe_load(raw)
        if not isinstance(result, dict):
            log("[replan] invalid response (not a dict), skipping")
            return None
        with open(os.path.join(run_dir, f"replan-{replan_seq}-raw.yaml"), "w") as f:
            f.write(raw)
        return result
    except Exception as e:
        log(f"[replan] replanner failed: {e}")
        return None

# ==== Replan Application

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
                log(f"[replan] failed to build node '{name}': {e}")

    errors = validate_workflow(nodes)
    if errors:
        for name in added:
            del nodes[name]
            del results[name]
        log(f"[replan] rejected (validation errors): {errors}")
        if removed:
            log(f"[replan] warning: {len(removed)} removed nodes lost in rollback")
        return {"seq": seq, "added": [], "removed": []}

    event = {"seq": seq, "added": added, "removed": removed,
             "justification": replan_result.get("justification", "")}
    save_json(os.path.join(run_dir, f"replan-{seq}.json"), event)
    return event

# ==== Replan UI

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
            log("  [confirm] stdin not a tty, auto-approving")
            return True
        log("  approve? [Y/n] ")
        answer = input().strip().lower()
        return answer in ("", "y", "yes")
    except (EOFError, KeyboardInterrupt):
        return False
