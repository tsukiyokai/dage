from __future__ import annotations

import glob as _glob
import json
import os
import re
import shutil
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

from dage.models import (Node, NodeResult, NodeType, Role, Status,
                          save_json, node_to_dict, node_artifact_dir)
from dage.workflow import (load_workflow, _build_one_node, build_nodes,
                           validate_workflow, interpolate, set_max_output,
                           topo_layers, next_runnable, find_blocked)
from dage.executor import (execute_node, run_claude, run_shell,
                           kill_active_procs, register_signal_handlers,
                           call_claude)
from dage.git_ops import merge_worktrees, prune_worktrees, auto_commit, default_branch
from dage.replan import (call_replanner, apply_replan,
                         _format_replan_proposal, _confirm_replan)
from dage.tui import (log, set_display, get_display, DageDisplay, _HAS_RICH,
                       print_summary)
from dage.prompts import (AUTOFIX_PROMPT, ANNOTATE_PROMPT, REFLECT_PROMPT,
                          PRODUCE_REFLECT_PROMPT,
                          LONG_REPORT_PROMPT, SHORT_REPORT_PROMPT)

# ==== Execution Context

def build_context(wf: dict, results: dict[str, NodeResult], run_id: str,
                  discoveries: list[tuple[str, str]] | None = None) -> dict:
    disc_text = ""
    if discoveries:
        disc_text = "\n".join(f"[{src}] {text}" for src, text in discoveries[-20:])
    return {
        "vars":  wf.get("vars", {}),
        "nodes": results,
        "run":   {"id": run_id, "summary": _build_summary(results),
                  "default_branch": default_branch(os.getcwd()),
                  "discoveries": disc_text},
    }


def _detect_discoveries(node_name: str, output: str) -> list[tuple[str, str]]:
    """Extract [DISCOVERY: ...] signals from node output."""
    return [(node_name, m.group(1).strip())
            for m in re.finditer(r'\[DISCOVERY:\s*(.+?)\]', output)]

def _surface_outputs_from_worktree(node: Node, wt_name: str,
                                    repo_dir: str) -> list[str]:
    """Copy declared outputs from worktree to main repo before git merge.

    This ensures output files survive even if git merge aborts due to
    conflicts on shared files (e.g. ccx_notes.md).
    """
    from dage.git_ops import worktree_path
    wt_path = worktree_path(repo_dir, wt_name)
    if not os.path.isdir(wt_path):
        return []
    surfaced: list[str] = []
    for pattern in node.outputs:
        for src in sorted(_glob.glob(os.path.join(wt_path, pattern),
                                      recursive=True)):
            if not os.path.isfile(src):
                continue
            rel = os.path.relpath(src, wt_path)
            dst = os.path.join(repo_dir, rel)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copy2(src, dst)
            surfaced.append(rel)
    if surfaced:
        log(f"  surface: {node.name} -> {len(surfaced)} files")
    return surfaced

def _collect_artifacts(node: Node, repo_dir: str) -> list[dict]:
    """Resolve output glob patterns and collect file metadata."""
    artifacts: list[dict] = []
    seen: set[str] = set()
    for pattern in node.outputs:
        for path in sorted(_glob.glob(os.path.join(repo_dir, pattern),
                                      recursive=True)):
            rel = os.path.relpath(path, repo_dir)
            if rel in seen or not os.path.isfile(path):
                continue
            seen.add(rel)
            size = os.path.getsize(path)
            try:
                with open(path) as f:
                    lines = sum(1 for _ in f)
            except (UnicodeDecodeError, OSError):
                lines = 0
            artifacts.append({"path": rel, "size": size, "lines": lines})
    return artifacts

def _build_summary(results: dict[str, NodeResult]) -> str:
    return "\n".join(f"  {n}: {r.status.value} ({r.duration:.0f}s)"
                     for n, r in results.items())

# ==== Condition Evaluation

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

# ==== Design Doc Annotation

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

    prompt = ANNOTATE_PROMPT.format(
        design_docs  = ", ".join(design_docs),
        impl_summary = impl_summary or "(no details)",
        date         = time.strftime("%Y-%m-%d"),
    )

    fix_node = Node(name=f"_annotate_{gate_name}", type=NodeType.CLAUDE,
                    role=Role.PRODUCE, prompt=prompt, max_runs=1,
                    skills=wf.get("defaults", {}).get("skills", []))
    log(f"[_annotate_{gate_name}] reviewing design docs ...")
    result = run_claude(fix_node, prompt, run_dir, run_id, repo_dir)
    log(f"[_annotate_{gate_name}] {'ok' if result.status == Status.SUCCESS else 'skip'}"
        f"  {result.duration:.1f}s")

# ==== Gate Reflection

def _build_gate_context(gate: Node, gate_result: NodeResult,
                        nodes: dict[str, Node], results: dict[str, NodeResult],
                        ctx: dict, run_dir: str, repo_dir: str) -> dict:
    """Build rich context for gate failure analysis."""
    upstream = "\n".join(
        f"[{d}] goal: {nodes[d].prompt[:2000]}"
        for d in gate.deps if d in nodes and nodes[d].prompt)

    # read changesets from .patch files
    changeset_parts = []
    for d in gate.deps:
        patch_path = os.path.join(node_artifact_dir(run_dir, d), "patch")
        if os.path.exists(patch_path):
            with open(patch_path) as f:
                p = f.read()
            if len(p) > 5000:
                p = p[:5000] + f"\n... ({len(p)} chars total)"
            changeset_parts.append(f"=== {d} ===\n{p}")
        elif d in results and results[d].changeset:
            changeset_parts.append(f"=== {d} (stat only) ===\n{results[d].changeset}")

    # file status from declared outputs
    file_parts: list[str] = []
    for d in gate.deps:
        if d not in nodes or not nodes[d].outputs:
            continue
        found_any = False
        for pattern in nodes[d].outputs:
            for path in sorted(_glob.glob(os.path.join(repo_dir, pattern),
                                          recursive=True)):
                if os.path.isfile(path):
                    rel = os.path.relpath(path, repo_dir)
                    file_parts.append(f"  {rel} ({os.path.getsize(path)}B)")
                    found_any = True
        if not found_any:
            file_parts.append(f"  {d}: {nodes[d].outputs} (none found)")

    upstream_names = [d for d in gate.deps if d in nodes]
    return {
        "cmd":                  interpolate(gate.cmd, ctx),
        "error_output":         gate_result.output[-3000:],
        "upstream_context":     upstream or "(none)",
        "changeset_context":    "\n\n".join(changeset_parts) or "(no changesets)",
        "file_status":          "\n".join(file_parts) or "(no outputs declared)",
        "valid_upstream_names": ", ".join(upstream_names),
        "upstream_names":       upstream_names,
    }


def _parse_reflection(output: str, valid_upstreams: list[str]) -> tuple[str, str | None]:
    """Parse reflection output for action classification."""
    m = re.search(r'\[RERUN:(\w+)\]', output)
    if m and m.group(1).strip() in valid_upstreams:
        return "RERUN", m.group(1).strip()
    m = re.search(r'\[REPLAN:\s*(.+?)\]', output)
    if m:
        return "REPLAN", m.group(1).strip()
    return "LOCAL_FIX", None


def _reflect_on_gate_failure(gate: Node, gate_result: NodeResult,
                             nodes: dict[str, Node], results: dict[str, NodeResult],
                             ctx: dict, wf: dict,
                             run_dir: str, run_id: str,
                             repo_dir: str) -> tuple[str, str | None, NodeResult]:
    """Analyze gate failure, classify root cause, attempt fix if LOCAL_FIX.

    Returns (action, detail, fix_result):
        action: "LOCAL_FIX" | "REPLAN" | "RERUN"
        detail: None | reason | node_name
        fix_result: result of retry gate (only meaningful for LOCAL_FIX)
    """
    gc = _build_gate_context(gate, gate_result, nodes, results, ctx, run_dir, repo_dir)
    prompt = REFLECT_PROMPT.format(**{k: gc[k] for k in gc if k != "upstream_names"})

    defaults = wf.get("defaults", {})
    reflect_node = Node(
        name=f"_reflect_{gate.name}", type=NodeType.CLAUDE, role=Role.PRODUCE,
        prompt=prompt, max_runs=defaults.get("max_runs", 0),
        timeout="10m", skills=defaults.get("skills", []))

    log(f"[_reflect_{gate.name}] analyzing gate failure ...")
    result = run_claude(reflect_node, prompt, run_dir, run_id, repo_dir)
    log(f"[_reflect_{gate.name}] {result.duration:.1f}s")

    action, detail = _parse_reflection(result.output, gc["upstream_names"])
    log(f"[_reflect_{gate.name}] verdict: {action}" +
        (f" ({detail})" if detail else ""))

    # for LOCAL_FIX, the reflection node already attempted the fix — retry gate
    retry = NodeResult()
    if action == "LOCAL_FIX":
        log(f"[{gate.name}] retrying after reflection fix ...")
        retry = run_shell(gate, gc["cmd"], cwd=repo_dir)
        log(f"[{gate.name}] retry {'ok' if retry.status == Status.SUCCESS else 'FAIL'}"
            f"  {retry.duration:.1f}s")

    return action, detail, retry


def _backtrack_node(target: str, gate_name: str,
                    nodes: dict[str, Node], results: dict[str, NodeResult],
                    run_dir: str, rerun_counts: dict[str, int]) -> bool:
    """Reset a node to PENDING for re-execution. Max 1 rerun per node."""
    if target not in nodes:
        log(f"[backtrack] '{target}' not found")
        return False
    if target not in nodes[gate_name].deps:
        log(f"[backtrack] '{target}' is not a dep of gate '{gate_name}'")
        return False
    if rerun_counts.get(target, 0) >= 1:
        log(f"[backtrack] '{target}' already re-run once")
        return False

    rerun_counts[target] = rerun_counts.get(target, 0) + 1
    results[target] = NodeResult()

    # clean up artifacts from previous run
    target_dir = node_artifact_dir(run_dir, target)
    for f in [os.path.join(target_dir, "patch"),
              os.path.join(target_dir, "notes.md")]:
        if os.path.exists(f):
            os.remove(f)

    log(f"[backtrack] '{target}' reset to PENDING")
    return True


def _trigger_gate_replan(gate_name: str, reason: str,
                         nodes: dict[str, Node], results: dict[str, NodeResult],
                         blocked: set[str], wf: dict, run_dir: str,
                         cfg: dict, replan_count: int) -> int:
    """Trigger replan from gate failure reflection."""
    if replan_count >= cfg["max_replans"] or len(nodes) >= cfg["max_nodes"]:
        log(f"[replan] limit reached")
        return replan_count

    seq = replan_count + 1
    log(f"[replan {seq}/{cfg['max_replans']}] gate '{gate_name}': {reason}")

    if cfg["replan_mode"] == "log":
        save_json(os.path.join(run_dir, f"replan-{seq}-signal.json"),
                  {"seq": seq, "trigger": gate_name, "reason": reason,
                   "source": "gate_reflection"})
        return seq

    replan_result = call_replanner(wf, nodes, results, gate_name, reason, seq, run_dir)
    if not replan_result or not replan_result.get("justification"):
        log("[replan] rejected: no result or justification")
        return seq

    log(f"[replan] proposal:\n{_format_replan_proposal(replan_result)}")
    if cfg["replan_mode"] == "confirm" and not _confirm_replan():
        log("[replan] rejected by user")
        return seq

    event = apply_replan(nodes, results, blocked, replan_result,
                         wf.get("defaults", {}), run_dir, seq)
    display = get_display()
    if display:
        display.replan_count = seq
    log(f"[replan] +{len(event['added'])} -{len(event['removed'])} nodes")
    return seq


# ==== Produce Failure

def _handle_produce_fail(name: str, nodes: dict[str, Node], results: dict[str, NodeResult],
                         reflected: set[str], wf: dict, run_dir: str, run_id: str,
                         repo_dir: str, replan_count: int, cfg: dict,
                         blocked: set[str]) -> int:
    """Handle failed produce node: reflect → retry_focused / replan / skip."""
    if name in reflected:
        return replan_count
    reflected.add(name)

    node = nodes[name]
    # read ccx log for diagnosis
    ccx_log_path = os.path.join(node_artifact_dir(run_dir, node.name), "ccx.log")
    ccx_log = ""
    if os.path.exists(ccx_log_path):
        with open(ccx_log_path) as f:
            ccx_log = f.read()[-3000:]

    prompt = PRODUCE_REFLECT_PROMPT.format(
        node_name      = name,
        role           = node.role.value,
        original_prompt = (node.prompt or node.cmd)[:2000],
        failure_reason = results[name].output,
        ccx_log        = ccx_log or "(no log)",
    )

    defaults = wf.get("defaults", {})
    reflect_node = Node(
        name=f"_reflect_{name}", type=NodeType.CLAUDE, role=Role.PRODUCE,
        prompt=prompt, max_runs=defaults.get("max_runs", 0),
        timeout="10m", skills=defaults.get("skills", []))

    log(f"[_reflect_{name}] analyzing produce failure ...")
    result = run_claude(reflect_node, prompt, run_dir, run_id, repo_dir)
    log(f"[_reflect_{name}] {result.duration:.1f}s")

    output = result.output.strip()
    first_line = output.split("\n")[0] if output else ""

    if "[RETRY_FOCUSED]" in first_line:
        new_prompt = "\n".join(output.split("\n")[1:]).strip()
        if new_prompt:
            log(f"[{name}] retrying with focused prompt ({len(new_prompt)} chars)")
            node.prompt = new_prompt
            results[name] = NodeResult()  # reset to PENDING
        else:
            log(f"[{name}] RETRY_FOCUSED but no new prompt provided")

    elif "[REPLAN:" in first_line:
        m = re.search(r'\[REPLAN:\s*(.+?)\]', first_line)
        reason = m.group(1).strip() if m else "produce node failed"
        log(f"[{name}] reflection suggests replan: {reason}")
        replan_count = _trigger_gate_replan(
            name, reason, nodes, results, blocked,
            wf, run_dir, cfg, replan_count)

    elif "[SKIP]" in first_line:
        log(f"[{name}] reflection says safe to skip")
        results[name] = NodeResult(status=Status.SKIPPED,
                                   output="skipped by reflection: non-critical")
    else:
        log(f"[{name}] reflection gave no actionable tag, keeping failed")

    return replan_count


# ==== Gate Failure

def _handle_gate_fail(name: str, nodes: dict[str, Node], results: dict[str, NodeResult],
                      blocked: set[str], reflected: set[str], cfg: dict,
                      ctx: dict, wf: dict, run_dir: str, run_id: str,
                      repo_dir: str, replan_count: int,
                      rerun_counts: dict[str, int]) -> int:
    """Handle failed gate: reflect → classify → act. Returns updated replan_count."""
    if cfg["autofix"] and name not in reflected:
        reflected.add(name)
        action, detail, retry = _reflect_on_gate_failure(
            nodes[name], results[name], nodes, results,
            ctx, wf, run_dir, run_id, repo_dir)

        if action == "LOCAL_FIX" and retry.status == Status.SUCCESS:
            results[name] = retry
            log(f"[{name}] gate passed after reflection fix")
            return replan_count

        if action == "REPLAN" and detail:
            return _trigger_gate_replan(
                name, detail, nodes, results, blocked,
                wf, run_dir, cfg, replan_count)

        if action == "RERUN" and detail:
            if _backtrack_node(detail, name, nodes, results, run_dir, rerun_counts):
                results[name] = NodeResult()    # reset gate
                reflected.discard(name)          # allow re-reflection
                return replan_count

    # fallback: block downstream
    downstream = find_blocked(nodes, name)
    blocked |= downstream
    log(f"[{name}] gate failed -> blocking {sorted(downstream)}")
    for b in downstream:
        if results.get(b, NodeResult()).status == Status.PENDING:
            results[b] = NodeResult(status=Status.SKIPPED,
                                    output="blocked by failed gate")
            log(f"[{b}] SKIPPED (gate)")
    return replan_count

# ==== Hot Reload

def _hot_reload(yaml_path: str, nodes: dict[str, Node],
                results: dict[str, NodeResult], blocked: set[str],
                wf: dict) -> bool:
    """Reload YAML and apply changes to pending nodes. Returns True if changed."""
    try:
        new_wf    = load_workflow(yaml_path)
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
            log(f"[hot-reload] rejected: {errors}")
            return False

        wf.update(new_wf)

        if added or updated or removed:
            log(f"[hot-reload] +{len(added)} ~{len(updated)} -{len(removed)}")
        return bool(added or updated or removed)
    except Exception as e:
        log(f"[hot-reload] failed: {e}")
        return False

# ==== Config

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

# ==== Replan Handling

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
    log(f"[replan {seq}/{cfg['max_replans']}] "
        f"triggered by '{name}': {reason}")

    if cfg["replan_mode"] == "log":
        log(f"[replan] mode=log, signal recorded but not acted on")
        save_json(os.path.join(run_dir, f"replan-{seq}-signal.json"),
                  {"seq": seq, "trigger": name, "reason": reason, "mode": "log"})
        return replan_count + 1

    replan_result = call_replanner(wf, nodes, results, name, reason, seq, run_dir)
    if not replan_result:
        return replan_count

    justification = replan_result.get("justification", "")
    if not justification:
        log("[replan] rejected: no justification provided")
        return replan_count + 1

    log(f"[replan] proposal:\n{_format_replan_proposal(replan_result)}")

    if cfg["replan_mode"] == "confirm" and not _confirm_replan():
        log("[replan] rejected by user")
        return replan_count + 1

    event = apply_replan(nodes, results, blocked, replan_result,
                         wf.get("defaults", {}), run_dir, seq)
    display = get_display()
    if display:
        display.replan_count = replan_count + 1
    log(f"[replan] +{len(event['added'])} -{len(event['removed'])} nodes")
    return replan_count + 1

# ==== DAG Runner

def run_dag(wf: dict, nodes: dict[str, Node], repo_dir: str,
            dry_run: bool = False, from_node: str | None = None) -> dict[str, NodeResult]:
    """Execute the full DAG with dynamic scheduling and adaptive replanning."""
    run_id  = time.strftime("%Y%m%d-%H%M%S")
    run_dir = os.path.join(repo_dir, ".dage", "runs", run_id)
    os.makedirs(run_dir, exist_ok=True)

    results: dict[str, NodeResult] = {name: NodeResult() for name in nodes}
    blocked: set[str] = set()
    reflected:    set[str]              = set()
    rerun_counts: dict[str, int]       = {}
    discoveries:  list[tuple[str, str]] = []
    cfg = _reload_config(wf)

    max_output = wf.get("max_output", 0)
    set_max_output(max_output)

    max_concurrent = wf.get("max_concurrent", 0) or None

    if from_node:
        results, blocked = _load_resume_state(nodes, from_node, repo_dir)
        resumed = [n for n, r in results.items() if r.status == Status.SUCCESS]
        if resumed:
            log(f"resumed: {sorted(resumed)}")

    replan_count = 0

    save_json(os.path.join(run_dir, "original-nodes.json"),
              {n: node_to_dict(nodes[n]) for n in nodes})

    yaml_path = wf.get("_yaml_path")
    yaml_mtime = os.path.getmtime(yaml_path) if yaml_path else 0

    opts = []
    if max_concurrent: opts.append(f"workers={max_concurrent}")
    if max_output:     opts.append(f"max_output={max_output}")
    opts_str = f"  ({', '.join(opts)})" if opts else ""
    log(f"run {run_id}  nodes={len(nodes)}{opts_str}")
    if dry_run:
        log("[dry-run mode]")
    log("")

    start_time = time.monotonic()
    if _HAS_RICH and sys.stderr.isatty() and not dry_run:
        display = DageDisplay(wf, nodes, results, start_time)
        display.start()
        set_display(display)

    prev_sigterm = register_signal_handlers()

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
                            set_max_output(wf.get("max_output", 0))

                layer = next_runnable(nodes, results, blocked)
                if not layer:
                    break

                # phase 1: condition filter
                to_run = []
                ctx = build_context(wf, results, run_id, discoveries)
                for name in layer:
                    node = nodes[name]
                    if should_skip(node, ctx):
                        results[name] = NodeResult(status=Status.SKIPPED,
                                                   output="condition not met")
                        log(f"[{name}] SKIPPED (condition)")
                        continue
                    log(f"[{name}] {node.role.value.upper()} ({node.type.value}) ...")
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
                    log(f"  auto-worktree: {sorted(auto_wt)}")

                # phase 2: parallel execution with inline gate handling
                for n in to_run:
                    results[n] = NodeResult(status=Status.RUNNING)
                    display = get_display()
                    if display:
                        display.node_start[n] = time.monotonic()
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
                        log(f"[{name}] {icon}  {r.duration:.1f}s"
                            + (f"  retries={r.retries}" if r.retries else ""))

                        # collect discoveries
                        new_disc = _detect_discoveries(name, r.output)
                        if new_disc:
                            discoveries.extend(new_disc)
                            log(f"[{name}] {len(new_disc)} discovery(ies)")

                        # inline failure handling
                        node = nodes[name]
                        if node.role == Role.PRODUCE and r.status == Status.FAILED:
                            replan_count = _handle_produce_fail(
                                name, nodes, results, reflected,
                                wf, run_dir, run_id, repo_dir,
                                replan_count, cfg, blocked)
                        elif node.role == Role.GATE and r.status == Status.FAILED:
                            replan_count = _handle_gate_fail(
                                name, nodes, results, blocked,
                                reflected, cfg, ctx, wf,
                                run_dir, run_id, repo_dir,
                                replan_count, rerun_counts)
                        elif node.role == Role.GATE and r.status == Status.SUCCESS:
                            gates_passed.append(name)

                        # inline replan
                        replan_count = _handle_replan(
                            name, nodes, results, blocked,
                            wf, run_dir, cfg, replan_count)

                # phase 2.5a: surface declared outputs (before merge, survives conflict)
                if auto_wt:
                    for n in to_run:
                        wt = auto_wt.get(n)
                        if wt and nodes[n].outputs:
                            _surface_outputs_from_worktree(nodes[n], wt, repo_dir)

                # phase 2.5b: merge worktree changes back to main
                if auto_wt:
                    merge_worktrees(auto_wt, repo_dir, run_id)

                # phase 2.6: collect declared artifacts
                for n in to_run:
                    r = results[n]
                    if r.status == Status.SUCCESS and nodes[n].outputs:
                        r.artifacts = _collect_artifacts(nodes[n], repo_dir)

                # phase 3: gate success actions (after worktree merge for safe ordering)
                for name in gates_passed:
                    if cfg["do_commit"]:
                        auto_commit(name, nodes, repo_dir, push=cfg["do_push"])
                    if wf.get("design_docs"):
                        _annotate_design_docs(wf, nodes, results, name,
                                              run_dir, run_id, repo_dir)

    except KeyboardInterrupt:
        log("\n[interrupted] killing child processes...")
        kill_active_procs()
    finally:
        signal.signal(signal.SIGTERM, prev_sigterm)
        display = get_display()
        if display:
            display.stop()
            set_display(None)

    save_state(run_dir, results)
    prune_worktrees(repo_dir)

    # clean up ccx residuals
    for residual in ["ccx_notes.md"]:
        p = os.path.join(repo_dir, residual)
        if os.path.exists(p):
            os.remove(p)

    log("")
    print_summary(results)

    # resume hint on failure
    failed = [n for n, r in results.items() if r.status == Status.FAILED]
    if failed and yaml_path:
        log(f"\nresume: dage run {yaml_path} --from {failed[0]}")

    # print report from meta nodes
    for name, node in nodes.items():
        r = results.get(name)
        if r and r.status == Status.SUCCESS and node.role == Role.META and r.output:
            log("")
            log(r.output.strip())

    # built-in reports
    _generate_long_report(run_dir, wf, nodes, results)
    short = _generate_short_report(wf, nodes, results)
    if short:
        log("")
        log(short)

    _postprocess_artifacts(run_dir, run_id, wf, nodes, results, repo_dir)
    save_latest_link(repo_dir, run_id)
    return results

# ==== Resume

def _load_resume_state(nodes: dict[str, Node], from_node: str,
                       repo_dir: str) -> tuple[dict[str, NodeResult], set[str]]:
    results = {name: NodeResult() for name in nodes}
    blocked: set[str] = set()

    latest = _find_latest_run(repo_dir)
    if not latest:
        log("warning: no prior run found, starting from scratch")
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

    # if resuming from a gate, check upstream produce outputs
    # reset produce to PENDING if declared outputs are missing/empty
    if from_node in nodes and nodes[from_node].role == Role.GATE:
        for dep in nodes[from_node].deps:
            if dep not in nodes or nodes[dep].role != Role.PRODUCE:
                continue
            if not nodes[dep].outputs:
                continue
            # check if declared outputs actually exist and are non-empty
            has_outputs = False
            for pattern in nodes[dep].outputs:
                for path in _glob.glob(os.path.join(repo_dir, pattern),
                                       recursive=True):
                    if os.path.isfile(path) and os.path.getsize(path) > 0:
                        has_outputs = True
                        break
                if has_outputs:
                    break
            if not has_outputs:
                log(f"  [{dep}] outputs missing/empty, resetting to PENDING")
                results[dep] = NodeResult()

    return results, blocked

# ==== State Persistence

def save_state(run_dir: str, results: dict[str, NodeResult]):
    save_json(os.path.join(run_dir, "results.json"),
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

# ==== Report Generation

def _build_node_details(nodes: dict[str, Node], results: dict[str, NodeResult],
                        max_output: int = 2000) -> str:
    """Build per-node detail text for report prompts."""
    parts = []
    for layer in topo_layers(nodes):
        for name in layer:
            n = nodes[name]
            r = results.get(name, NodeResult())
            out = (r.output or "").strip()
            if len(out) > max_output:
                out = out[:max_output] + f"\n... ({len(r.output)} chars total, truncated)"
            art = ""
            if r.artifacts:
                art = "\nartifacts:\n" + "\n".join(
                    f"  {a['path']} ({a['size']}B, {a['lines']} lines)"
                    for a in r.artifacts)
            cost = f" cost=${r.cost:.2f}" if r.cost > 0 else ""
            parts.append(
                f"[{name}] type={n.type.value} role={n.role.value} "
                f"status={r.status.value} duration={r.duration:.1f}s retries={r.retries}{cost}\n"
                f"output:\n{out or '(none)'}{art}"
            )
    return "\n\n".join(parts)


def _generate_long_report(run_dir: str, wf: dict, nodes: dict[str, Node],
                          results: dict[str, NodeResult]):
    """Generate detailed markdown report via Claude, save to run_dir/report.md."""
    desc    = wf.get("description", "dage workflow")
    total   = sum(r.duration for r in results.values())
    details = _build_node_details(nodes, results, max_output=2000)
    prompt  = LONG_REPORT_PROMPT.format(
        description  = desc,
        total_time   = total,
        node_details = details,
    )
    try:
        report = call_claude(prompt, timeout=300, quiet=True)
        path   = os.path.join(run_dir, "report.md")
        with open(path, "w") as f:
            f.write(report)
        log(f"report: {path}")
        # surface to workflow directory (user artifact)
        yaml_path = wf.get("_yaml_path")
        if yaml_path:
            stem = os.path.splitext(os.path.basename(yaml_path))[0]
            user_path = os.path.join(os.path.dirname(yaml_path),
                                     f"{stem}-report.md")
            with open(user_path, "w") as f:
                f.write(report)
            log(f"report: {user_path}")
    except Exception as e:
        log(f"[report] long report failed: {e}")


def _generate_short_report(wf: dict, nodes: dict[str, Node],
                           results: dict[str, NodeResult]) -> str | None:
    """Generate concise terminal summary via Claude (猫娘+雌小鬼 style)."""
    desc      = wf.get("description", "dage workflow")
    total     = sum(r.duration for r in results.values())
    total_cost = sum(r.cost for r in results.values())
    details   = _build_node_details(nodes, results, max_output=200)

    # collect all artifacts across nodes
    all_arts = []
    for name, r in results.items():
        for a in r.artifacts:
            all_arts.append(f"  {a['path']} ({a['lines']} lines)")
    art_summary = "\n".join(all_arts) if all_arts else "(none)"

    prompt = SHORT_REPORT_PROMPT.format(
        description  = desc,
        total_time   = total,
        node_details = details,
    )
    if total_cost > 0:
        prompt += f"\nTotal cost: ¥{total_cost * 7.2:.1f}\n"
    prompt += f"\nArtifacts produced:\n{art_summary}\n"
    try:
        return call_claude(prompt, timeout=120, quiet=True)
    except Exception as e:
        log(f"[report] short report failed: {e}")
        return None


# ==== Artifact Post-processing

def _postprocess_artifacts(run_dir: str, run_id: str, wf: dict,
                           nodes: dict[str, Node], results: dict[str, NodeResult],
                           repo_dir: str):
    """Generate manifest and clean up old runs."""
    # 1. manifest: single-file index of everything this run produced
    total_cost = sum(r.cost for r in results.values())
    total_time = sum(r.duration for r in results.values())
    manifest = {
        "run_id":      run_id,
        "description": wf.get("description", ""),
        "total_time":  round(total_time, 1),
        "total_cost":  round(total_cost, 4),
        "nodes": {},
    }
    for layer in topo_layers(nodes):
        for name in layer:
            n = nodes[name]
            r = results.get(name, NodeResult())
            entry = {
                "type":    n.type.value,
                "role":    n.role.value,
                "status":  r.status.value,
                "duration": round(r.duration, 1),
                "retries": r.retries,
            }
            if r.cost > 0:
                entry["cost"] = round(r.cost, 4)
            if r.artifacts:
                entry["artifacts"] = [a["path"] for a in r.artifacts]
            if r.changeset:
                entry["changeset"] = r.changeset
            if os.path.exists(os.path.join(node_artifact_dir(run_dir, name), "patch")):
                entry["patch"] = f"nodes/{name}/patch"
            manifest["nodes"][name] = entry

    # run-level files
    run_files = []
    for fname in ["report.md", "results.json", "original-nodes.json"]:
        if os.path.exists(os.path.join(run_dir, fname)):
            run_files.append(fname)
    if run_files:
        manifest["files"] = run_files

    save_json(os.path.join(run_dir, "manifest.json"), manifest)

    # 2. clean up old runs (keep last 10)
    runs_dir = os.path.join(repo_dir, ".dage", "runs")
    if not os.path.isdir(runs_dir):
        return
    runs = sorted(d for d in os.listdir(runs_dir)
                  if os.path.isdir(os.path.join(runs_dir, d)))
    max_keep = wf.get("max_runs_history", 10)
    for old in runs[:-max_keep]:
        old_path = os.path.join(runs_dir, old)
        try:
            shutil.rmtree(old_path)
            log(f"  pruned old run: {old}")
        except Exception:
            pass
