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
                          save_json, node_to_dict)
from dage.workflow import (load_workflow, _build_one_node, build_nodes,
                           validate_workflow, interpolate, set_max_output,
                           topo_layers, next_runnable, find_blocked)
from dage.executor import (execute_node, run_claude, run_shell,
                           kill_active_procs, register_signal_handlers,
                           call_claude)
from dage.git_ops import merge_worktrees, prune_worktrees, auto_commit
from dage.replan import (call_replanner, apply_replan,
                         _format_replan_proposal, _confirm_replan)
from dage.tui import (log, set_display, get_display, DageDisplay, _HAS_RICH,
                       print_summary)
from dage.prompts import (AUTOFIX_PROMPT, ANNOTATE_PROMPT,
                          LONG_REPORT_PROMPT, SHORT_REPORT_PROMPT)

# ==== Execution Context

def _detect_default_branch() -> str:
    """Detect the default branch (main/master/...) from git."""
    import subprocess
    for cmd in [
        ["git", "symbolic-ref", "refs/remotes/origin/HEAD"],
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
    ]:
        try:
            out = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            if out.returncode == 0:
                return out.stdout.strip().split("/")[-1]
        except Exception:
            pass
    return "main"

def build_context(wf: dict, results: dict[str, NodeResult], run_id: str) -> dict:
    return {
        "vars":  wf.get("vars", {}),
        "nodes": results,
        "run":   {"id": run_id, "summary": _build_summary(results),
                  "default_branch": _detect_default_branch()},
    }

def _surface_outputs_from_worktree(node: Node, wt_name: str,
                                    repo_dir: str) -> list[str]:
    """Copy declared outputs from worktree to main repo before git merge.

    This ensures output files survive even if git merge aborts due to
    conflicts on shared files (e.g. SHARED_TASK_NOTES.md).
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

# ==== Gate Autofix

def _autofix_gate(gate: Node, gate_result: NodeResult,
                  nodes: dict[str, Node], ctx: dict,
                  wf: dict, run_dir: str, run_id: str,
                  repo_dir: str) -> NodeResult | None:
    """Spawn a temporary claude node to diagnose & fix a failed gate, then retry."""
    upstream = "\n".join(
        f"Upstream '{d}' goal:\n{nodes[d].prompt[:2000]}"
        for d in gate.deps if d in nodes and nodes[d].prompt
    )
    resolved_cmd = interpolate(gate.cmd, ctx)

    # build file status from declared outputs
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
                    size = os.path.getsize(path)
                    file_parts.append(f"  {rel} ({size}B)")
                    found_any = True
        if not found_any:
            file_parts.append(f"  {d}: {nodes[d].outputs} (none found)")
    file_status = "\n".join(file_parts) if file_parts else "(no outputs declared)"

    prompt = AUTOFIX_PROMPT.format(
        cmd              = resolved_cmd,
        error_output     = gate_result.output[-3000:],
        upstream_context = f"\n{upstream}" if upstream else "",
        file_status      = file_status,
    )

    fix_name = f"_autofix_{gate.name}"
    defaults = wf.get("defaults", {})
    fix_node = Node(
        name=fix_name, type=NodeType.CLAUDE, role=Role.PRODUCE,
        prompt=prompt, max_runs=defaults.get("max_runs", 0),
        timeout="10m", skills=defaults.get("skills", []),
    )

    log(f"[{fix_name}] attempting auto-fix ...")
    fix_result = run_claude(fix_node, prompt, run_dir, run_id, repo_dir)
    log(f"[{fix_name}] {'ok' if fix_result.status == Status.SUCCESS else 'FAIL'}"
        f"  {fix_result.duration:.1f}s")

    if fix_result.status != Status.SUCCESS:
        return None

    log(f"[{gate.name}] retrying after autofix ...")
    retry = run_shell(gate, resolved_cmd, cwd=repo_dir)
    log(f"[{gate.name}] retry {'ok' if retry.status == Status.SUCCESS else 'FAIL'}"
        f"  {retry.duration:.1f}s")
    return retry

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

# ==== Gate Failure

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
            log(f"[{name}] gate passed after autofix")
            return
    downstream = find_blocked(nodes, name)
    blocked |= downstream
    log(f"[{name}] gate failed -> blocking {sorted(downstream)}")
    for b in downstream:
        if results.get(b, NodeResult()).status == Status.PENDING:
            results[b] = NodeResult(status=Status.SKIPPED,
                                    output="blocked by failed gate")
            log(f"[{b}] SKIPPED (gate)")

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
    autofixed: set[str] = set()
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
                ctx = build_context(wf, results, run_id)
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
    log("")
    print_summary(results)

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
            parts.append(
                f"[{name}] type={n.type.value} role={n.role.value} "
                f"status={r.status.value} duration={r.duration:.1f}s retries={r.retries}\n"
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
        report = call_claude(prompt, timeout=300)
        path   = os.path.join(run_dir, "report.md")
        with open(path, "w") as f:
            f.write(report)
        log(f"report: {path}")
    except Exception as e:
        log(f"[report] long report failed: {e}")


def _generate_short_report(wf: dict, nodes: dict[str, Node],
                           results: dict[str, NodeResult]) -> str | None:
    """Generate concise terminal summary via Claude (猫娘+雌小鬼 style)."""
    desc    = wf.get("description", "dage workflow")
    total   = sum(r.duration for r in results.values())
    details = _build_node_details(nodes, results, max_output=200)
    prompt  = SHORT_REPORT_PROMPT.format(
        description  = desc,
        total_time   = total,
        node_details = details,
    )
    try:
        return call_claude(prompt, timeout=120)
    except Exception as e:
        log(f"[report] short report failed: {e}")
        return None
