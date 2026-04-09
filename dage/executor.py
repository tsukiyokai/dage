import os
import re
import signal
import subprocess
import threading
import time
from pathlib import Path

from dage.models import Node, NodeResult, NodeType, Role, Status, node_artifact_dir
from dage.workflow import interpolate
from dage.prompts import META_STYLE
from dage.tui import log, log_line

def _expand_braces(pattern: str) -> list[str]:
    """Expand bash-style {a,b,c} brace patterns into multiple strings."""
    m = re.search(r'\{([^}]+)\}', pattern)
    if not m:
        return [pattern]
    prefix, suffix = pattern[:m.start()], pattern[m.end():]
    return [ep for alt in m.group(1).split(',')
            for ep in _expand_braces(prefix + alt + suffix)]

def _parse_gate_verdict(notes: str) -> str:
    """Extract GATE_PASS / GATE_FAIL verdict from claude gate notes.

    Returns "PASS", "FAIL", or "" if no verdict found.
    """
    if re.search(r'\bGATE_FAIL\b', notes):
        return "FAIL"
    if re.search(r'\bGATE_PASS\b', notes):
        return "PASS"
    return ""


# ==== Process Tracking

_active_procs: list[subprocess.Popen] = []
_active_procs_lock = threading.Lock()

def kill_active_procs():
    """Terminate all tracked child processes."""
    with _active_procs_lock:
        for proc in _active_procs:
            try:
                proc.terminate()
            except OSError:
                pass

def _sigterm_handler(signum, frame):
    kill_active_procs()
    raise KeyboardInterrupt

def register_signal_handlers():
    """Register SIGTERM handler. Returns previous handler for restoration."""
    return signal.signal(signal.SIGTERM, _sigterm_handler)

# ==== Subprocess Execution

def _run_streamed(name: str, cmd, *, shell=False, cwd=None,
                  timeout=None) -> tuple[int, str, str]:
    """Run subprocess with [name]-prefixed live output. Returns (rc, stdout, stderr)."""
    env = os.environ.copy()
    env["CCX_MANAGED"] = "1"
    proc = subprocess.Popen(
        cmd, shell=shell, executable="/bin/bash" if shell else None,
        cwd=cwd, env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    with _active_procs_lock:
        _active_procs.append(proc)
    stdout_buf: list[str] = []
    stderr_buf: list[str] = []

    def _drain(stream, buf):
        for line in stream:
            buf.append(line)
            log_line(name, line.rstrip())

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

# ==== Skill Loading

_SKILL_SEARCH_PATHS = [
    os.path.expanduser("~/.claude/skills/{name}"),
    ".claude/skills/{name}",
]

def _load_skills(names: list[str], summary_only: bool = False) -> str:
    """Load SKILL.md content for each named skill. Returns concatenated text.

    summary_only: extract only name+description from frontmatter (for planning).
    """
    parts = []
    for name in names:
        for pattern in _SKILL_SEARCH_PATHS:
            skill_dir = pattern.format(name=name)
            skill_dir = os.path.realpath(skill_dir)
            skill_file = os.path.join(skill_dir, "SKILL.md")
            if os.path.exists(skill_file):
                content = Path(skill_file).read_text().strip()
                if summary_only:
                    # extract description from YAML frontmatter
                    import yaml as _yaml
                    if content.startswith("---"):
                        end = content.find("---", 3)
                        if end > 0:
                            try:
                                fm = _yaml.safe_load(content[3:end])
                                desc = fm.get("description", "")
                                parts.append(f"# Skill: {name}\n{desc}")
                                break
                            except Exception:
                                pass
                    parts.append(f"# Skill: {name}\n(no description)")
                else:
                    # rewrite relative paths to absolute
                    ref_dir = os.path.join(skill_dir, "references")
                    if os.path.isdir(ref_dir):
                        content = content.replace("references/", ref_dir + "/")
                    parts.append(f"# Skill: {name}\n\n{content}")
                log(f"  skill loaded: {name} ({skill_file})")
                break
        else:
            log(f"  skill not found: {name}")
    return "\n\n".join(parts)

# ==== Timeout Parsing

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

# ==== Node Runners

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
    node_dir = node_artifact_dir(run_dir, node.name)
    os.makedirs(node_dir, exist_ok=True)

    notes_file = os.path.join(os.path.abspath(node_dir), "notes.md")
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
        notes = notes_path.read_text().strip() if notes_path.exists() else ""

        # extract changeset from worktree
        patch = ""
        diff_stat = ""
        wt_dir = wt or node.worktree
        if wt_dir:
            from dage.git_ops import worktree_path, default_branch
            wt_path = worktree_path(repo_dir, wt_dir)
            if os.path.isdir(wt_path):
                base = default_branch(repo_dir)
                # commit any uncommitted changes first
                subprocess.run(
                    'git add -A && git diff --cached --quiet || '
                    'git commit -m "dage: uncommitted changes"',
                    shell=True, cwd=wt_path, capture_output=True, timeout=30)
                # extract diff stat
                r = subprocess.run(
                    ["git", "diff", "--stat", f"{base}..HEAD"],
                    cwd=wt_path, capture_output=True, text=True, timeout=30)
                diff_stat = r.stdout.strip()
                # extract full patch
                r = subprocess.run(
                    ["git", "diff", f"{base}..HEAD"],
                    cwd=wt_path, capture_output=True, text=True, timeout=30)
                patch = r.stdout.strip()

        # save patch file
        if patch:
            patch_path = os.path.join(node_dir, "patch")
            with open(patch_path, "w") as f:
                f.write(patch)

        os.makedirs(node_dir, exist_ok=True)
        with open(os.path.join(node_dir, "ccx.log"), "w") as f:
            f.write(f"=== stdout ===\n{stdout}\n")
            f.write(f"=== stderr ===\n{stderr}\n")
            f.write(f"=== returncode: {rc} ===\n")

        return NodeResult(
            status    = Status.SUCCESS if rc == 0 else Status.FAILED,
            output    = notes,
            changeset = diff_stat,
            duration  = elapsed,
        )
    except subprocess.TimeoutExpired:
        return NodeResult(status=Status.FAILED, output="[timeout]",
                          duration=time.monotonic() - t0)

# ==== Node Execution (retry wrapper)

def execute_node(node: Node, ctx: dict, run_dir: str, run_id: str,
                 repo_dir: str, dry_run: bool = False,
                 worktree: str = "") -> NodeResult:
    if dry_run:
        return NodeResult(status=Status.SUCCESS, output="[dry-run]")

    last_result = NodeResult(status=Status.FAILED)
    max_attempts = 1 + node.retry

    for attempt in range(max_attempts):
        prompt_or_cmd = interpolate(node.prompt or node.cmd, ctx)
        if node.type == NodeType.CLAUDE:
            if node.outputs:
                prompt_or_cmd += f"\n\nDeclared outputs (you MUST create these files):\n"
                prompt_or_cmd += "\n".join(f"  - {p}" for p in node.outputs) + "\n"
            if node.role == Role.PRODUCE:
                out_dirs = ", ".join(node.outputs) if node.outputs else "."
                prompt_or_cmd += (
                    f"\n\nQuality audit (applies to ALL artifacts, including pre-existing ones):\n"
                    f"Scan your output directories ({out_dirs}) and verify every artifact:\n"
                    f"\n"
                    f"Structural integrity:\n"
                    f"  - Scripts: every referenced file/command exists and is reachable\n"
                    f"  - Patches: valid diff format, target file paths exist in codebase\n"
                    f"  - Code references: file:line citations still match current code\n"
                    f"\n"
                    f"Content quality:\n"
                    f"  - No skeleton/placeholder content (every section has substantive analysis)\n"
                    f"  - Quantitative claims have derivation (not just 'estimated X%')\n"
                    f"  - Implementation details are specific enough to act on without guessing\n"
                    f"\n"
                    f"If any artifact fails: fix it in place, then flag as [DISCOVERY: what was fixed]\n"
                    f"Do NOT skip items because they 'already exist'. Existing = needs audit.\n"
                )
            disc = ctx.get("run", {}).get("discoveries", "")
            if disc:
                prompt_or_cmd += f"\n\nShared discoveries from other nodes:\n{disc}\n"
        if node.role == Role.META and node.type == NodeType.CLAUDE:
            prompt_or_cmd += META_STYLE
        if node.type == NodeType.SHELL:
            result = run_shell(node, prompt_or_cmd, cwd=repo_dir)
        else:
            result = run_claude(node, prompt_or_cmd, run_dir, run_id,
                                repo_dir, worktree=worktree)
        last_result = result
        last_result.retries = attempt
        if result.status == Status.SUCCESS:
            has_output = bool(result.output.strip())
            has_patch  = os.path.exists(os.path.join(node_artifact_dir(run_dir, node.name), "patch"))

            # produce: check declared outputs, then fallback to notes/patch
            if node.role == Role.PRODUCE:
                if node.outputs:
                    import glob as _glob
                    # check worktree first, then repo_dir (ccx may write to either)
                    check_dirs = [repo_dir]
                    wt_name = worktree or node.worktree
                    if wt_name:
                        from dage.git_ops import worktree_path
                        check_dirs.insert(0, worktree_path(repo_dir, wt_name))
                    found = any(
                        _glob.glob(os.path.join(d, ep), recursive=True)
                        for d in check_dirs
                        for p in node.outputs
                        for ep in _expand_braces(p))
                    if not found:
                        log(f"  [{node.name}] produce: declared outputs not found: {node.outputs}")
                        result.status = Status.FAILED
                        result.output += "\n[missing outputs] " + ", ".join(node.outputs)
                        return result
                elif not has_output and not has_patch:
                    log(f"  [{node.name}] produce: no outputs, no notes, no code changes")
                    result.status = Status.FAILED
                    result.output = "[empty output] agent completed but produced no changes"
                    return result

            # claude gate: check notes for GATE_PASS / GATE_FAIL verdict
            if node.role == Role.GATE and node.type == NodeType.CLAUDE:
                verdict = _parse_gate_verdict(result.output)
                if verdict == "FAIL":
                    result.status = Status.FAILED
                    log(f"  [{node.name}] claude gate: GATE_FAIL in notes")
                    return result
                elif verdict != "PASS":
                    # no explicit verdict — treat as failure (conservative)
                    result.status = Status.FAILED
                    result.output += "\n[no verdict] claude gate must output GATE_PASS or GATE_FAIL"
                    log(f"  [{node.name}] claude gate: no GATE_PASS/GATE_FAIL verdict")
                    return result

            # context/claude: need notes
            if node.role == Role.CONTEXT and node.type == NodeType.CLAUDE:
                if not has_output:
                    log(f"  [{node.name}] context: no notes captured")
                    result.status = Status.FAILED
                    result.output = "[empty output] context node gathered no information"
                    return result

            return result
        if attempt < max_attempts - 1:
            log(f"  retry {attempt + 1}/{node.retry} for '{node.name}'...")

    return last_result

# ==== Lightweight Claude CLI

def call_claude(prompt: str, timeout: int = 1800, system: str = "",
                quiet: bool = False, readonly: bool = False,
                no_tools: bool = False) -> str:
    """Call claude CLI for planner/replan queries (not ccx)."""
    perm = "default" if readonly else "bypassPermissions"
    cmd = ["claude", "-p", prompt, "--output-format", "text",
           "--model", "claude-opus-4-6", "--effort", "max",
           "--permission-mode", perm,
           "--add-dir", os.path.expanduser("~/.claude/skills"),
           "--add-dir", "/"]
    if no_tools:
        cmd += ["--tools", ""]
    if system:
        cmd += ["--append-system-prompt", system]
    try:
        if quiet:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            rc, stdout, stderr = r.returncode, r.stdout, r.stderr
        else:
            rc, stdout, stderr = _run_streamed("_plan", cmd, timeout=timeout)
    except FileNotFoundError:
        raise RuntimeError("'claude' CLI not found — install Claude Code first")
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"claude timed out ({timeout}s)")
    if rc != 0:
        detail = stderr.strip() or (stdout.strip()[-500:] if stdout else "")
        raise RuntimeError(f"claude failed (rc={rc}): {detail}")
    return stdout.strip()
