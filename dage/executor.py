import os
import re
import signal
import subprocess
import threading
import time
from pathlib import Path

from dage.models import Node, NodeResult, NodeType, Role, Status
from dage.workflow import interpolate
from dage.prompts import META_STYLE
from dage.tui import log, log_line

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
        cmd, shell=shell, cwd=cwd, env=env,
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
            if node.role == Role.PRODUCE and not result.output.strip():
                log(f"  [{node.name}] produce node succeeded but output is empty — marking failed")
                result.status = Status.FAILED
                result.output = "[empty output] agent completed but produced no changes"
                return result
            return result
        if attempt < max_attempts - 1:
            log(f"  retry {attempt + 1}/{node.retry} for '{node.name}'...")

    return last_result

# ==== Lightweight Claude CLI

def call_claude(prompt: str, timeout: int = 1800, system: str = "") -> str:
    """Call claude CLI for planner/replan queries (not ccx)."""
    cmd = ["claude", "-p", prompt, "--output-format", "text",
           "--model", "claude-opus-4-6", "--effort", "max",
           "--permission-mode", "bypassPermissions",
           "--add-dir", os.path.expanduser("~/.claude/skills"),
           "--add-dir", "/"]
    if system:
        cmd += ["--append-system-prompt", system]
    try:
        rc, stdout, stderr = _run_streamed("_plan", cmd, timeout=timeout)
    except FileNotFoundError:
        raise RuntimeError("'claude' CLI not found — install Claude Code first")
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"claude timed out ({timeout}s)")
    if rc != 0:
        raise RuntimeError(f"claude failed: {stderr.strip()}")
    return stdout.strip()
