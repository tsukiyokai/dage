import os
import subprocess

from dage.models import Node, Role
from dage.executor import _run_streamed
from dage.tui import log

# ==== Git Helpers

def _git_root(repo_dir: str) -> str:
    """Resolve git toplevel from repo_dir (ccx resolves worktrees relative to it)."""
    r = subprocess.run(["git", "rev-parse", "--show-toplevel"],
                       cwd=repo_dir, capture_output=True, text=True, timeout=5)
    return r.stdout.strip() if r.returncode == 0 else repo_dir

def default_branch(repo_dir: str) -> str:
    """Detect default branch (main/master/...) from git."""
    root = _git_root(repo_dir)
    for cmd in [
        ["git", "symbolic-ref", "refs/remotes/origin/HEAD"],
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
    ]:
        try:
            r = subprocess.run(cmd, cwd=root,
                               capture_output=True, text=True, timeout=5)
            if r.returncode == 0:
                return r.stdout.strip().split("/")[-1]
        except Exception:
            pass
    return "main"

def _list_worktrees(repo_dir: str) -> list[dict]:
    """Parse git worktree list --porcelain into [{path, branch}].

    Skips the main worktree (first entry).
    """
    root = _git_root(repo_dir)
    r = subprocess.run(["git", "worktree", "list", "--porcelain"],
                       cwd=root, capture_output=True, text=True, timeout=5)
    if r.returncode != 0:
        return []
    entries, cur = [], {}
    for line in r.stdout.splitlines():
        if line.startswith("worktree "):
            if cur:
                entries.append(cur)
            cur = {"path": line[9:]}
        elif line.startswith("branch "):
            cur["branch"] = line[7:].removeprefix("refs/heads/")
    if cur:
        entries.append(cur)
    return entries[1:]

# ==== Worktree Paths

def worktree_path(repo_dir: str, wt_name: str) -> str:
    """Return worktree working directory (with subdirectory offset).

    Resolution order:
      1) {root}/.dage/worktrees/{name}   (legacy / --worktree-base-dir)
      2) git worktree list discovery      (ccx creates {repo}-wt-{name} siblings)
      3) fallback to (1) for pre-creation
    """
    root = _git_root(repo_dir)
    repo_real, root_real = os.path.realpath(repo_dir), os.path.realpath(root)
    rel = os.path.relpath(repo_real, root_real)

    def _apply_offset(wt_root: str) -> str:
        if rel == ".":
            return os.path.realpath(wt_root)
        return os.path.realpath(os.path.join(wt_root, rel))

    # 1) legacy path
    legacy = os.path.join(root, ".dage", "worktrees", wt_name)
    if os.path.isdir(legacy):
        return _apply_offset(legacy)
    # 2) discover from git (ccx names directories {repo}-wt-{name})
    for wt in _list_worktrees(repo_dir):
        bname = os.path.basename(wt["path"])
        if bname.endswith(f"-wt-{wt_name}") or bname == wt_name:
            return _apply_offset(wt["path"])
    # 3) fallback for pre-creation
    return _apply_offset(legacy)


def worktree_root(repo_dir: str, wt_name: str) -> str:
    """Return worktree top-level directory (without subdirectory offset).

    Unlike worktree_path() which maps repo_dir into the worktree,
    this returns the worktree's git root. Use for git operations that
    must capture the full worktree state (add, commit, diff).
    """
    root = _git_root(repo_dir)
    legacy = os.path.join(root, ".dage", "worktrees", wt_name)
    if os.path.isdir(legacy):
        return os.path.realpath(legacy)
    for wt in _list_worktrees(repo_dir):
        bname = os.path.basename(wt["path"])
        if bname.endswith(f"-wt-{wt_name}") or bname == wt_name:
            return os.path.realpath(wt["path"])
    return os.path.realpath(legacy)

# ==== Worktree Merge

def _merge_single_worktree(node_name: str, wt_name: str,
                           repo_dir: str) -> bool:
    """Merge one worktree branch back to main. Returns True on success."""
    wt_root = worktree_root(repo_dir, wt_name)
    if not os.path.isdir(wt_root):
        return True
    branch = default_branch(repo_dir)
    try:
        # commit from worktree root to capture all changes (not just subdirectory)
        _run_streamed(
            f"_commit_{node_name}",
            f'cd "{wt_root}" && git add -A && '
            f'git diff --cached --quiet || git commit -m "dage: {node_name}"',
            shell=True)
        # attempt merge
        rc, out, err = _run_streamed(
            f"_merge_{node_name}",
            f'cd "{repo_dir}" && git merge --no-edit "{wt_name}"',
            shell=True)
        if rc != 0:
            _run_streamed(f"_abort_{node_name}",
                         f'cd "{repo_dir}" && git merge --abort 2>/dev/null; true',
                         shell=True)
            log(f"  CONFLICT merging {node_name} — resolve in: {wt_root}")
            return False
        log(f"  merge: {node_name} -> {branch}")
        _run_streamed(
            f"_reset_{node_name}",
            f'cd "{wt_root}" && git checkout -B "{wt_name}" HEAD 2>/dev/null; '
            f'git reset --hard {branch} 2>/dev/null; true',
            shell=True)
        return True
    except Exception as e:
        log(f"  merge failed ({node_name}): {e}")
        return False

def merge_worktrees(auto_wt: dict[str, str], repo_dir: str, run_id: str):
    """Merge worktree branches back to main via git merge."""
    for node_name, wt_name in auto_wt.items():
        _merge_single_worktree(node_name, wt_name, repo_dir)

def prune_worktrees(repo_dir: str):
    """Remove worktrees whose branches have been merged. Called at workflow end.

    Discovers worktrees via `git worktree list` (works regardless of where
    ccx placed them — under .dage/worktrees/ or as repo siblings).
    """
    root      = _git_root(repo_dir)
    branch    = default_branch(repo_dir)
    worktrees = _list_worktrees(repo_dir)
    if not worktrees:
        return
    pruned = []
    for wt in worktrees:
        wt_path   = wt["path"]
        name      = os.path.basename(wt_path)
        wt_branch = wt.get("branch", "")
        # auto-commit any dirty files so they don't block pruning forever
        _run_streamed(
            f"_save_{name}",
            f'cd "{wt_path}" && git add -A && '
            f'git diff --cached --quiet || '
            f'git commit -m "dage: auto-save before prune" 2>/dev/null; true',
            shell=True)
        # check if branch has unmerged changes vs default branch
        rc, _, _ = _run_streamed(
            f"_check_{name}",
            f'cd "{wt_path}" && git diff --quiet HEAD {branch} 2>/dev/null',
            shell=True)
        if rc != 0:
            log(f"  keeping {name}: unmerged changes")
            continue
        # HEAD == default branch: no real work, safe to remove
        try:
            _run_streamed(
                f"_prune_{name}",
                f'cd "{root}" && git worktree remove "{wt_path}" --force 2>/dev/null; '
                f'git branch -D "{wt_branch}" 2>/dev/null; true',
                shell=True)
            pruned.append(name)
        except Exception:
            pass
    if pruned:
        log(f"  pruned worktrees: {pruned}")

# ==== Pre-run Snapshot

def snapshot_before_run(repo_dir: str, run_id: str):
    """Commit any uncommitted changes as a snapshot before a new run.

    This preserves the previous iteration's artifacts in git history,
    so the new run can overwrite files in-place and git diff shows the delta.
    """
    try:
        r = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=repo_dir, capture_output=True, text=True, timeout=10)
        if r.returncode != 0 or not r.stdout.strip():
            return  # not a git repo or nothing to commit
        subprocess.run(
            'git add -A -- . ":!.dage" && '
            f'git commit -m "dage: snapshot before {run_id}"',
            shell=True, cwd=repo_dir, capture_output=True, timeout=30)
        log(f"[snapshot] committed pre-run state")
    except Exception:
        pass  # best-effort, don't block execution


# ==== Gate Auto-commit

def auto_commit(gate_name: str, nodes: dict[str, Node],
                repo_dir: str, push: bool = False):
    """Commit all changes after a gate passes. Optionally push."""
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
        log(f"[commit] {msg}")

        if push:
            rc, _, _ = _run_streamed(f"_push_{gate_name}",
                                     "git push", shell=True, cwd=repo_dir)
            log(f"[push] {'ok' if rc == 0 else 'FAIL (no remote?)'}")
    except Exception as e:
        log(f"[commit] failed: {e}")
