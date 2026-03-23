import os

from dage.models import Node, Role
from dage.executor import _run_streamed
from dage.tui import log

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
            # conflict -- abort merge, preserve worktree for manual resolution
            _run_streamed(f"_abort_{node_name}",
                         f'cd "{repo_dir}" && git merge --abort 2>/dev/null; true',
                         shell=True)
            log(f"  CONFLICT merging {node_name} — resolve in: {wt_path}")
            return False
        log(f"  merge: {node_name} -> main")
        # reset worktree to main HEAD for reuse next run
        _run_streamed(
            f"_reset_{node_name}",
            f'cd "{wt_path}" && git checkout -B "{wt_name}" HEAD 2>/dev/null; '
            f'git reset --hard main 2>/dev/null; true',
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
    """Remove worktrees whose branches have been merged. Called at workflow end."""
    wt_base = os.path.join(repo_dir, ".dage", "worktrees")
    if not os.path.isdir(wt_base):
        return
    pruned = []
    for name in os.listdir(wt_base):
        wt_path = os.path.join(wt_base, name)
        if not os.path.isdir(wt_path):
            continue
        # check for uncommitted working tree changes
        _, dirty, _ = _run_streamed(
            f"_dirty_{name}",
            f'cd "{wt_path}" && git status --porcelain 2>/dev/null',
            shell=True)
        if dirty.strip():
            log(f"  keeping {name}: uncommitted changes")
            continue
        # check if branch has unmerged changes vs main
        rc, _, _ = _run_streamed(
            f"_check_{name}",
            f'cd "{wt_path}" && git diff --quiet HEAD main 2>/dev/null',
            shell=True)
        if rc != 0:
            continue  # unmerged changes -- keep
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
        log(f"  pruned worktrees: {pruned}")

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
