# dage

DAG-based Agent Workflow Orchestrator. Single-file Python (dage.py ~1800 lines), orchestrates multi-step AI agent collaboration.

## Principles

- Code vs design doc divergence: think deeply, decide which to fix, inform user
- Destructive ops (git reset / delete files / clean worktree) require explicit confirmation
- Before cleaning worktree: check diff and merge first, never force remove
- Bounded nodes (context/scaffold/report) must cap max_runs; only open-ended impl nodes use unlimited
- ccx is an iterative dev-loop engine: prompts state goals only, no mechanical instructions (notes/completion signal handled by ccx)
- Skill tool unavailable in -p mode; use --append-system-prompt to inject skill content

## Tests

```bash
dage run examples/test-shell.yaml       # gate short-circuit + conditional skip + var interpolation
dage run tests/test_parallel.yaml       # intra-layer parallelism
dage run tests/test_parallel_gate.yaml  # gate blocks downstream
dage run tests/test_replan.yaml         # adaptive replan signal detection
dage run tests/test_replan_log.yaml     # replan log mode
dage run tests/test_replan_confirm.yaml # replan confirm mode (requires user approval)
```

test-shell.yaml uses `autofix: false` to prevent intentionally-failing gates from triggering autofix.
