# dage

_Orchestrating AI agents used to mean glue scripts, babysitting, and praying step three wouldn't crash before you fell asleep._

dage is the antidote. Describe your workflow as a YAML DAG, hit `run`, walk away. Nodes execute in topological order, gates short-circuit on failure, and everything that can run in parallel does. You come back to a clean log of what worked and what didn't.

```
 You                       dage                          AI agents
  │                          │                               │
  │  dage run workflow.yaml  │                               │
  │ ────────────────────────>│                               │
  │                          │  topo sort -> layer 0         │
  │                          │  ├─ scan ────────────────────>│  read code, write notes
  │                          │  └─ read_docs ──────────────>│  read docs, summarize
  │                          │  gate_test (cargo test)        │
  │                          │  ├─ impl_ir ────────────────>│  write IR types
  │                          │  └─ impl_topo ──────────────>│  write topology module
  │                          │  gate -> auto-commit + push   │
  │                          │  ...                          │
  │  <── full report ─────── │                               │
```

---

## Quick Start

```bash
dage plan "analyze codebase and refactor auth module"   # generate workflow
dage run workflow.yaml                                   # execute
dage run workflow.yaml --from report                     # resume from a node
```

## How It Works

Two node types: `claude` (spawns an AI agent via [ccx]) and `shell` (runs a command).
Nodes in the same layer run concurrently. A `gate` that fails skips all downstream nodes.

```yaml
description: "Build a plan compiler"

defaults:
  skills: [vibe-opt]                 # inject domain knowledge into all claude nodes

auto_commit:
  push: true                         # commit + push after each gate passes

nodes:
  scan:
    role: context
    max_runs: 1                      # bounded task: read docs, one iteration
    prompt: |
      Read the design doc. Summarize architecture and key types.

  implement:
    deps: [scan]                     # max_runs defaults to 0 (unlimited, completion-signal driven)
    prompt: |
      Implement the feature. Upstream summary: ${nodes.scan.output}

  gate_test:
    role: gate
    deps: [implement]
    type: shell
    cmd: "cargo test"

  report:
    role: meta
    max_runs: 1
    deps: [gate_test]
    prompt: |
      Write a summary. Test result: ${nodes.gate_test.status}
```

## Engine

```
while true:
    layer = next_runnable(nodes, results, blocked)
    if empty: break
    ┌─────────────────────────────────────────────────┐
    │  Phase 1    condition filter                    │
    │  Phase 2    parallel execution (ThreadPool)     │
    │  Phase 2.5  worktree merge (git merge)          │
    │  Phase 3    gate propagation + autofix + commit │
    │  Phase 4    adaptive replan                     │
    └─────────────────────────────────────────────────┘
    hot-reload: YAML changes take effect next iteration
```

Dynamic `while + next_runnable()`, not static `for layer in layers`. Replanned nodes are picked up automatically.

## Features

| Feature | Description |
|---------|-------------|
| Dynamic scheduling | `next_runnable()` recomputes runnable nodes each iteration |
| Parallel worktrees | Concurrent claude nodes get isolated git worktrees, merged back via `git merge` |
| Gate short-circuit | Gate failure skips all downstream nodes |
| Gate autofix | Failed gate triggers a claude agent to diagnose and fix, then retries |
| Auto-commit | Gate pass triggers `git add -A && commit`, optionally push |
| Adaptive replan | `adaptive: true` nodes emit `[REPLAN: reason]` to trigger AI replanning |
| Replan governance | `mode: auto/confirm/log` controls autonomy; mandatory `justification` field |
| Skill injection | `skills: [name]` injects skill content via `--append-system-prompt` in `-p` mode |
| YAML hot-reload | Edit the YAML mid-run; changes apply on the next iteration |
| TUI dashboard | Full-screen rich panel: DAG status + colored log stream, auto-scrolling |
| `--from` resume | Resume from a specific node, skipping completed upstream |
| Interpolation | `${nodes.NAME.output}` / `${vars.X}` / `${run.summary}` |

## Adaptive Replan

```
step1 (adaptive: true)
  output: "analysis complete [REPLAN: need validation step]"
      |
      v
  engine detects [REPLAN: ...] --> calls AI replanner
      |
      v
  replanner returns:
    justification: "insert validation gate before final step"
    remove: [step2]
    add:
      validate: { type: shell, deps: [step1], cmd: "make check" }
      step2:    { type: shell, deps: [validate], cmd: "make final" }
      |
      v
  mode: auto    --> apply immediately
  mode: confirm --> pause for human approval
  mode: log     --> record signal, no action
```

## TUI

```
       scaffold │ Cargo.toml created
       scaffold │ cargo build: 0 warnings
        impl_ir │ reading plan.rs...
        impl_ir │ PlanHeader repr(C) + serialize
      impl_topo │ writing topo.rs
╭──────────────────── Planck v0.1 Phase A ────────────────────╮
│  L0  ✓ read_design 54s  read_plan 43s  scan 0s              │
│  L1  ✓ scaffold 3:12                   ◐ impl_ir 5:12       │
│  L2  ✓ gate_build 2s                     Plan IR types...   │
│  L3  ◐ impl_ir 5:12  ◐ impl_topo 3:08                       │
│  L4  ○ gate_ir_topo                    ◐ impl_topo 3:08     │
│      ⋮  (9 more)                         hccs_8card()...    │
│                                                              │
│  ◐ 2 running   ✓ 5 success   ○ 12 pending                   │
╰─────────────────────────── 5/19 ── 10:14 ───────────────────╯
```

Logs scroll above, status panel stays at the bottom. Node names are color-coded and right-aligned. Full-screen mode, 0.5s refresh. Panel auto-scrolls to track the active layer.

## File Layout

```
.dage/
  runs/{run_id}/
    original-nodes.json       initial node snapshot
    results.json              final results
    replan-{n}.json           replan event: {seq, added, removed, justification}
    replan-{n}-raw.yaml       raw replanner output
    {node}/ccx.log            per-node ccx log
    {node}.notes.md           node output (referenced via ${nodes.NAME.output})
  worktrees/
    dage-{node}/              stable worktrees, reused across runs
  latest                      most recent run_id
```

## Roadmap

```
v0.1  Static DAG execution                         done
v0.2  Intra-layer parallelism                       done
v0.3  AI plan generation (dage plan)                done
v0.4  Adaptive replan + two-layer governance        done
v0.5  Skills, auto-commit, worktree, TUI, autofix   done
v0.6  Goal-directed loop                            next
v0.7  Multi-repo orchestration                      future
```

## TODO

| Pri | Item | Notes |
|-----|------|-------|
| P0 | Worktree merge conflicts | Need fallback strategy when `git merge` conflicts |
| P0 | Interrupt recovery | Ctrl+C may leave results.json unwritten, worktrees orphaned |
| P1 | Goal-directed loop | `dage goal "desc" --verify "cmd"` outer loop |
| P1 | Replan scope constraints | Limit what types of nodes the replanner can add |
| P1 | Cost tracking | Accumulate per-node ccx spend ($) |
| P2 | Multi-repo orchestration | Cross-repository DAGs |
| P2 | Web UI | Browser-based alternative to terminal TUI |
| P2 | Notifications | Slack/email on gate failure or workflow completion |
| P3 | DAG export | Mermaid / Graphviz visualization |
| P3 | History analytics | Cross-run performance trends |

## Requirements

Python 3.9+, PyYAML, [rich] (optional, for TUI), [ccx] for claude nodes.
One machine, one YAML, one command.

[rich]: https://github.com/Textualize/rich
[ccx]: https://github.com/tsukiyokai/dotfiles/blob/main/bin/ccx

## Changelog

- 0.1 — DAG engine, shell/claude executors, gate short-circuit, interpolation, `--from` resume
- 0.2 — Intra-layer parallel execution via ThreadPoolExecutor
- 0.3 — `dage plan`: natural language to workflow YAML (two-phase brainstorm)
- 0.4 — Adaptive replanning with two-layer governance (approval modes + justification)
- 0.5 — Skill injection, auto-commit, worktree merge/reuse, gate autofix, TUI, hot-reload
