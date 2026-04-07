# dage

dage is a DAG-based agent workflow orchestrator. It compiles task descriptions
into multi-step YAML workflows, then executes them as a directed acyclic graph
where each node is either an AI coding agent (claude/ccx) or a shell command.

Unlike linear agent pipelines, dage separates *what to do* (workflow YAML)
from *how to recover* (reflection, replan, backtrack). Gate nodes enforce
mechanical quality checks; when they fail, a reflection agent diagnoses the
root cause and chooses between local fix, structural replan, or upstream rerun.

```
                          dage Execution Flow

  YAML        plan compiler        DAG scheduler        node executor
  ====        =============        =============        =============
  task   ---> 4-phase AI     ---> topo-sort       ---> ThreadPool
  desc        generation          layer-by-layer        parallel exec
              (mature ->          scheduling            (ccx / shell)
               streams ->
               DAG design ->
               YAML)

                      +--- gate pass ---> next layer
                      |
  node result --------+--- gate fail ---> reflection
                      |                     |
                      +--- produce fail --> |--- LOCAL_FIX (retry)
                                            |--- REPLAN   (restructure DAG)
                                            |--- RERUN    (backtrack upstream)
                                            |--- SKIP     (soft continue)
```

## Workflow DAG

A typical workflow has context, produce, and gate nodes arranged in layers.
Nodes in the same layer run in parallel. Gates block downstream on failure.

```
  Example: 3-stream implementation workflow

  Layer 0          Layer 1          Layer 2          Layer 3
  (context)        (produce)        (gate)           (produce)

  +-------+
  | scan  |--.
  +-------+  |   +---------+     +----------+
             +-->| s1_impl |---->| s1_gate  |---.
  +-------+  |  +---------+     +----------+   |
  | setup |--+                                  |   +-----------+   +----------+
  +-------+  |  +---------+     +----------+   +-->| synthesize |-->| s3_gate  |
             +-->| s2_impl |---->| s2_gate  |---'   +-----------+   +----------+
                 +---------+     +----------+

  Legend:
    ---->  hard dep (must succeed)
    - ->   soft dep (inject output if available, don't block)

  Parallelism: s1_impl and s2_impl run concurrently (same layer, no mutual deps)
  Short-circuit: if s1_gate fails, synthesize is blocked (unless s1 is a soft_dep)
```

## Node State Machine

Each node follows this state machine. Produce and gate failures trigger
reflection, which can redirect the node back to PENDING via different paths.

```
                           Node Lifecycle FSM

                        +-----------------------------------+
                        |                                   |
                        v                                   |
                   +---------+                              |
            .----->| PENDING |------.                       |
            |      +---------+      |                       |
            |           |       (deps not met,              |
            |      (all deps      gate blocked)             |
            |       SUCCESS)        |                       |
            |           |           v                       |
            |           |      +---------+                  |
            |           |      | SKIPPED |                  |
            |           |      +---------+                  |
            |           v                                   |
            |      +---------+                              |
            |      | RUNNING |                              |
            |      +---------+                              |
            |       /       \                               |
            |    exit=0    exit!=0                          |
            |     /           \                             |
            |    v             v                            |
            | +----------+  +--------+                      |
            | | (checks) |  | FAILED |-----> (no recovery)  |
            | +----------+  +--------+                      |
            |   |      |                                    |
            |   |  (produce: empty     +------------+       |
            |   |   output/missing     | REFLECTION |       |
            |   |   declared files)    +------------+       |
            |   |      |               /    |     \         |
            |   |      +-----------> LOCAL  REPLAN RERUN    |
            |   |                    FIX      |    |        |
            |   v                     |       |    |        |
            | +---------+        (retry gate) |  (reset     |
            | | SUCCESS |            |        |  upstream   |
            | +---------+            v        | to PENDING) |
            |                   pass/fail     |    |        |
            |                                 |    +--------+
            +--- RETRY_FOCUSED ---------------+
                 (produce only:
                  new focused prompt)
```

## Node Roles

| Role    | Type   | Purpose                        | Success Criterion            |
|:--------|:-------|:-------------------------------|:-----------------------------|
| context | claude | Gather information (read-only) | Notes captured (non-empty)   |
| produce | claude | Create/modify code             | Declared outputs exist       |
|         |        |                                | OR notes + changeset present |
| produce | shell  | Generate files (build/codegen) | Declared outputs exist       |
| gate    | shell  | Verify quality (test/lint)     | Exit code 0                  |
| gate    | claude | Audit error paths / logic      | Exit code 0                  |
| meta    | claude | Summarize / report             | (always passes)              |

## Recovery Mechanisms

```
  Failure Type       Mechanism         Trigger               Action
  ==============     ==============    ====================  =====================
  Gate fail          Reflection        gate exit != 0        LOCAL_FIX / REPLAN /
                                                             RERUN upstream
  Produce fail       Reflection        empty output or       RETRY_FOCUSED /
                                       missing outputs       REPLAN / SKIP
  Adaptive replan    Replan signal     [REPLAN: reason]      Add/remove DAG nodes
                                       in node output
  Discovery          Shared context    [DISCOVERY: fact]     Inject into all
                                       in node output        subsequent nodes
```

## Interpolation

Node prompts access upstream results via `${...}` references:

```yaml
synthesize:
  deps: [scan, implement]
  prompt: |
    Codebase context: ${nodes.scan.output}
    Implementation diff: ${nodes.implement.changeset}
    Produced files: ${nodes.implement.artifacts}
    Build status: ${nodes.gate.status}
    Default branch: ${run.default_branch}
    Team discoveries: ${run.discoveries}

    # Truncation: ${nodes.scan.output:300} limits to first 300 chars
```

| Path                       | Resolves To                          |
|:---------------------------|:-------------------------------------|
| `${nodes.X.output}`        | Node's notes text                    |
| `${nodes.X.changeset}`     | Git diff stat from worktree          |
| `${nodes.X.artifacts}`     | Declared output file paths           |
| `${nodes.X.status}`        | success / failed / skipped / pending |
| `${vars.KEY}`              | Workflow-level variables             |
| `${run.default_branch}`    | Auto-detected git default branch     |
| `${run.discoveries}`       | Accumulated [DISCOVERY: ...] signals |

## Usage

```bash
dage plan "task description"             # AI-generate workflow YAML
dage plan "desc" --skills vibe-iris       # inject domain skills
dage plan "desc" --from-design spec.md   # generate from existing design
dage plan "desc" --run                   # generate then execute

dage run  workflow.yaml                  # execute workflow
dage run  workflow.yaml --dry-run        # preview execution plan
dage run  workflow.yaml --from s2_impl   # resume from specific node

dage validate workflow.yaml              # check YAML syntax and topology
dage status                              # show latest run status
```

## YAML Schema

```yaml
description: "what this workflow does"

defaults:
  type: claude
  timeout: "1h"
  skills: [vibe-iris]

vars:
  repo_dir: /path/to/repo

replan:
  mode: auto          # auto | confirm | log
  max_replans: 3

nodes:
  scan:
    type: claude
    role: context
    prompt: "Scan codebase structure and key modules."

  implement:
    type: claude
    role: produce
    deps: [scan]
    outputs: ["src/**/*.py"]       # required for produce nodes
    prompt: |
      Implement the feature.
      Context: ${nodes.scan.output}

  test:
    type: shell
    role: gate
    deps: [implement]
    cmd: "pytest -x"

  report:
    type: claude
    role: meta
    deps: [test]
    prompt: "Summarize: impl=${nodes.implement.changeset}, test=${nodes.test.status}"
```

## Run Output

Each run produces:

```
.dage/runs/<timestamp>/
  results.json              Node status, output, duration, cost
  report.md                 AI-generated detailed report (Chinese)
  <node>.patch              Git changeset per produce node
  <node>.notes.md           Agent notes per claude node
  <node>/ccx.log            Raw ccx stdout/stderr
```

Terminal output after each run:
1. Status table (node / status / time / retries)
2. Resume hint on failure: `dage run workflow.yaml --from <failed_node>`
3. Long report path (report.md)
4. Short report (terminal summary)

## Project Structure

```
dage/
  models.py        Node, NodeResult, Role, Status enums (92 LOC)
  workflow.py       YAML loading, validation, interpolation, topo sort (209 LOC)
  executor.py       ccx/shell execution, call_claude, output checks (313 LOC)
  engine.py         DAG scheduler, reflection, backtrack, replan, reports (883 LOC)
  git_ops.py        Worktree merge, prune, auto-commit (126 LOC)
  replan.py         Replan detection, replanner call, DAG modification (140 LOC)
  planner.py        4-phase plan generation (mature -> streams -> DAG -> YAML) (60 LOC)
  prompts.py        All prompt templates (439 LOC)
  tui.py            Rich live display, logging, status printing (279 LOC)
  cli.py            Argparse CLI (run/validate/status/plan) (198 LOC)
```

## Install

```bash
pip install -e .
```

## Requires

```
  claude CLI    plan / run / reports     https://claude.ai/code
  ccx           claude nodes in run      https://github.com/AnandChowdhary/continuous-claude
```

## License

MIT
