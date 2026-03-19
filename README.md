# dage

*Once upon a time, orchestrating AI agents meant writing glue scripts, babysitting each step, and praying the third one wouldn't crash before you went to bed. dage is the antidote: describe your workflow as a YAML DAG, hit run, and walk away. Nodes execute in topological order, gates short-circuit on failure, and everything that can run in parallel does. You wake up to a clean log of what worked and what didn't.*

The idea: you define a directed acyclic graph of agent steps — some talk to Claude, some run shell commands — and dage figures out the execution order, parallelism, and failure handling. The only thing you touch is the YAML. The agents do the rest.

## How it works

The repo is deliberately kept small. There are really only two things that matter:

- **workflow YAML** — the DAG definition. Nodes, dependencies, roles, prompts. This is what you iterate on.
- **dage CLI** — the engine. Parses the YAML, resolves the DAG, executes nodes layer by layer, and handles retries, gates, and resume.

By design, each node is either a `claude` call (AI agent via ccx) or a `shell` command. Nodes within the same DAG layer run concurrently. A `gate` node that fails causes all its descendants to be skipped — no wasted compute on a doomed pipeline.

## Quick start

```bash
dage plan "describe your task"
dage run workflow.yaml
```

That's it. `plan` asks Claude to generate a workflow YAML from your description. `run` executes it. If something fails halfway:

```bash
dage run workflow.yaml --from report
```

This skips everything upstream of `report` (already done) and picks up where you left off.

## Workflow YAML

```yaml
defaults:
  type: claude
  max_runs: 5
  timeout: 30m

vars:
  repo_dir: /path/to/repo

nodes:
  scan:
    role: context
    prompt: "Analyze the codebase structure and identify key components..."

  gate_test:
    role: gate
    deps: [scan]
    type: shell
    cmd: "make test"

  report:
    role: produce
    deps: [gate_test]
    prompt: "Summarize findings based on: ${nodes.scan.output}"
```

Nodes form a DAG through `deps`. The engine resolves layers and runs each layer in parallel:

```
Layer 0:   [scan]              run immediately, no deps
Layer 1:   [gate_test]         waits for scan
Layer 2:   [report]            waits for gate_test
```

## Node roles

Most roles (`context`, `produce`, `evaluate`, `gc`, `meta`) fail gracefully — they don't block anything else. The exception is `gate`: if a gate fails, every node downstream of it is marked `skipped`. This is how you express "don't bother writing the report if the tests don't pass."

## Interpolation

Prompts can reference other nodes' outputs and run metadata:

```
${vars.X}                  global variable
${nodes.scan.output}       output of the scan node
${nodes.scan.status}       success / failed / skipped
${run.id}                  unique run identifier
${run.summary}             aggregated summary of all nodes
```

Each node writes its output to `SHARED_TASK_NOTES.md` in its working directory. Downstream nodes read it via `${nodes.NAME.output}`.

## Requirements

Python 3.9+, PyYAML, and [ccx](https://github.com/tsukiyokai/dotfiles/blob/main/bin/ccx) for claude-type nodes. That's it. No distributed infra, no complex configs. One machine, one YAML, one command.

## Changelog

- **0.1.0** — initial release. DAG engine, shell/claude executors, gate short-circuit, variable interpolation, `--from` resume.
- **0.2.0** — intra-layer parallel execution. Nodes without mutual dependencies run concurrently via ThreadPoolExecutor.
- **0.3.0** — `dage plan`. Ask Claude to generate a workflow YAML from natural language, validate it, and preview the execution plan before running.
- **0.4.0** — adaptive replanning. Nodes with `adaptive: true` can signal `[REPLAN: reason]` in their output, triggering AI-driven DAG mutation mid-run. Engine dynamically adds/removes pending nodes while preserving completed work.
