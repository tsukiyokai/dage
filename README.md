# dager

DAG-based Agent Workflow Orchestrator. 将多个AI agent步骤按DAG拓扑序编排执行。

## 用法

```bash
dager run workflow.yaml              # 执行workflow
dager run workflow.yaml --dry-run    # 显示执行计划
dager run workflow.yaml --from test  # 从test节点恢复
dager validate workflow.yaml         # 验证YAML
dager status                         # 查看最近运行结果
```

## Workflow YAML

```yaml
defaults:
  type: claude          # claude | shell
  max_runs: 5
  timeout: 30m

vars:
  repo_dir: /path/to/repo

nodes:
  scan:
    role: context
    prompt: "Analyze the codebase..."
  gate_test:
    role: gate           # gate失败 -> 所有下游skipped
    deps: [scan]
    type: shell
    cmd: "make test"
  report:
    role: produce
    deps: [gate_test]
    prompt: "Summarize: ${nodes.scan.output}"
```

## 节点角色

| Role     | 失败语义              |
|----------|-----------------------|
| context  | 正常失败              |
| produce  | 正常失败              |
| gate     | 失败 -> 阻断所有下游  |
| evaluate | 正常失败              |
| gc       | 正常失败              |
| meta     | 正常失败              |

## 插值

- `${vars.X}` — 全局变量
- `${nodes.NAME.output}` — 节点输出 (SHARED_TASK_NOTES.md)
- `${nodes.NAME.status}` — success/failed/skipped
- `${run.id}` — 运行ID
- `${run.summary}` — 所有节点摘要

## 依赖

- Python 3.9+
- PyYAML
- ccx (claude类型节点)

## Changelog

- v0.1.0: 初始实现 — DAG引擎, shell/claude执行器, gate短路, 变量插值, --from恢复
