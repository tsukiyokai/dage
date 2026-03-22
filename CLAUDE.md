# dage

DAG-based Agent Workflow Orchestrator. 单文件Python (dage.py ~1500行)，编排AI agent多步骤协作。

## 开发规则

- 代码与设计文档偏差时: 深入思考后决定改文档还是改代码，改后告知用户
- 破坏性操作(git reset/删文件/清worktree)前必须先确认，不能自作主张
- 清理worktree前必须先检查diff和merge，不能直接force remove
- bounded任务节点(context/scaffold/report)必须cap max_runs，只有开放式impl节点用unlimited
- ccx是迭代式开发循环引擎，prompt只写目标不写机制性指令(notes/completion signal由ccx处理)
- -p模式下Skill tool不可用，用--append-system-prompt注入skill内容

## 架构

```
dage.py
  YAML Loading     load_workflow / build_nodes / validate
  Interpolation    ${nodes.X.output} / ${vars.Y}
  Topo + Sched     topo_layers (preview) / next_runnable (runtime)
  Executors        run_shell / run_claude (via ccx)
  Worktree         _merge_worktrees (git merge) / stable naming
  Gate             autofix / auto-commit+push
  Replan           detect / call_replanner / apply / governance (mode+justification)
  Hot-reload       mtime check per iteration
  TUI              DageDisplay (rich Live + Layout)
  Plan Gen         brainstorm + YAML generation
  CLI              run / validate / status / plan
```

## 测试

```bash
dage run examples/test-shell.yaml      # gate短路 + 条件跳过 + 变量插值
dage run tests/test_parallel.yaml      # 层内并行
dage run tests/test_parallel_gate.yaml # gate阻断
dage run tests/test_replan.yaml        # adaptive replan信号检测
dage run tests/test_replan_log.yaml    # replan log模式
```

test-shell.yaml带`autofix: false`，避免故意失败的gate触发autofix。
