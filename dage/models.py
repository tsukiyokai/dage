from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum

# ==== Enums

class Role(Enum):
    CONTEXT  = "context"
    PRODUCE  = "produce"
    GATE     = "gate"
    EVALUATE = "evaluate"
    GC       = "gc"
    META     = "meta"

class NodeType(Enum):
    CLAUDE = "claude"
    SHELL  = "shell"

class Status(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED  = "failed"
    SKIPPED = "skipped"

# ==== Data Structures

@dataclass
class Node:
    name:      str
    type:      NodeType
    role:      Role
    deps:      list[str]       = field(default_factory=list)
    prompt:    str             = ""
    cmd:       str             = ""
    condition: str             = ""
    max_runs:  int             = 0
    worktree:  str             = ""
    timeout:   str             = ""
    retry:     int             = 0
    adaptive:  bool            = False
    skills:    list[str]       = field(default_factory=list)
    outputs:   list[str]       = field(default_factory=list)

@dataclass
class NodeResult:
    status:   Status  = Status.PENDING
    output:   str     = ""
    duration: float   = 0.0
    retries:  int     = 0
    cost:      float      = 0.0
    artifacts: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = {"status": self.status.value, "output_len": len(self.output),
             "duration": round(self.duration, 1), "retries": self.retries}
        if self.output:
            d["output"] = self.output
        if self.cost > 0:
            d["cost"] = round(self.cost, 4)
        if self.artifacts:
            d["artifacts"] = self.artifacts
        return d

# ==== Constants

_ROLE_MAX_RUNS = {Role.CONTEXT: 1, Role.META: 1, Role.GATE: 1}

# ==== Data Utilities

def save_json(path: str, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def node_to_dict(node: Node) -> dict:
    d = {"type": node.type.value, "role": node.role.value}
    if node.deps:      d["deps"]      = node.deps
    if node.prompt:    d["prompt"]    = node.prompt
    if node.cmd:       d["cmd"]       = node.cmd
    if node.condition: d["condition"] = node.condition
    if node.adaptive:  d["adaptive"]  = True
    if node.retry:     d["retry"]     = node.retry
    if node.timeout:   d["timeout"]   = node.timeout
    if node.outputs:   d["outputs"]   = node.outputs
    return d
