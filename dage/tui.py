from __future__ import annotations

import json
import os
import re
import sys
import time

from dage.models import Status, Node, NodeResult
from dage.workflow import topo_layers

# ==== Rich (optional)

try:
    from rich.console import Console as RichConsole
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table as RichTable
    from rich.text import Text
    _HAS_RICH = True
except ImportError:
    _HAS_RICH = False

# ==== Constants

_ANSI_COLORS = [36, 32, 33, 35, 34, 91, 96, 92, 93, 95]  # cyan,green,yellow,magenta,blue,...
_ANSI_RESET  = "\033[0m"

_STATUS_ICON = {
    Status.SUCCESS: ("✓", "green"),
    Status.RUNNING: ("◐", "yellow"),
    Status.PENDING: ("○", "dim"),
    Status.FAILED:  ("✗", "red"),
    Status.SKIPPED: ("⊘", "dim"),
}

# ==== Live Proxy

class _LiveProxy:
    """Proxy that calls fn() on each Rich render, enabling live data refresh."""
    def __init__(self, fn): self.fn = fn
    def __rich__(self):     return self.fn()

# ==== DageDisplay

class DageDisplay:
    """Real-time DAG status panel + log tail, rendered as one Live block."""

    def __init__(self, wf, nodes, results, start_time):
        self.wf          = wf
        self.nodes        = nodes
        self.results      = results
        self.start_time   = start_time
        self.node_start:  dict[str, float] = {}
        self.node_last:   dict[str, str]   = {}
        self.node_lines:  dict[str, int]   = {}
        self.replan_count = 0
        self.log_buf: list[str] = []
        self.console      = RichConsole(stderr=True)
        self.live         = Live(_LiveProxy(self._render), console=self.console,
                                 refresh_per_second=2, screen=True)

    def start(self):
        self.live.start()

    def stop(self):
        self.live.stop()

    def log(self, msg: str):
        self.log_buf.append(msg)
        if len(self.log_buf) > 200:
            self.log_buf = self.log_buf[-200:]
        self.live.refresh()

    def _fmt_dur(self, s: float) -> str:
        return _fmt_dur(s)

    def _render(self) -> Panel:
        elapsed = time.monotonic() - self.start_time
        total   = len(self.nodes)
        done    = sum(1 for r in self.results.values()
                      if r.status not in (Status.PENDING, Status.RUNNING))

        lines = []
        layers = topo_layers(self.nodes)
        max_show = 8

        # find first active layer (has RUNNING or PENDING nodes)
        first_active = 0
        for i, layer in enumerate(layers):
            if any(self.results.get(n, NodeResult()).status in (Status.RUNNING, Status.PENDING)
                   for n in layer):
                first_active = i
                break

        # scroll window: show 1 completed layer for context, then active+pending
        start = max(0, first_active - 1)
        if start > 0:
            done_nodes = sum(len(layers[j]) for j in range(start))
            lines.append(f"  [dim]✓ L0-L{start-1}  ({done_nodes} nodes done)[/]")

        shown = 0
        for i in range(start, len(layers)):
            if shown >= max_show:
                remaining = sum(len(layers[j]) for j in range(i, len(layers)))
                lines.append(f"  [dim]     ⋮  ({remaining} more)[/]")
                break
            layer = layers[i]
            parts = []
            for name in layer:
                r = self.results.get(name, NodeResult())
                icon, style = _STATUS_ICON.get(r.status, ("?", "dim"))
                if r.status == Status.RUNNING:
                    t0 = self.node_start.get(name, time.monotonic())
                    dur = self._fmt_dur(time.monotonic() - t0)
                    parts.append(f"[{style}]{icon} {name} {dur}[/]")
                elif r.status == Status.SUCCESS:
                    parts.append(f"[green]{icon} {name}[/] [dim]{self._fmt_dur(r.duration)}[/]")
                elif r.status == Status.FAILED:
                    parts.append(f"[{style}]{icon} {name}[/]")
                else:
                    parts.append(f"[{style}]{icon} {name}[/]")
                shown += 1
            lines.append(f"  [dim]L{i:<2}[/]  {'   '.join(parts)}")

        counts = {}
        for r in self.results.values():
            counts[r.status] = counts.get(r.status, 0) + 1
        status_parts = []
        for s in (Status.RUNNING, Status.SUCCESS, Status.FAILED, Status.SKIPPED, Status.PENDING):
            if counts.get(s, 0):
                icon, style = _STATUS_ICON[s]
                status_parts.append(f"[{style}]{icon} {counts[s]} {s.value}[/]")

        lines.append("")
        rp = f"  [dim]replans {self.replan_count}[/]" if self.replan_count else ""
        lines.append(f"  {'   '.join(status_parts)}{rp}")

        # right column: running node details
        right_lines = []
        for name in sorted(self.nodes):
            r = self.results.get(name, NodeResult())
            if r.status != Status.RUNNING:
                continue
            t0 = self.node_start.get(name, time.monotonic())
            dur = self._fmt_dur(time.monotonic() - t0)
            prompt = self.nodes[name].prompt.strip().split("\n")[0] if self.nodes[name].prompt else ""
            n_lines = self.node_lines.get(name, 0)
            last = self.node_last.get(name, "")

            right_lines.append(f"[yellow]◐ {name}[/] [dim]{dur}  {n_lines} lines[/]")
            if prompt:
                right_lines.append(f"  [dim]{prompt}[/]")
            if last:
                right_lines.append(f"  {last}")
            right_lines.append("")

        desc = self.wf.get("description", "dage")
        left_text  = Text.from_markup("\n".join(lines))

        if right_lines:
            right_text = Text.from_markup("\n".join(right_lines))
            table = RichTable(show_header=False, show_edge=False, box=None,
                              pad_edge=False, expand=True, padding=(0, 1))
            table.add_column(ratio=3, no_wrap=True, overflow="ellipsis")
            table.add_column(ratio=2, no_wrap=True, overflow="ellipsis")
            table.add_row(left_text, right_text)
            body = table
        else:
            body = left_text

        # panel height = max(left lines, right lines) + border
        content_h = max(len(lines), len(right_lines)) + 2
        panel = Panel(body,
                      title=f"[bold] {desc} [/]",
                      subtitle=f"[dim] {done}/{total} ── {self._fmt_dur(elapsed)} [/]",
                      border_style="blue", padding=(0, 1))

        try:
            term_h = os.get_terminal_size().lines
        except OSError:
            term_h = 40
        log_h = max(term_h - content_h, 3)
        log_text = Text.from_ansi("\n".join(self.log_buf[-log_h:]))

        layout = Layout()
        layout.split_column(
            Layout(log_text, name="log"),
            Layout(panel, name="status", size=content_h),
        )
        return layout

# ==== Module State

_display: DageDisplay | None = None

def set_display(d: DageDisplay | None):
    global _display
    _display = d

def get_display() -> DageDisplay | None:
    return _display

# ==== Logging

def log(msg: str):
    if _display:
        _display.log(msg)
    else:
        print(msg, file=sys.stderr)

def log_line(name: str, line: str):
    """Format node output: color-coded right-aligned name | content."""
    if _display and not name.startswith("_"):
        stripped = re.sub(r'\[[\d;]*m', '', line).strip()
        if stripped:
            _display.node_last[name] = stripped
        _display.node_lines[name] = _display.node_lines.get(name, 0) + 1
    if name.startswith("_"):
        log(f"\033[2m  {name:>18} │ {line}{_ANSI_RESET}")
    else:
        c = _ANSI_COLORS[hash(name) % len(_ANSI_COLORS)]
        log(f"  \033[{c}m{name:>15}{_ANSI_RESET} │ {line}")

# ==== Duration Formatting

def _fmt_dur(s: float) -> str:
    """Format duration with human-readable units: s/m/h/d."""
    if s < 60:    return f"{s:.1f}s"
    if s < 3600:  return f"{int(s)//60}m{int(s)%60:02d}s"
    if s < 86400: return f"{int(s)//3600}h{int(s)%3600//60:02d}m"
    return f"{int(s)//86400}d{int(s)%86400//3600:02d}h"

# ==== Print Helpers

def print_summary(results: dict[str, NodeResult]):
    log("=" * 60)
    log(f"{'Node':<20} {'Status':<10} {'Time':>8}  {'Retries':>7}")
    log("-" * 60)
    total_time = 0.0
    counts: dict[str, int] = {}
    for name, r in results.items():
        s = r.status.value
        counts[s] = counts.get(s, 0) + 1
        total_time += r.duration
        log(f"{name:<20} {s:<10} {_fmt_dur(r.duration):>8}  {r.retries:>7}")
    log("-" * 60)
    parts = [f"{v} {k}" for k, v in sorted(counts.items())]
    log(f"total: {' / '.join(parts)}  time: {_fmt_dur(total_time)}")
    log("=" * 60)

def print_plan(nodes: dict[str, Node]):
    layers = topo_layers(nodes)
    log("Execution plan:")
    log("")
    for i, layer in enumerate(layers):
        log(f"  layer {i}:")
        for name in layer:
            node = nodes[name]
            deps  = f" <- [{', '.join(node.deps)}]" if node.deps else ""
            adapt = " [adaptive]" if node.adaptive else ""
            log(f"    {name} ({node.type.value}/{node.role.value}){adapt}{deps}")
    log("")

def print_status(run_dir: str):
    state_file = os.path.join(run_dir, "results.json")
    if not os.path.exists(state_file):
        log("no results found")
        return
    with open(state_file) as f:
        data = json.load(f)
    log(f"latest run: {os.path.basename(run_dir)}")
    log("")
    log(f"{'Node':<20} {'Status':<10} {'Time':>8}  {'Retries':>7}")
    log("-" * 60)
    for name, r in data.items():
        log(f"{name:<20} {r['status']:<10} {_fmt_dur(r['duration']):>8}  {r['retries']:>7}")
    log("-" * 60)
