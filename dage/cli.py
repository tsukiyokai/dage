from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import yaml

from dage.workflow import load_workflow, build_nodes, validate_workflow
from dage.engine import run_dag, _find_latest_run
from dage.planner import generate_plan, _generate_yaml
from dage.executor import _load_skills
from dage.tui import log, print_plan, print_status
from dage.models import Role, Status

# ==== CLI

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dage",
        description="DAG-based Agent Workflow Orchestrator",
    )
    sub = parser.add_subparsers(dest="command")

    # run
    p_run = sub.add_parser("run", help="execute a workflow")
    p_run.add_argument("workflow", help="path to workflow YAML")
    p_run.add_argument("--dry-run", action="store_true", help="show plan only")
    p_run.add_argument("--from", dest="from_node", help="resume from node")
    p_run.add_argument("--repo-dir", default=".", help="repo working directory")

    # validate
    p_val = sub.add_parser("validate", help="validate a workflow YAML")
    p_val.add_argument("workflow", help="path to workflow YAML")

    # status
    p_st = sub.add_parser("status", help="show latest run status")
    p_st.add_argument("--repo-dir", default=".", help="repo working directory")

    # plan
    p_plan = sub.add_parser("plan", help="AI-generate workflow from description")
    p_plan.add_argument("description", help="task description or path to idea file")
    p_plan.add_argument("-o", "--output",
                        help="output file (default: .dage/workflows/<timestamp>.yaml)")
    p_plan.add_argument("--run", action="store_true",
                        help="generate then immediately execute")
    p_plan.add_argument("--from-design",
                        help="skip phases 1-3, generate YAML from existing design file")
    p_plan.add_argument("--resume",
                        help="resume from cached phases (timestamp, e.g. 20260327-112233)")
    p_plan.add_argument("--skills", nargs="+", default=[],
                        help="inject skill knowledge into plan phases")

    return parser

# ==== Commands

def cmd_run(args):
    wf    = load_workflow(args.workflow)
    wf["_yaml_path"] = os.path.abspath(args.workflow)
    nodes = build_nodes(wf)
    errors = validate_workflow(nodes)
    if errors:
        for e in errors:
            log(f"error: {e}")
        sys.exit(1)

    repo_dir = os.path.abspath(
        wf.get("vars", {}).get("repo_dir", args.repo_dir)
    )

    if args.dry_run:
        print_plan(nodes)
        return

    results = run_dag(wf, nodes, repo_dir, from_node=args.from_node)
    if any(r.status == Status.PENDING for r in results.values()):
        sys.exit(130)  # interrupted
    if any(r.status == Status.FAILED for name, r in results.items()
           if nodes[name].role != Role.GATE):
        sys.exit(1)

def cmd_validate(args):
    wf    = load_workflow(args.workflow)
    nodes = build_nodes(wf)
    errors = validate_workflow(nodes)
    if errors:
        for e in errors:
            log(f"error: {e}")
        sys.exit(1)
    log(f"valid: {len(nodes)} nodes, {sum(len(n.deps) for n in nodes.values())} edges")
    print_plan(nodes)

def cmd_status(args):
    repo_dir = os.path.abspath(args.repo_dir)
    run_dir = _find_latest_run(repo_dir)
    if not run_dir:
        log("no runs found")
        return
    print_status(run_dir)

def cmd_plan(args):
    desc = args.description
    if os.path.isfile(desc):
        desc = Path(desc).read_text().strip()
        log(f"loaded idea from: {args.description}")

    ts = time.strftime("%Y%m%d-%H%M%S")

    if args.from_design:
        # resume: skip phases 1-3, only run phase 4
        design_path = args.from_design
        if not os.path.isfile(design_path):
            log(f"error: design file not found: {design_path}")
            sys.exit(1)
        design = Path(design_path).read_text().strip()
        log(f"  resuming from design: {design_path}")
        skill_ctx = _load_skills(args.skills) if args.skills else ""
        raw = _generate_yaml(design, desc, skill_ctx)
    else:
        log("generating workflow...")
        try:
            resume_ts = getattr(args, 'resume', '') or ''
            raw, design = generate_plan(desc, skills=args.skills,
                                        resume_ts=resume_ts)
        except RuntimeError as e:
            log(f"error: {e}")
            # show resume hint with the timestamp used for phase caching
            cached = [f for f in os.listdir(os.path.join(".dage", "plans"))
                      if f.startswith(resume_ts or ts)] if os.path.isdir(os.path.join(".dage", "plans")) else []
            if cached:
                plan_ts = (resume_ts or ts)
                log(f"\nresume: dage plan \"{args.description}\" --resume {plan_ts}"
                    + (f" --skills {' '.join(args.skills)}" if args.skills else ""))
            sys.exit(1)

        # always save design + description regardless of phase 4 result
        plan_dir = os.path.join(".dage", "plans")
        os.makedirs(plan_dir, exist_ok=True)
        design_file = os.path.join(plan_dir, f"{ts}-design.md")
        desc_file   = os.path.join(plan_dir, f"{ts}-desc.txt")
        with open(design_file, "w") as f:
            f.write(f"# Design: {desc[:80]}\n\n{design}\n")
        with open(desc_file, "w") as f:
            f.write(desc)
        log(f"  design: {design_file}")

    if raw is None:
        log("error: YAML generation failed (see above)")
        df = args.from_design or design_file
        # use persisted desc file if available, otherwise quote the original arg
        desc_arg = desc_file if not args.from_design and os.path.exists(desc_file) \
                   else f'"{args.description}"'
        skills_flag = f" --skills {' '.join(args.skills)}" if args.skills else ""
        log(f"\nretry: dage plan {desc_arg} --from-design {df}{skills_flag}")
        sys.exit(1)

    # validate generated YAML (semantic check)
    try:
        wf     = yaml.safe_load(raw)
        nodes  = build_nodes(wf)
        errors = validate_workflow(nodes)
        if errors:
            for e in errors:
                log(f"  warning: {e}")
        else:
            print_plan(nodes)
    except Exception as e:
        log(f"warning: validation failed: {e}")

    # always write YAML if extract_yaml passed (structurally valid)
    wf_dir = os.path.join(".dage", "workflows")
    os.makedirs(wf_dir, exist_ok=True)
    out = args.output or os.path.join(wf_dir, f"{ts}.yaml")
    with open(out, "w") as f:
        f.write(raw + "\n")
    log(f"wrote {out}")

    if not args.run:
        log(f"\nnext: dage run {out}")

    if args.run:
        try:
            yaml.safe_load(Path(out).read_text())
        except Exception as e:
            log(f"error: generated YAML is invalid, cannot --run: {e}")
            sys.exit(1)
        log("")
        wf = load_workflow(out)
        wf["_yaml_path"] = os.path.abspath(out)
        nodes = build_nodes(wf)
        errors = validate_workflow(nodes)
        if errors:
            for e in errors:
                log(f"error: {e}")
            sys.exit(1)
        repo_dir = os.path.abspath(
            wf.get("vars", {}).get("repo_dir", "."))
        results = run_dag(wf, nodes, repo_dir)
        if any(r.status == Status.FAILED for name, r in results.items()
               if nodes[name].role != Role.GATE):
            sys.exit(1)

# ==== Entry Point

def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "run":
        cmd_run(args)
    elif args.command == "validate":
        cmd_validate(args)
    elif args.command == "status":
        cmd_status(args)
    elif args.command == "plan":
        cmd_plan(args)
    else:
        parser.print_help()
