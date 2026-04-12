import os
import time
import yaml

from dage.workflow import extract_yaml
from dage.executor import call_claude, _load_skills
from dage.prompts import (PLAN_PROMPT, BRAINSTORM_PROMPT,
                          MATURE_PROMPT, PLAN_DOC_PROMPT)
from dage.tui import log

# ==== Phase Cache

def _plan_dir():
    d = os.path.join(".dage", "plans")
    os.makedirs(d, exist_ok=True)
    return d

def _save_phase(ts: str, phase: int, name: str, content: str):
    path = os.path.join(_plan_dir(), f"{ts}-p{phase}-{name}.md")
    with open(path, "w") as f:
        f.write(content)
    return path

def _load_phase(ts: str, phase: int, name: str) -> str | None:
    path = os.path.join(_plan_dir(), f"{ts}-p{phase}-{name}.md")
    if os.path.exists(path):
        return open(path).read().strip()
    return None

# ==== YAML Generation (Phase 4)

def _generate_yaml(design: str, description: str, skill_ctx: str = "") -> str | None:
    log("  generating YAML from design...")
    gen_prompt = PLAN_PROMPT + (
        f"\nDesign document:\n{design}\n\n"
        f"Original task: {description}\n\n"
        "OUTPUT CONSTRAINT: Your entire response is piped directly into a YAML parser. "
        "First line must be a valid YAML key (e.g. 'nodes:' or 'defaults:'). "
        "FORBIDDEN in output: backticks, Insight blocks, Chinese text, commentary, markdown fences. "
        "RAW YAML ONLY — nothing before, nothing after."
    )
    raw = call_claude(gen_prompt, timeout=1800, system=skill_ctx,
                      no_tools=True)
    try:
        raw_yaml = extract_yaml(raw)
        parsed = yaml.safe_load(raw_yaml)
        if "nodes" not in parsed:
            raise ValueError("generated YAML has no 'nodes' key")
        return raw_yaml
    except ValueError as e:
        log(f"  error: {e}")
        return None

# ==== Four-Phase Plan Generation

def generate_plan(description: str, skills: list[str] = None,
                  resume_ts: str = "") -> tuple[str | None, str]:
    """Four-phase plan generation with intermediate caching.

    resume_ts: timestamp of a previous run to resume from cached phases.
    """
    skill_full    = _load_skills(skills) if skills else ""
    skill_summary = _load_skills(skills, summary_only=True) if skills else ""
    if skill_full:
        log(f"  skills: {skills}")

    ts = resume_ts or time.strftime("%Y%m%d-%H%M%S")

    # phase 1: mature
    mature = _load_phase(ts, 1, "design") if resume_ts else None
    if mature:
        log(f"  phase 1/4: cached ({len(mature)} chars)")
    else:
        log("  phase 1/4: maturing idea...")
        mature = call_claude(MATURE_PROMPT + description + "\n\n@.",
                             timeout=1800, system=skill_full, readonly=True)
        _save_phase(ts, 1, "design", mature)
        log(f"  design: {len(mature)} chars")

    # phase 2: work streams
    streams = _load_phase(ts, 2, "streams") if resume_ts else None
    if streams:
        log(f"  phase 2/4: cached ({len(streams)} chars)")
    else:
        log("  phase 2/4: decomposing work streams...")
        streams = call_claude(PLAN_DOC_PROMPT + mature,
                              timeout=1800, system=skill_summary)
        _save_phase(ts, 2, "streams", streams)
        log(f"  streams: {len(streams)} chars")

    # phase 3: DAG design
    dag_design = _load_phase(ts, 3, "dag") if resume_ts else None
    if dag_design:
        log(f"  phase 3/4: cached ({len(dag_design)} chars)")
    else:
        log("  phase 3/4: mapping to DAG...")
        dag_design = call_claude(BRAINSTORM_PROMPT + streams,
                                 timeout=1800, system=skill_summary)
        _save_phase(ts, 3, "dag", dag_design)
        log(f"  dag: {len(dag_design)} chars")

    # phase 4: generate YAML
    raw = _generate_yaml(dag_design, description, skill_summary)

    # inject user-specified skills into YAML defaults
    if raw and skills:
        skill_list = ", ".join(skills)
        if "defaults:" in raw:
            raw = raw.replace("defaults:", f"defaults:\n  skills: [{skill_list}]", 1)
        else:
            raw = f"defaults:\n  skills: [{skill_list}]\n\n{raw}"

    _prune_plans()
    return raw, dag_design


def _prune_plans(max_keep: int = 10):
    """Remove old plan phase caches, keeping the most recent groups."""
    plan_dir = _plan_dir()
    files = sorted(os.listdir(plan_dir))
    # extract unique timestamps (prefix before first '-p')
    timestamps = sorted({f.split("-p")[0] for f in files
                         if "-p" in f and f.endswith(".md")})
    for old_ts in timestamps[:-max_keep]:
        for f in files:
            if f.startswith(old_ts):
                try:
                    os.remove(os.path.join(plan_dir, f))
                except OSError:
                    pass
