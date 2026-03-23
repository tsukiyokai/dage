import yaml

from dage.workflow import extract_yaml
from dage.executor import call_claude, _load_skills
from dage.prompts import (PLAN_PROMPT, BRAINSTORM_PROMPT,
                          MATURE_PROMPT, PLAN_DOC_PROMPT)
from dage.tui import log

# ==== YAML Generation (Phase 4)

def _generate_yaml(design: str, description: str, skill_ctx: str = "") -> str | None:
    """Phase 4: turn design document into YAML workflow. Returns None on failure."""
    log("  generating YAML from design...")
    gen_prompt = PLAN_PROMPT + (
        f"\nDesign document:\n{design}\n\n"
        f"Original task: {description}\n\n"
        "OUTPUT CONSTRAINT: Your entire response is piped directly into a YAML parser. "
        "First line must be 'nodes:'. No preamble, no commentary, no Insight blocks, no markdown."
    )
    raw = call_claude(gen_prompt, timeout=1800, system=skill_ctx)
    try:
        raw_yaml = extract_yaml(raw)
        # planner-specific semantic check: YAML must contain 'nodes' key
        parsed = yaml.safe_load(raw_yaml)
        if "nodes" not in parsed:
            raise ValueError("generated YAML has no 'nodes' key")
        return raw_yaml
    except ValueError as e:
        log(f"  error: {e}")
        return None

# ==== Four-Phase Plan Generation

def generate_plan(description: str, skills: list[str] = None) -> tuple[str | None, str]:
    """Four-phase plan generation: mature -> work streams -> DAG design -> YAML."""
    skill_ctx = _load_skills(skills) if skills else ""
    if skill_ctx:
        log(f"  skills: {skills}")

    # phase 1: mature the raw idea into a well-scoped design
    log("  phase 1/4: maturing idea...")
    mature = call_claude(MATURE_PROMPT + description,
                         timeout=1800, system=skill_ctx)
    log(f"  design: {len(mature)} chars")

    # phase 2: decompose design into independent work streams + verification boundaries
    log("  phase 2/4: decomposing work streams...")
    streams = call_claude(PLAN_DOC_PROMPT + mature,
                          timeout=1800, system=skill_ctx)
    log(f"  streams: {len(streams)} chars")

    # phase 3: map work streams to dage DAG structure (nodes/roles/deps/gates)
    log("  phase 3/4: mapping to DAG...")
    design = call_claude(BRAINSTORM_PROMPT + streams,
                         timeout=1800, system=skill_ctx)
    log(f"  dag: {len(design)} chars")

    # phase 4: generate YAML
    raw = _generate_yaml(design, description, skill_ctx)
    return raw, design
