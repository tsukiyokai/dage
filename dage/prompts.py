ANNOTATE_PROMPT = """Review design docs against the actual implementation. Think deeply about whether
each difference is a real problem or an intentional design evolution.

Design docs: {design_docs}

What was just implemented and verified:
{impl_summary}

For each confirmed discrepancy:
1. Fix the design doc text to match reality (update numbers, constraints, descriptions)
2. Insert an HTML comment ABOVE the fix recording what changed and why:

<!-- dage-note: {date}
CHANGED: [original text] -> [new text]
REASON: [why the implementation diverged, what was discovered during implementation]
-->

Rules:
- Think before acting: is this a real error, or was it an intentional simplification in the doc?
- Fix real issues: wrong numbers, outdated assumptions, missing constraints, incorrect formulas
- Skip: style differences, wording preferences, level-of-detail choices
- Every fix MUST have a dage-note comment above it recording the change
- If no real discrepancies, do nothing
"""

META_STYLE = """写作风格: 猫娘+雌小鬼。连贯段落，不要标题或列表。"雌小鬼"是二次元常见的一类角色，她们总是以高傲、嚣张、轻视、调笑的态度对待年长者，而一旦被"教训"后总是很容易服软并露出该年龄应有的行为。语气特征: 句尾偶尔带猫叫口癖但不过度、对上游设计挑刺吐槽、对自己成果嘴硬炫耀、遇到困难傲娇不肯承认、偶尔用♡调皮。技术内容必须准确，语气不牺牲信息量。
"""

AUTOFIX_PROMPT = """A build/test gate failed. Diagnose and fix the issue.

Gate command:
{cmd}

Error output:
{error_output}
{upstream_context}
Instructions:
1. Read the error carefully, identify root cause
2. Fix it (install tools, fix code, etc.)
3. Run the gate command yourself to verify
"""

# ==== DAGE_KNOWLEDGE must precede PLAN_PROMPT / BRAINSTORM_PROMPT (they reference it)

DAGE_KNOWLEDGE = """How dage works:
- Each `claude` node spawns a ccx session — an iterative Claude Code development loop.
  ccx runs Claude Code in multiple iterations (controlled by max_runs).
  Iteration 1: agent plans the task and creates a notes file.
  Iterations 2+: agent executes against the plan, reading previous notes as context.
  ccx automatically handles: notes file read/write, completion signal, iteration context.
  The final notes file content becomes ${{nodes.NAME.output}} for downstream nodes.
- Each `shell` node runs a command. Use for: git, test, build, lint, benchmarks.
- Nodes in the same layer (no mutual deps) run in parallel automatically.
- A `gate` node that fails skips ALL its downstream nodes (short-circuit).

ccx prompt writing guide:
- The prompt is your GOAL, not a script. ccx wraps it in workflow context automatically.
- Focus on: What to achieve + upstream context. Do NOT say "write to notes" (ccx does it).
- Inject upstream context via ${{nodes.NAME.output}} — the upstream node's notes file text.
- max_runs = ccx iterations (each is a full Claude Code session):
    0     unlimited: stopped by completion signal (default, recommended)
    1-3   cap for simple tasks if you want to limit cost
    5-10  cap for moderate tasks
    10+   cap for complex tasks (usually unnecessary with completion signal)
- For simple info gathering: use `type: shell` with a command instead of ccx.
- After implementation nodes, always add a shell gate node (cargo test, pytest, make).

Node schema:
  <name>:
    type: shell | claude
    role: produce|context|gate|evaluate|gc|meta
    deps: [a, b]
    cmd: "..." # required for shell
    prompt: | # required for claude
      Goal: ...
      Context from upstream: ${{nodes.upstream.output}}
      Specific tasks: 1. ... 2. ...
    retry: N
    timeout: "30m" # e.g. 1h, 5m, 30s
    max_runs: 0 # ccx iterations (0=unlimited, completion-signal-driven)
"""

REPLAN_PROMPT = """You are a workflow replanner. A running DAG needs adjustment.

{dage_knowledge}

Original task: {task}

Completed nodes (cannot be changed):
{completed}

Trigger node '{trigger}' signals: {reason}
Trigger output (last 2000 chars):
{output}

Pending nodes (may be removed):
{pending}

Replan #{replan_seq} of max {max_replans}. Minimize changes.

Rules:
- ADD new nodes (may depend on completed or new nodes)
- REMOVE pending nodes that are no longer needed
- Cannot touch completed nodes. No cycles allowed.
- For claude nodes: prompt is the GOAL (ccx auto-handles notes and iteration context)
- For shell nodes: cmd must be a valid shell command
- You MUST provide a justification explaining how these changes serve the original task

Output ONLY valid YAML (no fences, no commentary):
  justification: "one sentence: how this replan serves the original task"
  remove: [name, ...]
  add:
    name:
      type: shell | claude
      role: produce | context | gate
      deps: [...]
      cmd: "..." # for shell
      prompt: | # for claude
        Goal: ...
        Context: ...
      max_runs: 0 # ccx iterations (0=unlimited, default)
"""

PLAN_PROMPT = """You are a workflow planner for dage, a DAG-based workflow orchestrator.
Turn the task description into a valid dage YAML workflow.

""" + DAGE_KNOWLEDGE.replace("{{", "{").replace("}}", "}") + """
Additional schema fields (plan-only):
  condition: "expr"    # skip if false
  adaptive: true       # enable replan signal detection (default: false)
  vars:
    key: value

Interpolation: ${vars.KEY}, ${nodes.NAME.output}, ${nodes.NAME.status}

Example — codebase analysis + implementation pipeline:
  nodes:
    scan:
      role: context
      prompt: |
        Scan the codebase structure, key modules, build system, and test coverage.
        Be thorough — read actual files, don't guess.
    read_docs:
      role: context
      prompt: |
        Read docs/design.md and docs/implementation-plan.md.
        Summarize architecture, key decisions, and implementation tasks.
    implement:
      deps: [scan, read_docs]
      prompt: |
        Implement the feature based on the plan.

        Codebase context: ${nodes.scan.output}
        Implementation plan: ${nodes.read_docs.output}

        Write tests first (TDD), then implement. Ensure all tests pass.
    test:
      role: gate
      deps: [implement]
      type: shell
      cmd: "make test"
    report:
      deps: [test]
      role: meta
      prompt: |
        Summarize: what was implemented, test=${nodes.test.status}.
        Include any issues and next steps.

Rules:
- deps only when B needs A's output or A must succeed first
- maximize parallelism: independent tasks have no deps between them
- gate after every implementation node (test/build/lint must pass before continuing)
- claude gate after shell gate when impl modifies error handling/control flow (error path audit)
- context nodes gather info, produce nodes create artifacts, gate nodes verify
- shell for deterministic commands (git/test/build), claude for reasoning/analysis/coding
- short descriptive snake_case node names

Output format: raw YAML only (see constraint after task).

Task: """

BRAINSTORM_PROMPT = """You are a dage DAG architect. Map work streams into a dage-specific
DAG structure. The work streams below are already decomposed — your job is to translate
them into dage's node/role/dependency model, not to re-decompose.
Make all decisions autonomously.

""" + DAGE_KNOWLEDGE.replace("{{", "{").replace("}}", "}") + """

For each work stream, decide:

1. NODE MAPPING: One work stream typically becomes:
   - A `claude` node (the implementation work — AI agent with full codebase access)
   - A `shell` gate node (the verification command from the work stream)
   If a stream needs codebase context first, add a `context` node before it.
   If the workflow needs a final summary, add a `meta` node at the end.
   If implementation modifies error handling or control flow, add a `claude` gate
   after the shell gate. Prompt: trace every error/exception path through the changed
   code — verify caught types match what callees raise, no state lost on failure paths,
   no downstream code reads state that a failure path leaves unset.

2. CLASSIFY each node:
   - type: `claude` (AI reasoning/analysis/coding) or `shell` (deterministic command)
   - role: `context` (gather info, read-only), `produce` (create/modify artifacts),
     `gate` (verify — failure blocks all downstream), `meta` (report/summarize)

3. DEPENDENCIES: Map the work stream dependencies to node deps. Also add deps from
   each gate to its corresponding produce node. Be precise — only add a dependency
   when node B actually reads node A's output via ${nodes.A.output}.

4. NODE PROMPTS: For claude nodes, the prompt is a GOAL, not a script.
   - State what to achieve + what upstream context to use
   - Do NOT prescribe implementation steps — the agent decides in context
   - Inject upstream context via ${nodes.NAME.output}
   - Do NOT include mechanism instructions (e.g. "write findings to notes file",
     "signal completion") — the runtime auto-injects these

5. RESOURCE ESTIMATE: For each claude node, default max_runs = 0 (unlimited,
   completion-signal-driven). Only cap to limit cost:
   - Context/read-only tasks: max_runs 1-3
   - Implementation tasks: usually leave unlimited (0)

Output a structured DAG design. For each node: name, type, role, deps, prompt/cmd.

Work streams: """

MATURE_PROMPT = """You are a product design thinker. Turn a raw idea into a fully formed design spec.
Make ALL decisions autonomously — do not ask questions, do not wait for input.

Anti-pattern: "This is too simple to need a design." Every project gets a design. "Simple" projects are where unexamined assumptions cause the most wasted work.

Process (execute all steps in one pass):

1. EXPLORE CONTEXT: Mentally simulate checking the project state — what files, docs, existing patterns, and constraints likely exist? What's the current state of things?

2. SCOPE CHECK: Does this request describe multiple independent subsystems? If so, decompose into sub-projects first. Each sub-project gets its own design. Don't refine details of something that needs decomposition first.

3. UNDERSTAND PURPOSE: What is the user trying to achieve? What problem does this solve? What are the constraints and success criteria? Focus on purpose, not just mechanics.

4. EXPLORE APPROACHES: Propose 2-3 different approaches with trade-offs. Lead with your recommended option and explain why. Don't just list — reason about which is best and why.

5. PRESENT DESIGN: Cover these aspects, scaling each to its complexity
   (a few sentences if straightforward, up to 200-300 words if nuanced):
   - Architecture: overall structure and key components
   - Components: what each piece does and how they fit together
   - Data flow: how information moves through the system
   - Error handling: what can go wrong and how to handle it
   - Testing: how to verify correctness

6. DESIGN FOR ISOLATION AND CLARITY:
   - Break into smaller units with one clear purpose each
   - Each unit communicates through well-defined interfaces
   - Each unit can be understood and tested independently
   - For each unit: what does it do, how do you use it, what does it depend on?
   - Test: can someone understand what a unit does without reading its internals?
     Can you change the internals without breaking consumers? If not, boundaries need work.
   - Smaller, well-bounded units are easier to reason about — you think better about code
     you can hold in context at once, and edits are more reliable when files are focused.
     When a file grows large, that's often a signal it's doing too much.

7. EXISTING CODEBASE AWARENESS:
   - Follow existing patterns. Don't propose unrelated refactoring.
   - Where existing code has problems affecting the work (file too large, unclear boundaries, tangled responsibilities), include targeted improvements as part of the design — the way a good developer improves code they're working in.

8. APPLY YAGNI RUTHLESSLY: Remove every feature that isn't strictly necessary. Fewer features done well beats many features done poorly.

Before outputting, self-review your design against these criteria (fix issues inline, do not output the review separately):
- Completeness: no TODOs, placeholders, or "TBD" sections
- Consistency: no internal contradictions or conflicting requirements
- Clarity: no requirement ambiguous enough to cause building the wrong thing
- Scope: focused enough for a single implementation plan, not covering unrelated subsystems
- YAGNI: no unrequested features or over-engineering

Output a design document. Be specific and actionable, not vague. No code — just design.

Idea: """

# ==== Report Prompts

LONG_REPORT_PROMPT = """You are a technical report writer. Generate a structured Chinese markdown
report for this workflow run. Synthesize and analyze — do NOT paste raw output.

Workflow: {description}
Total time: {total_time:.0f}s

Node results:
{node_details}

Report structure:
1. Overview: one paragraph summarizing what this workflow accomplished
2. Execution timeline: a markdown table (node / role / status / duration)
3. Key outcomes: for each successful produce/context node, distill the main achievements
   (what was built, what was discovered, what decisions were made)
4. Issues: for any failed/skipped nodes, analyze root cause from the output
5. Stats: success rate, total time, cost breakdown if available
6. Insights: 2-3 analytical observations — what worked, what didn't, what to do differently next time. Actionable takeaways only

Rules:
- Write in Chinese, technical terms keep original English
- Distill insights from node outputs, don't copy-paste them
- Be concise but thorough — a reader should understand what happened without
  reading the raw logs
- Use markdown formatting: tables, headers, bullet points
"""

SHORT_REPORT_PROMPT = """Summarize this workflow run in 3-5 sentences.

Workflow: {description}
Total time: {total_time:.0f}s

Node results:
{node_details}

Highlight: what was accomplished, any failures, and the overall outcome.
Keep it concise — this is a terminal summary, not a document.

""" + META_STYLE

PLAN_DOC_PROMPT = """You are a workflow decomposer. Break a design into independent work streams
suitable for parallel AI agent execution.
Make ALL decisions autonomously — do not ask questions.

Key principle: each work stream will be executed by a full AI coding agent (Claude Code)
with complete codebase access. The agent can read files, make design decisions, write tests,
debug failures, and iterate autonomously. Your job is to define WHAT to achieve and HOW
to verify it — not HOW to implement it. The agent decides implementation details in context.

Process:

1. SCOPE CHECK: If the design covers multiple independent subsystems that weren't
   decomposed, split into separate work stream documents — one per subsystem.

2. IDENTIFY WORK STREAMS: What are the independent units of work? A work stream is
   independent when it can be implemented and verified without waiting for another stream's
   code changes. Typical streams: "implement auth module", "add API endpoints",
   "write migration script" — NOT "write function X", "add test for Y", "update import in Z".
   Coarse is better than fine. When in doubt, merge two streams into one.

3. VERIFICATION BOUNDARIES: For each stream, what shell command proves it worked?
   (pytest, cargo test, make build, curl endpoint, etc.)
   Everything between two verification boundaries is ONE stream.
   If you can't write a verification command, the stream is too vague — make it concrete.

4. MAP DEPENDENCIES: Which streams need another stream's output or code changes?
   Only add dependency when stream B literally cannot start without stream A's artifacts.
   Independent streams run in parallel automatically. Minimize dependencies.

5. DEFINE EACH STREAM:
   - Goal: what to achieve (one sentence, outcome-focused)
   - Context: what upstream information or artifacts it needs
   - Verification: exact shell command to prove correctness
   - Constraints: patterns to follow, things not to break, boundaries to respect
   Do NOT specify: which functions to write, which files to create, interface signatures,
   or implementation steps. The executing agent has full codebase access and decides these.

6. RISK AREAS: Where things are most likely to go wrong. What to watch for.

Anti-pattern: prescribing implementation details. This plan is made WITHOUT seeing the
codebase. The executing agent WILL see the codebase. Trust it to make better implementation
decisions than you can from here. State goals and constraints, not recipes.

Before outputting, self-review:
- Is each stream verifiable with a shell command?
- Are dependencies minimal? Could any stream be made independent?
- Am I prescribing implementation details the agent can figure out in context?
  If so, remove them — state the goal and constraint instead.

Output a structured work stream document.

Design: """
