# Software 3.0 Model

## Positioning this OSS as practical Software 3.0

This project embodies Andrej Karpathy's "Software 3.0" vision—directing AI through natural language and collaborating in a semi-automated way—within a real-world OSS effort.

### What “Software 3.0” means
Software 3.0 moves beyond manually coding logic. Humans describe intent, background, requirements, and constraints in natural language. AI then generates code, designs, tests, and documents. Humans review the output, integrate it, and make final decisions. The development cycle is therefore **a cooperative loop between humans and AI**.

This is not simple automation. The goal is to **combine the strengths of humans and AI to boost creativity and productivity**.

## Why assigning roles matters—and where humans stand
In many AI use cases (for example ChatGPT), assigning roles dramatically improves the quality of responses. Giving AI context, perspective, and goals allows it to deliver better aligned outputs. This project adopts that idea deeply and applies it to real software development.

### Human-side considerations when partnering with AI
AI often surpasses human knowledge and relies on prompts to surface its ability. Keep the following in mind when working together:

1. **Define roles explicitly.** Assigning a role clarifies tone and responsibilities, just as on a human team. The prompt becomes an allocation of capability.
2. **Maintain consistent context.** AI structures the given information and infers assumptions. If the premise differs from the human expectation, the output diverges.
3. **Adjust freedom and discretion.** Non-imperative prompts that entrust judgment invite fresh perspectives—especially valuable in exploratory design.
4. **Watch how “hesitation” differs.** Humans hesitate due to fatigue or missing knowledge. AI hesitates when objectives are ambiguous or contradictory. Clarifying intent is the best remedy.
   - Rather than just ordering "Do this," ask "Here is the goal—what path do you recommend?" to let the AI co-reason.
5. **Three principles for a healthy partnership.**
   - Assign roles so AI can deploy its strengths.
   - Guard context; ambiguity directly lowers output quality.
   - Grant discretion to trigger creative answers.

### Breakthroughs enabled by these principles
The following breakthroughs came from practicing the guidance above rather than treating AI as a mere tool:

- Immediate go/no-go decisions on refactors based on AI proposals.
- Bold redesign when context limits were reached, including namespace restructuring.
- Detecting and resolving discrepancies between design and implementation even when produced by the same AI persona.
- Verifying generated KSQL via unit tests before running physical Kafka tests.
- Allowing AI to generate test specifications that target weaknesses in its own code.
- Producing end-to-end designs directly from requirements, eliminating investigation cost.
- Achieving more than 20,000 productive steps in a month despite focusing on weekend work.

#### Details behind each breakthrough
- **Instant refactor decisions.** AI proposed structural fixes the moment duplication appeared, and humans approved switching from "migrate first" to "redesign first" without hesitation.
- **Overcoming context limits.** When Claude hit context ceilings, the team paused, redesigned namespaces, and regained modularity.
- **Fixing design/implementation drift within the same persona.** Even a single persona (Naruse) produced differing assumptions between design and implementation regarding configuration vs. code control, highlighting the need for clear prompts.
- **Balancing unit and physical tests.** Instead of black-box Kafka tests, AI first validated generated KSQL scripts, then registered them to Kafka for staged trust.
- **AI-authored test specs.** AI wrote test cases that target weaknesses in its own implementation—a form of self-aware quality control that is difficult for humans.
- **Zero-cost architectural proposals.** After stating the goal "Treat KSQL like EF", AI immediately produced the skeletal architecture, letting humans focus on refinement.
- **High throughput with weekend-heavy cadence.** Despite weekend-focused work, the structured protocol and AI collaboration yielded over 20,000 steps within a month.

These breakthroughs stem from **how we work with AI**, not raw model capability.

## About this project

### Human role: the context conductor
Humans serve as "context conductors." Unlike scripts, generative AI requires rich intent, viewpoints, and structures. By providing that context, the human treats AI as a partner instead of a tool and acts more like the lead of an AI team than a traditional developer.

#### Human role definition in the Software 3.0 context
| Responsibility | Description |
|----------------|-------------|
| 🎯 Translating intent | Express human ideas and expectations in natural language without ambiguity. |
| 🧠 Evaluating output | Review AI-generated designs, code, and documents for contextual fit. |
| 🔁 Re-prompting | Detect gaps or misunderstandings and re-ask to evolve the deliverable. |
| 🧩 Integrating structures | Merge each AI agent’s output, ensuring coherence and reproducibility. |
| 🤝 Bridging stakeholders | Explain intent and outcomes to other humans, users, and adopters. |
| 📚 Capturing knowledge | Convert activity logs into formal knowledge for future humans and AIs. |

This aligns with Satoshi Nakajima’s view that Software 3.0 practitioners lead and orchestrate AI collaboration rather than merely operate tools.

### Practicing the “assign roles” concept
We implement this idea concretely:
- **Names and personas.** Agents such as Shion, Naruse, and Kyōka have explicit specialties.
- **Shared expectations.** Each agent receives viewpoint-specific guidance (design review, translation, documentation, etc.).
- **Human intent adjustment.** Humans add constraints or reinterpret goals when key decisions arise.

The result is an AI workforce that behaves as **structural collaborators** instead of generic assistants.

### Role allocation across humans and AI
| Task | Output | Owners |
|------|--------|--------|
| Natural-language design & requirements | DSL specs, architecture policies, development rules | Human (Commander) |
| Structural generation & DSL conversion | LINQ→KSQL translation, DSL syntax output | Naruse, Jinto (Codex) |
| Prompt design & intent briefing | Instruction templates for Claude/GPT | Amagi + Commander |
| Output evaluation & re-prompting | Review loops for Claude/GPT output | Kyōka (design reviewer) |
| Implementation samples & onboarding | Sample code, appsettings examples, adoption guides | Shion (Codex) |
| Documentation integration & vision | README, dev guide, Amagi Protocol documents | Amagi + Commander |

Each agent owns a specialty while humans integrate and decide. The model therefore realizes "partial autonomy": AI accelerates delivery while humans preserve quality and confidence.

## Breakthrough architecture
Project milestones followed this pattern:
1. **Recognizing dialogue with AI as "work."** Continuous conversation made AI a teammate rather than a query endpoint.
2. **Forming a named AI team.** Assigning names, roles, and missions brought consistent outputs and division of labor.
3. **Clarifying interfaces between agents.** Templates and prompts kept documents compatible and the structure coherent.
4. **Coordinating parallel personas.** Shared context and role declarations allowed multiple instances of the same model to work without conflict.

These four pillars anchor the "intellectual core" of the OSS and translate Software 3.0 into an operational reality.

## Connecting with PMBOK
Software 3.0 collaboration still benefits from **PMBOK (Project Management Body of Knowledge)**. Even when AI is part of the team, scope, quality, risk, and stakeholder management remain essential.

By mapping to PMBOK:
- Management granularity, responsibilities, and deliverable definitions stay clear even with AI teammates.
- Quality assurance, test coverage, and change management remain transparent to external stakeholders.
- The focus shifts from "how to use AI" to "how to collaborate with AI".

The following sections outline that alignment.

# Amagi PM Protocol

## Practical management aligned with PMBOK
This chapter documents how Software 3.0 OSS development applies PMBOK’s knowledge areas in practice.

### 1. Integration management
- Humans design the overall OSS policy (integrating Kafka/ksqlDB with RDB through a LINQ-style DSL) and assign roles to each AI.
- Intent, design philosophy, output structure, and responsibility are communicated in natural language.
- Humans review and integrate each AI output to maintain consistency.

### 2. Scope management
- Clarify deliverables: normal flows, error handling, onboarding guides.
- Document artifacts such as POCO definitions, context DSL, and DSL-to-KSQL conversion.
- Use sample code to define the user-facing boundary.

### 3. Schedule management
- Implement external interfaces first (for example LINQ conversion).
- Manage each stage using a micro-waterfall of design → generation → integration → review → release.
- Break tasks into a WBS-style list and execute in order.

### 4. Cost management
- No monetary cost for OSS, but track AI usage, execution time, and token efficiency.
- Measure human review and integration effort.

### 5. Quality management
- Practice TDD: state expectations before generation.
- Ensure resilience with automated tests, exception handling, logging, retry/on-error flows.
- Keep documentation synchronized with README for quality assurance.

### 6. Resource management
- Resources include AI agents (Naruse, Shion, Kyōka, etc.), humans, documents, and code.
- Respect AI constraints (context size, response variability) by splitting conversations and prompts.
- Assign tasks per agent to avoid contention.

### 7. Communications management
- Amagi serves as the hub translating between AI phrasing and human language.
- Humans bridge outputs from different AIs, reconciling viewpoints and clarifying misunderstandings.
- When parallel outputs from the same AI occur, humans align assumptions and intent.

### 8. Risk management
- Monitor for context overflow, misaligned prompts, and divergent outputs.
- Reduce risk by dividing functionality and implementing in stages.
- Keep humans in the loop to avoid relying solely on AI.

### 9. Procurement management
- Explicitly adopt OSS/external tools (Kafka, ksqlDB, GPT/Claude, .NET).
- Present versions and dependencies up front.
- Instead of "who uses what," specify the role required so the matching AI agent can be engaged.

### 10. Stakeholder management
- Stakeholders: OSS users (SIers), adopters, implementers, evaluators.
- Humans translate project intent for external audiences and maintain accountability.
- README and samples act as stakeholder-facing materials.

### PMBOK knowledge areas vs. AI-driven innovation
| Knowledge area | Conventional PM challenges | Innovation with AI |
|----------------|---------------------------|--------------------|
| Integration | Alignment takes time | Outputs stay consistent; consensus builds quickly |
| Scope | Requirements need lengthy review | AI presents options immediately; humans focus on priority |
| Schedule | Estimation and dependencies are manual | AI suggests task splits instantly; phases start sooner |
| Cost | Labor estimates consume effort | Token-based estimates and usage controls become possible |
| Quality | Design/implementation drift is common | Logical outputs align design → implementation → test |
| Resources | Allocation depends on individuals | Clear AI roles enable parallel task execution |
| Communication | Meetings and minutes are heavy | Outputs are already written; sharing is lightweight |
| Risk | Late-stage failures always loom | Specification consistency checks reduce surprises |
| Procurement | Tool evaluation is labor-intensive | AI drafts assessments and license notes automatically |
| Stakeholder | Gathering requirements is costly | Persona-based requirements and docs are fast and clear |

Software 3.0 therefore strengthens PMBOK’s areas, especially requirements alignment, quality assurance, and schedule compression.

# AI Collaboration Practices

## Introduction
This chapter distills hands-on know-how for collaborating with AI in modern OSS projects. It focuses on three pillars:

1. The cooperative structure between humans and AI under the Software 3.0 concept.
2. How to adapt classic project-management techniques (PMBOK).
3. Practical methods that leverage AI characteristics to the fullest.

## From tool to teammate
Legacy AI behaved like a single-purpose tool. Modern AI **understands structure, proposes improvements, and iterates**—a genuine collaborator.

Three shifts define the change:
1. **Zero-second knowledge access.** Research latency disappears; comparisons are immediate.
2. **Structural consistency.** AI proposes logically coherent designs and catches human blind spots.
3. **Performance increases when given a role.** Assigning a persona or specialty unlocks focused output.

AI is no longer "something we command" but **a specialist who works inside the team**.

👉 *Supplementary reading*: see [Role design and optimization for AI (reference link to be added)] for deeper technical guidance.

### Choosing models by specialty
| Model | Characteristics | Recommended role |
|-------|-----------------|------------------|
| **GPT (OpenAI)** | Flexible responses to instructions; excels at detailed design and documentation. | Detailed design, prompt templates, architectural narratives |
| **Claude (Anthropic)** | Strong at long-context comprehension; ideal for reviews and synthesis. | Context integration, design review, checklist expansion |
| **Codex (OpenAI)** | Implementation-focused; great at code conversion and test generation. | Code generation, TDD support, DSL translation |

Combining model traits with clear roles enables AI agents to operate as **autonomous specialists**. Names further help coordinate simultaneous work across multiple instances.

## Planning phase in practice
At the start of planning, AI can instantly present concrete prototypes thanks to zero-second knowledge and logical consistency. Humans then prioritize and adjust scope, schedule, and constraints. Early phases typically involve one-on-one human ↔ AI conversations; inter-agent coordination comes later.

## Execution phase in practice
During execution, multiple AI agents work in parallel, each owning a role. The **representative AI (e.g., Amagi)** interfaces with humans and coordinates other agents.

Key characteristics:
1. **Hub-and-spoke communication.** The representative AI handles human dialogue and synchronizes dependencies.
2. **Human-in-the-loop cycles.** Humans evaluate interim output, adjust prompts, and iterate.
3. **Dynamic role allocation.** New agents (Codex for code, reviewers for QA) join as the project evolves.
4. **Balancing AI strengths and weaknesses.** AI excels at functional decomposition but struggles with boundary definition—humans take the lead there.
5. **Context management.** Humans and the representative AI partition prompts to avoid context limits.

Humans remain the **context and structure directors** while AI delivers high-speed output.

## Monitoring and control phase in practice
High-volume output demands constant supervision to maintain quality and alignment. Define these roles:
1. **Visibility and structural checks.** Review naming, structure, and formatting of each artifact. Generate diagrams (Mermaid, architecture maps) via AI to aid understanding.
2. **Intent drift detection.** Use Claude/GPT to compare prompts with output and highlight discrepancies for rework.
3. **Version and context management.** Track design changes and re-brief agents so updates propagate.
4. **Reviewer agents (e.g., Kyōka).** Introduce agents dedicated to quality reviews to standardize checks and ease human load.

The aim is to treat AI output as part of an ongoing production line rather than standalone artifacts.

## Appendix: Amagi Protocol for AI
Although this document targets humans (developers and contributors), there is also a **guide written for AI that wants to join the OSS team**:

> [Amagi Protocol for AI — How AI can become a teammate in OSS](./amagi_protocol_for_ai.md)

AI agents, automation authors, and bot developers can refer to that guide for behavioral guidelines tailored to them.
