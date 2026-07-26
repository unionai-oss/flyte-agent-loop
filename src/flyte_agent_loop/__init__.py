"""flyte-agent-loop — a minimal **loop-engineering** system built on Flyte 2.

Three scheduled agent pipelines cooperate over a shared, durable
:class:`flyte.ai.agents.MemoryStore` to take GitHub issues to reviewed PRs — and then
grade themselves. This package is meant to be *read*, so the modules are organized to
flow from the high-level entrypoint down to the low-level building blocks.

Reading guide (top to bottom)
-----------------------------

1. **Deploy** — how the whole thing is shipped and scheduled.
   * :mod:`flyte_agent_loop.deploy`        — CLI entrypoint: deploy the env + triggers, or run one pipeline now.
   * :mod:`flyte_agent_loop.environments`  — the single :class:`flyte.TaskEnvironment` (image + secrets) every task attaches to.

2. **The pipelines (the "loop")** — three ``@env.task`` s on cron triggers. Each reads
   top-to-bottom as one story: *claim → propose ↔ verify → apply → record*.
   * :mod:`flyte_agent_loop.builder_agent`   — every 5 min: an open issue → a PR (or, for a spec, new issues).
   * :mod:`flyte_agent_loop.reviewer_agent`  — every 5 min: an agent PR → verified fixes pushed.
   * :mod:`flyte_agent_loop.distiller_agent` — every 10 min: run history → a compact "lessons" memory + a report.
   * :mod:`flyte_agent_loop.pipeline`        — the run bookkeeping the builder & reviewer share.

3. **Agent definitions** — how the agents themselves are built.
   * :mod:`flyte_agent_loop.agents`          — the Agent factories, their prompts, and the output parsers.
   * :mod:`flyte_agent_loop.llm`             — the custom ``call_llm`` callback (sets a generous ``max_tokens``).

4. **Tools** — what the agents can *do*.
   * :mod:`flyte_agent_loop.tools`           — durable ``@env.task`` GitHub tools (read issues/PRs, open PRs, ...).
   * :mod:`flyte_agent_loop.staging`         — in-process "stage a proposal" tools the pipeline verifies then applies.

5. **Building blocks** — the tested, mostly-pure helpers everything rests on.
   * :mod:`flyte_agent_loop.dibs`            — the cooperative-claim ("dibs") state machine.
   * :mod:`flyte_agent_loop.github_client`   — a small, mockable GitHub REST client.
   * :mod:`flyte_agent_loop.memory`          — read/write helpers over the shared MemoryStore.
   * :mod:`flyte_agent_loop.evals`           — the ``RunRecord``, metrics, and context compaction.
   * :mod:`flyte_agent_loop.introspect`      — read a run's durable sub-actions (its "reasoning trace").
   * :mod:`flyte_agent_loop.reports`         — Flyte report rendering (metrics, memory, traces) + styling.
   * :mod:`flyte_agent_loop.config`          — env-var :class:`Settings`.
   * :mod:`flyte_agent_loop.common`          — the current run's id/name + UTC time helpers.
"""

from ._version import __version__

__all__ = ["__version__"]
